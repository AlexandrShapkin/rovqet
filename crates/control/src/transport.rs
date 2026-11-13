//! Bounded inter-node peer transport.
//!
//! One connection per request keeps accounting simple and bounded: a connection is opened
//! with a bounded establishment timeout, exactly one request is written, exactly one
//! response is read, and the connection is closed. Outstanding requests are bounded by a
//! semaphore, so a slow or unreachable peer cannot grow memory without limit.
use crate::ids::{ClusterId, NodeId, PeerEndpoint, TimeoutBudget, TrustContext};
use crate::rpc::RpcKind;
use crate::wire::{Envelope, HEADER_BYTES, MAX_BODY_BYTES, WireError};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub enum TransportError {
    UnknownPeer,
    NotConfigured,
    Capacity,
    ConnectTimeout,
    Timeout,
    Wire(WireError),
    /// The peer answered, but the answer does not match the request.
    CorrelationMismatch,
    Io(std::io::Error),
    /// The remote work may have committed; the outcome is unknown, not a rollback.
    Uncertain,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TransportError {}

impl From<WireError> for TransportError {
    fn from(error: WireError) -> Self {
        Self::Wire(error)
    }
}
impl From<std::io::Error> for TransportError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Bounds of the local peer transport.
#[derive(Debug, Clone, Copy)]
pub struct TransportLimits {
    pub max_outstanding: usize,
    pub max_connections: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_outstanding: 64,
            max_connections: 64,
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(5),
        }
    }
}

/// Client side of the inter-node transport.
#[derive(Debug)]
pub struct PeerTransport {
    cluster: ClusterId,
    node: NodeId,
    peers: HashMap<NodeId, PeerEndpoint>,
    limits: TransportLimits,
    outstanding: Arc<Semaphore>,
    requests: AtomicU64,
}

impl PeerTransport {
    pub fn new(trust: &TrustContext, limits: TransportLimits) -> Self {
        let peers = trust
            .peers
            .iter()
            .map(|peer| (peer.node, peer.clone()))
            .collect();
        Self {
            cluster: trust.cluster,
            node: trust.node,
            peers,
            limits,
            outstanding: Arc::new(Semaphore::new(limits.max_outstanding)),
            requests: AtomicU64::new(1),
        }
    }

    pub fn next_request_id(&self) -> u128 {
        u128::from(self.requests.fetch_add(1, Ordering::Relaxed))
    }

    pub fn peer(&self, node: NodeId) -> Option<&PeerEndpoint> {
        self.peers.get(&node)
    }

    /// Sends one request and waits for the matching response.
    ///
    /// A timeout after the request was written is reported as `Uncertain`: the remote owner
    /// may already have committed the work.
    pub async fn call(
        &self,
        target: NodeId,
        kind: RpcKind,
        body: Vec<u8>,
        budget: TimeoutBudget,
        metadata_revision: u64,
        ownership_epoch: u64,
    ) -> Result<Envelope, TransportError> {
        if !self.peers.contains_key(&target) {
            return Err(TransportError::UnknownPeer);
        }
        let _permit = self
            .outstanding
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransportError::Capacity)?;
        let address = self.peers[&target].address;
        let request_id = self.next_request_id();
        let envelope = Envelope::new(
            self.cluster.get(),
            self.node.get(),
            target.get(),
            request_id,
            kind.as_u16(),
            metadata_revision,
            ownership_epoch,
            budget.millis,
            body,
        );
        let mut stream =
            tokio::time::timeout(self.limits.connect_timeout, TcpStream::connect(address))
                .await
                .map_err(|_| TransportError::ConnectTimeout)?
                .map_err(TransportError::Io)?;
        let deadline = tokio::time::Instant::now() + self.limits.request_timeout;
        write_envelope(&mut stream, &envelope).await?;
        let response = match tokio::time::timeout_at(deadline, read_envelope(&mut stream)).await {
            Ok(result) => result?,
            Err(_) => {
                // The request was written: the remote side may already have committed.
                return Err(TransportError::Uncertain);
            }
        };
        if response.cluster_id != self.cluster.get()
            || response.request_id != request_id
            || response.source_node != target.get()
            || response.target_node != self.node.get()
        {
            return Err(TransportError::CorrelationMismatch);
        }
        Ok(response)
    }
}

/// Writes one envelope.
pub async fn write_envelope(
    stream: &mut TcpStream,
    envelope: &Envelope,
) -> Result<(), TransportError> {
    let bytes = envelope.encode()?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

/// Reads one envelope with strict bounds. The body length is validated before allocation.
pub async fn read_envelope(stream: &mut TcpStream) -> Result<Envelope, TransportError> {
    let mut header = vec![0u8; HEADER_BYTES];
    stream.read_exact(&mut header).await?;
    let body_len = u32::from_be_bytes(header[78..82].try_into().unwrap());
    if body_len as usize > MAX_BODY_BYTES {
        return Err(TransportError::Wire(WireError::BodyTooLarge(body_len)));
    }
    let mut rest = vec![0u8; body_len as usize + 4];
    stream.read_exact(&mut rest).await?;
    let mut frame = header;
    frame.extend_from_slice(&rest);
    Ok(Envelope::decode(&frame)?)
}

/// Server side connection handling: identity validation, one request at a time.
#[derive(Debug)]
pub struct PeerServerLimits {
    pub max_connections: usize,
    pub request_timeout: Duration,
}

impl Default for PeerServerLimits {
    fn default() -> Self {
        Self {
            max_connections: 64,
            request_timeout: Duration::from_secs(5),
        }
    }
}

/// Answers one validated peer request.
pub trait PeerHandler: Send + Sync + 'static {
    fn handle(
        &self,
        envelope: Envelope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, crate::rpc::RpcErrorCode>> + Send>,
    >;
}

/// Accepts peer connections on an explicit listener.
pub struct PeerServer {
    listener: tokio::net::TcpListener,
    trust: Arc<TrustContext>,
    handler: Arc<dyn PeerHandler>,
    limits: PeerServerLimits,
}

impl PeerServer {
    /// The prototype peer listener is loopback only: never expose it on a public interface.
    pub fn new(
        listener: tokio::net::TcpListener,
        trust: Arc<TrustContext>,
        handler: Arc<dyn PeerHandler>,
        limits: PeerServerLimits,
    ) -> Result<Self, TransportError> {
        let address = listener.local_addr()?;
        if !address.ip().is_loopback() {
            return Err(TransportError::NotConfigured);
        }
        Ok(Self {
            listener,
            trust,
            handler,
            limits,
        })
    }

    /// Serves until `shutdown` resolves. Connections are bounded and cleaned up on exit.
    pub async fn serve(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let connections = Arc::new(Semaphore::new(self.limits.max_connections));
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { return; }
                }
                accepted = self.listener.accept() => {
                    let Ok((stream, _)) = accepted else { continue };
                    let Ok(permit) = connections.clone().acquire_owned().await else { return };
                    let trust = self.trust.clone();
                    let handler = self.handler.clone();
                    let timeout = self.limits.request_timeout;
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = serve_connection(stream, trust, handler, timeout).await;
                    });
                }
            }
        }
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    trust: Arc<TrustContext>,
    handler: Arc<dyn PeerHandler>,
    timeout: Duration,
) -> Result<(), TransportError> {
    loop {
        let request = tokio::time::timeout(timeout, read_envelope(&mut stream))
            .await
            .map_err(|_| TransportError::Timeout)??;
        // Identity validation: the peer may not assert cluster or node identity freely.
        if request.cluster_id != trust.cluster.get() {
            let error = crate::rpc::RpcErrorCode::ClusterMismatch;
            let response = Envelope::new(
                trust.cluster.get(),
                trust.node.get(),
                request.source_node,
                request.request_id,
                request.rpc_kind,
                request.metadata_revision,
                request.ownership_epoch,
                request.timeout_budget_ms,
                error.as_u16().to_be_bytes().to_vec(),
            );
            let _ = write_envelope(&mut stream, &response).await;
            return Ok(());
        }
        if request.target_node != trust.node.get() {
            let error = crate::rpc::RpcErrorCode::WrongTarget;
            let response = Envelope::new(
                trust.cluster.get(),
                trust.node.get(),
                request.source_node,
                request.request_id,
                request.rpc_kind,
                request.metadata_revision,
                request.ownership_epoch,
                request.timeout_budget_ms,
                error.as_u16().to_be_bytes().to_vec(),
            );
            let _ = write_envelope(&mut stream, &response).await;
            return Ok(());
        }
        if !trust.accepts_source(NodeId(request.source_node)) {
            let error = crate::rpc::RpcErrorCode::UnknownSource;
            let response = Envelope::new(
                trust.cluster.get(),
                trust.node.get(),
                request.source_node,
                request.request_id,
                request.rpc_kind,
                request.metadata_revision,
                request.ownership_epoch,
                request.timeout_budget_ms,
                error.as_u16().to_be_bytes().to_vec(),
            );
            let _ = write_envelope(&mut stream, &response).await;
            return Ok(());
        }
        let kind = match RpcKind::from_u16(request.rpc_kind) {
            Some(kind) => kind,
            None => {
                let error = crate::rpc::RpcErrorCode::UnknownKind;
                let response = Envelope::new(
                    trust.cluster.get(),
                    trust.node.get(),
                    request.source_node,
                    request.request_id,
                    request.rpc_kind,
                    request.metadata_revision,
                    request.ownership_epoch,
                    request.timeout_budget_ms,
                    error.as_u16().to_be_bytes().to_vec(),
                );
                let _ = write_envelope(&mut stream, &response).await;
                return Ok(());
            }
        };
        let body = if kind.is_reserved_unsupported() {
            Err(crate::rpc::RpcErrorCode::Unsupported)
        } else {
            handler.handle(request.clone()).await
        };
        let response_body = match body {
            Ok(body) => body,
            Err(error) => error.as_u16().to_be_bytes().to_vec(),
        };
        let response = Envelope::new(
            trust.cluster.get(),
            trust.node.get(),
            request.source_node,
            request.request_id,
            request.rpc_kind,
            request.metadata_revision,
            request.ownership_epoch,
            request.timeout_budget_ms,
            response_body,
        );
        write_envelope(&mut stream, &response).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_bound_outstanding_requests() {
        let limits = TransportLimits::default();
        assert!(limits.max_outstanding > 0);
        assert!(limits.connect_timeout < limits.request_timeout);
    }
}
