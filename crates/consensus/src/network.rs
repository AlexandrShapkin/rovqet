//! Bounded TCP transport for one Q2 shard group.
//!
//! Each RPC is one length-prefixed, CRC32C-checksummed JSON frame over a fresh loopback
//! connection. The transport never invents consensus: it only carries the library's own
//! vote, append-entries, snapshot and client-write messages.
use crate::types::{
    ShardCommand, ShardCommandResponse, ShardNode, ShardNodeId, ShardRaftTypeConfig,
};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};

/// Largest frame this transport accepts or sends.
pub const MAX_RPC_BYTES: usize = 8 * 1024 * 1024;

/// CRC-32C (Castagnoli) of `bytes`, the checksum of every frame on the wire.
///
/// The Q2 log carries its own checksum implementation; this one is deliberately local so
/// the transport stays usable without the log module and has no hidden state.
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82F6_3B78 & mask);
        }
    }
    !crc
}

/// One request of the Q2 shard Raft protocol.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ShardRpc {
    Vote(VoteRequest<ShardNodeId>),
    /// A client write that a follower forwards to the leader.
    ClientWrite(ShardCommand),
    AppendEntries(AppendEntriesRequest<ShardRaftTypeConfig>),
    InstallSnapshot {
        vote: openraft::Vote<ShardNodeId>,
        meta: openraft::SnapshotMeta<ShardNodeId, ShardNode>,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
}

/// One response of the Q2 shard Raft protocol.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ShardRpcResponse {
    Vote(VoteResponse<ShardNodeId>),
    /// The answer to a forwarded client write.
    ClientWrite(ShardCommandResponse),
    AppendEntries(AppendEntriesResponse<ShardNodeId>),
    InstallSnapshot(InstallSnapshotResponse<ShardNodeId>),
    Error(String),
}

/// Network factory of one Q2 shard group.
pub struct ShardNetworkFactory {
    peers: Vec<(ShardNodeId, SocketAddr)>,
    timeout: Duration,
}

impl ShardNetworkFactory {
    pub fn new(peers: Vec<(ShardNodeId, SocketAddr)>, timeout: Duration) -> Self {
        Self { peers, timeout }
    }
}

impl RaftNetworkFactory<ShardRaftTypeConfig> for ShardNetworkFactory {
    type Network = TcpShardNetwork;

    async fn new_client(&mut self, target: ShardNodeId, node: &ShardNode) -> Self::Network {
        let address = self
            .peers
            .iter()
            .find(|(id, _)| *id == target)
            .map(|(_, address)| *address)
            .or_else(|| node.address.parse().ok())
            .unwrap_or_else(|| "127.0.0.1:0".parse().expect("loopback address"));
        TcpShardNetwork {
            target,
            address,
            timeout: self.timeout,
        }
    }
}

/// Client side of one Q2 shard group.
pub struct TcpShardNetwork {
    target: ShardNodeId,
    address: SocketAddr,
    timeout: Duration,
}

impl TcpShardNetwork {
    async fn call(
        &mut self,
        request: ShardRpc,
    ) -> Result<ShardRpcResponse, RPCError<ShardNodeId, ShardNode, RaftError<ShardNodeId>>> {
        match rpc_call(self.address, &request, self.timeout).await {
            Ok(response) => Ok(response),
            Err(error) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(format!(
                    "shard RPC to node {} failed: {error}",
                    self.target
                )),
            ))),
        }
    }
}

impl RaftNetwork<ShardRaftTypeConfig> for TcpShardNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ShardRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ShardNodeId>,
        RPCError<ShardNodeId, ShardNode, RaftError<ShardNodeId>>,
    > {
        match self.call(ShardRpc::AppendEntries(rpc)).await? {
            ShardRpcResponse::AppendEntries(response) => Ok(response),
            ShardRpcResponse::Error(error) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(error),
            ))),
            _ => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other("shard RPC response mismatch"),
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ShardRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<ShardNodeId>,
        RPCError<ShardNodeId, ShardNode, RaftError<ShardNodeId, InstallSnapshotError>>,
    > {
        let request = ShardRpc::InstallSnapshot {
            vote: rpc.vote,
            meta: rpc.meta,
            offset: rpc.offset,
            data: rpc.data,
            done: rpc.done,
        };
        let response = self.call(request).await.map_err(|error| match error {
            RPCError::Unreachable(unreachable) => RPCError::Unreachable(unreachable),
            other => RPCError::Unreachable(Unreachable::new(&other)),
        })?;
        match response {
            ShardRpcResponse::InstallSnapshot(response) => Ok(response),
            ShardRpcResponse::Error(error) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(error),
            ))),
            _ => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other("shard RPC response mismatch"),
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ShardNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ShardNodeId>, RPCError<ShardNodeId, ShardNode, RaftError<ShardNodeId>>>
    {
        match self.call(ShardRpc::Vote(rpc)).await? {
            ShardRpcResponse::Vote(response) => Ok(response),
            ShardRpcResponse::Error(error) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(error),
            ))),
            _ => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other("shard RPC response mismatch"),
            ))),
        }
    }
}

/// Sends one client write to the node that leads the group.
///
/// A follower cannot commit a proposal itself, and a caller that has no leader of its own
/// must not invent one: the proposal is forwarded to the committed leader and the answer
/// is returned verbatim.
pub async fn forward_client_write(
    address: SocketAddr,
    leader: ShardNodeId,
    command: ShardCommand,
    timeout: Duration,
) -> Result<ShardCommandResponse, std::io::Error> {
    match rpc_call(address, &ShardRpc::ClientWrite(command), timeout).await {
        Ok(ShardRpcResponse::ClientWrite(response)) => Ok(response),
        Ok(ShardRpcResponse::Error(error)) => Err(std::io::Error::other(format!(
            "the leader {leader} refused the write: {error}"
        ))),
        Ok(_) => Err(std::io::Error::other("unexpected shard RPC response")),
        Err(error) => Err(error),
    }
}

/// One request/response exchange over a fresh frame connection.
async fn rpc_call(
    address: SocketAddr,
    request: &ShardRpc,
    timeout: Duration,
) -> Result<ShardRpcResponse, std::io::Error> {
    let call = async {
        let payload = serde_json::to_vec(request)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let mut stream = TcpStream::connect(address).await?;
        write_frame(&mut stream, &payload).await?;
        let body = read_frame(&mut stream).await?;
        serde_json::from_slice(&body)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    };
    match tokio::time::timeout(timeout, call).await {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("shard RPC to {address} timed out"),
        )),
    }
}

/// Reads one request frame from a stream.
pub async fn read_rpc(stream: &mut TcpStream) -> Result<ShardRpc, std::io::Error> {
    let body = read_frame(stream).await?;
    serde_json::from_slice(&body)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

/// Writes one response frame to a stream.
pub async fn write_rpc(
    stream: &mut TcpStream,
    response: &ShardRpcResponse,
) -> Result<(), std::io::Error> {
    let payload = serde_json::to_vec(response)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    write_frame(stream, &payload).await
}

/// Reads one bounded, checksummed frame body.
pub async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, std::io::Error> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let length = u32::from_be_bytes(header[..4].try_into().expect("four bytes")) as usize;
    if length > MAX_RPC_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "shard RPC frame exceeds the payload bound",
        ));
    }
    let checksum = u32::from_be_bytes(header[4..].try_into().expect("four bytes"));
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    if crc32c(&body) != checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "shard RPC checksum mismatch",
        ));
    }
    Ok(body)
}

/// Writes one bounded, checksummed frame body.
pub async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<(), std::io::Error> {
    if payload.len() > MAX_RPC_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "shard RPC payload exceeds the payload bound",
        ));
    }
    let mut frame = Vec::with_capacity(payload.len() + 8);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&crc32c(payload).to_be_bytes());
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await?;
    stream.flush().await
}

/// Handles one decoded peer request and returns the encoded response body.
///
/// The handler owns the decisions: the server only bounds, frames and times out the work.
pub trait ShardPeerHandler: Send + Sync + 'static {
    fn handle(
        &self,
        envelope: ShardRpc,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + '_>>;
}

/// Bounds of the peer listener.
#[derive(Debug, Clone, Copy)]
pub struct PeerServerLimits {
    /// Concurrently served connections. A connection beyond the bound is refused instead
    /// of being queued without limit.
    pub max_connections: usize,
    /// Wall-clock bound of one request, from its first byte to its flushed response.
    pub request_timeout: Duration,
}

impl Default for PeerServerLimits {
    fn default() -> Self {
        Self {
            max_connections: 64,
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// Bounded peer listener of one Q2 shard voter.
pub struct ShardPeerServer {
    listener: TcpListener,
    trusted: Vec<SocketAddr>,
    handler: Arc<dyn ShardPeerHandler>,
    limits: PeerServerLimits,
}

impl ShardPeerServer {
    /// Binds the request handling to `handler`.
    ///
    /// `trusted` is the address list of the shard voters: when it is non-empty, a
    /// connection from an address outside the list is closed without being read.
    pub fn new(
        listener: TcpListener,
        trusted: Vec<(ShardNodeId, SocketAddr)>,
        handler: Arc<dyn ShardPeerHandler>,
        limits: PeerServerLimits,
    ) -> Self {
        Self {
            listener,
            trusted: trusted.into_iter().map(|(_, address)| address).collect(),
            handler,
            limits,
        }
    }

    /// Serves peers until `stop` carries `true`.
    pub async fn serve(self, mut stop: watch::Receiver<bool>) -> Result<(), std::io::Error> {
        let permits = Arc::new(Semaphore::new(self.limits.max_connections.max(1)));
        loop {
            let permit = tokio::select! {
                changed = stop.changed() => {
                    match changed {
                        Ok(()) => if *stop.borrow() { return Ok(()) },
                        Err(_) => return Ok(()),
                    }
                    continue;
                }
                permit = permits.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => return Ok(()),
                },
            };
            let accepted = tokio::select! {
                changed = stop.changed() => {
                    match changed {
                        Ok(()) => if *stop.borrow() { return Ok(()) },
                        Err(_) => return Ok(()),
                    }
                    continue;
                }
                accepted = self.listener.accept() => accepted,
            };
            let (mut stream, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(_) => {
                    drop(permit);
                    continue;
                }
            };
            if !self.trusted.is_empty() && !self.trusted.iter().any(|addr| addr.ip() == peer.ip()) {
                // An unknown remote host: refuse before reading a single byte.
                drop(permit);
                continue;
            }
            let handler = Arc::clone(&self.handler);
            let timeout = self.limits.request_timeout;
            tokio::spawn(async move {
                let response = match tokio::time::timeout(timeout, read_rpc(&mut stream)).await {
                    Ok(Ok(envelope)) => {
                        match tokio::time::timeout(timeout, handler.handle(envelope)).await {
                            Ok(Ok(body)) => body,
                            Ok(Err(error)) => error_body(&error),
                            Err(_) => error_body("the shard request handler timed out"),
                        }
                    }
                    Ok(Err(error)) => error_body(&format!("invalid shard RPC frame: {error}")),
                    Err(_) => error_body("the shard request read timed out"),
                };
                let _ = tokio::time::timeout(timeout, write_frame(&mut stream, &response)).await;
                drop(permit);
            });
        }
    }
}

/// Encodes a transport-level failure as a protocol error response.
fn error_body(message: &str) -> Vec<u8> {
    serde_json::to_vec(&ShardRpcResponse::Error(message.to_string())).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_checksum_matches_the_standard_crc32c_vectors() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0u8]), 0x527D_5351);
        assert_ne!(crc32c(b"shard"), crc32c(b"sharD"));
    }
}
