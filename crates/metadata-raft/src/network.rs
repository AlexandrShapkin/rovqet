//! Bounded TCP transport for the metadata Raft group.
//!
//! Each RPC is one length-prefixed, checksummed JSON frame over a fresh loopback
//! connection. The transport never invents consensus: it only carries the library's own
//! vote, append-entries and snapshot messages.
use crate::types::{MetaNode, MetaNodeId, MetadataRaftTypeConfig};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_RPC_BYTES: usize = 8 * 1024 * 1024;

/// One request of the metadata Raft protocol.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RaftRpc {
    Vote(VoteRequest<MetaNodeId>),
    /// A client write that a follower forwards to the leader.
    ClientWrite(crate::types::MetadataCommand),
    AppendEntries(AppendEntriesRequest<MetadataRaftTypeConfig>),
    InstallSnapshot {
        vote: openraft::Vote<MetaNodeId>,
        meta: openraft::SnapshotMeta<MetaNodeId, MetaNode>,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
}

/// One response of the metadata Raft protocol.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RaftRpcResponse {
    Vote(VoteResponse<MetaNodeId>),
    /// The answer to a forwarded client write.
    ClientWrite(crate::types::MetadataCommandResponse),
    AppendEntries(AppendEntriesResponse<MetaNodeId>),
    InstallSnapshot(InstallSnapshotResponse<MetaNodeId>),
    Error(String),
}

/// Network factory of the metadata group.
pub struct RaftNetworkFactoryImpl {
    peers: Vec<(MetaNodeId, SocketAddr)>,
    timeout: Duration,
}

impl RaftNetworkFactoryImpl {
    pub fn new(peers: Vec<(MetaNodeId, SocketAddr)>, timeout: Duration) -> Self {
        Self { peers, timeout }
    }
}

impl RaftNetworkFactory<MetadataRaftTypeConfig> for RaftNetworkFactoryImpl {
    type Network = TcpRaftNetwork;

    async fn new_client(&mut self, target: MetaNodeId, node: &MetaNode) -> Self::Network {
        let address = self
            .peers
            .iter()
            .find(|(id, _)| *id == target)
            .map(|(_, address)| *address)
            .or_else(|| node.address.parse().ok())
            .unwrap_or_else(|| "127.0.0.1:0".parse().expect("loopback address"));
        TcpRaftNetwork {
            target,
            address,
            timeout: self.timeout,
        }
    }
}

/// Client side of the metadata Raft protocol.
pub struct TcpRaftNetwork {
    target: MetaNodeId,
    address: SocketAddr,
    timeout: Duration,
}

impl TcpRaftNetwork {
    async fn call(
        &mut self,
        request: RaftRpc,
    ) -> Result<RaftRpcResponse, RPCError<MetaNodeId, MetaNode, RaftError<MetaNodeId>>> {
        let payload = serde_json::to_vec(&request)
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?;
        if payload.len() > MAX_RPC_BYTES {
            return Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "metadata RPC payload too large",
                ),
            )));
        }
        let call = async {
            let mut stream = TcpStream::connect(self.address).await?;
            let mut frame = Vec::with_capacity(payload.len() + 8);
            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            frame.extend_from_slice(&crate::storage::crc32c(&payload).to_be_bytes());
            frame.extend_from_slice(&payload);
            stream.write_all(&frame).await?;
            stream.flush().await?;
            let mut header = [0u8; 8];
            stream.read_exact(&mut header).await?;
            let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            if length > MAX_RPC_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "metadata RPC response too large",
                ));
            }
            let checksum = u32::from_be_bytes(header[4..].try_into().unwrap());
            let mut body = vec![0u8; length];
            stream.read_exact(&mut body).await?;
            if crate::storage::crc32c(&body) != checksum {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "metadata RPC response checksum mismatch",
                ));
            }
            Ok::<RaftRpcResponse, std::io::Error>(serde_json::from_slice(&body)?)
        };
        match tokio::time::timeout(self.timeout, call).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(format!(
                    "metadata RPC to node {} failed: {error}",
                    self.target
                )),
            ))),
            Err(_) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("metadata RPC to node {} timed out", self.target),
                ),
            ))),
        }
    }
}

impl RaftNetwork<MetadataRaftTypeConfig> for TcpRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetadataRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<MetaNodeId>,
        RPCError<MetaNodeId, MetaNode, RaftError<MetaNodeId>>,
    > {
        match self.call(RaftRpc::AppendEntries(rpc)).await? {
            RaftRpcResponse::AppendEntries(response) => Ok(response),
            RaftRpcResponse::Error(error) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(error),
            ))),
            _ => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other("metadata RPC response mismatch"),
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetadataRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<MetaNodeId>,
        RPCError<MetaNodeId, MetaNode, RaftError<MetaNodeId, InstallSnapshotError>>,
    > {
        let request = RaftRpc::InstallSnapshot {
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
            RaftRpcResponse::InstallSnapshot(response) => Ok(response),
            RaftRpcResponse::Error(error) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(error),
            ))),
            _ => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other("metadata RPC response mismatch"),
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<MetaNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<MetaNodeId>, RPCError<MetaNodeId, MetaNode, RaftError<MetaNodeId>>>
    {
        match self.call(RaftRpc::Vote(rpc)).await? {
            RaftRpcResponse::Vote(response) => Ok(response),
            RaftRpcResponse::Error(error) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(error),
            ))),
            _ => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other("metadata RPC response mismatch"),
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
    command: crate::types::MetadataCommand,
    timeout: Duration,
) -> Result<crate::types::MetadataCommandResponse, std::io::Error> {
    let call = async {
        let mut stream = TcpStream::connect(address).await?;
        let payload = serde_json::to_vec(&RaftRpc::ClientWrite(command))
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        if payload.len() > MAX_RPC_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metadata RPC payload too large",
            ));
        }
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&crate::storage::crc32c(&payload).to_be_bytes());
        frame.extend_from_slice(&payload);
        stream.write_all(&frame).await?;
        stream.flush().await?;
        let mut header = [0u8; 8];
        stream.read_exact(&mut header).await?;
        let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        if length > MAX_RPC_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metadata RPC response too large",
            ));
        }
        let checksum = u32::from_be_bytes(header[4..].try_into().unwrap());
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).await?;
        if crate::storage::crc32c(&body) != checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metadata RPC response checksum mismatch",
            ));
        }
        match serde_json::from_slice::<RaftRpcResponse>(&body) {
            Ok(RaftRpcResponse::ClientWrite(response)) => Ok(response),
            Ok(RaftRpcResponse::Error(error)) => Err(std::io::Error::other(format!(
                "the leader refused the write: {error}"
            ))),
            _ => Err(std::io::Error::other("unexpected metadata RPC response")),
        }
    };
    match tokio::time::timeout(timeout, call).await {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "forwarded metadata write timed out",
        )),
    }
}

/// Reads one request frame from a stream.
pub async fn read_rpc(stream: &mut TcpStream) -> Result<RaftRpc, std::io::Error> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
    if length > MAX_RPC_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "metadata RPC too large",
        ));
    }
    let checksum = u32::from_be_bytes(header[4..].try_into().unwrap());
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    if crate::storage::crc32c(&body) != checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "metadata RPC checksum mismatch",
        ));
    }
    serde_json::from_slice(&body)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

/// Writes one response frame to a stream.
pub async fn write_rpc(
    stream: &mut TcpStream,
    response: &RaftRpcResponse,
) -> Result<(), std::io::Error> {
    let payload = serde_json::to_vec(response)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut frame = Vec::with_capacity(payload.len() + 8);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&crate::storage::crc32c(&payload).to_be_bytes());
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).await?;
    stream.flush().await
}
