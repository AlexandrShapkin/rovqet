//! Filesystem-backed Q1 WAL. A caller must provide one exclusive owner per directory.
//! Appends reach the page cache; only `sync` establishes a recoverable boundary.
//! Blocking methods belong on a dedicated storage thread, never the Q0 executor.
#![forbid(unsafe_code)]
use rovqet_core::{DurableOffset, PartitionRef};
use std::{
    collections::VecDeque,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub const HEADER_BYTES: usize = 52;
pub const RECORD_OVERHEAD: usize = 88;
pub const FOOTER_BYTES: usize = 20;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

/// All byte limits include persistent encoding overhead. Segment bounds include header/footer.
/// Defaults are development placeholders: callers must supply their assigned partition identity
/// and an explicitly created, unique log incarnation before using real history.
#[derive(Debug, Clone)]
pub struct Config {
    pub cluster_id: u64,
    pub node_id: u64,
    pub partition: PartitionRef,
    pub incarnation: u128,
    pub max_segment_bytes: u64,
    pub max_record_bytes: usize,
    pub max_pending_bytes: usize,
    pub max_pending_records: usize,
    pub max_segments: usize,
    pub max_records: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            cluster_id: 1,
            node_id: 1,
            partition: PartitionRef {
                tenant_id: 1,
                topic_id: 1,
                partition_id: 0,
            },
            incarnation: 1,
            max_segment_bytes: 64 * 1024 * 1024,
            max_record_bytes: 1024 * 1024,
            max_pending_bytes: 4 * 1024 * 1024,
            max_pending_records: 1024,
            max_segments: 1024,
            max_records: 1_000_000,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    Message = 1,
    ProducerOutcome = 2,
    ConsumerAck = 3,
    TerminalDisposition = 4,
    CursorCheckpoint = 5,
    Tombstone = 6,
}
impl RecordKind {
    fn decode(v: u8) -> Result<Self> {
        Ok(match v {
            1 => Self::Message,
            2 => Self::ProducerOutcome,
            3 => Self::ConsumerAck,
            4 => Self::TerminalDisposition,
            5 => Self::CursorCheckpoint,
            6 => Self::Tombstone,
            _ => return Err(Error::Corruption("unknown record kind")),
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub kind: RecordKind,
    pub logical_offset: u64,
    pub owner_epoch: u64,
    pub producer_id: u128,
    pub producer_epoch: u64,
    pub sequence: u64,
    pub message_id: u128,
    pub server_timestamp: u64,
    pub payload: Vec<u8>,
}
/// Machine-readable filesystem failure phase. Sync failures have an uncertain outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoFailureKind {
    Read,
    Write,
    Sync,
}
#[derive(Debug)]
pub enum Error {
    Io {
        kind: IoFailureKind,
        operation: &'static str,
        source: io::Error,
    },
    Corruption(&'static str),
    UnsupportedVersion,
    IdentityMismatch,
    IncarnationMismatch,
    InvalidOffset,
    Capacity,
    Poisoned,
    InvalidConfig,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
fn ioerr(operation: &'static str, source: io::Error) -> Error {
    let kind = match operation {
        "segment sync"
        | "manifest sync"
        | "manifest rename"
        | "directory sync"
        | "truncate sync"
        | "recovery directory sync" => IoFailureKind::Sync,
        "append open"
        | "append"
        | "partial append"
        | "injected partial append"
        | "create segment"
        | "write header"
        | "seal open"
        | "seal"
        | "manifest temp"
        | "manifest write"
        | "truncate uncommitted tail"
        | "remove orphan" => IoFailureKind::Write,
        _ => IoFailureKind::Read,
    };
    Error::Io {
        kind,
        operation,
        source,
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    PartialAppend,
    Append,
    SegmentSync,
    ManifestWrite,
    ManifestSync,
    ManifestRename,
    DirectorySync,
}
#[derive(Clone)]
struct Segment {
    id: u64,
    len: u64,
    count: u64,
    last: u64,
    sealed: bool,
}
#[derive(Clone)]
struct Entry {
    segment: u64,
    position: u64,
    len: usize,
    offset: u64,
}
pub struct Wal {
    dir: PathBuf,
    config: Config,
    segments: Vec<Segment>,
    index: Vec<Entry>,
    committed_count: usize,
    generation: u64,
    pending: VecDeque<Record>,
    pending_bytes: usize,
    poisoned: bool,
    fault: Option<FaultPoint>,
}
fn crc(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in bytes {
        c ^= u32::from(b);
        for _ in 0..8 {
            c = (c >> 1) ^ (0x82f63b78u32 & (0u32.wrapping_sub(c & 1)));
        }
    }
    !c
}
fn u16b(b: &mut Vec<u8>, v: u16) {
    b.extend(v.to_be_bytes())
}
fn u32b(b: &mut Vec<u8>, v: u32) {
    b.extend(v.to_be_bytes())
}
fn u64b(b: &mut Vec<u8>, v: u64) {
    b.extend(v.to_be_bytes())
}
fn u128b(b: &mut Vec<u8>, v: u128) {
    b.extend(v.to_be_bytes())
}
fn checked_crc(b: &[u8]) -> Result<()> {
    if b.len() < 4
        || crc(&b[..b.len() - 4]) != u32::from_be_bytes(b[b.len() - 4..].try_into().unwrap())
    {
        Err(Error::Corruption("checksum"))
    } else {
        Ok(())
    }
}
struct Decoder<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Decoder<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .p
            .checked_add(n)
            .ok_or(Error::Corruption("length overflow"))?;
        let v = self
            .b
            .get(self.p..end)
            .ok_or(Error::Corruption("truncated field"))?;
        self.p = end;
        Ok(v)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128> {
        Ok(u128::from_be_bytes(self.take(16)?.try_into().unwrap()))
    }
}
fn encode_record(r: &Record) -> Result<Vec<u8>> {
    let n = RECORD_OVERHEAD
        .checked_add(r.payload.len())
        .ok_or(Error::Capacity)?;
    let mut b = Vec::with_capacity(n);
    u32b(&mut b, u32::try_from(n).map_err(|_| Error::Capacity)?);
    b.extend([1, r.kind as u8]);
    u16b(&mut b, 0);
    u64b(&mut b, r.logical_offset);
    u64b(&mut b, r.owner_epoch);
    u128b(&mut b, r.producer_id);
    u64b(&mut b, r.producer_epoch);
    u64b(&mut b, r.sequence);
    u128b(&mut b, r.message_id);
    u64b(&mut b, r.server_timestamp);
    u32b(&mut b, r.payload.len() as u32);
    let c = crc_after_payload(&mut b, &r.payload);
    u32b(&mut b, c);
    Ok(b)
}
fn crc_after_payload(b: &mut Vec<u8>, p: &[u8]) -> u32 {
    b.extend(p);
    crc(b)
}
fn decode_record(b: &[u8]) -> Result<Record> {
    checked_crc(b)?;
    let mut d = Decoder { b, p: 0 };
    if d.u32()? as usize != b.len() {
        return Err(Error::Corruption("record length"));
    }
    if d.u8()? != 1 {
        return Err(Error::UnsupportedVersion);
    }
    let kind = RecordKind::decode(d.u8()?)?;
    if d.u16()? != 0 {
        return Err(Error::Corruption("record flags"));
    }
    let logical_offset = d.u64()?;
    let owner_epoch = d.u64()?;
    let producer_id = d.u128()?;
    let producer_epoch = d.u64()?;
    let sequence = d.u64()?;
    let message_id = d.u128()?;
    let server_timestamp = d.u64()?;
    let n = d.u32()? as usize;
    let payload = d.take(n)?.to_vec();
    if d.p + 4 != b.len() {
        return Err(Error::Corruption("payload length"));
    }
    Ok(Record {
        kind,
        logical_offset,
        owner_epoch,
        producer_id,
        producer_epoch,
        sequence,
        message_id,
        server_timestamp,
        payload,
    })
}
impl Wal {
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        if config.max_record_bytes < RECORD_OVERHEAD
            || config.max_record_bytes > u32::MAX as usize
            || config.max_segment_bytes
                < (HEADER_BYTES + FOOTER_BYTES) as u64 + config.max_record_bytes as u64
            || config.max_pending_bytes < config.max_record_bytes
            || config.max_pending_records == 0
            || config.max_segments == 0
            || config.max_records == 0
            || config.max_segments > 100_000
        {
            return Err(Error::InvalidConfig);
        }
        let dir = path.as_ref().to_path_buf();
        if !dir.is_dir() {
            return Err(ioerr(
                "open directory",
                io::Error::new(io::ErrorKind::NotFound, "WAL directory must already exist"),
            ));
        }
        let mut w = Self {
            dir,
            config,
            segments: Vec::new(),
            index: Vec::new(),
            committed_count: 0,
            generation: 0,
            pending: VecDeque::new(),
            pending_bytes: 0,
            poisoned: false,
            fault: None,
        };
        let manifest = w.dir.join("MANIFEST");
        if manifest.exists() {
            w.load_manifest()?;
            w.recover()?;
        } else {
            // Existing segment history without its manifest must never be reset.
            for e in fs::read_dir(&w.dir).map_err(|e| ioerr("discover", e))? {
                let e = e.map_err(|e| ioerr("discover", e))?;
                if e.file_name().to_string_lossy().starts_with("segment-")
                    || e.file_name() == "MANIFEST.tmp"
                {
                    return Err(Error::Corruption("missing manifest"));
                }
            }
            w.create_segment(0)?;
            w.sync()?;
        }
        Ok(w)
    }
    fn path(&self, id: u64) -> PathBuf {
        self.dir.join(format!("segment-{id:020}.wal"))
    }
    fn ready(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    fn failpoint(&mut self, p: FaultPoint) -> Result<()> {
        if self.fault == Some(p) {
            self.fault = None;
            Err(ioerr(
                match p {
                    FaultPoint::Append | FaultPoint::PartialAppend => "append",
                    FaultPoint::SegmentSync => "segment sync",
                    FaultPoint::ManifestWrite => "manifest write",
                    FaultPoint::ManifestSync => "manifest sync",
                    FaultPoint::ManifestRename => "manifest rename",
                    FaultPoint::DirectorySync => "directory sync",
                },
                io::Error::other(format!("{p:?}")),
            ))
        } else {
            Ok(())
        }
    }
    #[doc(hidden)]
    pub fn inject_fault(&mut self, p: FaultPoint) {
        self.fault = Some(p)
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }
    pub fn committed_position(&self) -> Option<DurableOffset> {
        self.index
            .get(self.committed_count.checked_sub(1)?)
            .map(|e| DurableOffset {
                log_incarnation: self.config.incarnation,
                sequence: e.offset,
            })
    }
    pub fn enqueue(&mut self, r: Record) -> Result<()> {
        self.ready()?;
        let n = RECORD_OVERHEAD
            .checked_add(r.payload.len())
            .ok_or(Error::Capacity)?;
        if n > self.config.max_record_bytes
            || self.pending.len() >= self.config.max_pending_records
            || self
                .pending_bytes
                .checked_add(n)
                .is_none_or(|x| x > self.config.max_pending_bytes)
            || self.index.len() + self.pending.len() >= self.config.max_records
        {
            return Err(Error::Capacity);
        }
        let expected = self
            .pending
            .back()
            .map(|r| r.logical_offset)
            .or_else(|| self.index.last().map(|e| e.offset))
            .map(|v| v.checked_add(1).ok_or(Error::Capacity))
            .transpose()?
            .unwrap_or(0);
        if r.logical_offset != expected {
            return Err(Error::InvalidOffset);
        }
        self.pending_bytes += n;
        self.pending.push_back(r);
        Ok(())
    }
    /// Cancel records still in memory. Written records cannot be cancelled.
    pub fn cancel_pending(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
    }
    pub fn append(&mut self, r: Record) -> Result<()> {
        self.enqueue(r)?;
        self.flush()
    }
    pub fn flush(&mut self) -> Result<()> {
        self.ready()?;
        let result = self.flush_inner();
        if result.is_err() {
            self.poisoned = true
        }
        result
    }
    fn flush_inner(&mut self) -> Result<()> {
        while let Some(r) = self.pending.pop_front() {
            let bytes = encode_record(&r)?;
            self.pending_bytes -= bytes.len();
            let s = self.segments.last().unwrap();
            if s.sealed
                || s.len + bytes.len() as u64 + FOOTER_BYTES as u64 > self.config.max_segment_bytes
            {
                self.rotate()?;
            }
            self.failpoint(FaultPoint::Append)?;
            let s = self.segments.last().unwrap();
            let id = s.id;
            let pos = s.len;
            let mut f = OpenOptions::new()
                .append(true)
                .open(self.path(id))
                .map_err(|e| ioerr("append open", e))?;
            if self.fault == Some(FaultPoint::PartialAppend) {
                self.fault = None;
                f.write_all(&bytes[..bytes.len() / 2])
                    .map_err(|e| ioerr("partial append", e))?;
                return Err(ioerr(
                    "injected partial append",
                    io::Error::other("partial write"),
                ));
            }
            f.write_all(&bytes).map_err(|e| ioerr("append", e))?;
            let s = self.segments.last_mut().unwrap();
            s.len += bytes.len() as u64;
            s.count += 1;
            s.last = r.logical_offset;
            self.index.push(Entry {
                segment: id,
                position: pos,
                len: bytes.len(),
                offset: r.logical_offset,
            });
        }
        Ok(())
    }
    fn header(&self, id: u64) -> Vec<u8> {
        let mut b = b"RQWL".to_vec();
        u16b(&mut b, 1);
        u16b(&mut b, 0);
        u64b(&mut b, id);
        u64b(&mut b, self.config.partition.partition_id as u64);
        u128b(&mut b, self.config.incarnation);
        u64b(&mut b, 0);
        let c = crc(&b);
        u32b(&mut b, c);
        b
    }
    fn create_segment(&mut self, id: u64) -> Result<()> {
        if self.segments.len() >= self.config.max_segments {
            return Err(Error::Capacity);
        }
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path(id))
            .map_err(|e| ioerr("create segment", e))?;
        f.write_all(&self.header(id))
            .map_err(|e| ioerr("write header", e))?;
        self.segments.push(Segment {
            id,
            len: HEADER_BYTES as u64,
            count: 0,
            last: u64::MAX,
            sealed: false,
        });
        Ok(())
    }
    fn rotate(&mut self) -> Result<()> {
        if self.segments.len() >= self.config.max_segments {
            return Err(Error::Capacity);
        }
        let s = self.segments.last().unwrap().clone();
        if !s.sealed {
            let mut f = OpenOptions::new()
                .read(true)
                .append(true)
                .open(self.path(s.id))
                .map_err(|e| ioerr("seal open", e))?;
            let mut footer = Vec::new();
            u64b(&mut footer, s.last);
            u64b(&mut footer, s.count);
            let checksum = segment_crc(&mut f, s.len, &footer)?;
            u32b(&mut footer, checksum);
            f.write_all(&footer).map_err(|e| ioerr("seal", e))?;
            let last = self.segments.last_mut().unwrap();
            last.len += FOOTER_BYTES as u64;
            last.sealed = true;
        }
        self.create_segment(s.id.checked_add(1).ok_or(Error::Capacity)?)
    }
    /// Commit all written records as a group, including records and producer metadata.
    /// Any failure poisons this owner; reopen to resolve an uncertain outcome.
    pub fn sync(&mut self) -> Result<Option<DurableOffset>> {
        self.ready()?;
        let result = self.sync_inner();
        if result.is_err() {
            self.poisoned = true
        }
        result
    }
    fn sync_inner(&mut self) -> Result<Option<DurableOffset>> {
        self.flush_inner()?;
        self.failpoint(FaultPoint::SegmentSync)?;
        for s in &self.segments {
            File::open(self.path(s.id))
                .and_then(|f| f.sync_all())
                .map_err(|e| ioerr("segment sync", e))?;
        }
        let generation = self.generation.checked_add(1).ok_or(Error::Capacity)?;
        let bytes = self.manifest(generation)?;
        self.failpoint(FaultPoint::ManifestWrite)?;
        let mut temp = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.dir.join("MANIFEST.tmp"))
            .map_err(|e| ioerr("manifest temp", e))?;
        temp.write_all(&bytes)
            .map_err(|e| ioerr("manifest write", e))?;
        self.failpoint(FaultPoint::ManifestSync)?;
        temp.sync_all().map_err(|e| ioerr("manifest sync", e))?;
        self.failpoint(FaultPoint::ManifestRename)?;
        fs::rename(self.dir.join("MANIFEST.tmp"), self.dir.join("MANIFEST"))
            .map_err(|e| ioerr("manifest rename", e))?;
        self.failpoint(FaultPoint::DirectorySync)?;
        File::open(&self.dir)
            .and_then(|f| f.sync_all())
            .map_err(|e| ioerr("directory sync", e))?;
        self.generation = generation;
        self.committed_count = self.index.len();
        Ok(self.committed_position())
    }
    fn manifest(&self, generation: u64) -> Result<Vec<u8>> {
        let mut b = b"RQWM".to_vec();
        u16b(&mut b, 1);
        u16b(&mut b, 0);
        u64b(&mut b, self.config.cluster_id);
        u64b(&mut b, self.config.node_id);
        u64b(&mut b, self.config.partition.tenant_id);
        u64b(&mut b, self.config.partition.topic_id);
        u32b(&mut b, self.config.partition.partition_id);
        b.push(1);
        u128b(&mut b, self.config.incarnation);
        u64b(&mut b, generation);
        u64b(&mut b, self.index.len() as u64);
        u32b(&mut b, self.segments.len() as u32);
        for s in &self.segments {
            u64b(&mut b, s.id);
            u64b(&mut b, s.len);
            u64b(&mut b, s.count);
            u64b(&mut b, s.last);
            b.push(u8::from(s.sealed));
        }
        let c = crc(&b);
        u32b(&mut b, c);
        if b.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(Error::Capacity);
        }
        Ok(b)
    }
    fn load_manifest(&mut self) -> Result<()> {
        let mut f = File::open(self.dir.join("MANIFEST")).map_err(|e| ioerr("manifest open", e))?;
        let n = f
            .metadata()
            .map_err(|e| ioerr("manifest metadata", e))?
            .len();
        if n > MAX_MANIFEST_BYTES {
            return Err(Error::Corruption("oversized manifest"));
        }
        let mut b = Vec::with_capacity(n as usize);
        f.read_to_end(&mut b)
            .map_err(|e| ioerr("manifest read", e))?;
        checked_crc(&b)?;
        let mut d = Decoder { b: &b, p: 0 };
        if d.take(4)? != b"RQWM" {
            return Err(Error::Corruption("manifest magic"));
        }
        if d.u16()? != 1 || d.u16()? != 0 {
            return Err(Error::UnsupportedVersion);
        }
        if d.u64()? != self.config.cluster_id
            || d.u64()? != self.config.node_id
            || d.u64()? != self.config.partition.tenant_id
            || d.u64()? != self.config.partition.topic_id
            || d.u32()? != self.config.partition.partition_id
            || d.u8()? != 1
        {
            return Err(Error::IdentityMismatch);
        }
        if d.u128()? != self.config.incarnation {
            return Err(Error::IncarnationMismatch);
        }
        self.generation = d.u64()?;
        if self.generation == 0 {
            return Err(Error::Corruption("zero manifest generation"));
        }
        self.committed_count = usize::try_from(d.u64()?).map_err(|_| Error::Capacity)?;
        if self.committed_count > self.config.max_records {
            return Err(Error::Capacity);
        }
        let count = d.u32()? as usize;
        if count == 0 || count > self.config.max_segments {
            return Err(Error::Capacity);
        }
        for i in 0..count {
            let s = Segment {
                id: d.u64()?,
                len: d.u64()?,
                count: d.u64()?,
                last: d.u64()?,
                sealed: match d.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(Error::Corruption("sealed flag")),
                },
            };
            if s.id != i as u64
                || s.len < HEADER_BYTES as u64
                || s.len > self.config.max_segment_bytes
                || s.count > self.committed_count as u64
                || (s.sealed && s.len < (HEADER_BYTES + FOOTER_BYTES) as u64)
                || (i + 1 < count && !s.sealed)
            {
                return Err(Error::Corruption("segment inventory"));
            }
            self.segments.push(s);
        }
        if d.p + 4 != b.len() {
            return Err(Error::Corruption("manifest trailing bytes"));
        }
        Ok(())
    }
    fn recover(&mut self) -> Result<()> {
        for s in &self.segments {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.path(s.id))
                .map_err(|e| ioerr("recover segment", e))?;
            if f.metadata()
                .map_err(|e| ioerr("segment metadata", e))?
                .len()
                < s.len
            {
                return Err(Error::Corruption("missing committed bytes"));
            }
            let mut h = [0; HEADER_BYTES];
            f.read_exact(&mut h).map_err(|e| ioerr("read header", e))?;
            checked_crc(&h)?;
            if h.as_slice() != self.header(s.id) {
                return Err(Error::Corruption("segment header identity/version"));
            }
            let end = s.len - if s.sealed { FOOTER_BYTES as u64 } else { 0 };
            let mut pos = HEADER_BYTES as u64;
            let start = self.index.len();
            while pos < end {
                if end - pos < 4 {
                    return Err(Error::Corruption("partial committed header"));
                }
                let mut n = [0; 4];
                f.read_exact(&mut n)
                    .map_err(|e| ioerr("record length", e))?;
                let n = u32::from_be_bytes(n) as usize;
                if n < RECORD_OVERHEAD || n > self.config.max_record_bytes || n as u64 > end - pos {
                    return Err(Error::Corruption("committed record bounds"));
                }
                let mut b = vec![0; n];
                b[..4].copy_from_slice(&(n as u32).to_be_bytes());
                f.read_exact(&mut b[4..])
                    .map_err(|e| ioerr("read committed record", e))?;
                let r = decode_record(&b)?;
                if r.logical_offset != self.index.len() as u64 {
                    return Err(Error::Corruption("logical offset gap"));
                }
                if self.index.len() >= self.config.max_records {
                    return Err(Error::Capacity);
                }
                self.index.push(Entry {
                    segment: s.id,
                    position: pos,
                    len: n,
                    offset: r.logical_offset,
                });
                pos += n as u64;
            }
            if (self.index.len() - start) as u64 != s.count
                || self
                    .index
                    .last()
                    .filter(|e| e.segment == s.id)
                    .map(|e| e.offset)
                    .unwrap_or(u64::MAX)
                    != s.last
            {
                return Err(Error::Corruption("segment record inventory"));
            }
            if s.sealed {
                let mut footer = [0; FOOTER_BYTES];
                f.read_exact(&mut footer)
                    .map_err(|e| ioerr("read footer", e))?;
                let mut expected = Vec::new();
                u64b(&mut expected, s.last);
                u64b(&mut expected, s.count);
                if footer[..16] != expected
                    || u32::from_be_bytes(footer[16..].try_into().unwrap())
                        != segment_crc(&mut f, end, &expected)?
                {
                    return Err(Error::Corruption("sealed footer"));
                }
            } // No byte after the manifest-established boundary is exposed.
            f.set_len(s.len)
                .map_err(|e| ioerr("truncate uncommitted tail", e))?;
            f.sync_all().map_err(|e| ioerr("truncate sync", e))?;
        }
        if self.index.len() != self.committed_count {
            return Err(Error::Corruption("recovery watermark"));
        }
        // Unlisted segments were never published by a successful barrier.
        for e in fs::read_dir(&self.dir).map_err(|e| ioerr("discover", e))? {
            let e = e.map_err(|e| ioerr("discover", e))?;
            let name = e.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = name
                .strip_prefix("segment-")
                .and_then(|v| v.strip_suffix(".wal"))
                .and_then(|v| v.parse::<u64>().ok())
            {
                if !self.segments.iter().any(|s| s.id == id) {
                    fs::remove_file(e.path()).map_err(|e| ioerr("remove orphan", e))?;
                }
            }
        }
        File::open(&self.dir)
            .and_then(|f| f.sync_all())
            .map_err(|e| ioerr("recovery directory sync", e))?;
        Ok(())
    }
    pub fn read(
        &self,
        from: DurableOffset,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        self.ready()?;
        if from.log_incarnation != self.config.incarnation {
            return Err(Error::IncarnationMismatch);
        }
        let start = usize::try_from(from.sequence).map_err(|_| Error::InvalidOffset)?;
        if start >= self.committed_count {
            return Err(Error::InvalidOffset);
        }
        let mut result = Vec::new();
        let mut bytes = 0usize;
        for e in self.index[start..self.committed_count]
            .iter()
            .take(max_records)
        {
            if bytes.checked_add(e.len).is_none_or(|v| v > max_bytes) {
                break;
            }
            let mut f = File::open(self.path(e.segment)).map_err(|e| ioerr("read open", e))?;
            f.seek(SeekFrom::Start(e.position))
                .map_err(|e| ioerr("read seek", e))?;
            let mut b = vec![0; e.len];
            f.read_exact(&mut b).map_err(|e| ioerr("read", e))?;
            let r = decode_record(&b)?;
            if r.logical_offset != e.offset {
                return Err(Error::Corruption("read index mismatch"));
            }
            bytes += e.len;
            result.push(r);
        }
        Ok(result)
    }
}
fn segment_crc(f: &mut File, len: u64, suffix: &[u8]) -> Result<u32> {
    f.seek(SeekFrom::Start(0))
        .map_err(|e| ioerr("checksum seek", e))?;
    let mut remaining = len;
    let mut c = !0u32;
    let mut b = [0; 8192];
    while remaining > 0 {
        let n = remaining.min(b.len() as u64) as usize;
        f.read_exact(&mut b[..n])
            .map_err(|e| ioerr("segment checksum read", e))?;
        for &v in &b[..n] {
            c ^= u32::from(v);
            for _ in 0..8 {
                c = (c >> 1) ^ (0x82f63b78 & (0u32.wrapping_sub(c & 1)));
            }
        }
        remaining -= n as u64;
    }
    for &v in suffix {
        c ^= u32::from(v);
        for _ in 0..8 {
            c = (c >> 1) ^ (0x82f63b78 & (0u32.wrapping_sub(c & 1)));
        }
    }
    Ok(!c)
}

#[cfg(test)]
mod tests {
    #[test]
    fn crc32c_known_vector() {
        assert_eq!(super::crc(b"123456789"), 0xe3069283);
    }
}
