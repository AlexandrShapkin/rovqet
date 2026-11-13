//! Direct WAL append/fsync latency profile for one local Q1 log.
//!
//! Usage: fsync_profile <directory> <count> [payload_bytes]
//! The directory must be a disposable, already-created path owned by the caller.
use rovqet_core::PartitionRef;
use rovqet_wal::{Config, Record, RecordKind, Wal};
use std::time::Instant;

#[derive(Default)]
struct Percentiles {
    p50: u128,
    p95: u128,
    p99: u128,
    p999: u128,
    min: u128,
    max: u128,
}
fn percentiles(samples: &mut [u128]) -> Percentiles {
    samples.sort_unstable();
    let pick = |q: f64| -> u128 {
        if samples.is_empty() {
            return 0;
        }
        let index = ((samples.len() as f64 - 1.0) * q).round() as usize;
        samples[index]
    };
    Percentiles {
        p50: pick(0.50),
        p95: pick(0.95),
        p99: pick(0.99),
        p999: pick(0.999),
        min: samples.first().copied().unwrap_or(0),
        max: samples.last().copied().unwrap_or(0),
    }
}
fn object(name: &str, values: &Percentiles) -> String {
    format!(
        "\"{name}\":{{\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"p999_ns\":{},\"min_ns\":{},\"max_ns\":{}}}",
        values.p50, values.p95, values.p99, values.p999, values.min, values.max
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !(2..=3).contains(&args.len()) {
        return Err("usage: fsync_profile directory count [payload_bytes]".into());
    }
    let directory = std::path::Path::new(&args[0]);
    let count: usize = args[1].parse()?;
    let payload_bytes: usize = args.get(2).map(|v| v.parse()).transpose()?.unwrap_or(64);
    let config = Config {
        partition: PartitionRef {
            tenant_id: 1,
            topic_id: 2,
            partition_id: 3,
        },
        incarnation: 42,
        ..Config::default()
    };
    let mut wal = Wal::open(directory, config)?;
    let mut append_ns = Vec::with_capacity(count);
    let mut sync_ns = Vec::with_capacity(count);
    let mut total_ns = Vec::with_capacity(count);
    let started = Instant::now();
    for index in 0..count {
        let offset = (index * 2) as u64;
        let payload = vec![b'x'; payload_bytes];
        let message = Record {
            kind: RecordKind::Message,
            logical_offset: offset,
            owner_epoch: 7,
            producer_id: 11,
            producer_epoch: 0,
            sequence: index as u64,
            message_id: 100 + index as u128,
            server_timestamp: 1,
            payload,
        };
        let mut outcome_payload = Vec::with_capacity(30);
        outcome_payload.extend_from_slice(b"RQPO\0\x01");
        outcome_payload.extend_from_slice(&10u128.to_be_bytes());
        outcome_payload.extend_from_slice(&offset.to_be_bytes());
        let outcome = Record {
            kind: RecordKind::ProducerOutcome,
            logical_offset: offset + 1,
            owner_epoch: message.owner_epoch,
            producer_id: message.producer_id,
            producer_epoch: message.producer_epoch,
            sequence: message.sequence,
            message_id: message.message_id,
            server_timestamp: message.server_timestamp,
            payload: outcome_payload,
        };
        let one = Instant::now();
        wal.append(message)?;
        wal.append(outcome)?;
        let after_append = Instant::now();
        wal.sync()?;
        let after_sync = Instant::now();
        append_ns.push((after_append - one).as_nanos());
        sync_ns.push((after_sync - after_append).as_nanos());
        total_ns.push((after_sync - one).as_nanos());
    }
    let elapsed = started.elapsed().as_nanos();
    let append = percentiles(&mut append_ns);
    let sync = percentiles(&mut sync_ns);
    let total = percentiles(&mut total_ns);
    println!(
        "{{\"schema\":\"rovqet-wal-fsync-profile-v1\",\"directory\":\"{}\",\"count\":{},\"payload_bytes\":{},\"records\":{},{} ,{}, {}, \"wall_ns\":{},\"committed_position\":{}}}",
        args[0],
        count,
        payload_bytes,
        count * 2,
        object("append", &append),
        object("sync", &sync),
        object("append_plus_sync", &total),
        elapsed,
        wal.committed_position()
            .map_or("null".to_string(), |p| p.sequence.to_string()),
    );
    Ok(())
}
