//! End-to-end Q1 publication/ACK latency measurement over the real TCP SDK.
//!
//! Usage:
//!   q1_measure publish topic tenant topic_id partition incarnation producer epoch start_seq payload_bytes count
//!   q1_measure consume topic tenant topic_id partition incarnation group generation count wait_ms
//!   q1_measure read topic tenant topic_id partition incarnation offset max_messages max_bytes
use rovqet_sdk::{
    AckOutcome, Client, ClientConfig, DurableOffset, GroupOptions, PartitionRef, Q1PublishOptions,
    ReadOptions,
};
use std::time::{Duration, Instant};

fn percentiles(samples: &mut [u128]) -> String {
    samples.sort_unstable();
    let pick = |q: f64| -> u128 {
        if samples.is_empty() {
            return 0;
        }
        samples[((samples.len() as f64 - 1.0) * q).round() as usize]
    };
    format!(
        "{{\"n\":{},\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"p999_ns\":{},\"min_ns\":{},\"max_ns\":{}}}",
        samples.len(),
        pick(0.50),
        pick(0.95),
        pick(0.99),
        pick(0.999),
        samples.first().copied().unwrap_or(0),
        samples.last().copied().unwrap_or(0),
    )
}

fn partition(args: &[String]) -> PartitionRef {
    PartitionRef {
        tenant_id: args[2].parse().unwrap(),
        topic_id: args[3].parse().unwrap(),
        partition_id: args[4].parse().unwrap(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 6 {
        return Err(
            "usage: q1_measure publish|consume|read topic tenant topic_id partition ...".into(),
        );
    }
    let mut config = ClientConfig {
        request_timeout: Duration::from_secs(60),
        ..ClientConfig::default()
    };
    if let Ok(endpoint) = std::env::var("ROVQET_ADDR") {
        config.endpoint = endpoint.parse()?;
    }
    let mut client = Client::connect(config).await?;
    let partition = partition(&args);
    let topic = &args[1];
    let started = Instant::now();
    match args.get(0).map(String::as_str) {
        Some("publish") => {
            let incarnation: u128 = args[5].parse()?;
            let producer: u128 = args[6].parse()?;
            let epoch: u64 = args[7].parse()?;
            let start: u64 = args[8].parse()?;
            let payload_bytes: usize = args[9].parse()?;
            let count: u64 = args[10].parse()?;
            let payload: Vec<u8> = (0..payload_bytes).map(|i| b'a' + (i % 26) as u8).collect();
            let mut samples = Vec::with_capacity(count as usize);
            let mut first = None;
            let mut last = 0;
            for sequence in start..start + count {
                let one = Instant::now();
                let outcome = client
                    .publish_q1(
                        topic,
                        &payload,
                        Q1PublishOptions {
                            producer_id: producer,
                            producer_epoch: epoch,
                            sequence,
                            message_id: 100_000 + u128::from(sequence),
                            partition_id: partition.partition_id,
                            log_incarnation: incarnation,
                            ownership_epoch: 7,
                            ttl: Duration::from_secs(30),
                        },
                    )
                    .await?;
                samples.push(one.elapsed().as_nanos());
                let rovqet_sdk::PublishOutcome::CommittedLocal(offset) = outcome else {
                    return Err("unexpected publication outcome".into());
                };
                first.get_or_insert(offset.sequence);
                last = offset.sequence;
            }
            println!(
                "{{\"schema\":\"rovqet-q1-measure-v1\",\"mode\":\"publish\",\"count\":{count},\"payload_bytes\":{payload_bytes},\"first_offset\":{},\"last_offset\":{last},\"elapsed_ns\":{},\"latency\":{}}}",
                first.unwrap_or(0),
                started.elapsed().as_nanos(),
                percentiles(&mut samples),
            );
        }
        Some("consume") => {
            let incarnation: u128 = args[5].parse()?;
            let group: u64 = args[6].parse()?;
            let generation: u64 = args[7].parse()?;
            let count: usize = args[8].parse()?;
            let wait: u64 = args[9].parse()?;
            let mut deliveries = 0usize;
            let mut acks = 0usize;
            let mut fetch_samples = Vec::new();
            let mut ack_samples = Vec::new();
            let mut next = 0u64;
            while deliveries < count {
                let one = Instant::now();
                let batch = client
                    .fetch_q1_group(
                        topic,
                        GroupOptions {
                            partition,
                            incarnation,
                            group_id: group,
                            generation: (generation != 0).then_some(generation),
                            max_messages: 64,
                            max_bytes: 65536,
                            wait: Duration::from_millis(wait),
                        },
                    )
                    .await?;
                fetch_samples.push(one.elapsed().as_nanos());
                next = batch.next.sequence;
                if batch.messages.is_empty() {
                    break;
                }
                for message in &batch.messages {
                    let one = Instant::now();
                    let outcome = client.ack_q1_group(topic, &message.token).await?;
                    ack_samples.push(one.elapsed().as_nanos());
                    let AckOutcome::Committed { .. } = outcome;
                    deliveries += 1;
                    acks += 1;
                    if deliveries >= count {
                        break;
                    }
                }
            }
            println!(
                "{{\"schema\":\"rovqet-q1-measure-v1\",\"mode\":\"consume\",\"delivered\":{deliveries},\"acked\":{acks},\"next\":{next},\"elapsed_ns\":{},\"fetch\":{},\"ack\":{}}}",
                started.elapsed().as_nanos(),
                percentiles(&mut fetch_samples),
                percentiles(&mut ack_samples),
            );
        }
        Some("read") => {
            let incarnation: u128 = args[5].parse()?;
            let offset: u64 = args[6].parse()?;
            let max_messages: u32 = args[7].parse()?;
            let max_bytes: u32 = args[8].parse()?;
            let one = Instant::now();
            let batch = client
                .read_q1(
                    topic,
                    ReadOptions {
                        partition,
                        from: DurableOffset {
                            log_incarnation: incarnation,
                            sequence: offset,
                        },
                        max_messages,
                        max_bytes,
                        wait: Duration::ZERO,
                    },
                )
                .await?;
            println!(
                "{{\"schema\":\"rovqet-q1-measure-v1\",\"mode\":\"read\",\"records\":{},\"next\":{},\"elapsed_ns\":{},\"latency_ns\":{}}}",
                batch.records.len(),
                batch.next.sequence,
                started.elapsed().as_nanos(),
                one.elapsed().as_nanos(),
            );
        }
        _ => return Err("unknown measurement mode".into()),
    }
    let _ = client.close().await;
    Ok(())
}
