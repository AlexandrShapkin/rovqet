//! External durable consumer: fetch leased Q1 messages and optionally acknowledge them.
//!
//! Usage: consume_q1 topic tenant topic_id partition incarnation group generation
//!        max_messages max_bytes wait_ms ack_count [match_offset]
//!
//! `ack_count` acknowledges the first N deliveries. With `match_offset`, only that offset
//! is acknowledged; every other delivery is left leased deliberately.
use rovqet_sdk::{AckOutcome, Client, ClientConfig, GroupOptions, PartitionRef};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !(11..=12).contains(&args.len()) {
        return Err("usage: consume_q1 topic tenant topic_id partition incarnation group generation max_messages max_bytes wait_ms ack_count [match_offset]".into());
    }
    let mut config = ClientConfig::default();
    if let Ok(addr) = std::env::var("ROVQET_ADDR") {
        config.endpoint = addr.parse()?;
    }
    if let Ok(ms) = std::env::var("ROVQET_REQUEST_TIMEOUT_MS") {
        config.request_timeout = Duration::from_millis(ms.parse()?);
    }
    let mut client = Client::connect(config).await?;
    let generation: u64 = args[6].parse()?;
    let batch = client
        .fetch_q1_group(
            &args[0],
            GroupOptions {
                partition: PartitionRef {
                    tenant_id: args[1].parse()?,
                    topic_id: args[2].parse()?,
                    partition_id: args[3].parse()?,
                },
                incarnation: args[4].parse()?,
                group_id: args[5].parse()?,
                generation: (generation != 0).then_some(generation),
                max_messages: args[7].parse()?,
                max_bytes: args[8].parse()?,
                wait: Duration::from_millis(args[9].parse()?),
            },
        )
        .await?;
    for message in &batch.messages {
        let hex: String = message.payload.iter().map(|b| format!("{b:02x}")).collect();
        println!(
            "DELIVERY {} {} {} {} {} {} {} {}",
            message.token.offset.log_incarnation,
            message.token.offset.sequence,
            message.message_id,
            message.token.generation,
            message.token.lease_id,
            message.token.attempt,
            message.ack_deadline_ms,
            hex
        );
    }
    println!(
        "GROUP_BATCH {} {} {} {} {}",
        batch.group_id,
        batch.generation,
        batch.next.sequence,
        u8::from(batch.has_more),
        u8::from(batch.end_of_current_history)
    );
    let ack_count: usize = args[10].parse()?;
    let matched: Option<u64> = args.get(11).map(|v| v.parse()).transpose()?;
    for message in batch.messages.iter().take(ack_count) {
        if matched.is_some_and(|offset| offset != message.token.offset.sequence) {
            continue;
        }
        match client.ack_q1_group(&args[0], &message.token).await {
            Ok(AckOutcome::Committed { offset, generation }) => println!(
                "ACK_COMMITTED {} {} {}",
                offset.log_incarnation, offset.sequence, generation
            ),
            Err(error) => {
                println!("ACK_ERROR {error:?}");
                return Err(format!("ack rejected: {error:?}").into());
            }
        }
    }
    client.close().await?;
    Ok(())
}
