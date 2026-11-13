//! External durable consumer used for NACK/retry/terminal tests.
//!
//! Usage: nack_q1 topic tenant topic_id partition incarnation group generation
//!        max_messages max_bytes wait action [reason]
//!
//! action: nack | ack | nack-first | ack-first
use rovqet_sdk::{AckOutcome, Client, ClientConfig, GroupOptions, NackOutcome, PartitionRef};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !(11..=12).contains(&args.len()) {
        return Err("usage: nack_q1 topic tenant topic_id partition incarnation group generation max_messages max_bytes wait action [reason]".into());
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
    let action = args[10].as_str();
    let reason: u8 = args
        .get(11)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(1);
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
    for (index, message) in batch.messages.iter().enumerate() {
        let nack = match action {
            "nack" => true,
            "ack" => false,
            "nack-first" => index == 0,
            "ack-first" => index != 0,
            "none" => continue,
            _ => return Err("unknown action".into()),
        };
        if nack {
            match client.nack_q1_group(&args[0], &message.token, reason).await {
                Ok(outcome) => {
                    let (code, delay, disposition) = match outcome {
                        NackOutcome::RetryScheduled { retry_after } => {
                            (1u8, retry_after.as_millis() as u64, 0u128)
                        }
                        NackOutcome::RetryExhausted => (2, 0, 0),
                        NackOutcome::TerminalCommitted { disposition_id } => (3, 0, disposition_id),
                        NackOutcome::TerminalPending { disposition_id } => (4, 0, disposition_id),
                        NackOutcome::AlreadyClosed { disposition_id } => (5, 0, disposition_id),
                    };
                    println!(
                        "NACK_RESULT {} {} {} {} {}",
                        message.token.offset.sequence,
                        code,
                        delay,
                        disposition,
                        message.token.generation
                    );
                }
                Err(error) => println!("NACK_ERROR {error:?}"),
            }
        } else {
            match client.ack_q1_group(&args[0], &message.token).await {
                Ok(AckOutcome::Committed { .. }) => println!(
                    "ACK_COMMITTED {} {} {}",
                    message.token.offset.log_incarnation,
                    message.token.offset.sequence,
                    message.token.generation
                ),
                Err(error) => println!("ACK_ERROR {error:?}"),
            }
        }
    }
    let _ = client.close().await;
    Ok(())
}
