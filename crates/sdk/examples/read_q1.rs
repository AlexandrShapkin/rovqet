//! Standalone read-only inspection; prints records only after a validated READ_END.
use rovqet_sdk::{Client, ClientConfig, DurableOffset, PartitionRef, ReadOptions};
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 9 {
        return Err("usage: read_q1 topic tenant topic_id partition incarnation offset max_messages max_bytes wait_ms".into());
    }
    let mut config = ClientConfig::default();
    if let Ok(addr) = std::env::var("ROVQET_ADDR") {
        config.endpoint = addr.parse()?;
    }
    if let Ok(ms) = std::env::var("ROVQET_REQUEST_TIMEOUT_MS") {
        config.request_timeout = Duration::from_millis(ms.parse()?);
    }
    let mut client = Client::connect(config).await?;
    let result = client
        .read_q1(
            &args[0],
            ReadOptions {
                partition: PartitionRef {
                    tenant_id: args[1].parse()?,
                    topic_id: args[2].parse()?,
                    partition_id: args[3].parse()?,
                },
                from: DurableOffset {
                    log_incarnation: args[4].parse()?,
                    sequence: args[5].parse()?,
                },
                max_messages: args[6].parse()?,
                max_bytes: args[7].parse()?,
                wait: Duration::from_millis(args[8].parse()?),
            },
        )
        .await?;
    for record in result.records {
        let hex: String = record.payload.iter().map(|b| format!("{b:02x}")).collect();
        println!(
            "READ_RECORD {} {} {} {}",
            record.offset.log_incarnation, record.offset.sequence, record.message_id, hex
        );
    }
    println!(
        "READ_END {} {} {} {} {}",
        result.next.log_incarnation,
        result.next.sequence,
        u8::from(result.has_more),
        result.status as u8,
        result.encoded_bytes
    );
    client.close().await?;
    Ok(())
}
