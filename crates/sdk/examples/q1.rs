//! Explicit-token Q1 publisher for the isolated loopback prototype.
//! Usage: q1 topic producer_id producer_epoch sequence message_id partition_id incarnation owner_epoch payload [ttl_ms]
use rovqet_sdk::{Client, ClientConfig, PublishOutcome, Q1PublishOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !(9..=10).contains(&args.len()) {
        return Err("usage: q1 topic producer_id producer_epoch sequence message_id partition_id incarnation owner_epoch payload [ttl_ms]".into());
    }
    let options = Q1PublishOptions {
        producer_id: args[1].parse()?,
        producer_epoch: args[2].parse()?,
        sequence: args[3].parse()?,
        message_id: args[4].parse()?,
        partition_id: args[5].parse()?,
        log_incarnation: args[6].parse()?,
        ownership_epoch: args[7].parse()?,
        ttl: Duration::from_millis(
            args.get(9)
                .map(|s| s.parse())
                .transpose()?
                .unwrap_or(30_000),
        ),
    };
    let mut config = ClientConfig {
        request_timeout: Duration::from_secs(60),
        ..ClientConfig::default()
    };
    if let Ok(endpoint) = std::env::var("ROVQET_ADDR") {
        config.endpoint = endpoint.parse()?;
    }
    if let Ok(ms) = std::env::var("ROVQET_REQUEST_TIMEOUT_MS") {
        config.request_timeout = Duration::from_millis(ms.parse()?);
    }
    let mut client = Client::connect(config).await?;
    match client
        .publish_q1(&args[0], args[8].as_bytes(), options)
        .await?
    {
        PublishOutcome::CommittedLocal(offset) => {
            println!(
                "COMMITTED_LOCAL {} {}",
                offset.log_incarnation, offset.sequence
            );
        }
        _ => return Err("unexpected Q1 outcome".into()),
    }
    // A close failure cannot undo the already validated durable acknowledgment.
    let _ = client.close().await;
    Ok(())
}
