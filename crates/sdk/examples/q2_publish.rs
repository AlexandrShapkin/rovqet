//! Real Q2 publication over the TCP SDK against a live broker.
//!
//! The acknowledgment is produced by the broker's Q2 route after a Raft quorum
//! commit; nothing in this example fabricates a `PUB_ACK`. One JSON object is
//! printed per attempt so a parent campaign can journal externally observed
//! outcomes.
//!
//! Usage:
//!   q2_publish topic tenant topic_id partition incarnation owner_epoch \
//!              producer_id producer_epoch start_sequence message_id payload count
//!
//! Environment:
//!   ROVQET_ADDR  broker endpoint (default from ClientConfig)
use rovqet_sdk::{
    Client, ClientConfig, ClientError, PartitionRef, PublishOutcome, Q2PublishOptions,
};
use std::time::Duration;

fn main_value(error: &ClientError) -> (String, String) {
    match error {
        ClientError::Remote {
            code: Some(code), ..
        } => ("rejected".to_string(), format!("{code:?}")),
        ClientError::Remote { code: None, .. } => {
            ("unknown".to_string(), "remote-error".to_string())
        }
        ClientError::OutcomeUnknown => ("unknown".to_string(), "OutcomeUnknown".to_string()),
        other => ("unknown".to_string(), other.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 11 {
        return Err(
            "usage: q2_publish topic tenant topic_id partition incarnation \
                    owner_epoch producer_id producer_epoch start_sequence message_id \
                    payload count"
                .into(),
        );
    }
    let mut config = ClientConfig {
        request_timeout: Duration::from_secs(20),
        ..ClientConfig::default()
    };
    if let Ok(endpoint) = std::env::var("ROVQET_ADDR") {
        config.endpoint = endpoint.parse()?;
    }
    let partition = PartitionRef {
        tenant_id: args[1].parse()?,
        topic_id: args[2].parse()?,
        partition_id: args[3].parse()?,
    };
    let incarnation: u128 = args[4].parse()?;
    let ownership_epoch: u64 = args[5].parse()?;
    let producer_id: u128 = args[6].parse()?;
    let producer_epoch: u64 = args[7].parse()?;
    let start_sequence: u64 = args[8].parse()?;
    let start_message_id: u128 = args[9].parse()?;
    let payload = args[10].clone().into_bytes();
    let count: u64 = args[11].parse().unwrap_or(1);
    let topic = args[0].clone();

    let mut client = Client::connect(config).await?;
    for index in 0..count {
        let options = Q2PublishOptions {
            partition,
            incarnation,
            ownership_epoch,
            producer_id,
            producer_epoch,
            sequence: start_sequence + index,
            message_id: start_message_id + index as u128,
            payload: payload.clone(),
            ttl_ms: 30_000,
        };
        let attempt = client.publish_q2(&topic, options).await;
        let line = match attempt {
            Ok(PublishOutcome::CommittedQuorum(offset)) => format!(
                "{{\"outcome\":\"committed_quorum\",\"sequence\":{},\"offset\":{},\
                  \"incarnation\":{}}}",
                start_sequence + index,
                offset.sequence,
                offset.log_incarnation,
            ),
            Ok(other) => format!(
                "{{\"outcome\":\"unexpected\",\"detail\":\"{}\"}}",
                match other {
                    PublishOutcome::CommittedLocal(_) => "committed_local",
                    PublishOutcome::Admitted => "admitted",
                    _ => "other",
                }
            ),
            Err(error) => {
                let (outcome, detail) = main_value(&error);
                format!(
                    "{{\"outcome\":\"{}\",\"detail\":\"{}\",\"sequence\":{}}}",
                    outcome,
                    detail,
                    start_sequence + index,
                )
            }
        };
        println!("{line}");
    }
    Ok(())
}
