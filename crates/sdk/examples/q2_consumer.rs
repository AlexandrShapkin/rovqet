//! External consumer for the parent-owned C-05 journal. No synthetic SDK results.
use rovqet_sdk::{
    AckOutcome, Client, ClientConfig, DeliveryToken, DurableOffset, GroupMessage, GroupOptions,
    PartitionRef,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let mut client = Client::connect(ClientConfig {
        endpoint: std::env::var("ROVQET_ADDR")?.parse()?,
        request_timeout: Duration::from_secs(20),
        ..Default::default()
    })
    .await?;
    let partition = PartitionRef {
        tenant_id: 1,
        topic_id: 4,
        partition_id: 0,
    };
    match a.first().map(String::as_str) {
        Some("fetch") if a.len() == 3 => {
            let generation: u64 = a[2].parse()?;
            match client.fetch_q2_group("durable-q2", GroupOptions {
                partition, incarnation: 42, group_id: a[1].parse()?,
                generation: (generation != 0).then_some(generation),
                max_messages: 128, max_bytes: 4 * 1024 * 1024, wait: Duration::ZERO,
            }).await {
                Ok(batch) => {
                    let messages: Vec<String> = batch.messages.iter().map(|m| {
                        let t = &m.token;
                        let payload: String = m.payload.iter().map(|b| format!("{b:02x}")).collect();
                        format!("{{\"tenant_id\":1,\"topic_id\":4,\"partition_id\":0,\"incarnation\":{},\"group_id\":{},\"generation\":{},\"lease_id\":{},\"metadata_epoch\":{},\"offset\":{},\"attempt\":{},\"principal\":1,\"message_id\":{},\"payload\":\"{}\"}}",
                            t.offset.log_incarnation, t.group_id, t.generation, t.lease_id,
                            t.ownership_epoch, t.offset.sequence, t.attempt, m.message_id, payload)
                    }).collect();
                    println!("{{\"outcome\":\"batch\",\"generation\":{},\"next\":{},\"has_more\":{},\"messages\":[{}]}}",
                        batch.generation, batch.next.sequence, batch.has_more, messages.join(","));
                }
                Err(error) => println!("{{\"outcome\":\"error\",\"detail\":{:?}}}", format!("{error:?}")),
            }
        }
        Some("ack") if a.len() == 10 => {
            let message = GroupMessage {
                token: DeliveryToken { partition, group_id: a[1].parse()?,
                    generation: a[2].parse()?, lease_id: a[3].parse()?,
                    ownership_epoch: a[4].parse()?,
                    offset: DurableOffset { log_incarnation: a[5].parse()?, sequence: a[6].parse()? },
                    attempt: a[7].parse()? },
                message_id: a[8].parse()?, payload: vec![], ack_deadline_ms: 30_000,
            };
            // Final argument is a parent-owned attempt label, never an SDK token.
            match client.ack_q2_group("durable-q2", &message).await {
                Ok(AckOutcome::Committed { offset, generation }) => println!(
                    "{{\"outcome\":\"ack_committed\",\"offset\":{},\"generation\":{}}}", offset.sequence, generation),
                Err(error) => println!("{{\"outcome\":\"error\",\"detail\":{:?}}}", format!("{error:?}")),
            }
        }
        _ => return Err("usage: q2_consumer fetch group generation | ack group generation lease epoch incarnation offset attempt message_id label".into()),
    }
    Ok(())
}
