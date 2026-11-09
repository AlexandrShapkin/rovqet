//! Run independently:
//! cargo run -p rovqet-sdk --example q0 -- sub demo
//! cargo run -p rovqet-sdk --example q0 -- pub demo hello
use rovqet_sdk::{Client, ClientConfig, PublishOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("sub");
    let topic = args.get(1).map(String::as_str).unwrap_or("demo");
    let mut config = ClientConfig::default();
    // Requests have a one-minute deadline; receive waits with heartbeats.
    config.request_timeout = std::time::Duration::from_secs(60);
    if let Ok(endpoint) = std::env::var("ROVQET_ADDR") {
        config.endpoint = endpoint.parse()?;
    }
    let mut client = Client::connect(config).await?;
    match mode {
        "pub" => {
            let body = args.get(2).map(String::as_str).unwrap_or("hello");
            println!(
                "{:?}",
                client
                    .publish(topic, body.as_bytes(), PublishOptions::default())
                    .await?
            );
        }
        "sub" => {
            let id = client.subscribe(topic).await?;
            println!("SUBSCRIBED {id}");
            use std::io::Write;
            std::io::stdout().flush()?;
            let message = client.receive().await?;
            println!(
                "{}: {}",
                message.topic,
                String::from_utf8_lossy(&message.payload)
            );
            client.unsubscribe(id).await?;
        }
        _ => return Err("expected sub or pub".into()),
    }
    client.close().await?;
    Ok(())
}
