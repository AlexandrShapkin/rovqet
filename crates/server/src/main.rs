//! Isolated development broker: plaintext and static identity on loopback only.
#![forbid(unsafe_code)]
use rovqet_transport::{Config, serve};
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--version"] {
        println!(
            "rovqet {} (wire v1; development profile)",
            env!("CARGO_PKG_VERSION")
        );
        return Ok(());
    }
    if args == ["--help"] || args == ["-h"] {
        println!(
            "Usage: rovqet [--listen 127.0.0.1:7443]\nDevelopment only: plaintext, static dev identity, loopback required.\nQ0 topics: demo, telemetry."
        );
        return Ok(());
    }
    let addr = match args.as_slice() {
        [] => "127.0.0.1:7443",
        [flag, addr] if flag == "--listen" => addr,
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "expected --listen <LOOPBACK:PORT>, --help or --version",
            ));
        }
    };
    let addr: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    if !addr.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "development listener must be loopback",
        ));
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!(
        "rovqet development profile listening on {}",
        listener.local_addr()?
    );
    let broker = rovqet_q0::Broker::development();
    let (stop, mut rx) = tokio::sync::watch::channel(false);
    let dispatcher = tokio::spawn(broker.clone().run(async move {
        let _ = rx.changed().await;
    }));
    let drain = broker.clone();
    let result = serve(listener, Config::default(), broker, async move {
        let _ = tokio::signal::ctrl_c().await;
        drain.drain();
    })
    .await;
    let _ = stop.send(true);
    let _ = dispatcher.await;
    result
}
