//! End-to-end proofs through actual loopback sockets, including rejection paths.
use rovqet_protocol::ErrorCode;
use rovqet_q0::Broker;
use rovqet_sdk::{Client, ClientConfig, ClientError, PublishOptions, PublishOutcome, QoS};
use rovqet_transport::{Config, serve};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle, time::timeout};

struct Running {
    endpoint: SocketAddr,
    stop: watch::Sender<bool>,
    server: JoinHandle<std::io::Result<()>>,
    worker: JoinHandle<()>,
    broker: Arc<Broker>,
}
impl Running {
    async fn start() -> Self {
        let broker = Broker::development();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let (stop, mut server_stop) = watch::channel(false);
        let mut worker_stop = stop.subscribe();
        let server = tokio::spawn(serve(
            listener,
            Config::default(),
            broker.clone(),
            async move {
                let _ = server_stop.changed().await;
            },
        ));
        let worker = tokio::spawn(broker.clone().run(async move {
            let _ = worker_stop.changed().await;
        }));
        Self {
            endpoint,
            stop,
            server,
            worker,
            broker,
        }
    }
    async fn client(&self) -> Client {
        Client::connect(ClientConfig {
            endpoint: self.endpoint,
            request_timeout: Duration::from_secs(2),
            ..ClientConfig::default()
        })
        .await
        .unwrap()
    }
    async fn finish(self) {
        self.stop.send(true).unwrap();
        timeout(Duration::from_secs(7), self.server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), self.worker)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn socket_fanout_exact_matching_unsubscribe_and_reconnect() {
    let running = Running::start().await;
    let mut publisher = running.client().await;
    let mut first = running.client().await;
    let mut second = running.client().await;
    let first_id = first.subscribe("demo").await.unwrap();
    let second_id = second.subscribe("demo").await.unwrap();
    assert_ne!(first_id, second_id);
    assert_eq!(
        publisher
            .publish("demo", b"one", PublishOptions::default())
            .await
            .unwrap(),
        PublishOutcome::Admitted
    );
    assert_eq!(first.receive().await.unwrap().payload, b"one");
    assert_eq!(second.receive().await.unwrap().payload, b"one");

    // A subscription belongs to its authenticated socket session.
    assert!(matches!(
        first.unsubscribe(second_id).await,
        Err(ClientError::Remote { .. })
    ));
    second.unsubscribe(second_id).await.unwrap();
    second.subscribe("telemetry").await.unwrap();
    publisher
        .publish("demo", b"only-first", PublishOptions::default())
        .await
        .unwrap();
    publisher
        .publish("telemetry", b"barrier", PublishOptions::default())
        .await
        .unwrap();
    assert_eq!(first.receive().await.unwrap().payload, b"only-first");
    let delivery = second.receive().await.unwrap();
    assert_eq!(delivery.topic, "telemetry");
    assert_eq!(delivery.payload, b"barrier");

    let mappings = first.reconnect().await.unwrap();
    assert_eq!(mappings.len(), 1);
    assert_eq!(mappings[0].0, first_id);
    assert_ne!(mappings[0].1, first_id);
    publisher
        .publish("demo", b"after-reconnect", PublishOptions::default())
        .await
        .unwrap();
    assert_eq!(first.receive().await.unwrap().payload, b"after-reconnect");
    assert_eq!(
        running
            .broker
            .counters
            .admitted
            .load(std::sync::atomic::Ordering::Relaxed),
        4
    );
    publisher.close().await.unwrap();
    first.close().await.unwrap();
    second.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn socket_policy_errors_are_typed_and_do_not_break_session() {
    let running = Running::start().await;
    let mut publisher = running.client().await;
    let mut subscriber = running.client().await;
    assert!(matches!(
        subscriber.subscribe("missing").await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::PermissionDenied),
            ..
        })
    ));
    assert!(matches!(
        publisher
            .publish(
                "demo",
                b"q1",
                PublishOptions {
                    qos: QoS::Q1,
                    ..PublishOptions::default()
                }
            )
            .await,
        Err(ClientError::UnsupportedGuarantee)
    ));
    assert!(matches!(
        publisher
            .publish(
                "demo",
                b"expired",
                PublishOptions {
                    ttl: Duration::ZERO,
                    ..PublishOptions::default()
                }
            )
            .await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::Expired),
            ..
        })
    ));
    subscriber.subscribe("demo").await.unwrap();
    assert_eq!(
        publisher
            .publish("demo", b"valid", PublishOptions::default())
            .await
            .unwrap(),
        PublishOutcome::Admitted
    );
    assert_eq!(subscriber.receive().await.unwrap().payload, b"valid");
    assert_eq!(
        running
            .broker
            .counters
            .admitted
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    publisher.close().await.unwrap();
    subscriber.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn socket_one_to_thousand_fanout() {
    let running = Running::start().await;
    let mut publisher = running.client().await;
    let mut subscribers = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let mut client = running.client().await;
        client.subscribe("demo").await.unwrap();
        subscribers.push(client);
    }
    assert_eq!(
        publisher
            .publish("demo", b"fanout-1000", PublishOptions::default())
            .await
            .unwrap(),
        PublishOutcome::Admitted
    );
    for client in &mut subscribers {
        assert_eq!(client.receive().await.unwrap().payload, b"fanout-1000");
    }
    assert_eq!(
        running
            .broker
            .counters
            .enqueued
            .load(std::sync::atomic::Ordering::Relaxed),
        1000
    );
    for client in &mut subscribers {
        client.close().await.unwrap();
    }
    publisher.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn socket_ingress_full_rejects_then_queued_ttl_expires() {
    use rovqet_router::{OverflowPolicy, TopicConfig};
    use rovqet_sdk::Priority;
    use std::sync::atomic::Ordering;
    let broker = Broker::new(
        vec![TopicConfig {
            tenant_id: 1,
            topic_id: 1,
            name: "demo".into(),
            partitions: 1,
            allowed_qos: vec![QoS::Q0],
            priority: Priority::Telemetry,
            max_ttl_ms: 30_000,
            max_subscriptions: 8,
            overflow: OverflowPolicy::RejectPublish,
            max_ingress_bytes: 1024 * 1024,
        }],
        8,
        2,
        1024 * 1024,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let (stop, mut server_stop) = watch::channel(false);
    let server = tokio::spawn(serve(
        listener,
        Config::default(),
        broker.clone(),
        async move {
            let _ = server_stop.changed().await;
        },
    ));
    let config = ClientConfig {
        endpoint,
        request_timeout: Duration::from_secs(2),
        ..ClientConfig::default()
    };
    let mut subscriber = Client::connect(config.clone()).await.unwrap();
    let mut publisher = Client::connect(config).await.unwrap();
    subscriber.subscribe("demo").await.unwrap();
    // Dispatch is deliberately withheld: admission is independent of delivery.
    assert_eq!(
        publisher
            .publish(
                "demo",
                b"will-expire",
                PublishOptions {
                    ttl: Duration::from_millis(1),
                    ..PublishOptions::default()
                }
            )
            .await
            .unwrap(),
        PublishOutcome::Admitted
    );
    assert!(matches!(
        publisher
            .publish("demo", b"no-slot", PublishOptions::default())
            .await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::ResourceExhausted),
            ..
        })
    ));
    assert_eq!(broker.counters.admitted.load(Ordering::Relaxed), 1);
    assert_eq!(broker.counters.enqueued.load(Ordering::Relaxed), 0);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mut worker_stop = stop.subscribe();
    let worker = tokio::spawn(broker.clone().run(async move {
        let _ = worker_stop.changed().await;
    }));
    timeout(Duration::from_secs(2), async {
        while broker.counters.dropped_expired.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(broker.counters.enqueued.load(Ordering::Relaxed), 0);
    publisher
        .publish("demo", b"fresh", PublishOptions::default())
        .await
        .unwrap();
    assert_eq!(subscriber.receive().await.unwrap().payload, b"fresh");
    publisher.close().await.unwrap();
    subscriber.close().await.unwrap();
    stop.send(true).unwrap();
    timeout(Duration::from_secs(7), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn socket_stalled_subscriber_sheds_without_blocking_healthy_subscriber() {
    use std::sync::atomic::Ordering;
    let running = Running::start().await;
    let mut publisher = running.client().await;
    let mut stalled = running.client().await;
    let mut healthy = running.client().await;
    stalled.subscribe("demo").await.unwrap();
    healthy.subscribe("demo").await.unwrap();
    let receiver = tokio::spawn(async move {
        loop {
            let delivery = healthy.receive().await.unwrap();
            if delivery.payload == b"healthy-barrier" {
                break;
            }
        }
        healthy.close().await.unwrap();
    });
    let payload = vec![42u8; 64 * 1024];
    for _ in 0..256 {
        assert_eq!(
            publisher
                .publish("demo", &payload, PublishOptions::default())
                .await
                .unwrap(),
            PublishOutcome::Admitted
        );
    }
    assert!(
        running
            .broker
            .counters
            .dropped_overflow
            .load(Ordering::Relaxed)
            > 0,
        "stalled socket must eventually fill its bounded outbound budget"
    );
    publisher
        .publish("demo", b"healthy-barrier", PublishOptions::default())
        .await
        .unwrap();
    timeout(Duration::from_secs(4), receiver)
        .await
        .unwrap()
        .unwrap();
    publisher.close().await.unwrap();
    // Close by dropping the unread socket; no client receive is required to free it.
    drop(stalled);
    running.finish().await;
}
