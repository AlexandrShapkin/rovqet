//! Real socket proofs for bounded credits and the four Q0 overflow choices.
use rovqet_core::{Priority, QoS};
use rovqet_protocol::{ErrorCode, Field, Frame, FrameType, Limits, registry::field as f};
use rovqet_q0::{Broker, RateLimit, ResourceConfig};
use rovqet_router::{OverflowPolicy, TopicConfig};
use rovqet_scheduler::OverloadState;
use rovqet_sdk::{Client, ClientConfig, ClientError, PublishOptions, PublishOutcome};
use rovqet_transport::{Config, TransportStats, read_frame, serve, write_frame};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};

struct Running {
    endpoint: SocketAddr,
    broker: Arc<Broker>,
    stats: Arc<TransportStats>,
    stop: watch::Sender<bool>,
    server: JoinHandle<std::io::Result<()>>,
    worker: JoinHandle<()>,
}
impl Running {
    async fn start(policy: OverflowPolicy, outbound_messages: usize) -> Self {
        Self::with_resources(policy, outbound_messages, ResourceConfig::default()).await
    }
    async fn with_resources(
        policy: OverflowPolicy,
        outbound_messages: usize,
        resources: ResourceConfig,
    ) -> Self {
        let demo = TopicConfig {
            tenant_id: 1,
            topic_id: 1,
            name: "demo".into(),
            partitions: 1,
            allowed_qos: vec![QoS::Q0],
            priority: Priority::Telemetry,
            max_ttl_ms: 30_000,
            max_subscriptions: 16,
            overflow: policy,
            max_ingress_bytes: 8 * 1024 * 1024,
        };
        let other = TopicConfig {
            topic_id: 2,
            name: "telemetry".into(),
            ..demo.clone()
        };
        let broker =
            Broker::with_resources(vec![demo, other], 16, 32, 8 * 1024 * 1024, resources).unwrap();
        let config = Config {
            outbound_messages,
            ..Config::default()
        };
        let stats = config.stats.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let (stop, mut server_stop) = watch::channel(false);
        let mut worker_stop = stop.subscribe();
        let server = tokio::spawn(serve(listener, config, broker.clone(), async move {
            let _ = server_stop.changed().await;
        }));
        let worker = tokio::spawn(broker.clone().run(async move {
            let _ = worker_stop.changed().await;
        }));
        Self {
            endpoint,
            broker,
            stats,
            stop,
            server,
            worker,
        }
    }
    async fn client(&self) -> Client {
        Client::connect(ClientConfig {
            endpoint: self.endpoint,
            request_timeout: Duration::from_secs(2),
            auto_replenish: false,
            ..ClientConfig::default()
        })
        .await
        .unwrap()
    }
    async fn settled(&self) {
        timeout(Duration::from_secs(2), async {
            while self.broker.ingress_usage().messages != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
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
        assert_eq!(self.broker.ingress_usage().messages, 0);
        assert_eq!(self.broker.ingress_usage().bytes, 0);
        assert_eq!(self.broker.egress_usage().messages, 0);
        assert_eq!(self.broker.egress_usage().bytes, 0);
        assert_eq!(self.broker.active_subscriptions(), 0);
        assert_eq!(self.broker.active_sessions(), 0);
    }
}
fn options(key: &[u8]) -> PublishOptions {
    PublishOptions {
        key: key.to_vec(),
        ..PublishOptions::default()
    }
}
async fn publish(running: &Running, client: &mut Client, payload: &[u8], key: &[u8]) {
    assert_eq!(
        client.publish("demo", payload, options(key)).await.unwrap(),
        PublishOutcome::Admitted
    );
    running.settled().await;
}
async fn receive(client: &mut Client) -> Vec<u8> {
    timeout(Duration::from_secs(2), client.receive())
        .await
        .unwrap()
        .unwrap()
        .payload
}

#[tokio::test]
async fn zero_credit_queue_applies_each_shedding_policy_without_silent_fallback() {
    for policy in [
        OverflowPolicy::DropNewest,
        OverflowPolicy::DropOldest,
        OverflowPolicy::CoalesceLatest,
    ] {
        let running = Running::start(policy, 2).await;
        let mut subscriber = running.client().await;
        let id = subscriber
            .subscribe_with_credit("demo", 0, 0)
            .await
            .unwrap();
        let mut publisher = running.client().await;
        publish(&running, &mut publisher, b"old-a", b"a").await;
        publish(&running, &mut publisher, b"other-b", b"b").await;
        publish(&running, &mut publisher, b"new-a", b"a").await;
        assert_eq!(running.stats.delivered.load(Ordering::Relaxed), 0);
        subscriber.add_credit(id, 2, 16 * 1024).await.unwrap();
        let mut received = vec![
            receive(&mut subscriber).await,
            receive(&mut subscriber).await,
        ];
        received.sort();
        let mut expected = match policy {
            OverflowPolicy::DropNewest => vec![b"old-a".to_vec(), b"other-b".to_vec()],
            _ => vec![b"new-a".to_vec(), b"other-b".to_vec()],
        };
        expected.sort();
        assert_eq!(received, expected, "{policy:?}");
        match policy {
            OverflowPolicy::DropNewest => {
                assert_eq!(running.stats.dropped_newest.load(Ordering::Relaxed), 1)
            }
            OverflowPolicy::DropOldest => {
                assert_eq!(running.stats.dropped_oldest.load(Ordering::Relaxed), 1)
            }
            OverflowPolicy::CoalesceLatest => {
                assert_eq!(running.stats.coalesced.load(Ordering::Relaxed), 1)
            }
            _ => unreachable!(),
        }
        assert_eq!(
            running.stats.delivered.load(Ordering::Relaxed)
                + running.stats.dropped_newest.load(Ordering::Relaxed)
                + running.stats.dropped_oldest.load(Ordering::Relaxed)
                + running.stats.coalesced.load(Ordering::Relaxed),
            3,
            "every physical copy is delivered or policy-dropped"
        );
        subscriber.unsubscribe(id).await.unwrap();
        subscriber.close().await.unwrap();
        publisher.close().await.unwrap();
        running.finish().await;
    }
}

#[tokio::test]
async fn reject_publish_reserves_outbound_before_admitted_and_releases_after_delivery() {
    let running = Running::start(OverflowPolicy::RejectPublish, 2).await;
    let mut subscriber = running.client().await;
    let id = subscriber
        .subscribe_with_credit("demo", 0, 0)
        .await
        .unwrap();
    let mut publisher = running.client().await;
    publish(&running, &mut publisher, b"first", b"a").await;
    publish(&running, &mut publisher, b"second", b"b").await;
    assert!(matches!(
        publisher.publish("demo", b"rejected", options(b"c")).await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::ResourceExhausted),
            ..
        })
    ));
    assert_eq!(running.broker.counters.admitted.load(Ordering::Relaxed), 2);
    subscriber.add_credit(id, 2, 16 * 1024).await.unwrap();
    assert_eq!(receive(&mut subscriber).await, b"first");
    assert_eq!(receive(&mut subscriber).await, b"second");
    publish(&running, &mut publisher, b"fresh", b"c").await;
    subscriber.add_credit(id, 1, 1024).await.unwrap();
    assert_eq!(receive(&mut subscriber).await, b"fresh");
    subscriber.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn credits_exhaust_replenish_and_expired_queued_delivery_does_not_consume_credit() {
    let running = Running::start(OverflowPolicy::DropNewest, 4).await;
    let mut subscriber = running.client().await;
    let id = subscriber
        .subscribe_with_credit("demo", 1, 4096)
        .await
        .unwrap();
    let mut publisher = running.client().await;
    publish(&running, &mut publisher, b"first", b"a").await;
    publish(&running, &mut publisher, b"second", b"b").await;
    assert_eq!(receive(&mut subscriber).await, b"first");
    assert_eq!(running.stats.delivered.load(Ordering::Relaxed), 1);
    subscriber.add_credit(id, 1, 0).await.unwrap();
    assert_eq!(receive(&mut subscriber).await, b"second");
    publisher
        .publish(
            "demo",
            b"expired",
            PublishOptions {
                ttl: Duration::from_millis(20),
                ..options(b"expired")
            },
        )
        .await
        .unwrap();
    running.settled().await;
    timeout(Duration::from_secs(2), async {
        while running.stats.writer_expired.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    subscriber.add_credit(id, 1, 0).await.unwrap();
    publish(&running, &mut publisher, b"after-expiry", b"fresh").await;
    assert_eq!(receive(&mut subscriber).await, b"after-expiry");
    assert_eq!(running.stats.delivered.load(Ordering::Relaxed), 3);
    subscriber.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}

async fn raw_exchange(stream: &mut TcpStream, frame: Frame) -> Frame {
    write_frame(stream, &frame, Limits::default())
        .await
        .unwrap();
    timeout(
        Duration::from_secs(2),
        read_frame(stream, Limits::default()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}
async fn raw_client(endpoint: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(endpoint).await.unwrap();
    let mut hello = Frame::new(FrameType::Hello, 1);
    hello.fields = vec![
        Field::required(f::PROTOCOL_MINOR, 0u16.to_be_bytes()),
        Field::required(f::MAX_FRAME_BYTES, 1_048_576u32.to_be_bytes()),
    ];
    assert_eq!(
        raw_exchange(&mut stream, hello).await.frame_type,
        FrameType::Welcome
    );
    let mut auth = Frame::new(FrameType::Auth, 2);
    auth.fields = vec![
        Field::required(f::AUTH_METHOD, [0]),
        Field::required(f::AUTH_DATA, b"dev".to_vec()),
    ];
    assert_eq!(
        raw_exchange(&mut stream, auth).await.frame_type,
        FrameType::AuthOk
    );
    stream
}
fn flow(id: u64, messages: u32, bytes: u32) -> Frame {
    let mut frame = Frame::new(FrameType::FlowCredit, 3);
    frame.fields = vec![
        Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()),
        Field::required(f::FLOW_CREDIT_MESSAGES, messages.to_be_bytes()),
        Field::required(f::FLOW_CREDIT_BYTES, bytes.to_be_bytes()),
    ];
    frame
}
fn error_code(frame: &Frame) -> ErrorCode {
    assert_eq!(frame.frame_type, FrameType::Error);
    ErrorCode::try_from(u16::from_be_bytes(
        frame.field(f::ERROR_CODE).unwrap().try_into().unwrap(),
    ))
    .unwrap()
}

#[tokio::test]
async fn wire_credit_ownership_zero_delta_and_overflow_are_explicit_errors() {
    let running = Running::start(OverflowPolicy::DropNewest, 4).await;
    let mut owner = raw_client(running.endpoint).await;
    let mut subscribe = Frame::new(FrameType::Subscribe, 3);
    subscribe.fields = vec![
        Field::required(f::TOPIC_NAME, b"demo".to_vec()),
        Field::optional(f::FLOW_CREDIT_MESSAGES, 128u32.to_be_bytes()),
        Field::optional(f::FLOW_CREDIT_BYTES, 1_048_576u32.to_be_bytes()),
    ];
    let ack = raw_exchange(&mut owner, subscribe).await;
    assert_eq!(ack.frame_type, FrameType::SubAck);
    let id = u64::from_be_bytes(ack.field(f::SUBSCRIPTION_ID).unwrap().try_into().unwrap());
    assert_eq!(
        error_code(&raw_exchange(&mut owner, flow(id, 0, 0)).await),
        ErrorCode::BadFrame
    );
    assert_eq!(
        error_code(&raw_exchange(&mut owner, flow(id, 1, 0)).await),
        ErrorCode::QuotaExceeded
    );
    let mut other = raw_client(running.endpoint).await;
    assert_eq!(
        error_code(&raw_exchange(&mut other, flow(id, 1, 1)).await),
        ErrorCode::PermissionDenied
    );
    let mut unsubscribe = Frame::new(FrameType::Unsubscribe, 4);
    unsubscribe
        .fields
        .push(Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()));
    assert_eq!(
        raw_exchange(&mut owner, unsubscribe).await.frame_type,
        FrameType::SubAck
    );
    assert_eq!(
        error_code(&raw_exchange(&mut owner, flow(id, 1, 1)).await),
        ErrorCode::PermissionDenied
    );
    drop(owner);
    drop(other);
    running.finish().await;
}

#[tokio::test]
async fn disconnect_and_reconnect_discard_old_zero_credit_subscriptions() {
    let running = Running::start(OverflowPolicy::RejectPublish, 2).await;
    let mut subscriber = running.client().await;
    let mut id = subscriber
        .subscribe_with_credit("demo", 0, 0)
        .await
        .unwrap();
    let mut publisher = running.client().await;
    for _ in 0..4 {
        publish(&running, &mut publisher, b"stale", b"stale").await;
        let mappings = subscriber.reconnect().await.unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].0, id);
        assert_ne!(mappings[0].1, id);
        id = mappings[0].1;
    }
    subscriber.add_credit(id, 1, 4096).await.unwrap();
    publish(&running, &mut publisher, b"barrier", b"fresh").await;
    assert_eq!(receive(&mut subscriber).await, b"barrier");
    subscriber.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn sdk_automatically_replenishes_a_single_message_window() {
    let running = Running::start(OverflowPolicy::DropNewest, 4).await;
    let mut subscriber = Client::connect(ClientConfig {
        endpoint: running.endpoint,
        request_timeout: Duration::from_secs(2),
        delivery_credit_messages: 1,
        delivery_credit_bytes: 1024,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    let id = subscriber.subscribe("demo").await.unwrap();
    let mut publisher = running.client().await;
    for sequence in 0..8u8 {
        publish(&running, &mut publisher, &[sequence], b"sequence").await;
        let delivery = timeout(Duration::from_secs(2), subscriber.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.subscription_id, Some(id));
        assert_eq!(delivery.payload, [sequence]);
        assert!(delivery.wire_bytes > delivery.payload.len() as u32);
    }
    assert_eq!(running.stats.delivered.load(Ordering::Relaxed), 8);
    subscriber.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn byte_credit_is_independent_of_message_credit() {
    let running = Running::start(OverflowPolicy::DropNewest, 2).await;
    let mut subscriber = running.client().await;
    let id = subscriber
        .subscribe_with_credit("demo", 1, 1)
        .await
        .unwrap();
    let mut publisher = running.client().await;
    publish(&running, &mut publisher, b"needs-wire-bytes", b"bytes").await;
    assert_eq!(running.stats.delivered.load(Ordering::Relaxed), 0);
    subscriber.add_credit(id, 0, 1024).await.unwrap();
    assert_eq!(receive(&mut subscriber).await, b"needs-wire-bytes");
    assert_eq!(running.stats.delivered.load(Ordering::Relaxed), 1);
    subscriber.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn physical_fanout_is_charged_to_every_egress_domain_and_recovers() {
    for scope in 0..3 {
        let mut resources = ResourceConfig::default();
        let limit = RateLimit {
            bytes_per_sec: 1024 * 1024,
            deliveries_per_sec: 1,
        };
        match scope {
            0 => resources.global_egress = limit,
            1 => resources.tenant_egress = limit,
            _ => resources.topic_egress = limit,
        };
        let running = Running::with_resources(OverflowPolicy::DropNewest, 4, resources).await;
        let mut first = running.client().await;
        first.subscribe("demo").await.unwrap();
        let mut second = running.client().await;
        let second_id = second.subscribe("demo").await.unwrap();
        let mut publisher = running.client().await;
        assert!(
            matches!(
                publisher
                    .publish(
                        "demo",
                        b"two-physical-recipients",
                        PublishOptions::default()
                    )
                    .await,
                Err(ClientError::Remote {
                    code: Some(ErrorCode::EgressQuota),
                    ..
                })
            ),
            "scope {scope}"
        );
        assert_eq!(running.broker.counters.admitted.load(Ordering::Relaxed), 0);
        second.unsubscribe(second_id).await.unwrap();
        // Failed reservation must not charge an ancestor, so one recipient now fits.
        publish(&running, &mut publisher, b"one-recipient", b"first").await;
        assert_eq!(receive(&mut first).await, b"one-recipient");
        assert!(matches!(
            publisher
                .publish("demo", b"quota-exhausted", PublishOptions::default())
                .await,
            Err(ClientError::Remote {
                code: Some(ErrorCode::EgressQuota),
                ..
            })
        ));
        timeout(Duration::from_secs(3), async {
            loop {
                match publisher
                    .publish("demo", b"after-refill", PublishOptions::default())
                    .await
                {
                    Ok(PublishOutcome::Admitted) => break,
                    Err(ClientError::Remote {
                        code: Some(ErrorCode::EgressQuota),
                        ..
                    }) => tokio::time::sleep(Duration::from_millis(10)).await,
                    result => panic!("unexpected refill outcome: {result:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(receive(&mut first).await, b"after-refill");
        assert!(
            running
                .broker
                .counters
                .egress_rejected
                .load(Ordering::Relaxed)
                >= 2
        );
        first.close().await.unwrap();
        second.close().await.unwrap();
        publisher.close().await.unwrap();
        running.finish().await;
    }
}

#[tokio::test]
async fn overload_terminal_states_reject_and_recovery_obeys_minimum_dwell() {
    let running = Running::with_resources(
        OverflowPolicy::DropNewest,
        4,
        ResourceConfig {
            overload_dwell: Duration::from_millis(50),
            ..ResourceConfig::default()
        },
    )
    .await;
    let mut subscriber = running.client().await;
    subscriber.subscribe("demo").await.unwrap();
    let mut publisher = running.client().await;
    running.broker.drain();
    assert_eq!(running.broker.overload_state(), OverloadState::Drain);
    assert!(matches!(
        publisher
            .publish("demo", b"draining", PublishOptions::default())
            .await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::ServerDraining),
            ..
        })
    ));
    running.broker.unavailable();
    assert_eq!(running.broker.overload_state(), OverloadState::Unavailable);
    assert!(matches!(
        publisher
            .publish("demo", b"unavailable", PublishOptions::default())
            .await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::ResourceExhausted),
            ..
        })
    ));
    running.broker.recover();
    assert_eq!(running.broker.overload_state(), OverloadState::Shed);
    for _ in 0..8 {
        assert_eq!(running.broker.overload_state(), OverloadState::Shed);
    }
    timeout(Duration::from_secs(2), async {
        while running.broker.overload_state() != OverloadState::Pressure {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for _ in 0..8 {
        assert_eq!(running.broker.overload_state(), OverloadState::Pressure);
    }
    timeout(Duration::from_secs(2), async {
        while running.broker.overload_state() != OverloadState::Normal {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    publish(&running, &mut publisher, b"recovered", b"normal").await;
    assert_eq!(receive(&mut subscriber).await, b"recovered");
    assert!(
        running
            .broker
            .counters
            .overload_transitions
            .load(Ordering::Relaxed)
            >= 2
    );
    subscriber.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn topic_egress_override_isolates_an_unrelated_topic() {
    let mut resources = ResourceConfig::default();
    resources.topic_overrides.insert(
        "demo".into(),
        RateLimit {
            bytes_per_sec: 1024 * 1024,
            deliveries_per_sec: 1,
        },
    );
    let running = Running::with_resources(OverflowPolicy::DropNewest, 4, resources).await;
    let mut first = running.client().await;
    first.subscribe("demo").await.unwrap();
    let mut other = running.client().await;
    other.subscribe("telemetry").await.unwrap();
    let mut publisher = running.client().await;
    publish(&running, &mut publisher, b"demo-fits", b"demo").await;
    assert_eq!(receive(&mut first).await, b"demo-fits");
    assert!(matches!(
        publisher
            .publish("demo", b"demo-over-quota", PublishOptions::default())
            .await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::EgressQuota),
            ..
        })
    ));
    assert_eq!(
        publisher
            .publish("telemetry", b"independent", PublishOptions::default())
            .await
            .unwrap(),
        PublishOutcome::Admitted
    );
    assert_eq!(receive(&mut other).await, b"independent");
    first.close().await.unwrap();
    other.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn failed_fanout_reservation_rolls_back_and_queued_unsubscribe_releases_egress() {
    let running = Running::with_resources(
        OverflowPolicy::RejectPublish,
        2,
        ResourceConfig {
            global_outbound_bytes: 300,
            ..ResourceConfig::default()
        },
    )
    .await;
    let mut first = running.client().await;
    let first_id = first.subscribe_with_credit("demo", 0, 0).await.unwrap();
    let mut second = running.client().await;
    let second_id = second.subscribe_with_credit("demo", 0, 0).await.unwrap();
    let mut publisher = running.client().await;
    // One encoded copy plus queue metadata fits; two physical recipients do not.
    assert!(matches!(
        publisher.publish("demo", b"x", options(b"k")).await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::ResourceExhausted),
            ..
        })
    ));
    assert_eq!(running.broker.egress_usage().messages, 0);
    assert_eq!(running.broker.egress_usage().bytes, 0);
    second.unsubscribe(second_id).await.unwrap();
    publish(&running, &mut publisher, b"queued", b"k").await;
    assert_eq!(running.broker.egress_usage().messages, 1);
    first.unsubscribe(first_id).await.unwrap();
    timeout(Duration::from_secs(2), async {
        while running.broker.egress_usage().messages != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(running.stats.delivered.load(Ordering::Relaxed), 0);
    let replacement = first.subscribe_with_credit("demo", 1, 4096).await.unwrap();
    assert_ne!(replacement, first_id);
    publish(&running, &mut publisher, b"fresh", b"k").await;
    assert_eq!(receive(&mut first).await, b"fresh");
    first.close().await.unwrap();
    second.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}

#[tokio::test]
async fn egress_byte_quota_charges_encoded_frame_not_only_payload() {
    let running = Running::with_resources(
        OverflowPolicy::RejectPublish,
        2,
        ResourceConfig {
            topic_egress: RateLimit {
                bytes_per_sec: 100,
                deliveries_per_sec: 100,
            },
            ..ResourceConfig::default()
        },
    )
    .await;
    let mut subscriber = running.client().await;
    subscriber.subscribe("demo").await.unwrap();
    let mut publisher = running.client().await;
    // Payload fits100 bytes; its protocol envelope makes the physical delivery larger.
    assert!(matches!(
        publisher
            .publish("demo", &[42; 80], PublishOptions::default())
            .await,
        Err(ClientError::Remote {
            code: Some(ErrorCode::EgressQuota),
            ..
        })
    ));
    assert_eq!(running.broker.counters.admitted.load(Ordering::Relaxed), 0);
    publish(&running, &mut publisher, b"x", b"").await;
    let delivery = timeout(Duration::from_secs(2), subscriber.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.payload, b"x");
    assert!(delivery.wire_bytes > 1 && delivery.wire_bytes <= 100);
    subscriber.close().await.unwrap();
    publisher.close().await.unwrap();
    running.finish().await;
}
