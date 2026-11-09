//! Static development topic policies and bounded, exact-topic subscriptions.
#![forbid(unsafe_code)]

use rovqet_core::{PartitionRef, Priority, QoS};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    DropNewest,
    DropOldest,
    CoalesceLatest,
    RejectPublish,
}

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub name: String,
    pub partitions: u32,
    pub allowed_qos: Vec<QoS>,
    pub priority: Priority,
    pub max_ttl_ms: u32,
    pub max_subscriptions: usize,
    pub overflow: OverflowPolicy,
    pub max_ingress_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterError {
    InvalidConfig,
    InvalidTopic,
    UnknownTopic,
    QosNotAllowed,
    InvalidTtl,
    ResourceExhausted,
    UnknownSubscription,
}
impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for RouterError {}

/// MVP names are exact ASCII paths, at most 255 bytes. Wildcards are unsupported.
pub fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name.split('/').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
        })
}

#[derive(Debug, Clone)]
pub struct Router {
    topics: Arc<HashMap<String, Arc<TopicConfig>>>,
    max_per_session: usize,
}
impl Router {
    pub fn new(topics: Vec<TopicConfig>, max_per_session: usize) -> Result<Self, RouterError> {
        if max_per_session == 0 || topics.is_empty() {
            return Err(RouterError::InvalidConfig);
        }
        let mut map = HashMap::new();
        let mut ids = std::collections::HashSet::new();
        for topic in topics {
            if !valid_topic_name(&topic.name)
                || topic.partitions == 0
                || topic.allowed_qos.is_empty()
                || (topic.allowed_qos.contains(&QoS::Q1) && topic.allowed_qos.contains(&QoS::Q2))
                || topic.max_ttl_ms == 0
                || topic.max_subscriptions == 0
                || topic.max_ingress_bytes == 0
                || !ids.insert((topic.tenant_id, topic.topic_id))
                || map.contains_key(&topic.name)
            {
                return Err(RouterError::InvalidConfig);
            }
            map.insert(topic.name.clone(), Arc::new(topic));
        }
        Ok(Self {
            topics: Arc::new(map),
            max_per_session,
        })
    }

    pub fn topic(&self, name: &str) -> Option<Arc<TopicConfig>> {
        self.topics.get(name).cloned()
    }

    pub fn validate_publish(
        &self,
        name: &str,
        qos: QoS,
        ttl_ms: u32,
    ) -> Result<Arc<TopicConfig>, RouterError> {
        if !valid_topic_name(name) {
            return Err(RouterError::InvalidTopic);
        }
        let topic = self.topic(name).ok_or(RouterError::UnknownTopic)?;
        if !topic.allowed_qos.contains(&qos) {
            return Err(RouterError::QosNotAllowed);
        }
        if ttl_ms == 0 || ttl_ms > topic.max_ttl_ms {
            return Err(RouterError::InvalidTtl);
        }
        Ok(topic)
    }

    /// FNV-1a 64-bit, fixed offset basis and prime (partition scheme v1).
    /// Input is tenant ID (u64 BE), topic ID (u64 BE), then key bytes.
    /// Hashing is independent of randomized Rust map hash seeds and process state.
    /// The topic must come from validated Router configuration (nonzero partitions).
    pub fn partition(topic: &TopicConfig, key: &[u8]) -> PartitionRef {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in topic
            .tenant_id
            .to_be_bytes()
            .iter()
            .chain(topic.topic_id.to_be_bytes().iter())
            .chain(key.iter())
        {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
        PartitionRef {
            tenant_id: topic.tenant_id,
            topic_id: topic.topic_id,
            partition_id: (hash % u64::from(topic.partitions)) as u32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Subscription<T> {
    pub id: u64,
    pub session_id: u128,
    pub priority: Priority,
    pub handle: T,
}

type Snapshot<T> = Arc<Vec<Subscription<T>>>;
struct TopicIndex<T> {
    config: Arc<TopicConfig>,
    subscribers: RwLock<Snapshot<T>>,
}
struct MutationState {
    next_id: u64,
    session_counts: HashMap<u128, usize>,
}

/// Mutations are atomic with session quotas. Readers acquire only their topic's
/// read lock to clone a bounded immutable snapshot; fan-out holds no index lock.
pub struct SubscriptionIndex<T> {
    topics: HashMap<String, TopicIndex<T>>,
    max_per_session: usize,
    mutations: Mutex<MutationState>,
}
impl<T: Clone> SubscriptionIndex<T> {
    pub fn new(router: &Router) -> Self {
        Self {
            topics: router
                .topics
                .iter()
                .map(|(name, config)| {
                    (
                        name.clone(),
                        TopicIndex {
                            config: config.clone(),
                            subscribers: RwLock::new(Arc::new(Vec::new())),
                        },
                    )
                })
                .collect(),
            max_per_session: router.max_per_session,
            mutations: Mutex::new(MutationState {
                next_id: 1,
                session_counts: HashMap::new(),
            }),
        }
    }

    pub fn subscribe(&self, topic: &str, session_id: u128, handle: T) -> Result<u64, RouterError> {
        let topic = self.topics.get(topic).ok_or(RouterError::UnknownTopic)?;
        let mut state = self
            .mutations
            .lock()
            .expect("subscription mutation lock poisoned");
        let mut subscribers = topic
            .subscribers
            .write()
            .expect("topic index lock poisoned");
        if subscribers.len() >= topic.config.max_subscriptions
            || state.session_counts.get(&session_id).copied().unwrap_or(0) >= self.max_per_session
            || state.next_id == u64::MAX
        {
            return Err(RouterError::ResourceExhausted);
        }
        let id = state.next_id;
        state.next_id += 1;
        Arc::make_mut(&mut subscribers).push(Subscription {
            id,
            session_id,
            priority: topic.config.priority,
            handle,
        });
        *state.session_counts.entry(session_id).or_default() += 1;
        Ok(id)
    }

    pub fn unsubscribe(&self, session_id: u128, id: u64) -> Result<(), RouterError> {
        let mut state = self
            .mutations
            .lock()
            .expect("subscription mutation lock poisoned");
        for topic in self.topics.values() {
            let mut subscribers = topic
                .subscribers
                .write()
                .expect("topic index lock poisoned");
            if let Some(pos) = subscribers
                .iter()
                .position(|s| s.id == id && s.session_id == session_id)
            {
                Arc::make_mut(&mut subscribers).swap_remove(pos);
                decrement(&mut state.session_counts, session_id, 1);
                return Ok(());
            }
        }
        Err(RouterError::UnknownSubscription)
    }

    pub fn remove_session(&self, session_id: u128) {
        let mut state = self
            .mutations
            .lock()
            .expect("subscription mutation lock poisoned");
        for topic in self.topics.values() {
            let mut subscribers = topic
                .subscribers
                .write()
                .expect("topic index lock poisoned");
            if subscribers.iter().any(|s| s.session_id == session_id) {
                Arc::make_mut(&mut subscribers).retain(|s| s.session_id != session_id);
            }
        }
        state.session_counts.remove(&session_id);
    }

    pub fn matches(&self, topic: &str) -> Snapshot<T> {
        self.topics
            .get(topic)
            .map(|t| {
                t.subscribers
                    .read()
                    .expect("topic index lock poisoned")
                    .clone()
            })
            .unwrap_or_default()
    }
}
fn decrement(counts: &mut HashMap<u128, usize>, session: u128, amount: usize) {
    if let Some(count) = counts.get_mut(&session) {
        *count -= amount;
        if *count == 0 {
            counts.remove(&session);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> TopicConfig {
        TopicConfig {
            tenant_id: 1,
            topic_id: 1,
            name: "test/events".into(),
            partitions: 16,
            allowed_qos: vec![QoS::Q0],
            priority: Priority::Telemetry,
            max_ttl_ms: 5000,
            max_subscriptions: 64,
            overflow: OverflowPolicy::DropNewest,
            max_ingress_bytes: 4096,
        }
    }
    #[test]
    fn pinned_hash_and_policy_validation() {
        let router = Router::new(vec![config()], 2).unwrap();
        let mut vector = config();
        vector.partitions = 1009;
        assert_eq!(Router::partition(&vector, b"hello").partition_id, 611);
        vector.tenant_id = 2;
        assert_ne!(Router::partition(&vector, b"hello").partition_id, 611);
        assert_eq!(Router::partition(&config(), b"hello").partition_id, 11);
        assert_eq!(Router::partition(&config(), b"").partition_id, 5);
        assert!(router.validate_publish("test/events", QoS::Q0, 1).is_ok());
        assert_eq!(
            router
                .validate_publish("test/events", QoS::Q1, 1)
                .unwrap_err(),
            RouterError::QosNotAllowed
        );
        assert_eq!(
            router
                .validate_publish("test/events", QoS::Q0, 0)
                .unwrap_err(),
            RouterError::InvalidTtl
        );
        assert_eq!(
            router
                .validate_publish("test/events", QoS::Q0, 5001)
                .unwrap_err(),
            RouterError::InvalidTtl
        );
        assert_eq!(
            router.validate_publish("missing", QoS::Q0, 1).unwrap_err(),
            RouterError::UnknownTopic
        );
        assert!(!valid_topic_name("test/*"));
        assert!(!valid_topic_name("test//events"));
        assert!(!valid_topic_name("é"));
    }
    #[test]
    fn bounded_owned_subscriptions_and_snapshot_removal() {
        let router = Router::new(vec![config()], 1).unwrap();
        let index = SubscriptionIndex::new(&router);
        let id = index.subscribe("test/events", 7, "writer").unwrap();
        let snapshot = index.matches("test/events");
        assert_eq!(
            index.subscribe("test/events", 7, "other"),
            Err(RouterError::ResourceExhausted)
        );
        assert_eq!(
            index.unsubscribe(8, id),
            Err(RouterError::UnknownSubscription)
        );
        index.unsubscribe(7, id).unwrap();
        assert!(index.matches("test/events").is_empty());
        assert_eq!(snapshot.len(), 1);
        let next = index.subscribe("test/events", 7, "new").unwrap();
        assert!(next > id);
        index.remove_session(7);
        assert!(index.matches("test/events").is_empty());
    }
    #[test]
    fn concurrent_registration_removal_and_topic_quota() {
        let mut topic = config();
        topic.max_subscriptions = 8;
        let router = Router::new(vec![topic], 2).unwrap();
        let index = Arc::new(SubscriptionIndex::new(&router));
        let threads: Vec<_> = (0..32)
            .map(|session| {
                let index = index.clone();
                std::thread::spawn(move || index.subscribe("test/events", session, ()))
            })
            .collect();
        assert_eq!(
            threads
                .into_iter()
                .filter_map(|t| t.join().unwrap().ok())
                .count(),
            8
        );
        assert_eq!(index.matches("test/events").len(), 8);
        std::thread::scope(|scope| {
            for session in 0..32 {
                let index = &index;
                scope.spawn(move || index.remove_session(session));
            }
        });
        assert!(index.matches("test/events").is_empty());
    }
    #[test]
    fn invalid_config_rejected() {
        let mut mixed = config();
        mixed.allowed_qos = vec![QoS::Q1, QoS::Q2];
        assert_eq!(
            Router::new(vec![mixed], 1).unwrap_err(),
            RouterError::InvalidConfig
        );
        let mut topic = config();
        topic.partitions = 0;
        assert_eq!(
            Router::new(vec![topic], 1).unwrap_err(),
            RouterError::InvalidConfig
        );
        assert_eq!(
            Router::new(vec![config(), config()], 1).unwrap_err(),
            RouterError::InvalidConfig
        );
    }
}
