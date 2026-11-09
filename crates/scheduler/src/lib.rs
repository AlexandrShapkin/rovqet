//! Bounded prototype admission and weighted service. No durability operations.
#![forbid(unsafe_code)]
use rovqet_core::Priority;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub messages: usize,
    pub bytes: usize,
}
#[derive(Debug)]
pub struct Budget {
    limit: Usage,
    used: Mutex<Usage>,
}
impl Budget {
    pub fn new(messages: usize, bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            limit: Usage { messages, bytes },
            used: Mutex::new(Usage::default()),
        })
    }
    pub fn usage(&self) -> Usage {
        *self.used.lock().unwrap()
    }
    /// Hierarchical reservation rolls back every prior charge on failure.
    pub fn reserve(budgets: &[Arc<Self>], bytes: usize) -> Option<Reservation> {
        let mut domains = budgets.to_vec();
        domains.sort_by_key(Arc::as_ptr);
        domains.dedup_by(|a, b| Arc::ptr_eq(a, b));
        let mut guards: Vec<_> = domains.iter().map(|b| b.used.lock().unwrap()).collect();
        if domains.iter().zip(guards.iter()).any(|(b, u)| {
            u.messages >= b.limit.messages || bytes > b.limit.bytes.saturating_sub(u.bytes)
        }) {
            return None;
        }
        for used in &mut guards {
            used.messages += 1;
            used.bytes += bytes;
        }
        drop(guards);
        Some(Reservation {
            budgets: domains,
            bytes,
        })
    }
}
#[derive(Debug)]
pub struct Reservation {
    budgets: Vec<Arc<Budget>>,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        for budget in &self.budgets {
            let mut used = budget.used.lock().unwrap();
            used.messages -= 1;
            used.bytes -= self.bytes;
        }
    }
}

/// Four bounded lanes; reserved CONTROL capacity cannot be used by lower classes.
pub struct Scheduler<T> {
    lanes: [VecDeque<T>; 4],
    capacity: usize,
    control_reserve: usize,
    cursor: usize,
}
const SLOTS: [usize; 15] = [0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0];
impl<T> Scheduler<T> {
    pub fn new(capacity: usize, control_reserve: usize) -> Self {
        assert!(capacity > control_reserve);
        Self {
            lanes: std::array::from_fn(|_| VecDeque::new()),
            capacity,
            control_reserve,
            cursor: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.lanes.iter().map(VecDeque::len).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn push(&mut self, priority: Priority, value: T) -> Result<(), T> {
        let limit = self.capacity
            - if priority == Priority::Control {
                0
            } else {
                self.control_reserve
            };
        if self.len() >= limit {
            return Err(value);
        }
        self.lanes[priority as usize].push_back(value);
        Ok(())
    }
    pub fn pop(&mut self) -> Option<T> {
        for _ in 0..SLOTS.len() {
            let lane = SLOTS[self.cursor];
            self.cursor = (self.cursor + 1) % SLOTS.len();
            if let Some(value) = self.lanes[lane].pop_front() {
                return Some(value);
            }
        }
        None
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hierarchy_rolls_back_and_cancel_releases() {
        let global = Budget::new(2, 10);
        let topic = Budget::new(1, 4);
        assert!(Budget::reserve(&[global.clone(), topic.clone()], 5).is_none());
        assert_eq!(global.usage(), Usage::default());
        let held = Budget::reserve(&[global.clone(), topic.clone()], 4).unwrap();
        assert!(Budget::reserve(&[global.clone(), topic.clone()], 1).is_none());
        assert_eq!(global.usage().messages, 1);
        drop(held);
        assert_eq!(topic.usage(), Usage::default());
    }
    #[test]
    fn bounded_lanes_reserve_control_and_service_bulk() {
        let mut s = Scheduler::new(16, 1);
        for i in 0..15 {
            s.push(Priority::Bulk, i).unwrap();
        }
        assert!(s.push(Priority::Bulk, 16).is_err());
        s.push(Priority::Control, 99).unwrap();
        assert!(s.push(Priority::Control, 100).is_err());
        assert_eq!(s.pop(), Some(99));
        assert_eq!(s.pop(), Some(0));
        let mut s = Scheduler::new(100, 1);
        for _ in 0..20 {
            for p in [
                Priority::Control,
                Priority::Interactive,
                Priority::Telemetry,
                Priority::Bulk,
            ] {
                s.push(p, p).unwrap();
            }
        }
        let mut counts = [0; 4];
        for _ in 0..15 {
            counts[s.pop().unwrap() as usize] += 1;
        }
        assert_eq!(counts, [8, 4, 2, 1]);
    }
}

/// Two-dimensional token bucket. Burst equals one second of configured capacity.
/// Reservations across a hierarchy lock all domains before mutation.
#[derive(Debug)]
pub struct RateQuota {
    bytes_per_sec: usize,
    deliveries_per_sec: usize,
    state: Mutex<RateState>,
}
#[derive(Debug)]
struct RateState {
    bytes: f64,
    deliveries: f64,
    updated: std::time::Instant,
}
pub struct RateReservation {
    quotas: Vec<Arc<RateQuota>>,
    bytes: usize,
    deliveries: usize,
    committed: bool,
}
impl RateQuota {
    pub fn new(bytes_per_sec: usize, deliveries_per_sec: usize) -> Arc<Self> {
        assert!(bytes_per_sec > 0 && deliveries_per_sec > 0);
        Arc::new(Self {
            bytes_per_sec,
            deliveries_per_sec,
            state: Mutex::new(RateState {
                bytes: bytes_per_sec as f64,
                deliveries: deliveries_per_sec as f64,
                updated: std::time::Instant::now(),
            }),
        })
    }
    pub fn reserve(
        quotas: &[Arc<Self>],
        bytes: usize,
        deliveries: usize,
    ) -> Option<RateReservation> {
        // Stable address ordering also handles callers presenting different hierarchy order.
        let mut domains: Vec<_> = quotas.to_vec();
        domains.sort_by_key(Arc::as_ptr);
        domains.dedup_by(|a, b| Arc::ptr_eq(a, b));
        let mut guards: Vec<_> = domains.iter().map(|q| q.state.lock().unwrap()).collect();
        let now = std::time::Instant::now();
        for (quota, state) in domains.iter().zip(guards.iter_mut()) {
            let elapsed = now.duration_since(state.updated).as_secs_f64();
            state.bytes = (state.bytes + elapsed * quota.bytes_per_sec as f64)
                .min(quota.bytes_per_sec as f64);
            state.deliveries = (state.deliveries + elapsed * quota.deliveries_per_sec as f64)
                .min(quota.deliveries_per_sec as f64);
            state.updated = now;
        }
        if guards
            .iter()
            .any(|s| s.bytes < bytes as f64 || s.deliveries < deliveries as f64)
        {
            return None;
        }
        for state in &mut guards {
            state.bytes -= bytes as f64;
            state.deliveries -= deliveries as f64;
        }
        drop(guards);
        Some(RateReservation {
            quotas: domains,
            bytes,
            deliveries,
            committed: false,
        })
    }
    pub fn can_fit(&self, bytes: usize, deliveries: usize) -> bool {
        bytes <= self.bytes_per_sec && deliveries <= self.deliveries_per_sec
    }
}
impl RateReservation {
    pub fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for RateReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for quota in &self.quotas {
            let mut state = quota.state.lock().unwrap();
            state.bytes = (state.bytes + self.bytes as f64).min(quota.bytes_per_sec as f64);
            state.deliveries =
                (state.deliveries + self.deliveries as f64).min(quota.deliveries_per_sec as f64);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverloadState {
    Normal,
    Pressure,
    Shed,
    Drain,
    Unavailable,
}
/// Queue occupancy hysteresis: enter at 70/90%, recover at 50/65%, minimum dwell.
pub struct Overload {
    state: OverloadState,
    changed: std::time::Instant,
    dwell: std::time::Duration,
}
impl Overload {
    pub fn new(dwell: std::time::Duration) -> Self {
        Self {
            state: OverloadState::Normal,
            changed: std::time::Instant::now(),
            dwell,
        }
    }
    pub fn state(&self) -> OverloadState {
        self.state
    }
    pub fn set(&mut self, state: OverloadState) {
        self.state = state;
        self.changed = std::time::Instant::now();
    }
    pub fn observe(&mut self, percent: usize, now: std::time::Instant) -> OverloadState {
        use OverloadState::*;
        let next = match self.state {
            Drain | Unavailable => self.state,
            _ if percent >= 90 => Shed,
            Normal if percent >= 70 => Pressure,
            Pressure if percent <= 50 && now.duration_since(self.changed) >= self.dwell => Normal,
            Shed if percent <= 65 && now.duration_since(self.changed) >= self.dwell => Pressure,
            s => s,
        };
        if next != self.state {
            self.state = next;
            self.changed = now;
        }
        self.state
    }
}
#[cfg(test)]
mod resource_tests {
    use super::*;
    #[test]
    fn rate_hierarchy_cancel_and_failure_are_atomic() {
        let a = RateQuota::new(100, 10);
        let b = RateQuota::new(20, 1);
        assert!(RateQuota::reserve(&[a.clone(), b.clone()], 21, 1).is_none());
        let r = RateQuota::reserve(&[a.clone(), b.clone()], 20, 1).unwrap();
        assert!(RateQuota::reserve(&[a.clone(), b.clone()], 1, 1).is_none());
        drop(r);
        RateQuota::reserve(&[a.clone(), b.clone()], 20, 1)
            .unwrap()
            .commit();
        assert!(RateQuota::reserve(&[a, b], 1, 1).is_none());
    }
    #[test]
    fn hysteresis_has_dwell_and_explicit_terminal_states() {
        use std::time::{Duration, Instant};
        let mut o = Overload::new(Duration::from_millis(100));
        let now = Instant::now();
        assert_eq!(o.observe(75, now), OverloadState::Pressure);
        assert_eq!(o.observe(95, now), OverloadState::Shed);
        assert_eq!(o.observe(20, now), OverloadState::Shed);
        assert_eq!(
            o.observe(60, now + Duration::from_secs(1)),
            OverloadState::Pressure
        );
        assert_eq!(
            o.observe(60, now + Duration::from_secs(2)),
            OverloadState::Pressure
        );
        assert_eq!(
            o.observe(20, now + Duration::from_secs(2)),
            OverloadState::Normal
        );
        o.set(OverloadState::Drain);
        assert_eq!(o.observe(0, Instant::now()), OverloadState::Drain);
        o.set(OverloadState::Unavailable);
        assert_eq!(o.observe(0, Instant::now()), OverloadState::Unavailable);
    }
}
