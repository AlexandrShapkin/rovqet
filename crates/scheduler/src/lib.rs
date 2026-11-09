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
        let mut result = Reservation {
            budgets: Vec::new(),
            bytes,
        };
        for budget in budgets {
            let mut used = budget.used.lock().unwrap();
            if used.messages >= budget.limit.messages
                || bytes > budget.limit.bytes.saturating_sub(used.bytes)
            {
                return None;
            }
            used.messages += 1;
            used.bytes += bytes;
            result.budgets.push(budget.clone());
        }
        Some(result)
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
