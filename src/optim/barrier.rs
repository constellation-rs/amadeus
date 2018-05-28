// A modification of the stdlib barrier to allow resizing.

// Copyright 2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

pub struct Barrier {
    lock: Mutex<BarrierState>,
    cvar: Condvar,
}

struct BarrierState {
    count: usize,
    generation_id: usize,
    num_threads: usize,
}

pub struct BarrierWaitResult(bool);

impl fmt::Debug for Barrier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad("Barrier { .. }")
    }
}

impl Barrier {
    pub fn new(n: usize) -> Barrier {
        Barrier {
            lock: Mutex::new(BarrierState {
                count: 0,
                generation_id: 0,
                num_threads: n,
            }),
            cvar: Condvar::new(),
        }
    }

    pub fn increment_num_threads(&self) {
        let mut lock = self.lock.lock().unwrap();
        if lock.generation_id != 0 {
            panic!("Can't register more threads after the first generation.");
        }

        lock.num_threads += 1;
    }

    pub fn decrement_num_threads(&self) {
        let mut lock = self.lock.lock().unwrap();
        lock.num_threads = lock.num_threads.saturating_sub(1);

        // Notify if deregistering makes the barrier conditions met.
        if lock.count >= lock.num_threads {
            lock.count = 0;
            lock.generation_id = lock.generation_id.wrapping_add(1);
            self.cvar.notify_all();
        }
    }

    pub fn wait(&self) -> BarrierWaitResult {
        let mut lock = self.lock.lock().unwrap();
        let local_gen = lock.generation_id;
        lock.count += 1;
        if lock.count < lock.num_threads {
            // We need a while loop to guard against spurious wakeups.
            // http://en.wikipedia.org/wiki/Spurious_wakeup
            while local_gen == lock.generation_id && lock.count < lock.num_threads {
                lock = self.cvar.wait(lock).unwrap();
            }
            BarrierWaitResult(false)
        } else {
            lock.count = 0;
            lock.generation_id = lock.generation_id.wrapping_add(1);
            self.cvar.notify_all();
            BarrierWaitResult(true)
        }
    }
}

impl fmt::Debug for BarrierWaitResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BarrierWaitResult")
            .field("is_leader", &self.is_leader())
            .finish()
    }
}

impl BarrierWaitResult {
    pub fn is_leader(&self) -> bool {
        self.0
    }
}

pub struct SynchronizationBarrier {
    core: Arc<SynchronizationBarrierCore>,
}

impl Default for SynchronizationBarrier {
    fn default() -> Self {
        Self::new()
    }
}

impl SynchronizationBarrier {
    pub fn new() -> Self {
        SynchronizationBarrier {
            core: Arc::new(SynchronizationBarrierCore::new()),
        }
    }

    pub(crate) fn register_thread(&self) -> SynchronizationBarrierGuard {
        self.core.register_thread();
        SynchronizationBarrierGuard {
            barrier: Arc::clone(&self.core),
        }
    }
}

struct SynchronizationBarrierCore {
    start_barrier: Barrier,
    end_barrier: Barrier,
    parameter_lock: Mutex<()>,
}

impl SynchronizationBarrierCore {
    fn new() -> Self {
        Self {
            start_barrier: Barrier::new(0),
            end_barrier: Barrier::new(0),
            parameter_lock: Mutex::default(),
        }
    }

    fn register_thread(&self) {
        self.start_barrier.increment_num_threads();
        self.end_barrier.increment_num_threads();
    }

    fn deregister_thread(&self) {
        self.start_barrier.decrement_num_threads();
        self.end_barrier.decrement_num_threads();
    }

    pub(crate) fn start_wait(&self) {
        self.start_barrier.wait();
    }

    pub(crate) fn end_wait(&self) {
        self.end_barrier.wait();
    }
}

pub struct SynchronizationBarrierGuard {
    barrier: Arc<SynchronizationBarrierCore>,
}

impl SynchronizationBarrierGuard {
    pub fn start_wait(&self) {
        self.barrier.start_wait();
    }

    pub fn end_wait(&self) {
        self.barrier.end_wait();
    }

    pub fn lock(&self) -> MutexGuard<()> {
        self.barrier.parameter_lock.lock().unwrap()
    }
}

impl Drop for SynchronizationBarrierGuard {
    fn drop(&mut self) {
        self.barrier.deregister_thread();
    }
}
