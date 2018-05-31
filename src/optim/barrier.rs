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
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};

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

    pub fn increment_num_threads(&self) -> usize {
        let mut lock = self.lock.lock().unwrap();
        if lock.generation_id != 0 {
            panic!("Can't register more threads after the first generation.");
        }

        lock.num_threads += 1;

        lock.num_threads
    }

    pub fn decrement_num_threads(&self) -> usize {
        let mut lock = self.lock.lock().unwrap();
        lock.num_threads = lock.num_threads.saturating_sub(1);

        // Notify if deregistering makes the barrier conditions met.
        if lock.count >= lock.num_threads {
            lock.count = 0;
            lock.generation_id = lock.generation_id.wrapping_add(1);
            self.cvar.notify_all();
        }

        lock.num_threads
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
        SynchronizationBarrierGuard {
            thread_num: self.core.register_thread(),
            barrier: Arc::clone(&self.core),
        }
    }
}

struct SynchronizationBarrierCore {
    start_barrier: Barrier,
    end_barrier: Barrier,
    thread_queue: Mutex<VecDeque<usize>>,
}

impl SynchronizationBarrierCore {
    fn new() -> Self {
        Self {
            start_barrier: Barrier::new(0),
            end_barrier: Barrier::new(0),
            thread_queue: Mutex::new(VecDeque::default()),
        }
    }

    fn register_thread(&self) -> usize {
        self.start_barrier.increment_num_threads();
        let thread_num = self.end_barrier.increment_num_threads();

        self.thread_queue.lock().unwrap().push_back(thread_num);

        thread_num
    }

    fn deregister_thread(&self, thread_id: usize) {
        self.start_barrier.decrement_num_threads();
        self.end_barrier.decrement_num_threads();

        self.thread_queue
            .lock()
            .unwrap()
            .retain(|&x| x != thread_id);
    }

    fn should_update(&self, thread_id: usize) -> bool {
        self.thread_queue.lock().unwrap().front().unwrap() == &thread_id
    }

    fn advance_queue(&self) {
        let mut queue_lock = self.thread_queue.lock().unwrap();
        let thread_id = queue_lock.pop_front().expect("Thread queue empty.").clone();

        // Requeue at the end
        queue_lock.push_back(thread_id);
    }

    fn start_wait(&self) {
        self.start_barrier.wait();
    }

    fn end_wait(&self) {
        self.end_barrier.wait();
    }
}

/// Synchronizes parameter updates so that no two threads can update
/// variables at the same time, and that all updates always occur in
/// the same order, making results reproducible even when multiple
/// threads are used.
pub struct SynchronizationBarrierGuard {
    thread_num: usize,
    barrier: Arc<SynchronizationBarrierCore>,
}

impl SynchronizationBarrierGuard {
    /// Returns a synchronization guard, guaranteeing that no other thread
    /// will be active at the same time.
    ///
    /// The result of this _must_ be assigned; when dropped, it
    /// allows the next thread to proceed.
    pub fn synchronize(&self) -> Result<ThreadQueueGuard, ()> {
        self.barrier.start_wait();
        while !self.barrier.should_update(self.thread_num) {}

        Ok(ThreadQueueGuard { guard: self })
    }

    /// Return the id of the current thread.
    /// Threads always update parameters in the
    /// same (ascending) sequence of thread ids.
    pub fn thread_id(&self) -> usize {
        self.thread_num
    }
}

impl Drop for SynchronizationBarrierGuard {
    fn drop(&mut self) {
        self.barrier.deregister_thread(self.thread_num);
    }
}

pub struct ThreadQueueGuard<'a> {
    guard: &'a SynchronizationBarrierGuard,
}

impl<'a> Drop for ThreadQueueGuard<'a> {
    fn drop(&mut self) {
        self.guard.barrier.advance_queue();
        self.guard.barrier.end_wait();
    }
}
