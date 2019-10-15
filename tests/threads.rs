#![allow(where_clauses_object_safety)]

use serde_closure::FnOnce;

#[cfg(feature = "constellation")]
use constellation::*;
use rand::{Rng, SeedableRng};
use std::{
	convert::TryInto, env, thread, time::{self, Duration, SystemTime}
};

use amadeus::prelude::*;

fn main() {
	#[cfg(feature = "constellation")]
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(10);

	let local_pool_time = {
		let local_pool = LocalPool::new();
		run(&local_pool, 10)
	};
	let thread_pool_time = {
		let thread_pool = ThreadPool::new(processes).unwrap();
		run(&thread_pool, processes * 10)
	};
	#[cfg(feature = "constellation")]
	let process_pool_time = {
		let process_pool = ProcessPool::new(processes, 1, Resources::default()).unwrap();
		run(&process_pool, processes * 10)
	};
	#[cfg(not(feature = "constellation"))]
	let process_pool_time = "-";

	println!(
		"in {:?} {:?} {:?}",
		local_pool_time, thread_pool_time, process_pool_time
	);
	// println!("in {:?}", thread_pool_time);
}

fn run<P: amadeus_core::pool::ProcessPool>(pool: &P, parallel: usize) -> Duration {
	let start = SystemTime::now();

	let handles = (0..parallel)
		.map(|i| {
			pool.spawn(FnOnce!([i] move || -> String {
				let mut rng = rand::rngs::SmallRng::seed_from_u64(i.try_into().unwrap());
				thread::sleep(rng.gen_range(time::Duration::new(0,0),time::Duration::new(2,0)));
				format!("warm greetings from job {}", i)
			}))
		})
		.enumerate()
		.map(|(i, handle)| {
			thread::Builder::new()
				.name(format!("{}", i))
				.spawn(move || {
					println!("{}", handle.block().unwrap());
				})
				.unwrap()
		})
		.collect::<Vec<_>>();

	for handle in handles {
		handle.join().unwrap();
	}

	start.elapsed().unwrap()
}
