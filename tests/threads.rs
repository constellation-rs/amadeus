#![allow(where_clauses_object_safety)]

extern crate amadeus;
#[macro_use]
extern crate serde_closure;
extern crate constellation;
extern crate rand;

use amadeus::prelude::*;
use constellation::*;
use rand::Rng;
use std::{env, thread, time};

fn main() {
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(10);

	let pool = ProcessPool::new(processes, Resources::default()).unwrap();

	let handles = (0..processes * 3)
		.map(|i| {
			pool.spawn(FnOnce!([i] move || -> String {
				thread::sleep(rand::thread_rng().gen_range(time::Duration::new(0,0),time::Duration::new(5,0)));
				format!("warm greetings from job {}", i)
			}))
		})
		.map(|handle| {
			thread::spawn(move || {
				println!("{}", handle.join().unwrap());
			})
		})
		.collect::<Vec<_>>();

	for handle in handles {
		handle.join().unwrap();
	}
}
