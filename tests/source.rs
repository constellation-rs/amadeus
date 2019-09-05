#![allow(where_clauses_object_safety)]

#[macro_use]
extern crate serde_closure;

use amadeus::{
	prelude::*, source::aws::{CloudfrontRow, Error}
};
use constellation::*;
use std::{
	env, time::{Duration, SystemTime}
};
use warc_parser::WebpageOwned;

fn main() {
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(10);

	let local_pool_time = {
		// let local_pool = LocalPool::new();
		0 // run(&local_pool) // cloudfront too slow
	};
	let thread_pool_time = {
		let thread_pool = ThreadPool::new(processes).unwrap();
		run(&thread_pool)
	};
	let process_pool_time = {
		let process_pool = ProcessPool::new(processes, 1, Resources::default()).unwrap();
		run(&process_pool)
	};

	println!(
		"in {:?} {:?} {:?}",
		local_pool_time, thread_pool_time, process_pool_time
	);
}

fn run<P: amadeus_core::pool::ProcessPool>(pool: &P) -> Duration {
	let start = SystemTime::now();

	println!("commoncrawl");

	CommonCrawl::new("CC-MAIN-2018-43").unwrap().all(
		pool,
		FnMut!([start] move |x: Result<WebpageOwned,_>| -> bool {
			let _x = x.unwrap();
			// println!("{}", x.url);
			start.elapsed().unwrap() < Duration::new(10,0)
		}),
	);

	println!("cloudfront");

	let _ = DistributedIteratorMulti::<&Result<CloudfrontRow, Error>>::count(Identity);

	let ((), (count, count2)) = Cloudfront::new(
		rusoto_core::Region::UsEast1,
		"us-east-1.data-analytics",
		"cflogworkshop/raw/cf-accesslogs",
	)
	.unwrap()
	.dist_iter()
	.multi(
		pool,
		Identity.for_each(FnMut!(|x: Result<CloudfrontRow, _>| {
			let _x = x.unwrap();
			// println!("{:?}", x.url);
		})),
		(
			Identity.map(FnMut!(|_x: &Result<_, _>| {})).count(),
			Identity.cloned().count(),
			// DistributedIteratorMulti::<&Result<CloudfrontRow, Error>>::count(Identity),
		),
	);
	assert_eq!(count, count2);
	assert_eq!(count, 207_928);

	start.elapsed().unwrap()
}
