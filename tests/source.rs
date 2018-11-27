#![allow(where_clauses_object_safety)]

extern crate amadeus;
#[macro_use]
extern crate serde_closure;
extern crate constellation;

use amadeus::prelude::*;
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

	let start = SystemTime::now();

	let pool = ProcessPool::new(processes, Resources::default()).unwrap();

	CommonCrawl::new("CC-MAIN-2018-43").unwrap().all(
		&pool,
		FnMut!([start] move |x: Result<WebpageOwned,_>| -> bool {
			println!("{}", x.unwrap().url);
			start.elapsed().unwrap() < Duration::new(10,0)
		}),
	);
}
