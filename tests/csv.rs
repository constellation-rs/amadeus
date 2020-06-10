#[cfg(feature = "constellation")]
use constellation::*;
use std::{
	path::PathBuf, time::{Duration, SystemTime}
};

use amadeus::prelude::*;

#[tokio::main]
async fn main() {
	#[cfg(feature = "constellation")]
	init(Resources::default());

	let thread_pool_time = {
		let thread_pool = ThreadPool::new(None);
		run(&thread_pool).await
	};
	#[cfg(feature = "constellation")]
	let process_pool_time = {
		let process_pool = ProcessPool::new(None, None, Resources::default()).unwrap();
		run(&process_pool).await
	};
	#[cfg(not(feature = "constellation"))]
	let process_pool_time = "-";

	println!("in {:?} {:?}", thread_pool_time, process_pool_time);
}

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P) -> Duration {
	let start = SystemTime::now();

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct GameDerived {
		a: String,
		b: String,
		c: String,
		d: String,
		e: u32,
		f: String,
	}

	let rows = Csv::<_, GameDerived>::new(vec![PathBuf::from("amadeus-testing/csv/game.csv")])
		.await
		.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		100_000
	);

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct GameDerived2 {
		a: String,
		b: String,
		c: String,
		d: String,
		e: u64,
		f: String,
	}

	let rows = Csv::<_, Value>::new(vec![PathBuf::from("amadeus-testing/csv/game.csv")])
		.await
		.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				// println!("{:?}", value);
				let _: GameDerived2 = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		100_000
	);

	start.elapsed().unwrap()
}
