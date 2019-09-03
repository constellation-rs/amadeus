use constellation::*;
use serde_closure::FnMut;
use std::{
	env, path::PathBuf, time::{Duration, SystemTime}
};

use amadeus::{
	data::{
		types::{Date, Downcast, Value}, Data
	}, source::{Json, Source}, DistributedIterator, LocalPool, ProcessPool, ThreadPool
};

fn main() {
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(10);

	let local_pool_time = {
		let local_pool = LocalPool::new();
		run(&local_pool, 2)
	};
	let thread_pool_time = {
		let thread_pool = ThreadPool::new(processes).unwrap();
		run(&thread_pool, processes * 2)
	};
	let process_pool_time = {
		let process_pool = ProcessPool::new(processes, 1, Resources::default()).unwrap();
		run(&process_pool, processes * 2)
	};

	println!(
		"in {:?} {:?} {:?}",
		local_pool_time, thread_pool_time, process_pool_time
	);
}

fn run<P: amadeus_core::pool::ProcessPool>(pool: &P, tasks: usize) -> Duration {
	let start = SystemTime::now();

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct BitcoinDerived {
		date: Date,
		#[amadeus(name = "txVolume(USD)")]
		tx_volume_usd: Option<String>,
		#[amadeus(name = "adjustedTxVolume(USD)")]
		adjusted_tx_volume_usd: Option<String>,
		#[amadeus(name = "txCount")]
		tx_count: u32,
		#[amadeus(name = "marketcap(USD)")]
		marketcap_usd: Option<String>,
		#[amadeus(name = "price(USD)")]
		price_usd: Option<String>,
		#[amadeus(name = "exchangeVolume(USD)")]
		exchange_volume_usd: Option<String>,
		#[amadeus(name = "generatedCoins")]
		generated_coins: f64,
		fees: f64,
		#[amadeus(name = "activeAddresses")]
		active_addresses: u32,
		#[amadeus(name = "averageDifficulty")]
		average_difficulty: f64,
		#[amadeus(name = "paymentCount")]
		payment_count: Value,
		#[amadeus(name = "medianTxValue(USD)")]
		median_tx_value_usd: Option<String>,
		#[amadeus(name = "medianFee")]
		median_fee: Value,
		#[amadeus(name = "blockSize")]
		block_size: u32,
		#[amadeus(name = "blockCount")]
		block_count: u32,
	}

	// https://datahub.io/cryptocurrency/bitcoin
	let rows = Json::<BitcoinDerived>::new(vec![
		PathBuf::from("amadeus-testing/json/bitcoin2.json");
		tasks
	]);
	assert_eq!(
		rows.dist_iter()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool),
		3_605 * tasks
	);
	println!("a: {:?}", start.elapsed().unwrap());
	let b = SystemTime::now();

	let rows = Json::<Value>::new(vec![
		PathBuf::from("amadeus-testing/json/bitcoin2.json");
		tasks
	]);
	assert_eq!(
		rows.dist_iter()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				// println!("{:?}", value);
				let _: Result<BitcoinDerived, _> = value.clone().downcast();
				value
			}))
			.count(pool),
		3_605 * tasks
	);
	println!("b: {:?}", b.elapsed().unwrap());

	start.elapsed().unwrap()
}
