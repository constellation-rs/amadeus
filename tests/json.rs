use amadeus::{
	data::{
		types::{Date, Downcast, Value}, Data
	}, source::{Json, Source}, DistributedIterator, ProcessPool
};
use constellation::*;
use serde_closure::FnMut;
use std::{env, path::PathBuf, time::SystemTime};

fn main() {
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(10);

	let start = SystemTime::now();

	let pool = ProcessPool::new(processes, Resources::default()).unwrap();
	// let pool = amadeus::no_pool::NoPool;

	let tasks = processes * 2;

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
			.count(&pool),
		3_605 * tasks
	);

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
			.count(&pool),
		3_605 * tasks
	);

	println!("in {:?}", start.elapsed().unwrap());
}
