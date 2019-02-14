use amadeus::{
	data::{
		types::{Date, Downcast, List, Map, Timestamp, Value}, Data
	}, source::Postgres, DistributedIterator, ProcessPool
};
use constellation::*;
use serde_closure::FnMut;
use std::{env, path::PathBuf, time::SystemTime};

fn main() {
	// init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(10);

	let start = SystemTime::now();

	// let pool = ProcessPool::new(processes, Resources::default()).unwrap();
	let pool = amadeus::no_pool::NoPool;

	#[derive(Data, Clone, PartialEq, Debug)]
	struct BitcoinDerived {
		date: Date,
		#[amadeus(rename = "txVolume(USD)")]
		tx_volume_usd: Option<String>,
		#[amadeus(rename = "adjustedTxVolume(USD)")]
		adjusted_tx_volume_usd: Option<String>,
		#[amadeus(rename = "txCount")]
		tx_count: u32,
		#[amadeus(rename = "marketcap(USD)")]
		marketcap_usd: Option<String>,
		#[amadeus(rename = "price(USD)")]
		price_usd: Option<String>,
		#[amadeus(rename = "exchangeVolume(USD)")]
		exchange_volume_usd: Option<String>,
		#[amadeus(rename = "generatedCoins")]
		generated_coins: f64,
		fees: f64,
		#[amadeus(rename = "activeAddresses")]
		active_addresses: u32,
		#[amadeus(rename = "averageDifficulty")]
		average_difficulty: f64,
		#[amadeus(rename = "paymentCount")]
		payment_count: Value,
		#[amadeus(rename = "medianTxValue(USD)")]
		median_tx_value_usd: Option<String>,
		#[amadeus(rename = "medianFee")]
		median_fee: Value,
		#[amadeus(rename = "blockSize")]
		block_size: u32,
		#[amadeus(rename = "blockCount")]
		block_count: u32,
	}

	// https://datahub.io/cryptocurrency/bitcoin
	let rows = Postgres::<BitcoinDerived>::new(vec![(
		"postgres://postgres:a@localhost/alec".parse().unwrap(),
		vec![amadeus::source::postgres::Source::Table(
			"weather".parse().unwrap(),
		)],
	)]);
	assert_eq!(
		rows.unwrap()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(&pool),
		3_605
	);

	let rows = Postgres::<Value>::new(vec![(
		"postgres://postgres:a@localhost/alec".parse().unwrap(),
		vec![amadeus::source::postgres::Source::Table(
			"weather".parse().unwrap(),
		)],
	)]);
	assert_eq!(
		rows.unwrap()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				// println!("{:?}", value);
				// let _: GameDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(&pool),
		3_605
	);
}
