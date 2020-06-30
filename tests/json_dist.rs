#![type_length_limit = "1774739"]
#[cfg(feature = "constellation")]
use constellation::*;
use std::{
	path::PathBuf, time::{Duration, SystemTime}
};

use amadeus::dist::prelude::*;

fn main() {
	#[cfg(feature = "constellation")]
	init(Resources::default());

	tokio::runtime::Builder::new()
		.threaded_scheduler()
		.enable_all()
		.build()
		.unwrap()
		.block_on(async {
			let thread_pool_time = {
				let thread_pool = ThreadPool::new(None).unwrap();
				run(&thread_pool, 100).await
			};
			#[cfg(feature = "constellation")]
			let process_pool_time = {
				let process_pool = ProcessPool::new(None, None, Resources::default()).unwrap();
				run(&process_pool, 100).await
			};
			#[cfg(not(feature = "constellation"))]
			let process_pool_time = "-";

			println!("in {:?} {:?}", thread_pool_time, process_pool_time);
		})
}

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P, tasks: usize) -> Duration {
	let start = SystemTime::now();

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct BitcoinDerived {
		date: DateWithoutTimezone,
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
	let rows =
		Json::<_, BitcoinDerived>::new(vec![
			PathBuf::from("amadeus-testing/json/bitcoin2.json");
			tasks
		])
		.await
		.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		3_605 * tasks
	);
	println!("a: {:?}", start.elapsed().unwrap());
	let b = SystemTime::now();

	let rows = Json::<_, Value>::new(vec![
		PathBuf::from("amadeus-testing/json/bitcoin2.json");
		tasks
	])
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				// println!("{:?}", value);
				let _: Result<BitcoinDerived, _> = value.clone().downcast();
				value
			}))
			.count(pool)
			.await,
		3_605 * tasks
	);
	println!("b: {:?}", b.elapsed().unwrap());

	start.elapsed().unwrap()
}
