#![type_length_limit = "1393589"]
#![allow(clippy::suspicious_map)]

use std::{path::PathBuf, time::SystemTime};

use amadeus::prelude::*;

#[tokio::test]
async fn json() {
	let start = SystemTime::now();

	let pool = &ThreadPool::new(None).unwrap();
	let tasks = 100;

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
		rows.par_stream()
			.map(|row: Result<_, _>| row.unwrap())
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
		rows.par_stream()
			.map(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				// println!("{:?}", value);
				let _: Result<BitcoinDerived, _> = value.clone().downcast();
				value
			})
			.count(pool)
			.await,
		3_605 * tasks
	);
	println!("b: {:?}", b.elapsed().unwrap());

	println!("in {:?}", start.elapsed().unwrap());
}
