#![allow(clippy::suspicious_map)]

use std::{path::PathBuf, time::SystemTime};

use amadeus::prelude::*;

#[tokio::test]
async fn csv() {
	let start = SystemTime::now();

	let pool = &ThreadPool::new(None).unwrap();

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
		rows.par_stream()
			.map(|row: Result<_, _>| row.unwrap())
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
		rows.par_stream()
			.map(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				// println!("{:?}", value);
				let _: GameDerived2 = value.clone().downcast().unwrap();
				value
			})
			.count(pool)
			.await,
		100_000
	);

	println!("in {:?}", start.elapsed().unwrap());
}
