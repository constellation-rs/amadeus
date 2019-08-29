use amadeus::{
	data::{
		types::{Downcast, Value}, Data
	}, source::Csv, DistributedIterator, ProcessPool
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

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct GameDerived {
		a: String,
		b: String,
		c: String,
		d: String,
		e: u32,
		f: String,
	}

	let rows = Csv::<GameDerived>::new(vec![PathBuf::from("amadeus-testing/csv/game.csv")]);
	assert_eq!(
		rows.unwrap()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(&pool),
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

	let rows = Csv::<Value>::new(vec![PathBuf::from("amadeus-testing/csv/game.csv")]);
	assert_eq!(
		rows.unwrap()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				// println!("{:?}", value);
				let _: GameDerived2 = value.clone().downcast().unwrap();
				value
			}))
			.count(&pool),
		100_000
	);

	println!("in {:?}", start.elapsed().unwrap());
}
