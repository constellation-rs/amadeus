use amadeus::{
	data::{types::Date, Data}, source::Postgres, DistributedIterator, ProcessPool
};
use constellation::*;
use serde_closure::FnMut;
use std::{env, time::SystemTime};

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

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct Weather {
		city: Option<String>,
		temp_lo: Option<i32>,
		temp_hi: Option<i32>,
		prcp: Option<f32>,
		date: Option<Date>,
		invent: Option<InventoryItem>,
	}
	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct InventoryItem {
		name: Option<String>,
		supplier_id: Option<i32>,
		price: Option<f64>,
	}

	// https://datahub.io/cryptocurrency/bitcoin
	let rows = Postgres::<Weather>::new(vec![(
		"postgres://postgres:a@localhost/alec".parse().unwrap(),
		vec![amadeus::source::postgres::Source::Table(
			"weather".parse().unwrap(),
		)],
	)]);
	assert_eq!(
		rows.unwrap()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(&pool),
		4
	);

	// TODO

	// let rows = Postgres::<Value>::new(vec![(
	// 	"postgres://postgres:a@localhost/alec".parse().unwrap(),
	// 	vec![amadeus::source::postgres::Source::Table(
	// 		"weather".parse().unwrap(),
	// 	)],
	// )]);
	// assert_eq!(
	// 	rows.unwrap()
	// 		.map(FnMut!(|row: Result<Value, _>| -> Value {
	// 			let value = row.unwrap();
	// 			// println!("{:?}", value);
	// 			// let _: GameDerived = value.clone().downcast().unwrap();
	// 			value
	// 		}))
	// 		.count(&pool),
	// 	4
	// );

	println!("in {:?}", start.elapsed().unwrap());
}
