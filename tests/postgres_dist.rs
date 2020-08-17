#[cfg(feature = "constellation")]
use constellation::*;
use std::time::{Duration, SystemTime};

use amadeus::dist::prelude::*;

fn main() {
	if cfg!(miri) {
		return;
	}
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
		})
}

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P) -> Duration {
	let start = SystemTime::now();

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct Weather {
		city: Option<String>,
		temp_lo: Option<i32>,
		temp_hi: Option<i32>,
		prcp: Option<f32>,
		date: Option<DateWithoutTimezone>,
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
		vec![PostgresSelect::Table("weather".parse().unwrap())],
	)]);
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(&pool)
			.await,
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
	// 	rows.dist_stream()
	// 		.map(FnMut!(|row: Result<Value, _>| -> Value {
	// 			let value = row.unwrap();
	// 			// println!("{:?}", value);
	// 			// let _: GameDerived = value.clone().downcast().unwrap();
	// 			value
	// 		}))
	// 		.count(&pool).await,
	// 	4
	// );

	start.elapsed().unwrap()
}
