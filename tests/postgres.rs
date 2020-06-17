use std::time::SystemTime;

use amadeus::dist::prelude::*;

#[tokio::test]
async fn postgres() {
	let start = SystemTime::now();

	let pool = &ThreadPool::new(None);

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
		vec![amadeus::source::postgres::Source::Table(
			"weather".parse().unwrap(),
		)],
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

	println!("in {:?}", start.elapsed().unwrap());
}
