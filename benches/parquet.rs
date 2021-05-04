#![cfg(nightly)]
#![feature(test)]
#![allow(clippy::suspicious_map)]

extern crate test;

use arrow_parquet::file::reader::{FileReader, SerializedFileReader};
use once_cell::sync::Lazy;
use std::{fs, fs::File, future::Future, path::PathBuf};
use test::Bencher;
use tokio::runtime::Runtime;

use amadeus::prelude::*;

static RT: Lazy<Runtime> = Lazy::new(|| {
	tokio::runtime::Builder::new()
		.threaded_scheduler()
		.enable_all()
		.build()
		.unwrap()
});
static POOL: Lazy<ThreadPool> = Lazy::new(|| ThreadPool::new(None, None).unwrap());

#[derive(Data, Clone, PartialEq, Debug)]
struct TenKayVeeTwo {
	binary_field: List<u8>,
	int32_field: i32,
	int64_field: i64,
	boolean_field: bool,
	float_field: f32,
	double_field: f64,
	flba_field: List<u8>, // [u8;1024],
	int96_field: DateTime,
}

#[derive(Data, Clone, PartialEq, Debug)]
struct StockSimulated {
	bp1: Option<f64>,
	bp2: Option<f64>,
	bp3: Option<f64>,
	bp4: Option<f64>,
	bp5: Option<f64>,
	bs1: Option<f64>,
	bs2: Option<f64>,
	bs3: Option<f64>,
	bs4: Option<f64>,
	bs5: Option<f64>,
	ap1: Option<f64>,
	ap2: Option<f64>,
	ap3: Option<f64>,
	ap4: Option<f64>,
	ap5: Option<f64>,
	as1: Option<f64>,
	as2: Option<f64>,
	as3: Option<f64>,
	as4: Option<f64>,
	as5: Option<f64>,
	valid: Option<f64>,
	__index_level_0__: Option<i64>,
}

#[bench]
fn parquet_10k(b: &mut Bencher) {
	let file = "amadeus-testing/parquet/10k-v2.parquet"; // 669,034 bytes
	run(b, file, || async {
		let rows = Parquet::<_, TenKayVeeTwo>::new(PathBuf::from(file), None)
			.await
			.unwrap();
		assert_eq!(
			rows.par_stream()
				.map(|row: Result<_, _>| row.unwrap())
				.count(&*POOL)
				.await,
			10_000
		);
	})
}

#[bench]
fn parquet_stock(b: &mut Bencher) {
	let file = "amadeus-testing/parquet/stock_simulated.parquet"; // 1,289,419 bytes
	run(b, file, || async {
		let rows = Parquet::<_, StockSimulated>::new(PathBuf::from(file), None)
			.await
			.unwrap();
		assert_eq!(
			rows.par_stream()
				.map(|row: Result<_, _>| row.unwrap())
				.count(&*POOL)
				.await,
			42_000
		);
	})
}

#[bench]
fn parquet_10k_arrow(b: &mut Bencher) {
	let file = "amadeus-testing/parquet/10k-v2.parquet"; // 669,034 bytes
	run(b, file, || async {
		let parquet_reader = SerializedFileReader::new(File::open(file).unwrap()).unwrap();
		assert_eq!(parquet_reader.get_row_iter(None).unwrap().count(), 10_000);
	})
}

#[bench]
fn parquet_stock_arrow(b: &mut Bencher) {
	let file = "amadeus-testing/parquet/stock_simulated.parquet"; // 1,289,419 bytes
	run(b, file, || async {
		let parquet_reader = SerializedFileReader::new(File::open(file).unwrap()).unwrap();
		assert_eq!(parquet_reader.get_row_iter(None).unwrap().count(), 42_000);
	})
}

fn run<F>(b: &mut Bencher, file: &str, mut task: impl FnMut() -> F)
where
	F: Future<Output = ()>,
{
	RT.enter(|| {
		let _ = Lazy::force(&POOL);
		b.bytes = fs::metadata(file).unwrap().len();
		b.iter(|| RT.handle().block_on(task()))
	})
}
