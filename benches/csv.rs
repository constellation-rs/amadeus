#![cfg(nightly)]
#![feature(test)]
#![allow(clippy::suspicious_map)]

extern crate test;

use once_cell::sync::Lazy;
use serde::Deserialize;
use std::{fs, future::Future, path::PathBuf};
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
static POOL: Lazy<ThreadPool> = Lazy::new(|| ThreadPool::new(None).unwrap());

#[derive(Data, Clone, Deserialize, PartialEq, PartialOrd, Debug)]
struct GameDerived {
	a: String,
	b: String,
	c: String,
	d: String,
	e: u32,
	f: String,
}

#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
struct GameDerived2 {
	a: String,
	b: String,
	c: String,
	d: String,
	e: u64,
	f: String,
}

#[bench]
fn csv_typed(b: &mut Bencher) {
	let file = "amadeus-testing/csv/game.csv"; // 2,600,000 bytes
	run(b, file, || async {
		let rows = Csv::<_, GameDerived>::new(vec![PathBuf::from(file)])
			.await
			.unwrap();
		assert_eq!(
			rows.par_stream()
				.map(|row: Result<_, _>| row.unwrap())
				.count(&*POOL)
				.await,
			100_000
		);
	})
}

#[bench]
fn csv_typed_serde(b: &mut Bencher) {
	let file = "amadeus-testing/csv/game.csv"; // 2,600,000 bytes
	run(b, file, || async {
		let mut rows = serde_csv::ReaderBuilder::new()
			.has_headers(false)
			.from_path(file)
			.unwrap();
		assert_eq!(rows.deserialize::<GameDerived>().count(), 100_000);
	});
}

#[bench]
fn csv_untyped(b: &mut Bencher) {
	let file = "amadeus-testing/csv/game.csv"; // 2,600,000 bytes
	run(b, file, || async {
		let rows = Csv::<_, Value>::new(vec![PathBuf::from(file)])
			.await
			.unwrap();
		assert_eq!(
			rows.par_stream()
				.map(|row: Result<_, _>| row.unwrap())
				.count(&*POOL)
				.await,
			100_000
		);
	})
}

#[bench]
fn csv_untyped_serde(b: &mut Bencher) {
	let file = "amadeus-testing/csv/game.csv"; // 2,600,000 bytes
	run(b, file, || async {
		let mut rows = serde_csv::ReaderBuilder::new()
			.has_headers(false)
			.from_path(file)
			.unwrap();
		assert_eq!(rows.records().count(), 100_000);
	});
}

#[bench]
fn csv_untyped_downcase(b: &mut Bencher) {
	let file = "amadeus-testing/csv/game.csv"; // 2,600,000 bytes
	run(b, file, || async {
		let rows = Csv::<_, Value>::new(vec![PathBuf::from(file)])
			.await
			.unwrap();
		assert_eq!(
			rows.par_stream()
				.map(|row: Result<_, _>| {
					let _: GameDerived2 = row.unwrap().downcast().unwrap();
				})
				.count(&*POOL)
				.await,
			100_000
		);
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
