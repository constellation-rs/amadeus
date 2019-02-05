#![feature(test)]

extern crate test;

// use amadeus::prelude::*;
use amadeus::{source::Data, DistributedIterator, ProcessPool};
use constellation::*;
use parquet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, file::reader::{ParquetReader, SerializedFileReader}, record::{
		types::{Downcast, *}, Reader, Record
	}, schema::types::{ColumnDescPtr, ColumnPath, Type}
};
use std::{
	collections::HashMap, env, fmt::{self, Display}, fs::File, marker::PhantomData, path::{Path, PathBuf}, time::SystemTime
};
use test::Bencher;

#[rustfmt::skip]
fn main() {
	init(Resources::default());

	// Accept the number of processes at the command line, defaulting to 10
	let processes = env::args()
		.nth(1)
		.and_then(|arg| arg.parse::<usize>().ok())
		.unwrap_or(10);

	let start = SystemTime::now();

	let pool = ProcessPool::new(processes, Resources::default()).unwrap();

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/stock_simulated.parquet")).unwrap()).unwrap();

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct A {
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
	let rows = amadeus::source::Parquet::<A>::new(vec![PathBuf::from("./amadeus-testing/parquet/stock_simulated.parquet")]);
	println!("{}", rows.unwrap().count(&pool));

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,
		(
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<f64>,
			Option<i64>,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	#[derive(Record)]
	struct B {
		bs5: Option<f64>,
		__index_level_0__: Option<i64>,
	}
	let rows = read::<_,B>(&file);
	println!("{}", rows.unwrap().count());

	#[derive(Record)]
	struct C {
	}
	let rows = read::<_,C>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/10k-v2.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Vec<u8>,
			i32,
			i64,
			bool,
			f32,
			f64,
			[u8;1024],
			Timestamp,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/alltypes_dictionary.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<i32>,
			Option<bool>,
			Option<i32>,
			Option<i32>,
			Option<i32>,
			Option<i64>,
			Option<f32>,
			Option<f64>,
			Option<Vec<u8>>,
			Option<Vec<u8>>,
			Option<Timestamp>,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/alltypes_plain.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<i32>,
			Option<bool>,
			Option<i32>,
			Option<i32>,
			Option<i32>,
			Option<i64>,
			Option<f32>,
			Option<f64>,
			Option<Vec<u8>>,
			Option<Vec<u8>>,
			Option<Timestamp>,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/alltypes_plain.snappy.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<i32>,
			Option<bool>,
			Option<i32>,
			Option<i32>,
			Option<i32>,
			Option<i64>,
			Option<f32>,
			Option<f64>,
			Option<Vec<u8>>,
			Option<Vec<u8>>,
			Option<Timestamp>,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	// let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/nation.dict-malformed.parquet")).unwrap()).unwrap();
	// let rows = read::<_,
	// 	(
	// 		Option<i32>,
	// 		Option<Vec<u8>>,
	// 		Option<i32>,
	// 		Option<Vec<u8>>,
	// 	)
	// >(&file);
	// println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/nested_lists.snappy.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<List<Option<List<Option<List<Option<String>>>>>>>,
			i32,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/nested_maps.snappy.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<Map<String,Option<Map<i32,bool>>>>,
			i32,
			f64,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/nonnullable.impala.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			i64,
			List<i32>,
			List<List<i32>>,
			Map<String,i32>,
			List<Map<String,i32>>,
			(
				i32,
				List<i32>,
				(List<List<(i32,String)>>,),
				Map<String,((List<f64>,),)>,
			)
		)
		>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/nullable.impala.parquet")).unwrap()).unwrap();

	type Nullable = (
		Option<i64>,
		Option<List<Option<i32>>>,
		Option<List<Option<List<Option<i32>>>>>,
		Option<Map<String,Option<i32>>>,
		Option<List<Option<Map<String,Option<i32>>>>>,
		Option<(
			Option<i32>,
			Option<List<Option<i32>>>,
			Option<(Option<List<Option<List<Option<(Option<i32>,Option<String>)>>>>>,)>,
			Option<Map<String,Option<(Option<(Option<List<Option<f64>>>,)>,)>>>,
		)>
	);
	let rows = read::<_,Nullable>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().map(|x| -> Nullable { x.downcast().unwrap() }).count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/nulls.snappy.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<(Option<i32>,)>,
		)
		>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/repeated_no_annotation.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			i32,
			Option<(List<(i64,Option<String>)>,)>,
		)
		>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/datapage_v2.snappy.parquet")).unwrap()).unwrap();

	type TestDatapage = (
		Option<String>,
		i32,
		f64,
		bool,
		Option<List<i32>>,
	);
	let rows = read::<_,TestDatapage>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().map(|x| -> TestDatapage { x.downcast().unwrap() }).count());

	let file = SerializedFileReader::new(File::open(&Path::new("./amadeus-testing/parquet/commits.parquet")).unwrap()).unwrap();

	type Commits = (
		Option<String>, // id
		Option<i32>, // delay
		Option<i32>, // age
		Option<bool>, // ismerge
		Option<i32>, // squashof
		Option<String>, // author_name
		Option<String>, // author_email
		Option<String>, // committer_name
		Option<String>, // committer_email
		Option<Timestamp>, // author_time (TIMESTAMP_MILLIS)
		Option<Timestamp>, // committer_time (TIMESTAMP_MILLIS)
		Option<i64>, // loc_d
		Option<i64>, // loc_i
		Option<i64>, // comp_d
		Option<i64>, // comp_i
		Option<u16>, // nfiles
		Option<String>, // message
		Option<u16>, // ndiffs
		Option<String>, // author_email_dedup
		Option<String>, // author_name_dedup
		Option<String>, // committer_email_dedup
		Option<String>, // committer_name_dedup
		Option<i64>, // __index_level_0__
	);
	let rows = read::<_,Commits>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Row>(&file);
	println!("{}", rows.unwrap().map(|x| -> Commits { x.downcast().unwrap() }).count());
}

#[bench]
fn record_reader_10k_collect(bench: &mut Bencher) {
	let path = Path::new("./amadeus-testing/parquet/10k-v2.parquet");
	let file = File::open(&path).unwrap();
	let len = file.metadata().unwrap().len();
	let parquet_reader = SerializedFileReader::new(file).unwrap();

	bench.bytes = len;
	bench.iter(|| {
		let iter = parquet_reader.get_row_iter(None).unwrap();
		println!("{}", iter.count());
	})
}
#[bench]
fn record_reader_stock_simulated_collect(bench: &mut Bencher) {
	let path = Path::new("./amadeus-testing/parquet/stock_simulated.parquet");
	let file = File::open(&path).unwrap();
	let len = file.metadata().unwrap().len();
	let parquet_reader = SerializedFileReader::new(file).unwrap();

	bench.bytes = len;
	bench.iter(|| {
		let iter = parquet_reader.get_row_iter(None).unwrap();
		println!("{}", iter.count());
	})
}

#[bench]
fn record_reader_10k_collect_2(bench: &mut Bencher) {
	let file = File::open(&Path::new("./amadeus-testing/parquet/10k-v2.parquet")).unwrap();
	let len = file.metadata().unwrap().len();
	let parquet_reader = SerializedFileReader::new(file).unwrap();

	bench.bytes = len;
	bench.iter(|| {
		let iter =
			read2::<_, (Vec<u8>, i32, i64, bool, f32, f64, [u8; 1024], Timestamp)>(&parquet_reader);
		println!("{}", iter.unwrap().count());
	})
}
#[bench]
fn record_reader_stock_simulated_collect_2(bench: &mut Bencher) {
	let path = Path::new("./amadeus-testing/parquet/stock_simulated.parquet");
	let file = File::open(&path).unwrap();
	let len = file.metadata().unwrap().len();
	let parquet_reader = SerializedFileReader::new(file).unwrap();

	bench.bytes = len;
	bench.iter(|| {
		let iter = read2::<
			_,
			(
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<f64>,
				Option<i64>,
			),
		>(&parquet_reader);
		println!("{}", iter.unwrap().count());
	})
}

use parquet::file::reader::FileReader;

// fn read<'a, R: ParquetReader + 'static, T>(
// 	reader: &'a SerializedFileReader<R>,
// ) -> Result<impl Iterator<Item = T> + 'a, ParquetError>
// where
// 	T: Record,
// 	<Root<T> as Record>::Schema: 'a,
// 	<Root<T> as Record>::Reader: 'a,
// {
fn read<'a, R: ParquetReader + 'static, T: 'static>(
	reader: &'a SerializedFileReader<R>,
) -> Result<impl Iterator<Item = T> + 'a, ParquetError>
where
	T: Record,
	// <Root<T> as Record>::Schema: 'a,
	// <Root<T> as Record>::Reader: 'a,
{
	reader
		.get_row_iter(None)
		.map(|iter| iter.map(Result::unwrap))
	// let file_schema = reader.metadata().file_metadata().schema_descr_ptr();
	// let file_schema = file_schema.root_schema();
	// let schema = <Root<T> as Record>::parse(file_schema).map_err(|err| {
	// 	// let schema: Type = <Root<T> as Record>::render("", &<Root<T> as Record>::placeholder());
	// 	let mut b = Vec::new();
	// 	print_schema(&mut b, file_schema);
	// 	// let mut a = Vec::new();
	// 	// print_schema(&mut a, &schema);
	// 	ParquetError::General(format!(
	// 		"Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
	// 		String::from_utf8(b).unwrap(),
	// 		// String::from_utf8(a).unwrap(),
	// 		DisplayDisplayType::<<Root<T> as Record>::Schema>::new(),
	// 		err
	// 	))
	// }).unwrap().1;
	// // let dyn_schema = <Root<T>>::render("", &schema);
	// // print_schema(&mut std::io::stdout(), &dyn_schema);
	// // assert!(file_schema.check_contains(&dyn_schema));
	// // println!("{:#?}", schema);
	// // let iter = reader.get_row_iter(None).unwrap();
	// // println!("{:?}", iter.count());
	// // print_parquet_metadata(&mut std::io::stdout(), &reader.metadata());
	// {
	// 	// println!("file: {:#?}", reader.metadata().file_metadata());
	// 	// print_file_metadata(&mut std::io::stdout(), &*reader.metadata().file_metadata());
	// 	let schema = reader.metadata().file_metadata().schema_descr_ptr().clone();
	// 	let schema = schema.root_schema();
	// 	// println!("{:#?}", schema);
	// 	print_schema(&mut std::io::stdout(), &schema);
	// 	// let mut iter = reader.get_row_iter(None).unwrap();
	// 	// while let Some(record) = iter.next() {
	// 	// 	// See record API for different field accessors
	// 	// 	// println!("{}", record);
	// 	// }
	// }
	// // print_parquet_metadata(&mut std::io::stdout(), reader.metadata());
	// // println!("file: {:#?}", reader.metadata().file_metadata());
	// // println!("file: {:#?}", reader.metadata().row_groups());

	// // let descr = Rc::new(SchemaDescriptor::new(Rc::new(dyn_schema)));

	// // let tree_builder = parquet::record::reader::TreeBuilder::new();
	// let schema = Rc::new(schema); // TODO!
	// Ok((0..reader.num_row_groups()).flat_map(move |i| {
	// 	// let schema = &schema;
	// 	let row_group = reader.get_row_group(i).unwrap();

	// 	let mut paths: HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)> = HashMap::new();
	// 	let row_group_metadata = row_group.metadata();

	// 	for col_index in 0..row_group.num_columns() {
	// 		let col_meta = row_group_metadata.column(col_index);
	// 		let col_path = col_meta.column_path().clone();
	// 		// println!("path: {:?}", col_path);
	// 		let col_descr = row_group
	// 			.metadata()
	// 			.column(col_index)
	// 			.column_descr_ptr();
	// 		let col_reader = row_group.get_column_reader(col_index).unwrap();

	// 		let x = paths.insert(col_path, (col_descr, col_reader));
	// 		assert!(x.is_none());
	// 	}

	// 	let mut path = Vec::new();

	// 	let mut reader = <Root<T>>::reader(&schema, &mut path, 0, 0, &mut paths);

	// 	// let mut reader = tree_builder.build(descr.clone(), &*row_group);
	// 	reader.advance_columns();
	// 	// for row in tree_builder.as_iter(descr.clone(), &*row_group) {
	// 	// 	println!("{:?}", row);
	// 	// }
	// 	// std::iter::empty()
	// 	// println!("{:?}", reader.read());
	// 	let schema = schema.clone();
	// 	(0..row_group.metadata().num_rows()).map(move |_| {
	// 		// println!("row");
	// 		reader.read().unwrap().0
	// 		// unimplemented!()
	// 		// <Root<T>>::read(&schema, &mut reader).unwrap().0
	// 	})
	// }))
}
fn write<R: ParquetReader + 'static, T>(reader: R, schema: <Root<T> as Record>::Schema) -> ()
where
	T: Record,
{
	// let schema = <Root<T>>::render("", &schema);
	// print_schema(&mut std::io::stdout(), &schema);
	// println!("{:#?}", schema);
	let reader = SerializedFileReader::new(reader).unwrap();
	// let iter = reader.get_row_iter(None).unwrap();
	// println!("{:?}", iter.count());
	// print_parquet_metadata(&mut std::io::stdout(), &reader.metadata());
	unimplemented!()
}
