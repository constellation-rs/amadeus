#![feature(maybe_uninit,try_from,test,box_patterns,transpose_result,existential_type,specialization)]

extern crate test;

use parquet::{
	basic::{LogicalType, Repetition, Type as PhysicalType}, errors::ParquetError, file::reader::{FileReader, ParquetReader, SerializedFileReader}, record::{reader::{BoolReader,I32Reader,I64Reader,I96Reader,F32Reader,F64Reader,ByteArrayReader,FixedLenByteArrayReader,OptionReader,RepeatedReader,KeyValueReader,RRReader}}, schema::{
		printer::{print_file_metadata, print_parquet_metadata, print_schema}, types::{BasicTypeInfo, SchemaDescPtr, SchemaDescriptor, Type}
	}
};
use parquet::record::TypedTripletIter;
use parquet::data_type::{BoolType,Int32Type,Int64Type,Int96Type,FloatType,DoubleType,ByteArrayType,FixedLenByteArrayType};
use parquet::column::reader::ColumnReader;
use parquet::schema::types::ColumnDescPtr;
use parquet::schema::types::ColumnPath;
use parquet::data_type::Int96;
use parquet::schema::parser::parse_message_type;
use std::{fs::File, path::Path, rc::Rc, str, fmt, fmt::Debug};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::convert::TryInto;
use std::num::TryFromIntError;
use test::Bencher;
use std::convert::TryFrom;
use either::Either;
use std::string::FromUtf8Error;
use std::error::Error;
use std::hash::Hasher;

struct DebugDebugType<T>(PhantomData<fn(T)>) where T: DebugType;
impl<T> DebugDebugType<T> where T: DebugType {
	fn new() -> Self {
		Self(PhantomData)
	}
}
impl<T> Debug for DebugDebugType<T> where T: DebugType {
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		T::fmt(f)
	}
}

macro_rules! impl_parquet_deserialize_struct {
	($struct:ident $struct_schema:ident $struct_reader:ident $($name:ident: $type_:ty,)*) => (
		#[derive(Debug)]
		struct $struct_schema {
			$($name: <$type_ as Deserialize>::Schema,)*
		}
		impl DebugType for $struct_schema {
			fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
				f.debug_struct(stringify!($struct_schema))
					$(.field(stringify!($name), &DebugDebugType::<<$type_ as Deserialize>::Schema>::new()))*
					.finish()
			}
		}
		struct $struct_reader {
			$($name: <$type_ as Deserialize>::Reader,)*
		}
		impl RRReader for $struct_reader {
			type Item = $struct;

			fn read_field(&mut self) -> Result<Self::Item, ParquetError> {
				Ok($struct {
					$($name: self.$name.read_field()?,)*
				})
			}
			fn advance_columns(&mut self) {
				$(self.$name.advance_columns();)*
			}
			fn has_next(&self) -> bool {
				// self.$first_name.has_next()
				$(self.$name.has_next() &&)* true
			}
			fn current_def_level(&self) -> i16 {
				$(if true { self.$name.current_def_level() } else)*
				{
					panic!("Current definition level: empty group reader")
				}
			}
			fn current_rep_level(&self) -> i16 {
				$(if true { self.$name.current_rep_level() } else)*
				{
					panic!("Current repetition level: empty group reader")
				}
			}
		}
		impl Deserialize for $struct {
			type Schema = $struct_schema;
			type Reader = $struct_reader;

			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED {
					let fields = schema.get_fields().iter().map(|field|(field.name(),field)).collect::<HashMap<_,_>>();
					let schema_ = $struct_schema{$($name: fields.get(stringify!($name)).ok_or(ParquetError::General(format!("Struct {} missing field {}", stringify!($struct), stringify!($name)))).and_then(|x|<$type_ as Deserialize>::parse(&**x))?.1,)*};
					return Ok((schema.name().to_owned(), schema_))
				}
				Err(ParquetError::General(format!("Struct {}", stringify!($struct))))
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
				$(
					path.push(stringify!($name).to_owned());
					let $name = <$type_ as Deserialize>::reader(&schema.$name, path, curr_def_level, curr_rep_level, paths);
					path.pop().unwrap();
				)*
				$struct_reader { $($name,)* }
			}
		}
		impl Deserialize for Root<$struct> {
			type Schema = RootSchema<$struct, $struct_schema>;
			type Reader = RootReader<$struct_reader>;

			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_schema() {
					let fields = schema.get_fields().iter().map(|field|(field.name(),field)).collect::<HashMap<_,_>>();
					let schema_ = $struct_schema{$($name: fields.get(stringify!($name)).ok_or(ParquetError::General(format!("Struct {} missing field {}", stringify!($struct), stringify!($name)))).and_then(|x|<$type_ as Deserialize>::parse(&**x))?.1,)*};
					return Ok((String::from(""), RootSchema(schema.name().to_owned(), schema_, PhantomData)))
				}
				Err(ParquetError::General(format!("Struct {}", stringify!($struct))))
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
				RootReader(<$struct as Deserialize>::reader(&schema.1, path, curr_def_level, curr_rep_level, paths))
			}
		}
	);
}

#[rustfmt::skip]
fn main() {
	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap()).unwrap();

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
	impl_parquet_deserialize_struct!(A ASchema AReader
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
	);
	let rows = read::<_,A>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
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

	struct B {
		bs5: Option<f64>,
		__index_level_0__: Option<i64>,
	}
	impl_parquet_deserialize_struct!(B BSchema BReader
		bs5: Option<f64>,
		__index_level_0__: Option<i64>,
	);
	let rows = read::<_,B>(&file);
	println!("{}", rows.unwrap().count());

	struct C {
	}
	impl_parquet_deserialize_struct!(C CSchema CReader
	);
	let rows = read::<_,C>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/10k-v2.parquet")).unwrap()).unwrap();

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

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/alltypes_dictionary.parquet")).unwrap()).unwrap();

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

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/alltypes_plain.parquet")).unwrap()).unwrap();

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

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/alltypes_plain.snappy.parquet")).unwrap()).unwrap();

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

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	// let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/nation.dict-malformed.parquet")).unwrap()).unwrap();
	// let rows = read::<_,
	// 	(
	// 		Option<i32>,
	// 		Option<Vec<u8>>,
	// 		Option<i32>,
	// 		Option<Vec<u8>>,
	// 	)
	// >(&file);
	// println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/nested_lists.snappy.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<List<Option<List<Option<List<Option<String>>>>>>>,
			i32,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/nested_maps.snappy.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<Map<String,Option<Map<i32,bool>>>>,
			i32,
			f64,
		)
	>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/nonnullable.impala.parquet")).unwrap()).unwrap();

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

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/nullable.impala.parquet")).unwrap()).unwrap();

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

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().map(|x| -> Nullable { x.downcast().unwrap() }).count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/nulls.snappy.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			Option<(Option<i32>,)>,
		)
		>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/repeated_no_annotation.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
			i32,
			Option<(List<(i64,Option<String>)>,)>,
		)
		>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/test_datapage_v2.snappy.parquet")).unwrap()).unwrap();

	type TestDatapage = (
		Option<String>,
		i32,
		f64,
		bool,
		Option<List<i32>>,
	);
	let rows = read::<_,TestDatapage>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().map(|x| -> TestDatapage { x.downcast().unwrap() }).count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/commits.parquet")).unwrap()).unwrap();

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

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().map(|x| -> Commits { x.downcast().unwrap() }).count());
}

#[bench]
fn record_reader_10k_collect(bench: &mut Bencher) {
	let path = Path::new("./parquet-rs/data/10k-v2.parquet");
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
	let path = Path::new("./parquet-rs/data/stock_simulated.parquet");
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
	let file = File::open(&Path::new("./parquet-rs/data/10k-v2.parquet")).unwrap();
	let len = file.metadata().unwrap().len();
	let parquet_reader = SerializedFileReader::new(file).unwrap();

	bench.bytes = len;
	bench.iter(|| {
	let iter = read2::<_,
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
	>(&parquet_reader);
		println!("{}", iter.unwrap().count());
	})
}
#[bench]
fn record_reader_stock_simulated_collect_2(bench: &mut Bencher) {
	let path = Path::new("./parquet-rs/data/stock_simulated.parquet");
	let file = File::open(&path).unwrap();
	let len = file.metadata().unwrap().len();
	let parquet_reader = SerializedFileReader::new(file).unwrap();

	bench.bytes = len;
	bench.iter(|| {
		let iter = read2::<_,
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
		>(&parquet_reader);
		println!("{}", iter.unwrap().count());
	})
}


#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct Root<T>(T);
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct Date(i32);
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct Time(i64);

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const SECONDS_PER_DAY: i64 = 86_400;
const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct Timestamp(Int96);
impl Timestamp {
	fn as_day_nanos(&self) -> (i64,i64) {
		let day = self.0.data()[2] as i64;
		let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
		(day, nanoseconds)
	}
	fn as_millis(&self) -> Option<i64> {
		let day = self.0.data()[2] as i64;
		let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
		let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
		Some(seconds * MILLIS_PER_SECOND + nanoseconds / 1_000_000)
	}
	fn as_micros(&self) -> Option<i64> {
		let day = self.0.data()[2] as i64;
		let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
		let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
		Some(seconds * MILLIS_PER_SECOND * MICROS_PER_MILLI + nanoseconds / 1_000)
	}
	fn as_nanos(&self) -> Option<i64> {
		let day = self.0.data()[2] as i64;
		let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
		let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
		Some(seconds * MILLIS_PER_SECOND * MICROS_PER_MILLI * 1_000 + nanoseconds)
	}
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct List<T>(Vec<T>);
#[derive(Clone, PartialEq, Eq, Debug)]
struct Map<K: Hash + Eq,V>(HashMap<K,V>);

#[derive(Clone, PartialEq, Debug)]
enum Value {
	Bool(bool),
	U8(u8),
	I8(i8),
	U16(u16),
	I16(i16),
	U32(u32),
	I32(i32),
	U64(u64),
	I64(i64),
	F32(f32),
	F64(f64),
	Timestamp(Timestamp),
	Array(Vec<u8>),
	String(String),
	List(List<Value>),
	Map(Map<Value,Value>),
	Group(Group),
	Option(Box<Option<Value>>),
}
impl Hash for Value {
	fn hash<H: Hasher>(&self, state: &mut H) {
		match self {
			Value::Bool(value) => {0u8.hash(state); value.hash(state);}
			Value::U8(value) => {1u8.hash(state); value.hash(state);}
			Value::I8(value) => {2u8.hash(state); value.hash(state);}
			Value::U16(value) => {3u8.hash(state); value.hash(state);}
			Value::I16(value) => {4u8.hash(state); value.hash(state);}
			Value::U32(value) => {5u8.hash(state); value.hash(state);}
			Value::I32(value) => {6u8.hash(state); value.hash(state);}
			Value::U64(value) => {7u8.hash(state); value.hash(state);}
			Value::I64(value) => {8u8.hash(state); value.hash(state);}
			Value::F32(value) => {9u8.hash(state);}
			Value::F64(value) => {10u8.hash(state);}
			Value::Timestamp(value) => {11u8.hash(state); value.hash(state);}
			Value::Array(value) => {12u8.hash(state); value.hash(state);}
			Value::String(value) => {13u8.hash(state); value.hash(state);}
			Value::List(value) => {14u8.hash(state); value.hash(state);}
			Value::Map(value) => {15u8.hash(state);}
			Value::Group(value) => {16u8.hash(state);}
			Value::Option(value) => {17u8.hash(state); value.hash(state);}
		}
	}
}
impl Eq for Value {}

impl Value {
	fn is_bool(&self) -> bool {
		if let Value::Bool(ret) = self { true } else { false }
	}
	fn as_bool(self) -> Result<bool, ParquetError> {
		if let Value::Bool(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_u8(&self) -> bool {
		if let Value::U8(ret) = self { true } else { false }
	}
	fn as_u8(self) -> Result<u8, ParquetError> {
		if let Value::U8(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_i8(&self) -> bool {
		if let Value::I8(ret) = self { true } else { false }
	}
	fn as_i8(self) -> Result<i8, ParquetError> {
		if let Value::I8(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_u16(&self) -> bool {
		if let Value::U16(ret) = self { true } else { false }
	}
	fn as_u16(self) -> Result<u16, ParquetError> {
		if let Value::U16(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_i16(&self) -> bool {
		if let Value::I16(ret) = self { true } else { false }
	}
	fn as_i16(self) -> Result<i16, ParquetError> {
		if let Value::I16(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_u32(&self) -> bool {
		if let Value::U32(ret) = self { true } else { false }
	}
	fn as_u32(self) -> Result<u32, ParquetError> {
		if let Value::U32(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_i32(&self) -> bool {
		if let Value::I32(ret) = self { true } else { false }
	}
	fn as_i32(self) -> Result<i32, ParquetError> {
		if let Value::I32(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_u64(&self) -> bool {
		if let Value::U64(ret) = self { true } else { false }
	}
	fn as_u64(self) -> Result<u64, ParquetError> {
		if let Value::U64(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_i64(&self) -> bool {
		if let Value::I64(ret) = self { true } else { false }
	}
	fn as_i64(self) -> Result<i64, ParquetError> {
		if let Value::I64(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_f32(&self) -> bool {
		if let Value::F32(ret) = self { true } else { false }
	}
	fn as_f32(self) -> Result<f32, ParquetError> {
		if let Value::F32(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_f64(&self) -> bool {
		if let Value::F64(ret) = self { true } else { false }
	}
	fn as_f64(self) -> Result<f64, ParquetError> {
		if let Value::F64(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_timestamp(&self) -> bool {
		if let Value::Timestamp(ret) = self { true } else { false }
	}
	fn as_timestamp(self) -> Result<Timestamp, ParquetError> {
		if let Value::Timestamp(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_array(&self) -> bool {
		if let Value::Array(ret) = self { true } else { false }
	}
	fn as_array(self) -> Result<Vec<u8>, ParquetError> {
		if let Value::Array(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_string(&self) -> bool {
		if let Value::String(ret) = self { true } else { false }
	}
	fn as_string(self) -> Result<String, ParquetError> {
		if let Value::String(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_list(&self) -> bool {
		if let Value::List(ret) = self { true } else { false }
	}
	fn as_list(self) -> Result<List<Value>, ParquetError> {
		if let Value::List(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_map(&self) -> bool {
		if let Value::Map(ret) = self { true } else { false }
	}
	fn as_map(self) -> Result<Map<Value,Value>, ParquetError> {
		if let Value::Map(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_group(&self) -> bool {
		if let Value::Group(ret) = self { true } else { false }
	}
	fn as_group(self) -> Result<Group, ParquetError> {
		if let Value::Group(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_option(&self) -> bool {
		if let Value::Option(ret) = self { true } else { false }
	}
	fn as_option(self) -> Result<Option<Value>, ParquetError> {
		if let Value::Option(ret) = self { Ok(*ret) } else { Err(ParquetError::General(String::from(""))) }
	}
}

trait Downcast<T> {
	fn downcast(self) -> Result<T,ParquetError>;
}
impl Downcast<Value> for Value {
	fn downcast(self) -> Result<Value,ParquetError> {
		Ok(self)
	}
}
impl Downcast<bool> for Value {
	fn downcast(self) -> Result<bool,ParquetError> {
		self.as_bool()
	}
}
impl Downcast<u8> for Value {
	fn downcast(self) -> Result<u8,ParquetError> {
		self.as_u8()
	}
}
impl Downcast<i8> for Value {
	fn downcast(self) -> Result<i8,ParquetError> {
		self.as_i8()
	}
}
impl Downcast<u16> for Value {
	fn downcast(self) -> Result<u16,ParquetError> {
		self.as_u16()
	}
}
impl Downcast<i16> for Value {
	fn downcast(self) -> Result<i16,ParquetError> {
		self.as_i16()
	}
}
impl Downcast<u32> for Value {
	fn downcast(self) -> Result<u32,ParquetError> {
		self.as_u32()
	}
}
impl Downcast<i32> for Value {
	fn downcast(self) -> Result<i32,ParquetError> {
		self.as_i32()
	}
}
impl Downcast<u64> for Value {
	fn downcast(self) -> Result<u64,ParquetError> {
		self.as_u64()
	}
}
impl Downcast<i64> for Value {
	fn downcast(self) -> Result<i64,ParquetError> {
		self.as_i64()
	}
}
impl Downcast<f32> for Value {
	fn downcast(self) -> Result<f32,ParquetError> {
		self.as_f32()
	}
}
impl Downcast<f64> for Value {
	fn downcast(self) -> Result<f64,ParquetError> {
		self.as_f64()
	}
}
impl Downcast<Timestamp> for Value {
	fn downcast(self) -> Result<Timestamp,ParquetError> {
		self.as_timestamp()
	}
}
impl Downcast<Vec<u8>> for Value {
	fn downcast(self) -> Result<Vec<u8>,ParquetError> {
		self.as_array()
	}
}
impl Downcast<String> for Value {
	fn downcast(self) -> Result<String,ParquetError> {
		self.as_string()
	}
}
impl<T> Downcast<List<T>> for Value where Value: Downcast<T> {
	default fn downcast(self) -> Result<List<T>,ParquetError> {
		let ret = self.as_list()?;
		ret.0.into_iter().map(Downcast::downcast).collect::<Result<Vec<_>,_>>().map(List)
	}
}
impl Downcast<List<Value>> for Value {
	fn downcast(self) -> Result<List<Value>,ParquetError> {
		self.as_list()
	}
}
impl<K,V> Downcast<Map<K,V>> for Value where Value: Downcast<K> + Downcast<V>, K: Hash+Eq {
	default fn downcast(self) -> Result<Map<K,V>,ParquetError> {
		let ret = self.as_map()?;
		ret.0.into_iter().map(|(k,v)|Ok((k.downcast()?,v.downcast()?))).collect::<Result<HashMap<_,_>,_>>().map(Map)
	}
}
impl Downcast<Map<Value,Value>> for Value {
	fn downcast(self) -> Result<Map<Value,Value>,ParquetError> {
		self.as_map()
	}
}
impl Downcast<Group> for Value {
	fn downcast(self) -> Result<Group,ParquetError> {
		self.as_group()
	}
}
impl<T> Downcast<Option<T>> for Value where Value: Downcast<T> {
	default fn downcast(self) -> Result<Option<T>,ParquetError> {
		let ret = self.as_option()?;
		match ret {
			Some(t) => Downcast::<T>::downcast(t).map(Some),
			None => Ok(None),
		}
	}
}
impl Downcast<Option<Value>> for Value {
	fn downcast(self) -> Result<Option<Value>,ParquetError> {
		self.as_option()
	}
}


#[derive(Debug)]
enum ValueSchema {
	Bool(BoolSchema),
	U8(U8Schema),
	I8(I8Schema),
	U16(U16Schema),
	I16(I16Schema),
	U32(U32Schema),
	I32(I32Schema),
	U64(U64Schema),
	I64(I64Schema),
	F32(F32Schema),
	F64(F64Schema),
	Timestamp(TimestampSchema),
	Array(VecSchema),
	String(StringSchema),
	List(Box<ListSchema<ValueSchema>>),
	Map(Box<MapSchema<ValueSchema,ValueSchema>>),
	Group(GroupSchema),
	Option(Box<OptionSchema<ValueSchema>>),
}
impl DebugType for ValueSchema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("ValueSchema")
	}
}
impl ValueSchema {
	fn is_bool(&self) -> bool {
		if let ValueSchema::Bool(ret) = self { true } else { false }
	}
	fn as_bool(self) -> Result<BoolSchema, ParquetError> {
		if let ValueSchema::Bool(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_u8(&self) -> bool {
		if let ValueSchema::U8(ret) = self { true } else { false }
	}
	fn as_u8(self) -> Result<U8Schema, ParquetError> {
		if let ValueSchema::U8(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_i8(&self) -> bool {
		if let ValueSchema::I8(ret) = self { true } else { false }
	}
	fn as_i8(self) -> Result<I8Schema, ParquetError> {
		if let ValueSchema::I8(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_u16(&self) -> bool {
		if let ValueSchema::U16(ret) = self { true } else { false }
	}
	fn as_u16(self) -> Result<U16Schema, ParquetError> {
		if let ValueSchema::U16(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_i16(&self) -> bool {
		if let ValueSchema::I16(ret) = self { true } else { false }
	}
	fn as_i16(self) -> Result<I16Schema, ParquetError> {
		if let ValueSchema::I16(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_u32(&self) -> bool {
		if let ValueSchema::U32(ret) = self { true } else { false }
	}
	fn as_u32(self) -> Result<U32Schema, ParquetError> {
		if let ValueSchema::U32(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_i32(&self) -> bool {
		if let ValueSchema::I32(ret) = self { true } else { false }
	}
	fn as_i32(self) -> Result<I32Schema, ParquetError> {
		if let ValueSchema::I32(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_u64(&self) -> bool {
		if let ValueSchema::U64(ret) = self { true } else { false }
	}
	fn as_u64(self) -> Result<U64Schema, ParquetError> {
		if let ValueSchema::U64(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_i64(&self) -> bool {
		if let ValueSchema::I64(ret) = self { true } else { false }
	}
	fn as_i64(self) -> Result<I64Schema, ParquetError> {
		if let ValueSchema::I64(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_f32(&self) -> bool {
		if let ValueSchema::F32(ret) = self { true } else { false }
	}
	fn as_f32(self) -> Result<F32Schema, ParquetError> {
		if let ValueSchema::F32(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_f64(&self) -> bool {
		if let ValueSchema::F64(ret) = self { true } else { false }
	}
	fn as_f64(self) -> Result<F64Schema, ParquetError> {
		if let ValueSchema::F64(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_timestamp(&self) -> bool {
		if let ValueSchema::Timestamp(ret) = self { true } else { false }
	}
	fn as_timestamp(self) -> Result<TimestampSchema, ParquetError> {
		if let ValueSchema::Timestamp(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_array(&self) -> bool {
		if let ValueSchema::Array(ret) = self { true } else { false }
	}
	fn as_array(self) -> Result<VecSchema, ParquetError> {
		if let ValueSchema::Array(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_string(&self) -> bool {
		if let ValueSchema::String(ret) = self { true } else { false }
	}
	fn as_string(self) -> Result<StringSchema, ParquetError> {
		if let ValueSchema::String(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_list(&self) -> bool {
		if let ValueSchema::List(ret) = self { true } else { false }
	}
	fn as_list(self) -> Result<ListSchema<ValueSchema>, ParquetError> {
		if let ValueSchema::List(ret) = self { Ok(*ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_map(&self) -> bool {
		if let ValueSchema::Map(ret) = self { true } else { false }
	}
	fn as_map(self) -> Result<MapSchema<ValueSchema,ValueSchema>, ParquetError> {
		if let ValueSchema::Map(ret) = self { Ok(*ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_group(&self) -> bool {
		if let ValueSchema::Group(ret) = self { true } else { false }
	}
	fn as_group(self) -> Result<GroupSchema, ParquetError> {
		if let ValueSchema::Group(ret) = self { Ok(ret) } else { Err(ParquetError::General(String::from(""))) }
	}
	fn is_option(&self) -> bool {
		if let ValueSchema::Option(ret) = self { true } else { false }
	}
	fn as_option(self) -> Result<OptionSchema<ValueSchema>, ParquetError> {
		if let ValueSchema::Option(ret) = self { Ok(*ret) } else { Err(ParquetError::General(String::from(""))) }
	}
}

impl Downcast<ValueSchema> for ValueSchema {
	fn downcast(self) -> Result<ValueSchema,ParquetError> {
		Ok(self)
	}
}
impl Downcast<BoolSchema> for ValueSchema {
	fn downcast(self) -> Result<BoolSchema,ParquetError> {
		self.as_bool()
	}
}
impl Downcast<U8Schema> for ValueSchema {
	fn downcast(self) -> Result<U8Schema,ParquetError> {
		self.as_u8()
	}
}
impl Downcast<I8Schema> for ValueSchema {
	fn downcast(self) -> Result<I8Schema,ParquetError> {
		self.as_i8()
	}
}
impl Downcast<U16Schema> for ValueSchema {
	fn downcast(self) -> Result<U16Schema,ParquetError> {
		self.as_u16()
	}
}
impl Downcast<I16Schema> for ValueSchema {
	fn downcast(self) -> Result<I16Schema,ParquetError> {
		self.as_i16()
	}
}
impl Downcast<U32Schema> for ValueSchema {
	fn downcast(self) -> Result<U32Schema,ParquetError> {
		self.as_u32()
	}
}
impl Downcast<I32Schema> for ValueSchema {
	fn downcast(self) -> Result<I32Schema,ParquetError> {
		self.as_i32()
	}
}
impl Downcast<U64Schema> for ValueSchema {
	fn downcast(self) -> Result<U64Schema,ParquetError> {
		self.as_u64()
	}
}
impl Downcast<I64Schema> for ValueSchema {
	fn downcast(self) -> Result<I64Schema,ParquetError> {
		self.as_i64()
	}
}
impl Downcast<F32Schema> for ValueSchema {
	fn downcast(self) -> Result<F32Schema,ParquetError> {
		self.as_f32()
	}
}
impl Downcast<F64Schema> for ValueSchema {
	fn downcast(self) -> Result<F64Schema,ParquetError> {
		self.as_f64()
	}
}
impl Downcast<TimestampSchema> for ValueSchema {
	fn downcast(self) -> Result<TimestampSchema,ParquetError> {
		self.as_timestamp()
	}
}
impl Downcast<VecSchema> for ValueSchema {
	fn downcast(self) -> Result<VecSchema,ParquetError> {
		self.as_array()
	}
}
impl Downcast<StringSchema> for ValueSchema {
	fn downcast(self) -> Result<StringSchema,ParquetError> {
		self.as_string()
	}
}
impl<T> Downcast<ListSchema<T>> for ValueSchema where ValueSchema: Downcast<T> {
	default fn downcast(self) -> Result<ListSchema<T>,ParquetError> {
		let ret = self.as_list()?;
		Ok(ListSchema(ret.0.downcast()?, ret.1))
	}
}
impl Downcast<ListSchema<ValueSchema>> for ValueSchema {
	fn downcast(self) -> Result<ListSchema<ValueSchema>,ParquetError> {
		self.as_list()
	}
}
impl<K,V> Downcast<MapSchema<K,V>> for ValueSchema where ValueSchema: Downcast<K> + Downcast<V> {
	default fn downcast(self) -> Result<MapSchema<K,V>,ParquetError> {
		let ret = self.as_map()?;
		Ok(MapSchema(ret.0.downcast()?, ret.1.downcast()?, ret.2, ret.3, ret.4))
	}
}
impl Downcast<MapSchema<ValueSchema,ValueSchema>> for ValueSchema {
	fn downcast(self) -> Result<MapSchema<ValueSchema,ValueSchema>,ParquetError> {
		self.as_map()
	}
}
impl Downcast<GroupSchema> for ValueSchema {
	fn downcast(self) -> Result<GroupSchema,ParquetError> {
		self.as_group()
	}
}
impl<T> Downcast<OptionSchema<T>> for ValueSchema where ValueSchema: Downcast<T> {
	default fn downcast(self) -> Result<OptionSchema<T>,ParquetError> {
		let ret = self.as_option()?;
		ret.0.downcast().map(OptionSchema)
	}
}
impl Downcast<OptionSchema<ValueSchema>> for ValueSchema {
	fn downcast(self) -> Result<OptionSchema<ValueSchema>,ParquetError> {
		self.as_option()
	}
}


#[derive(Clone, PartialEq, Debug)]
struct Group(pub Vec<Value>,pub Rc<HashMap<String,usize>>);
type Row = Group;
#[derive(Debug)]
struct GroupSchema(Vec<ValueSchema>,HashMap<String,usize>);
impl DebugType for GroupSchema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("GroupSchema")
	}
}

impl Deserialize for Group {
	type Schema = GroupSchema;
	type Reader = GroupReader;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED {
			let mut map = HashMap::new();
			let fields = schema.get_fields().iter().enumerate().map(|(i,field)| {
				let (name, schema) = <Value as Deserialize>::parse(&**field)?;
				let x = map.insert(name, i);
				assert!(x.is_none());
				Ok(schema)
			}).collect::<Result<Vec<ValueSchema>,ParquetError>>()?;
			let schema_ = GroupSchema(fields, map);//$struct_schema{$($name: fields.get(stringify!($name)).ok_or(ParquetError::General(format!("Struct {} missing field {}", stringify!($struct), stringify!($name)))).and_then(|x|<$type_ as Deserialize>::parse(&**x))?.1,)*};
			return Ok((schema.name().to_owned(), schema_))
		}
		Err(ParquetError::General(format!("Struct {}", stringify!($struct))))
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let mut names_ = vec![None; schema.0.len()];
		for (name,&index) in schema.1.iter() {
			names_[index].replace(name.to_owned());
		}
		let readers = schema.0.iter().enumerate().map(|(i,field)| {
			path.push(names_[i].take().unwrap());
			let ret = Value::reader(field, path, curr_def_level, curr_rep_level, paths);
			path.pop().unwrap();
			ret
		}).collect();
		GroupReader{def_level: curr_def_level, readers, fields: Rc::new(schema.1.clone())}
	}
}

struct GroupReader {
  def_level: i16,
  readers: Vec<ValueReader>,
  fields: Rc<HashMap<String,usize>>,
}
impl RRReader for GroupReader {
  type Item = Group;

  fn read_field(&mut self) -> Result<Self::Item, ParquetError> {
    let mut fields = Vec::new();
    for reader in self.readers.iter_mut() {
      fields.push(reader.read_field()?);
    }
    Ok(Group(fields, self.fields.clone()))
  }
  fn advance_columns(&mut self) {
    for reader in self.readers.iter_mut() {
      reader.advance_columns();
    }
  }
  fn has_next(&self) -> bool {
    self.readers.first().unwrap().has_next()
  }
  fn current_def_level(&self) -> i16 {
    match self.readers.first() {
        Some(reader) => reader.current_def_level(),
        None => panic!("Current definition level: empty group reader"),
      }
  }
  fn current_rep_level(&self) -> i16 {
    match self.readers.first() {
      Some(reader) => reader.current_rep_level(),
      None => panic!("Current repetition level: empty group reader"),
    }
  }
}

trait DebugType {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error>;
}

struct RootSchema<T,S>(String, S, PhantomData<fn(T)>);
impl<T,S> Debug for RootSchema<T,S> where S: Debug {
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.debug_tuple("RootSchema").field(&self.0).field(&self.1).finish()
	}
}
impl<T,S> DebugType for RootSchema<T,S> where S: DebugType {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("RootSchema")
	}
}
#[derive(Debug)]
struct VecSchema(Option<u32>);
impl DebugType for VecSchema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("VecSchema")
	}
}
struct ArraySchema<T>(PhantomData<fn(T)>);
impl<T> Debug for ArraySchema<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.debug_tuple("ArraySchema").finish()
	}
}
impl<T> DebugType for ArraySchema<T> {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("ArraySchema<T>")
	}
}
struct TupleSchema<T>(T);
struct TupleReader<T>(T);
#[derive(Debug)]
struct MapSchema<K,V>(K,V,Option<String>,Option<String>,Option<String>);
impl<K,V> DebugType for MapSchema<K,V> where K: DebugType, V: DebugType {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("MapSchema")
	}
}
#[derive(Debug)]
struct OptionSchema<T>(T);
impl<T> DebugType for OptionSchema<T> where T: DebugType {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("OptionSchema")
	}
}
#[derive(Debug)]
struct ListSchema<T>(T,Option<(Option<String>,Option<String>)>);
impl<T> DebugType for ListSchema<T> where T: DebugType {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("ListSchema")
	}
}
#[derive(Debug)]
struct BoolSchema;
impl DebugType for BoolSchema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("BoolSchema")
	}
}
#[derive(Debug)]
struct U8Schema;
impl DebugType for U8Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("U8Schema")
	}
}
#[derive(Debug)]
struct I8Schema;
impl DebugType for I8Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("I8Schema")
	}
}
#[derive(Debug)]
struct U16Schema;
impl DebugType for U16Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("U16Schema")
	}
}
#[derive(Debug)]
struct I16Schema;
impl DebugType for I16Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("I16Schema")
	}
}
#[derive(Debug)]
struct U32Schema;
impl DebugType for U32Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("U32Schema")
	}
}
#[derive(Debug)]
struct I32Schema;
impl DebugType for I32Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("I32Schema")
	}
}
#[derive(Debug)]
struct U64Schema;
impl DebugType for U64Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("U64Schema")
	}
}
#[derive(Debug)]
struct I64Schema;
impl DebugType for I64Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("I64Schema")
	}
}
#[derive(Debug)]
struct F64Schema;
impl DebugType for F64Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("F64Schema")
	}
}
#[derive(Debug)]
struct F32Schema;
impl DebugType for F32Schema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("F32Schema")
	}
}
#[derive(Debug)]
struct StringSchema;
impl DebugType for StringSchema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("StringSchema")
	}
}
#[derive(Debug)]
enum TimestampSchema {
	Int96,
	Millis,
	Micros,
}
impl DebugType for TimestampSchema {
	fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.write_str("TimestampSchema")
	}
}


enum ValueReader {
	Bool(<bool as Deserialize>::Reader),
	U8(<u8 as Deserialize>::Reader),
	I8(<i8 as Deserialize>::Reader),
	U16(<u16 as Deserialize>::Reader),
	I16(<i16 as Deserialize>::Reader),
	U32(<u32 as Deserialize>::Reader),
	I32(<i32 as Deserialize>::Reader),
	U64(<u64 as Deserialize>::Reader),
	I64(<i64 as Deserialize>::Reader),
	F32(<f32 as Deserialize>::Reader),
	F64(<f64 as Deserialize>::Reader),
	Timestamp(<Timestamp as Deserialize>::Reader),
	Array(<Vec<u8> as Deserialize>::Reader),
	String(<String as Deserialize>::Reader),
	List(Box<<List<Value> as Deserialize>::Reader>),
	Map(Box<<Map<Value,Value> as Deserialize>::Reader>),
	Group(<Group as Deserialize>::Reader),
	Option(Box<<Option<Value> as Deserialize>::Reader>),
}
impl RRReader for ValueReader {
	type Item = Value;

	fn read_field(&mut self) -> Result<Self::Item, ParquetError> {
		match self {
			ValueReader::Bool(ref mut reader) => reader.read_field().map(Value::Bool),
			ValueReader::U8(ref mut reader) => reader.read_field().map(Value::U8),
			ValueReader::I8(ref mut reader) => reader.read_field().map(Value::I8),
			ValueReader::U16(ref mut reader) => reader.read_field().map(Value::U16),
			ValueReader::I16(ref mut reader) => reader.read_field().map(Value::I16),
			ValueReader::U32(ref mut reader) => reader.read_field().map(Value::U32),
			ValueReader::I32(ref mut reader) => reader.read_field().map(Value::I32),
			ValueReader::U64(ref mut reader) => reader.read_field().map(Value::U64),
			ValueReader::I64(ref mut reader) => reader.read_field().map(Value::I64),
			ValueReader::F32(ref mut reader) => reader.read_field().map(Value::F32),
			ValueReader::F64(ref mut reader) => reader.read_field().map(Value::F64),
			ValueReader::Timestamp(ref mut reader) => reader.read_field().map(Value::Timestamp),
			ValueReader::Array(ref mut reader) => reader.read_field().map(Value::Array),
			ValueReader::String(ref mut reader) => reader.read_field().map(Value::String),
			ValueReader::List(ref mut reader) => reader.read_field().map(Value::List),
			ValueReader::Map(ref mut reader) => reader.read_field().map(Value::Map),
			ValueReader::Group(ref mut reader) => reader.read_field().map(Value::Group),
			ValueReader::Option(ref mut reader) => reader.read_field().map(|x|Value::Option(Box::new(x))),
		}
	}
	fn advance_columns(&mut self) {
		match self {
			ValueReader::Bool(ref mut reader) => reader.advance_columns(),
			ValueReader::U8(ref mut reader) => reader.advance_columns(),
			ValueReader::I8(ref mut reader) => reader.advance_columns(),
			ValueReader::U16(ref mut reader) => reader.advance_columns(),
			ValueReader::I16(ref mut reader) => reader.advance_columns(),
			ValueReader::U32(ref mut reader) => reader.advance_columns(),
			ValueReader::I32(ref mut reader) => reader.advance_columns(),
			ValueReader::U64(ref mut reader) => reader.advance_columns(),
			ValueReader::I64(ref mut reader) => reader.advance_columns(),
			ValueReader::F32(ref mut reader) => reader.advance_columns(),
			ValueReader::F64(ref mut reader) => reader.advance_columns(),
			ValueReader::Timestamp(ref mut reader) => reader.advance_columns(),
			ValueReader::Array(ref mut reader) => reader.advance_columns(),
			ValueReader::String(ref mut reader) => reader.advance_columns(),
			ValueReader::List(ref mut reader) => reader.advance_columns(),
			ValueReader::Map(ref mut reader) => reader.advance_columns(),
			ValueReader::Group(ref mut reader) => reader.advance_columns(),
			ValueReader::Option(ref mut reader) => reader.advance_columns(),
		}
	}
	fn has_next(&self) -> bool {
		match self {
			ValueReader::Bool(ref reader) => reader.has_next(),
			ValueReader::U8(ref reader) => reader.has_next(),
			ValueReader::I8(ref reader) => reader.has_next(),
			ValueReader::U16(ref reader) => reader.has_next(),
			ValueReader::I16(ref reader) => reader.has_next(),
			ValueReader::U32(ref reader) => reader.has_next(),
			ValueReader::I32(ref reader) => reader.has_next(),
			ValueReader::U64(ref reader) => reader.has_next(),
			ValueReader::I64(ref reader) => reader.has_next(),
			ValueReader::F32(ref reader) => reader.has_next(),
			ValueReader::F64(ref reader) => reader.has_next(),
			ValueReader::Timestamp(ref reader) => reader.has_next(),
			ValueReader::Array(ref reader) => reader.has_next(),
			ValueReader::String(ref reader) => reader.has_next(),
			ValueReader::List(ref reader) => reader.has_next(),
			ValueReader::Map(ref reader) => reader.has_next(),
			ValueReader::Group(ref reader) => reader.has_next(),
			ValueReader::Option(ref reader) => reader.has_next(),
		}
	}
	fn current_def_level(&self) -> i16 {
		match self {
			ValueReader::Bool(ref reader) => reader.current_def_level(),
			ValueReader::U8(ref reader) => reader.current_def_level(),
			ValueReader::I8(ref reader) => reader.current_def_level(),
			ValueReader::U16(ref reader) => reader.current_def_level(),
			ValueReader::I16(ref reader) => reader.current_def_level(),
			ValueReader::U32(ref reader) => reader.current_def_level(),
			ValueReader::I32(ref reader) => reader.current_def_level(),
			ValueReader::U64(ref reader) => reader.current_def_level(),
			ValueReader::I64(ref reader) => reader.current_def_level(),
			ValueReader::F32(ref reader) => reader.current_def_level(),
			ValueReader::F64(ref reader) => reader.current_def_level(),
			ValueReader::Timestamp(ref reader) => reader.current_def_level(),
			ValueReader::Array(ref reader) => reader.current_def_level(),
			ValueReader::String(ref reader) => reader.current_def_level(),
			ValueReader::List(ref reader) => reader.current_def_level(),
			ValueReader::Map(ref reader) => reader.current_def_level(),
			ValueReader::Group(ref reader) => reader.current_def_level(),
			ValueReader::Option(ref reader) => reader.current_def_level(),
		}
	}
	fn current_rep_level(&self) -> i16 {
		match self {
			ValueReader::Bool(ref reader) => reader.current_rep_level(),
			ValueReader::U8(ref reader) => reader.current_rep_level(),
			ValueReader::I8(ref reader) => reader.current_rep_level(),
			ValueReader::U16(ref reader) => reader.current_rep_level(),
			ValueReader::I16(ref reader) => reader.current_rep_level(),
			ValueReader::U32(ref reader) => reader.current_rep_level(),
			ValueReader::I32(ref reader) => reader.current_rep_level(),
			ValueReader::U64(ref reader) => reader.current_rep_level(),
			ValueReader::I64(ref reader) => reader.current_rep_level(),
			ValueReader::F32(ref reader) => reader.current_rep_level(),
			ValueReader::F64(ref reader) => reader.current_rep_level(),
			ValueReader::Timestamp(ref reader) => reader.current_rep_level(),
			ValueReader::Array(ref reader) => reader.current_rep_level(),
			ValueReader::String(ref reader) => reader.current_rep_level(),
			ValueReader::List(ref reader) => reader.current_rep_level(),
			ValueReader::Map(ref reader) => reader.current_rep_level(),
			ValueReader::Group(ref reader) => reader.current_rep_level(),
			ValueReader::Option(ref reader) => reader.current_rep_level(),
		}
	}
}



impl Deserialize for Value {
	type Schema = ValueSchema;
	type Reader = ValueReader;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		let mut value = None;
		if schema.is_primitive() {
			value = Some(match (schema.get_physical_type(), schema.get_basic_info().logical_type()) {
				// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
				(PhysicalType::BOOLEAN,LogicalType::NONE) => ValueSchema::Bool(BoolSchema),
				(PhysicalType::INT32,LogicalType::UINT_8) => ValueSchema::U8(U8Schema),
				(PhysicalType::INT32,LogicalType::INT_8) => ValueSchema::I8(I8Schema),
				(PhysicalType::INT32,LogicalType::UINT_16) => ValueSchema::U16(U16Schema),
				(PhysicalType::INT32,LogicalType::INT_16) => ValueSchema::I16(I16Schema),
				(PhysicalType::INT32,LogicalType::UINT_32) => ValueSchema::U32(U32Schema),
				(PhysicalType::INT32,LogicalType::INT_32) | (PhysicalType::INT32, LogicalType::NONE) => ValueSchema::I32(I32Schema),
				(PhysicalType::INT32,LogicalType::DATE) => unimplemented!(),
				(PhysicalType::INT32,LogicalType::TIME_MILLIS) => unimplemented!(),
				(PhysicalType::INT32,LogicalType::DECIMAL) => unimplemented!(),
				(PhysicalType::INT64,LogicalType::UINT_64) => ValueSchema::U64(U64Schema),
				(PhysicalType::INT64,LogicalType::INT_64) | (PhysicalType::INT64,LogicalType::NONE) => ValueSchema::I64(I64Schema),
				(PhysicalType::INT64,LogicalType::TIME_MICROS) => unimplemented!(),
				// (PhysicalType::INT64,LogicalType::TIME_NANOS) => unimplemented!(),
				(PhysicalType::INT64,LogicalType::TIMESTAMP_MILLIS) => ValueSchema::Timestamp(TimestampSchema::Millis),
				(PhysicalType::INT64,LogicalType::TIMESTAMP_MICROS) => ValueSchema::Timestamp(TimestampSchema::Micros),
				// (PhysicalType::INT64,LogicalType::TIMESTAMP_NANOS) => unimplemented!(),
				(PhysicalType::INT64,LogicalType::DECIMAL) => unimplemented!(),
				(PhysicalType::INT96,LogicalType::NONE) => ValueSchema::Timestamp(TimestampSchema::Int96),
				(PhysicalType::FLOAT,LogicalType::NONE) => ValueSchema::F32(F32Schema),
				(PhysicalType::DOUBLE,LogicalType::NONE) => ValueSchema::F64(F64Schema),
				(PhysicalType::BYTE_ARRAY,LogicalType::UTF8) | (PhysicalType::BYTE_ARRAY,LogicalType::ENUM) | (PhysicalType::BYTE_ARRAY,LogicalType::JSON) | (PhysicalType::FIXED_LEN_BYTE_ARRAY,LogicalType::UTF8) | (PhysicalType::FIXED_LEN_BYTE_ARRAY,LogicalType::ENUM) | (PhysicalType::FIXED_LEN_BYTE_ARRAY,LogicalType::JSON) => ValueSchema::String(StringSchema),
				(PhysicalType::BYTE_ARRAY,LogicalType::NONE) | (PhysicalType::BYTE_ARRAY,LogicalType::BSON) | (PhysicalType::FIXED_LEN_BYTE_ARRAY,LogicalType::NONE) | (PhysicalType::FIXED_LEN_BYTE_ARRAY,LogicalType::BSON) => ValueSchema::Array(VecSchema(if schema.get_physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY { Some(schema.get_type_length().try_into().unwrap()) } else { None })),
				(PhysicalType::BYTE_ARRAY,LogicalType::DECIMAL) | (PhysicalType::FIXED_LEN_BYTE_ARRAY,LogicalType::DECIMAL) => unimplemented!(),
				(PhysicalType::BYTE_ARRAY,LogicalType::INTERVAL) | (PhysicalType::FIXED_LEN_BYTE_ARRAY,LogicalType::INTERVAL) => unimplemented!(),
				_ => return Err(ParquetError::General(String::from("Value"))),
			});
		}
		if value.is_none() && schema.is_group() && !schema.is_schema() && schema.get_basic_info().logical_type() == LogicalType::LIST && schema.get_fields().len() == 1 {
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 1 {
				let element = sub_schema.get_fields().into_iter().nth(0).unwrap();
				let list_name = if sub_schema.name() == "list" { None } else { Some(sub_schema.name().to_owned()) };
				let element_name = if element.name() == "element" { None } else { Some(element.name().to_owned()) };
				value = Some(ValueSchema::List(Box::new(ListSchema(Value::parse(&*element)?.1, Some((list_name, element_name))))));
			}
			// Err(ParquetError::General(String::from("List<T>")))
		}
		// TODO https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
		if value.is_none() && schema.is_group() && !schema.is_schema() && (schema.get_basic_info().logical_type() == LogicalType::MAP || schema.get_basic_info().logical_type() == LogicalType::MAP_KEY_VALUE) && schema.get_fields().len() == 1 {
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 2 {
				let mut fields = sub_schema.get_fields().into_iter();
				let (key, value_) = (fields.next().unwrap(), fields.next().unwrap());
				let key_value_name = if sub_schema.name() == "key_value" { None } else { Some(sub_schema.name().to_owned()) };
				let key_name = if key.name() == "key" { None } else { Some(key.name().to_owned()) };
				let value_name = if value_.name() == "value" { None } else { Some(value_.name().to_owned()) };
				value = Some(ValueSchema::Map(Box::new(MapSchema(Value::parse(&*key)?.1,Value::parse(&*value_)?.1, key_value_name, key_name, value_name))));
			}
		}

		if value.is_none() && schema.is_group() && !schema.is_schema() {
			let mut lookup = HashMap::new();
			value = Some(ValueSchema::Group(GroupSchema(schema.get_fields().iter().map(|schema|Value::parse(&*schema).map(|(name,schema)| {let x = lookup.insert(name, lookup.len()); assert!(x.is_none()); schema})).collect::<Result<Vec<_>,_>>()?, lookup)));
		}

		if value.is_none() {
			println!("errrrr");
			println!("{:?}", schema);
		}

		let mut value = value.ok_or(ParquetError::General(String::from("Value")))?;

		match schema.get_basic_info().repetition() {
			Repetition::OPTIONAL => {
				value = ValueSchema::Option(Box::new(OptionSchema(value)));
			}
			Repetition::REPEATED => {
				value = ValueSchema::List(Box::new(ListSchema(value, None)));
			}
			Repetition::REQUIRED => (),
		}

		Ok((schema.name().to_owned(),value))
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		match *schema {
			ValueSchema::Bool(ref schema) => ValueReader::Bool(<bool as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::U8(ref schema) => ValueReader::U8(<u8 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::I8(ref schema) => ValueReader::I8(<i8 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::U16(ref schema) => ValueReader::U16(<u16 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::I16(ref schema) => ValueReader::I16(<i16 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::U32(ref schema) => ValueReader::U32(<u32 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::I32(ref schema) => ValueReader::I32(<i32 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::U64(ref schema) => ValueReader::U64(<u64 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::I64(ref schema) => ValueReader::I64(<i64 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::F32(ref schema) => ValueReader::F32(<f32 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::F64(ref schema) => ValueReader::F64(<f64 as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::Timestamp(ref schema) => ValueReader::Timestamp(<Timestamp as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::Array(ref schema) => ValueReader::Array(<Vec<u8> as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::String(ref schema) => ValueReader::String(<String as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::List(ref schema) => ValueReader::List(Box::new(<List<Value> as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths))),
			ValueSchema::Map(ref schema) => ValueReader::Map(Box::new(<Map<Value,Value> as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths))),
			ValueSchema::Group(ref schema) => ValueReader::Group(<Group as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths)),
			ValueSchema::Option(ref schema) => ValueReader::Option(Box::new(<Option<Value> as Deserialize>::reader(schema, path, curr_def_level, curr_rep_level, paths))),
		}
	}
}
impl Deserialize for Root<Value> {
	type Schema = RootSchema<Value, ValueSchema>;
	type Reader = RootReader<ValueReader>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		assert!(schema.is_schema());
		let mut value = None;
		if value.is_none() && schema.is_group() && schema.get_basic_info().logical_type() == LogicalType::LIST && schema.get_fields().len() == 1 {
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 1 {
				let element = sub_schema.get_fields().into_iter().nth(0).unwrap();
				let list_name = if sub_schema.name() == "list" { None } else { Some(sub_schema.name().to_owned()) };
				let element_name = if element.name() == "element" { None } else { Some(element.name().to_owned()) };
				value = Some(ValueSchema::List(Box::new(ListSchema(Value::parse(&*element)?.1, Some((list_name, element_name))))));
			}
			// Err(ParquetError::General(String::from("List<T>")))
		}
		// TODO https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
		if value.is_none() && schema.is_group() && (schema.get_basic_info().logical_type() == LogicalType::MAP || schema.get_basic_info().logical_type() == LogicalType::MAP_KEY_VALUE) && schema.get_fields().len() == 1 {
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 2 {
				let mut fields = sub_schema.get_fields().into_iter();
				let (key, value_) = (fields.next().unwrap(), fields.next().unwrap());
				let key_value_name = if sub_schema.name() == "key_value" { None } else { Some(sub_schema.name().to_owned()) };
				let key_name = if key.name() == "key" { None } else { Some(key.name().to_owned()) };
				let value_name = if value_.name() == "value" { None } else { Some(value_.name().to_owned()) };
				value = Some(ValueSchema::Map(Box::new(MapSchema(Value::parse(&*key)?.1,Value::parse(&*value_)?.1, key_value_name, key_name, value_name))));
			}
		}

		if value.is_none() && schema.is_group() {
			let mut lookup = HashMap::new();
			value = Some(ValueSchema::Group(GroupSchema(schema.get_fields().iter().map(|schema|Value::parse(&*schema).map(|(name,schema)| {let x = lookup.insert(name, lookup.len()); assert!(x.is_none()); schema})).collect::<Result<Vec<_>,_>>()?, lookup)));
		}

		if value.is_none() {
			println!("errrrr");
			println!("{:?}", schema);
		}

		let mut value = value.ok_or(ParquetError::General(String::from("Value")))?;

		Ok((String::from(""),RootSchema(schema.name().to_owned(), value, PhantomData)))
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		RootReader(<Value as Deserialize>::reader(&schema.1, path, curr_def_level, curr_rep_level, paths))
	}
}

// impl Deserialize for Option<Value> {
// 	type Schema = OptionSchema<ValueSchema>;
// 	existential type Reader: RRReader<Item = Self>;

// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		<Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
// 			match schema {
// 				ValueSchema::Option(schema) => Ok((name, *schema)),
// 				_ => Err(ParquetError::General(String::from(""))),
// 			}
// 		})
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		OptionReader{def_level: curr_def_level, reader: <Value as Deserialize>::reader(&schema.0, path, curr_def_level+1, curr_rep_level, paths)}
// 	}
// }


trait Deserialize: Sized {
	type Schema: Debug + DebugType;
	type Reader: RRReader<Item = Self>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError>;
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader;
}


fn read<'a, R: ParquetReader + 'static, T>(
	reader: &'a SerializedFileReader<R>,
) -> Result<impl Iterator<Item = T> + 'a,ParquetError> where Root<T>: Deserialize, <Root<T> as Deserialize>::Schema: 'a, <Root<T> as Deserialize>::Reader: 'a {
	let file_schema = reader.metadata().file_metadata().schema_descr_ptr();
	let file_schema = file_schema.root_schema();
	let schema = <Root<T> as Deserialize>::parse(file_schema).map_err(|err| {
		// let schema: Type = <Root<T> as Deserialize>::render("", &<Root<T> as Deserialize>::placeholder());
		let mut b = Vec::new();
		print_schema(&mut b, file_schema);
		// let mut a = Vec::new();
		// print_schema(&mut a, &schema);
		ParquetError::General(format!(
			"Types don't match schema.\nSchema is:\n{}\nBut types require:\n{:#?}\nError: {}",
			String::from_utf8(b).unwrap(),
			// String::from_utf8(a).unwrap(),
			DebugDebugType::<<Root<T> as Deserialize>::Schema>::new(),
			err
		))
	}).unwrap().1;
	// let dyn_schema = <Root<T>>::render("", &schema);
	// print_schema(&mut std::io::stdout(), &dyn_schema);
	// assert!(file_schema.check_contains(&dyn_schema));
	// println!("{:#?}", schema);
	// let iter = reader.get_row_iter(None).unwrap();
	// println!("{:?}", iter.count());
	// print_parquet_metadata(&mut std::io::stdout(), &reader.metadata());
	{
		// println!("file: {:#?}", reader.metadata().file_metadata());
		// print_file_metadata(&mut std::io::stdout(), &*reader.metadata().file_metadata());
		let schema = reader.metadata().file_metadata().schema_descr_ptr().clone();
		let schema = schema.root_schema();
		// println!("{:#?}", schema);
		print_schema(&mut std::io::stdout(), &schema);
		// let mut iter = reader.get_row_iter(None).unwrap();
		// while let Some(record) = iter.next() {
		// 	// See record API for different field accessors
		// 	// println!("{}", record);
		// }
	}
	// print_parquet_metadata(&mut std::io::stdout(), reader.metadata());
	// println!("file: {:#?}", reader.metadata().file_metadata());
	// println!("file: {:#?}", reader.metadata().row_groups());

	// let descr = Rc::new(SchemaDescriptor::new(Rc::new(dyn_schema)));

	// let tree_builder = parquet::record::reader::TreeBuilder::new();
	let schema = Rc::new(schema); // TODO!
	Ok((0..reader.num_row_groups()).flat_map(move |i| {
		// let schema = &schema;
		let row_group = reader.get_row_group(i).unwrap();

		let mut paths: HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)> = HashMap::new();
		let row_group_metadata = row_group.metadata();

		for col_index in 0..row_group.num_columns() {
			let col_meta = row_group_metadata.column(col_index);
			let col_path = col_meta.column_path().clone();
			// println!("path: {:?}", col_path);
			let col_descr = row_group
				.metadata()
				.column(col_index)
				.column_descr_ptr();
			let col_reader = row_group.get_column_reader(col_index).unwrap();

			let x = paths.insert(col_path, (col_descr, col_reader));
			assert!(x.is_none());
		}

		let mut path = Vec::new();

		let mut reader = <Root<T>>::reader(&schema, &mut path, 0, 0, &mut paths);

		// let mut reader = tree_builder.build(descr.clone(), &*row_group);
		reader.advance_columns();
		// for row in tree_builder.as_iter(descr.clone(), &*row_group) {
		// 	println!("{:?}", row);
		// }
		// std::iter::empty()
		// println!("{:?}", reader.read());
		let schema = schema.clone();
		(0..row_group.metadata().num_rows()).map(move |_| {
			// println!("row");
			reader.read_field().unwrap().0
			// unimplemented!()
			// <Root<T>>::read(&schema, &mut reader).unwrap().0
		})
	}))
}
fn write<R: ParquetReader + 'static, T>(reader: R, schema: <Root<T> as Deserialize>::Schema) -> () where Root<T>: Deserialize {
	// let schema = <Root<T>>::render("", &schema);
	// print_schema(&mut std::io::stdout(), &schema);
	// println!("{:#?}", schema);
	let reader = SerializedFileReader::new(reader).unwrap();
	// let iter = reader.get_row_iter(None).unwrap();
	// println!("{:?}", iter.count());
	// print_parquet_metadata(&mut std::io::stdout(), &reader.metadata());
	unimplemented!()
}

const BATCH_SIZE: usize = 1024;

struct TryIntoReader<R: RRReader, T>(R, PhantomData<fn(T)>);
impl<R: RRReader, T> RRReader for TryIntoReader<R, T> where R::Item: TryInto<T>, <R::Item as TryInto<T>>::Error: Error {
	type Item = T;

	fn read_field(&mut self) -> Result<Self::Item, ParquetError> {
		self.0.read_field().and_then(|x|x.try_into().map_err(|err|ParquetError::General(err.description().to_owned())))
	}
	fn advance_columns(&mut self) {
		self.0.advance_columns()
	}
	fn has_next(&self) -> bool {
		self.0.has_next()
	}
	fn current_def_level(&self) -> i16 {
		self.0.current_def_level()
	}
	fn current_rep_level(&self) -> i16 {
		self.0.current_rep_level()
	}
}
struct MapReader<R: RRReader, F>(R, F);
impl<R: RRReader, F, T> RRReader for MapReader<R, F> where F: FnMut(R::Item) -> Result<T, ParquetError> {
	type Item = T;

	fn read_field(&mut self) -> Result<Self::Item, ParquetError> {
		self.0.read_field().and_then(&mut self.1)
	}
	fn advance_columns(&mut self) {
		self.0.advance_columns()
	}
	fn has_next(&self) -> bool {
		self.0.has_next()
	}
	fn current_def_level(&self) -> i16 {
		self.0.current_def_level()
	}
	fn current_rep_level(&self) -> i16 {
		self.0.current_rep_level()
	}
}

impl<K,V> Deserialize for Map<K,V>
where
	K: Deserialize + Hash + Eq,
	V: Deserialize,
{
	type Schema = MapSchema<K::Schema, V::Schema>;
	existential type Reader: RRReader<Item = Self>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED && (schema.get_basic_info().logical_type() == LogicalType::MAP || schema.get_basic_info().logical_type() == LogicalType::MAP_KEY_VALUE) && schema.get_fields().len() == 1 {
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 2 {
				let mut fields = sub_schema.get_fields().into_iter();
				let (key, value) = (fields.next().unwrap(), fields.next().unwrap());
				let key_value_name = if sub_schema.name() == "key_value" { None } else { Some(sub_schema.name().to_owned()) };
				let key_name = if key.name() == "key" { None } else { Some(key.name().to_owned()) };
				let value_name = if value.name() == "value" { None } else { Some(value.name().to_owned()) };
				return Ok((schema.name().to_owned(), MapSchema(K::parse(&*key)?.1,V::parse(&*value)?.1, key_value_name, key_name, value_name)));
			}
		}
		Err(ParquetError::General(String::from("Map<K,V>")))
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let key_value_name = schema.2.as_ref().map(|x|&**x).unwrap_or("key_value");
		let key_name = schema.3.as_ref().map(|x|&**x).unwrap_or("key");
		let value_name = schema.4.as_ref().map(|x|&**x).unwrap_or("value");

		path.push(key_value_name.to_owned());
		path.push(key_name.to_owned());
		let keys_reader = K::reader(&schema.0, path, curr_def_level + 1, curr_rep_level + 1, paths);
		path.pop().unwrap();
		path.push(value_name.to_owned());
		let values_reader = V::reader(&schema.1, path, curr_def_level + 1, curr_rep_level + 1, paths);
		path.pop().unwrap();
		path.pop().unwrap();

		MapReader(KeyValueReader{
			def_level: curr_def_level,
			rep_level: curr_rep_level,
			keys_reader,
			values_reader
		}, |x:Vec<_>|Ok(Map(x.into_iter().collect())))
	}
}
// impl<K,V> Deserialize for Option<Map<K,V>>
// where
// 	K: Deserialize + Hash + Eq,
// 	V: Deserialize,
// {
// 	type Schema = OptionSchema<MapSchema<K::Schema, V::Schema>>;
// 	existential type Reader: RRReader<Item = Self>;

// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && (schema.get_basic_info().logical_type() == LogicalType::MAP || schema.get_basic_info().logical_type() == LogicalType::MAP_KEY_VALUE) && schema.get_fields().len() == 1 {
// 			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
// 			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 2 {
// 				let mut fields = sub_schema.get_fields().into_iter();
// 				let (key, value) = (fields.next().unwrap(), fields.next().unwrap());
// 				let key_value_name = if sub_schema.name() == "key_value" { None } else { Some(sub_schema.name().to_owned()) };
// 				let key_name = if key.name() == "key" { None } else { Some(key.name().to_owned()) };
// 				let value_name = if value.name() == "value" { None } else { Some(value.name().to_owned()) };
// 				return Ok((schema.name().to_owned(), OptionSchema(MapSchema(K::parse(&*key)?.1,V::parse(&*value)?.1, key_value_name, key_name, value_name))));
// 			}
// 		}
// 		Err(ParquetError::General(String::from("Option<Map<K,V>>")))
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		OptionReader{def_level: curr_def_level, reader: <Map<K,V> as Deserialize>::reader(&schema.0, path, curr_def_level+1, curr_rep_level, paths)}
// 	}
// }


impl<T> Deserialize for List<T>
where
	T: Deserialize,
{
	type Schema = ListSchema<T::Schema>;
	existential type Reader: RRReader<Item = Self>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		// <Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
		// 	match schema {
		// 		ValueSchema::List(box ListSchema(schema, a)) => Ok((name, ListSchema(schema, a))),
		// 		_ => Err(ParquetError::General(String::from(""))),
		// 	}
		// })
		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_basic_info().logical_type() == LogicalType::LIST && schema.get_fields().len() == 1 {
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 1 {
				let element = sub_schema.get_fields().into_iter().nth(0).unwrap();
				let list_name = if sub_schema.name() == "list" { None } else { Some(sub_schema.name().to_owned()) };
				let element_name = if element.name() == "element" { None } else { Some(element.name().to_owned()) };
				return Ok((schema.name().to_owned(), ListSchema(T::parse(&*element)?.1, Some((list_name, element_name)))));
			}
		}
		if schema.get_basic_info().repetition() == Repetition::REPEATED {
			let mut schema2: Type = schema.clone();
			let basic_info = match schema2 {Type::PrimitiveType{ref mut basic_info,..} => basic_info, Type::GroupType{ref mut basic_info,..} => basic_info};
			basic_info.set_repetition(Some(Repetition::REQUIRED));
			return Ok((schema.name().to_owned(), ListSchema(T::parse(&schema2)?.1, None)));
		}
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
		Err(ParquetError::General(String::from("List<T>")))
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		MapReader(if let Some((ref list_name,ref element_name)) = schema.1 {
			let list_name = list_name.as_ref().map(|x|&**x).unwrap_or("list");
			let element_name = element_name.as_ref().map(|x|&**x).unwrap_or("element");

			path.push(list_name.to_owned());
			path.push(element_name.to_owned());
			let reader = T::reader(&schema.0, path, curr_def_level + 1, curr_rep_level + 1, paths);
			path.pop().unwrap();
			path.pop().unwrap();

			RepeatedReader{
				def_level: curr_def_level,
				rep_level: curr_rep_level,
				reader,
			}
		} else {
			let reader = T::reader(&schema.0, path, curr_def_level + 1, curr_rep_level + 1, paths);
			RepeatedReader{
				def_level: curr_def_level,
				rep_level: curr_rep_level,
				reader,
			}
		}, |x|Ok(List(x)))
	}
}
// impl<T> Deserialize for Option<List<T>>
// where
// 	T: Deserialize,
// {
// 	type Schema = OptionSchema<ListSchema<T::Schema>>;
// 	existential type Reader: RRReader<Item = Self>;

// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		// <Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
// 		// 	match schema {
// 		// 		ValueSchema::Option(box OptionSchema(ValueSchema::List(schema))) => Ok((name, OptionSchema(schema))),
// 		// 		_ => Err(ParquetError::General(String::from(""))),
// 		// 	}
// 		// })
// 		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_basic_info().logical_type() == LogicalType::LIST && schema.get_fields().len() == 1 {
// 			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
// 			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 1 {
// 				let element = sub_schema.get_fields().into_iter().nth(0).unwrap();
// 				let list_name = if sub_schema.name() == "list" { None } else { Some(sub_schema.name().to_owned()) };
// 				let element_name = if element.name() == "element" { None } else { Some(element.name().to_owned()) };
// 				return Ok((schema.name().to_owned(), OptionSchema(ListSchema(T::parse(&*element)?.1, Some((list_name, element_name))))));
// 			}
// 		}
// 		Err(ParquetError::General(String::from("Option<List<T>>")))
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		OptionReader{def_level: curr_def_level, reader: <List<T> as Deserialize>::reader(&schema.0, path, curr_def_level+1, curr_rep_level, paths)}
// 	}
// }

impl<T> Deserialize for Option<T> where T: Deserialize, ValueSchema: Downcast<<T as Deserialize>::Schema> {
	type Schema = OptionSchema<T::Schema>;
	type Reader = OptionReader<T::Reader>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		<Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
			Ok((name, OptionSchema(schema.as_option()?.0.downcast()?)))
		})
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		OptionReader{def_level: curr_def_level, reader: <T as Deserialize>::reader(&schema.0, path, curr_def_level+1, curr_rep_level, paths)}
	}
}

fn downcast<T>((name, schema): (String, ValueSchema)) -> Result<(String, T),ParquetError> where ValueSchema: Downcast<T> {
	schema.downcast().map(|schema| (name, schema))
}


impl Deserialize for bool {
	type Schema = BoolSchema;
	type Reader = BoolReader;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		BoolReader{column: TypedTripletIter::<BoolType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for f32 {
	type Schema = F32Schema;
	type Reader = F32Reader;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		F32Reader{column: TypedTripletIter::<FloatType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for f64 {
	type Schema = F64Schema;
	type Reader = F64Reader;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		F64Reader{column: TypedTripletIter::<DoubleType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for i8 {
	type Schema = I8Schema;
	type Reader = TryIntoReader<I32Reader, i8>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)
	}
}
impl Deserialize for u8 {
	type Schema = U8Schema;
	type Reader = TryIntoReader<I32Reader, u8>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)
	}
}
impl Deserialize for i16 {
	type Schema = I16Schema;
	type Reader = TryIntoReader<I32Reader, i16>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)
	}
}
impl Deserialize for u16 {
	type Schema = U16Schema;
	type Reader = TryIntoReader<I32Reader, u16>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)
	}
}
impl Deserialize for i32 {
	type Schema = I32Schema;
	type Reader = I32Reader;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for u32 {
	type Schema = U32Schema;
	existential type Reader: RRReader<Item = Self>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		MapReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |x|Ok(x as u32))
	}
}
impl Deserialize for i64 {
	type Schema = I64Schema;
	type Reader = I64Reader;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for u64 {
	type Schema = U64Schema;
	existential type Reader: RRReader<Item = Self>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		MapReader(I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |x|Ok(x as u64))
	}
}
impl Deserialize for Timestamp {
	type Schema = TimestampSchema;
	existential type Reader: RRReader<Item = Self>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		match schema {
			TimestampSchema::Int96 => {
				sum::Sum3::A(MapReader(I96Reader{column: TypedTripletIter::<Int96Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |x|Ok(Timestamp(x))))
			}
			TimestampSchema::Millis => {
				sum::Sum3::B(MapReader(I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |millis| {
					let day: i64 = ((JULIAN_DAY_OF_EPOCH * SECONDS_PER_DAY * MILLIS_PER_SECOND) + millis) / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
					let nanoseconds: i64 = (millis - ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND)) * 1_000_000;

					Ok(Timestamp(Int96::new((nanoseconds & 0xffff).try_into().unwrap(),((nanoseconds as u64) >> 32).try_into().unwrap(),day.try_into().map_err(|err:TryFromIntError|ParquetError::General(err.description().to_owned()))?)))
				}))
			}
			TimestampSchema::Micros => {
				sum::Sum3::C(MapReader(I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |micros| {
					let day: i64 = ((JULIAN_DAY_OF_EPOCH * SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI) + micros) / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
					let nanoseconds: i64 = (micros - ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI)) * 1_000;

					Ok(Timestamp(Int96::new((nanoseconds & 0xffff).try_into().unwrap(),((nanoseconds as u64) >> 32).try_into().unwrap(),day.try_into().map_err(|err:TryFromIntError|ParquetError::General(err.description().to_owned()))?)))
				}))
			}
		}
	}
}
// impl Deserialize for parquet::data_type::Decimal {
// 	type Schema = DecimalSchema;
// type Reader = Reader;
// fn placeholder() -> Self::Schema {
// 	
// }
// fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 	unimplemented!()
// }
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		Type::primitive_type_builder(name, PhysicalType::DOUBLE)
// 	.with_repetition(Repetition::REQUIRED)
// 	.with_logical_type(LogicalType::NONE)
// 	.with_length(-1)
// 	.with_precision(-1)
// 	.with_scale(-1)
// 	.build().unwrap()
// Type::PrimitiveType {
// 			basic_info: BasicTypeInfo {
// 				name: String::from(schema),
// 				repetition: Some(Repetition::REQUIRED),
// 				logical_type: LogicalType::DECIMAL,
// 				id: None,
// 			}
// 			physical_type: PhysicalType::
// 	}
// }
// struct DecimalSchema {
// 	scale: u32,
// 	precision: u32,
// }
impl Deserialize for Vec<u8> {
	type Schema = VecSchema;
	type Reader = ByteArrayReader;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		ByteArrayReader{column: TypedTripletIter::<ByteArrayType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for String {
	type Schema = StringSchema;
	existential type Reader: RRReader<Item = Self>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		Value::parse(schema).and_then(downcast)
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		MapReader(ByteArrayReader{column: TypedTripletIter::<ByteArrayType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |x|String::from_utf8(x).map_err(|err:FromUtf8Error|ParquetError::General(err.to_string())))
	}
}
impl Deserialize for [u8; 1024] {
	type Schema = ArraySchema<Self>;
	existential type Reader: RRReader<Item = Self>;

	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY && schema.get_basic_info().logical_type() == LogicalType::NONE && schema.get_type_length() == 1024 {
			return Ok((schema.name().to_owned(), ArraySchema(PhantomData)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		MapReader(FixedLenByteArrayReader{column: TypedTripletIter::<FixedLenByteArrayType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |bytes: Vec<_>| {
			let mut ret = std::mem::MaybeUninit::<Self>::uninitialized();
			assert_eq!(bytes.len(), unsafe { ret.get_ref().len() });
			unsafe {
				std::ptr::copy_nonoverlapping(
					bytes.as_ptr(),
					ret.get_mut().as_mut_ptr(),
					bytes.len(),
				)
			};
			Ok(unsafe { ret.into_inner() })
		})
	}
}

struct RootReader<R>(R);
impl<R> RRReader for RootReader<R> where R: RRReader {
	type Item = Root<R::Item>;

	fn read_field(&mut self) -> Result<Self::Item, ParquetError> {
		self.0.read_field().map(Root)
	}
	fn advance_columns(&mut self) {
		self.0.advance_columns();
	}
	fn has_next(&self) -> bool {
		self.0.has_next()
	}
	fn current_def_level(&self) -> i16 {
		self.0.current_def_level()
	}
	fn current_rep_level(&self) -> i16 {
		self.0.current_rep_level()
	}
}


macro_rules! impl_parquet_deserialize_tuple {
	($($t:ident $i:tt)*) => (
		impl<$($t,)*> RRReader for TupleReader<($($t,)*)> where $($t: RRReader,)* {
			type Item = ($($t::Item,)*);

			fn read_field(&mut self) -> Result<Self::Item, ParquetError> {
				Ok((
					$((self.0).$i.read_field()?,)*
				))
			}
			fn advance_columns(&mut self) {
				$((self.0).$i.advance_columns();)*
			}
			fn has_next(&self) -> bool {
				// self.$first_name.has_next()
				$((self.0).$i.has_next() &&)* true
			}
			fn current_def_level(&self) -> i16 {
				$(if true { (self.0).$i.current_def_level() } else)*
				{
					panic!("Current definition level: empty group reader")
				}
			}
			fn current_rep_level(&self) -> i16 {
				$(if true { (self.0).$i.current_rep_level() } else)*
				{
					panic!("Current repetition level: empty group reader")
				}
			}
		}
		impl<$($t,)*> str::FromStr for RootSchema<($($t,)*),TupleSchema<($((String,$t::Schema,),)*)>> where $($t: Deserialize,)* {
			type Err = ParquetError;

			fn from_str(s: &str) -> Result<Self, Self::Err> {
				parse_message_type(s).and_then(|x|<Root<($($t,)*)> as Deserialize>::parse(&x).map_err(|err| {
					// let x: Type = <Root<($($t,)*)> as Deserialize>::render("", &<Root<($($t,)*)> as Deserialize>::placeholder());
					let mut a = Vec::new();
					// print_schema(&mut a, &x);
					ParquetError::General(format!(
						"Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
						s,
						String::from_utf8(a).unwrap(),
						err
					))
				})).map(|x|x.1)
			}
		}
		impl<$($t,)*> Debug for TupleSchema<($((String,$t,),)*)> where $($t: Debug,)* {
			fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
				f.debug_tuple("TupleSchema")
					$(.field(&(self.0).$i))*
					.finish()
			}
		}
		impl<$($t,)*> DebugType for TupleSchema<($((String,$t,),)*)> where $($t: DebugType,)* {
			fn fmt(f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
				f.write_str("TupleSchema")
			}
		}
		impl<$($t,)*> Deserialize for Root<($($t,)*)> where $($t: Deserialize,)* {
			type Schema = RootSchema<($($t,)*),TupleSchema<($((String,$t::Schema,),)*)>>;
			type Reader = RootReader<TupleReader<($($t::Reader,)*)>>;

			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_schema() {
					let mut fields = schema.get_fields().iter();
					let schema_ = RootSchema(schema.name().to_owned(), TupleSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*)), PhantomData);
					if fields.next().is_none() {
						return Ok((String::from(""), schema_))
					}
				}
				Err(ParquetError::General(String::from("")))
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
				RootReader(<($($t,)*) as Deserialize>::reader(&schema.1, path, curr_def_level, curr_rep_level, paths))
			}
		}
		impl<$($t,)*> Deserialize for ($($t,)*) where $($t: Deserialize,)* {
			type Schema = TupleSchema<($((String,$t::Schema,),)*)>;
			type Reader = TupleReader<($($t::Reader,)*)>;

			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED {
					let mut fields = schema.get_fields().iter();
					let schema_ = TupleSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*));
					if fields.next().is_none() {
						return Ok((schema.name().to_owned(), schema_))
					}
				}
				Err(ParquetError::General(String::from("")))
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
				$(
					path.push((schema.0).$i.0.to_owned());
					let $t = <$t as Deserialize>::reader(&(schema.0).$i.1, path, curr_def_level, curr_rep_level, paths);
					path.pop().unwrap();
				)*;
				TupleReader(($($t,)*))
			}
		}
		// impl<$($t,)*> Deserialize for Option<($($t,)*)> where $($t: Deserialize,)* {
		// 	type Schema = OptionSchema<TupleSchema<($((String,$t::Schema,),)*)>>;
		// 	type Reader = OptionReader<TupleReader<($($t::Reader,)*)>>;

		// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		// 		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::OPTIONAL {
		// 			let mut fields = schema.get_fields().iter();
		// 			let schema_ = OptionSchema(TupleSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*)));
		// 			if fields.next().is_none() {
		// 				return Ok((schema.name().to_owned(), schema_))
		// 			}
		// 		}
		// 		Err(ParquetError::General(String::from("")))
		// 	}
		// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, curr_def_level: i16, curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		// 		OptionReader{def_level: curr_def_level, reader: <($($t,)*) as Deserialize>::reader(&schema.0, path, curr_def_level+1, curr_rep_level, paths)}
		// 	}
		// }
		impl<$($t,)*> Downcast<($($t,)*)> for Value where Value: $(Downcast<$t> +)* {
			fn downcast(self) -> Result<($($t,)*),ParquetError> {
				let mut fields = self.as_group()?.0.into_iter();
				Ok(($({$i;fields.next().unwrap().downcast()?},)*))
			}
		}
		impl<$($t,)*> Downcast<TupleSchema<($((String,$t,),)*)>> for ValueSchema where ValueSchema: $(Downcast<$t> +)* {
			fn downcast(self) -> Result<TupleSchema<($((String,$t,),)*)>,ParquetError> {
				let group = self.as_group()?;
				let mut fields = group.0.into_iter();
				let mut names = vec![None; group.1.len()];
				for (name,&index) in group.1.iter() {
					names[index].replace(name.to_owned());
				}
				let mut names = names.into_iter().map(Option::unwrap);
				Ok(TupleSchema(($({$i;(names.next().unwrap(),fields.next().unwrap().downcast()?)},)*)))
			}
		}
	);
}

impl_parquet_deserialize_tuple!();
impl_parquet_deserialize_tuple!(A 0);
impl_parquet_deserialize_tuple!(A 0 B 1);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31 AG 32);

