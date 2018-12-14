#![feature(maybe_uninit,try_from,test,box_patterns,transpose_result,existential_type)]

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
use std::{fs::File, path::Path, rc::Rc, str};
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

macro_rules! impl_parquet_deserialize_struct {
	($struct:ident $struct_schema:ident $struct_reader:ident $($name:ident: $type_:ty,)*) => (
		struct $struct_schema {
			$($name: <$type_ as Deserialize>::Schema,)*
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

			fn placeholder() -> Self::Schema {
				$struct_schema{$($name: <$type_ as Deserialize>::placeholder(),)*}
			}
			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED {
					let fields = schema.get_fields().iter().map(|field|(field.name(),field)).collect::<HashMap<_,_>>();
					let schema_ = $struct_schema{$($name: fields.get(stringify!($name)).ok_or(ParquetError::General(format!("Struct {} missing field {}", stringify!($struct), stringify!($name)))).and_then(|x|<$type_ as Deserialize>::parse(&**x))?.1,)*};
					return Ok((schema.name().to_owned(), schema_))
				}
				Err(ParquetError::General(format!("Struct {}", stringify!($struct))))
			}
			fn render(name: &str, schema: &Self::Schema) -> Type {
				Type::group_type_builder(name).with_repetition(Repetition::REQUIRED).with_fields(&mut vec![$(Rc::new(<$type_ as Deserialize>::render(stringify!($name), &schema.$name)),)*]).build().unwrap()
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
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

			fn placeholder() -> Self::Schema {
				RootSchema(String::from("<name>"), $struct_schema{$($name: <$type_ as Deserialize>::placeholder(),)*}, PhantomData)
			}
			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_schema() {
					let fields = schema.get_fields().iter().map(|field|(field.name(),field)).collect::<HashMap<_,_>>();
					let schema_ = $struct_schema{$($name: fields.get(stringify!($name)).ok_or(ParquetError::General(format!("Struct {} missing field {}", stringify!($struct), stringify!($name)))).and_then(|x|<$type_ as Deserialize>::parse(&**x))?.1,)*};
					return Ok((String::from(""), RootSchema(schema.name().to_owned(), schema_, PhantomData)))
				}
				Err(ParquetError::General(format!("Struct {}", stringify!($struct))))
			}
			fn render(name: &str, schema: &Self::Schema) -> Type {
				Type::group_type_builder(&schema.0).with_fields(&mut vec![$(Rc::new(<$type_ as Deserialize>::render(stringify!($name), &(schema.1).$name)),)*]).build().unwrap()
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
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

	let rows = read::<_,
		(
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
		)
		>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

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

	let rows = read::<_,
		(
			Option<String>,
			i32,
			f64,
			bool,
			Option<List<i32>>,
		)
		>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());

	let file = SerializedFileReader::new(File::open(&Path::new("./parquet-rs/data/commits.parquet")).unwrap()).unwrap();

	let rows = read::<_,
		(
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
		)
		>(&file);
	println!("{}", rows.unwrap().count());

	let rows = read::<_,Value>(&file);
	println!("{}", rows.unwrap().count());
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
impl Value {
	fn is_bool(&self) -> bool {
		if let Value::Bool(ret) = self { true } else { false }
	}
	fn as_bool(self) -> Option<bool> {
		if let Value::Bool(ret) = self { Some(ret) } else { None }
	}
	fn is_u8(&self) -> bool {
		if let Value::U8(ret) = self { true } else { false }
	}
	fn as_u8(self) -> Option<u8> {
		if let Value::U8(ret) = self { Some(ret) } else { None }
	}
	fn is_i8(&self) -> bool {
		if let Value::I8(ret) = self { true } else { false }
	}
	fn as_i8(self) -> Option<i8> {
		if let Value::I8(ret) = self { Some(ret) } else { None }
	}
	fn is_u16(&self) -> bool {
		if let Value::U16(ret) = self { true } else { false }
	}
	fn as_u16(self) -> Option<u16> {
		if let Value::U16(ret) = self { Some(ret) } else { None }
	}
	fn is_i16(&self) -> bool {
		if let Value::I16(ret) = self { true } else { false }
	}
	fn as_i16(self) -> Option<i16> {
		if let Value::I16(ret) = self { Some(ret) } else { None }
	}
	fn is_u32(&self) -> bool {
		if let Value::U32(ret) = self { true } else { false }
	}
	fn as_u32(self) -> Option<u32> {
		if let Value::U32(ret) = self { Some(ret) } else { None }
	}
	fn is_i32(&self) -> bool {
		if let Value::I32(ret) = self { true } else { false }
	}
	fn as_i32(self) -> Option<i32> {
		if let Value::I32(ret) = self { Some(ret) } else { None }
	}
	fn is_u64(&self) -> bool {
		if let Value::U64(ret) = self { true } else { false }
	}
	fn as_u64(self) -> Option<u64> {
		if let Value::U64(ret) = self { Some(ret) } else { None }
	}
	fn is_i64(&self) -> bool {
		if let Value::I64(ret) = self { true } else { false }
	}
	fn as_i64(self) -> Option<i64> {
		if let Value::I64(ret) = self { Some(ret) } else { None }
	}
	fn is_f32(&self) -> bool {
		if let Value::F32(ret) = self { true } else { false }
	}
	fn as_f32(self) -> Option<f32> {
		if let Value::F32(ret) = self { Some(ret) } else { None }
	}
	fn is_f64(&self) -> bool {
		if let Value::F64(ret) = self { true } else { false }
	}
	fn as_f64(self) -> Option<f64> {
		if let Value::F64(ret) = self { Some(ret) } else { None }
	}
	fn is_timestamp(&self) -> bool {
		if let Value::Timestamp(ret) = self { true } else { false }
	}
	fn as_timestamp(self) -> Option<Timestamp> {
		if let Value::Timestamp(ret) = self { Some(ret) } else { None }
	}
	fn is_array(&self) -> bool {
		if let Value::Array(ret) = self { true } else { false }
	}
	fn as_array(self) -> Option<Vec<u8>> {
		if let Value::Array(ret) = self { Some(ret) } else { None }
	}
	fn is_string(&self) -> bool {
		if let Value::String(ret) = self { true } else { false }
	}
	fn as_string(self) -> Option<String> {
		if let Value::String(ret) = self { Some(ret) } else { None }
	}
	fn is_list(&self) -> bool {
		if let Value::List(ret) = self { true } else { false }
	}
	fn as_list(self) -> Option<List<Value>> {
		if let Value::List(ret) = self { Some(ret) } else { None }
	}
	fn is_map(&self) -> bool {
		if let Value::Map(ret) = self { true } else { false }
	}
	fn as_map(self) -> Option<Map<Value,Value>> {
		if let Value::Map(ret) = self { Some(ret) } else { None }
	}
	fn is_group(&self) -> bool {
		if let Value::Group(ret) = self { true } else { false }
	}
	fn as_group(self) -> Option<Group> {
		if let Value::Group(ret) = self { Some(ret) } else { None }
	}
	fn is_option(&self) -> bool {
		if let Value::Option(ret) = self { true } else { false }
	}
	fn as_option(self) -> Option<Option<Value>> {
		if let Value::Option(ret) = self { Some(*ret) } else { None }
	}
}


// #[derive(Clone, PartialEq, Debug)]
// enum ValueRequired {
// 	Bool(bool),
// 	U8(u8),
// 	I8(i8),
// 	U16(u16),
// 	I16(i16),
// 	U32(u32),
// 	I32(i32),
// 	U64(u64),
// 	I64(i64),
// 	F32(f32),
// 	F64(f64),
// 	Timestamp(Timestamp),
// 	Array(Vec<u8>),
// 	String(String),
// 	List(List<Value>),
// 	Map(Map<Value,Value>),
// 	Group(Group),
// }
// enum ValueRequiredSchema {
// 	Bool(BoolSchema),
// 	U8(U8Schema),
// 	I8(I8Schema),
// 	U16(U16Schema),
// 	I16(I16Schema),
// 	U32(U32Schema),
// 	I32(I32Schema),
// 	U64(U64Schema),
// 	I64(I64Schema),
// 	F32(F32Schema),
// 	F64(F64Schema),
// 	Timestamp(TimestampSchema),
// 	Array(VecSchema),
// 	String(StringSchema),
// 	List(Box<ListSchema<ValueSchema>>),
// 	Map(Box<MapSchema<ValueSchema,ValueSchema>>),
// 	Group(GroupSchema),
// }
// impl TryFrom<ValueSchema> for ValueRequiredSchema {
// 	type Error = ParquetError;
// 	fn try_from(value: ValueSchema) -> Result<Self, Self::Error> {
// 		Ok(match value {
// 			ValueSchema::Bool(schema) => ValueRequiredSchema::Bool(schema),
// 			ValueSchema::U8(schema) => ValueRequiredSchema::U8(schema),
// 			ValueSchema::I8(schema) => ValueRequiredSchema::I8(schema),
// 			ValueSchema::U16(schema) => ValueRequiredSchema::U16(schema),
// 			ValueSchema::I16(schema) => ValueRequiredSchema::I16(schema),
// 			ValueSchema::U32(schema) => ValueRequiredSchema::U32(schema),
// 			ValueSchema::I32(schema) => ValueRequiredSchema::I32(schema),
// 			ValueSchema::U64(schema) => ValueRequiredSchema::U64(schema),
// 			ValueSchema::I64(schema) => ValueRequiredSchema::I64(schema),
// 			ValueSchema::F32(schema) => ValueRequiredSchema::F32(schema),
// 			ValueSchema::F64(schema) => ValueRequiredSchema::F64(schema),
// 			ValueSchema::Timestamp(schema) => ValueRequiredSchema::Timestamp(schema),
// 			ValueSchema::Array(schema) => ValueRequiredSchema::Array(schema),
// 			ValueSchema::String(schema) => ValueRequiredSchema::String(schema),
// 			ValueSchema::List(schema) => ValueRequiredSchema::List(schema),
// 			ValueSchema::Map(schema) => ValueRequiredSchema::Map(schema),
// 			ValueSchema::Group(schema) => ValueRequiredSchema::Group(schema),
// 			ValueSchema::Option(_) => return Err(ParquetError::General(String::from("ccc"))),
// 		})
// 	}
// }
// impl TryFrom<ValueSchema> for PrimitiveSchema {
// 	type Error = ParquetError;
// 	fn try_from(value: ValueSchema) -> Result<Self, Self::Error> {
// 		Ok(match value {
// 			ValueSchema::Bool(schema) => PrimitiveSchema::Bool(schema),
// 			ValueSchema::U8(schema) => PrimitiveSchema::U8(schema),
// 			ValueSchema::I8(schema) => PrimitiveSchema::I8(schema),
// 			ValueSchema::U16(schema) => PrimitiveSchema::U16(schema),
// 			ValueSchema::I16(schema) => PrimitiveSchema::I16(schema),
// 			ValueSchema::U32(schema) => PrimitiveSchema::U32(schema),
// 			ValueSchema::I32(schema) => PrimitiveSchema::I32(schema),
// 			ValueSchema::U64(schema) => PrimitiveSchema::U64(schema),
// 			ValueSchema::I64(schema) => PrimitiveSchema::I64(schema),
// 			ValueSchema::F32(schema) => PrimitiveSchema::F32(schema),
// 			ValueSchema::F64(schema) => PrimitiveSchema::F64(schema),
// 			ValueSchema::Timestamp(schema) => PrimitiveSchema::Timestamp(schema),
// 			ValueSchema::Array(schema) => PrimitiveSchema::Array(schema),
// 			ValueSchema::String(schema) => PrimitiveSchema::String(schema),
// 			ValueSchema::Option(OptionSchema(schema)) => return schema.try_into().map(OptionSchema).map(PrimitiveSchema::Option),
// 			ValueSchema::List(_) | ValueSchema::Map(_) | ValueSchema::Group(_) => return Err(ParquetError::General(String::from("ccc"))),
// 		})
// 	}
// }

// impl TryFrom<ValueRequiredSchema> for PrimitiveRequiredSchema {
// 	type Error = ParquetError;
// 	fn try_from(value: ValueRequiredSchema) -> Result<Self, Self::Error> {
// 		Ok(match value {
// 			ValueRequiredSchema::Bool(schema) => PrimitiveRequiredSchema::Bool(schema),
// 			ValueRequiredSchema::U8(schema) => PrimitiveRequiredSchema::U8(schema),
// 			ValueRequiredSchema::I8(schema) => PrimitiveRequiredSchema::I8(schema),
// 			ValueRequiredSchema::U16(schema) => PrimitiveRequiredSchema::U16(schema),
// 			ValueRequiredSchema::I16(schema) => PrimitiveRequiredSchema::I16(schema),
// 			ValueRequiredSchema::U32(schema) => PrimitiveRequiredSchema::U32(schema),
// 			ValueRequiredSchema::I32(schema) => PrimitiveRequiredSchema::I32(schema),
// 			ValueRequiredSchema::U64(schema) => PrimitiveRequiredSchema::U64(schema),
// 			ValueRequiredSchema::I64(schema) => PrimitiveRequiredSchema::I64(schema),
// 			ValueRequiredSchema::F32(schema) => PrimitiveRequiredSchema::F32(schema),
// 			ValueRequiredSchema::F64(schema) => PrimitiveRequiredSchema::F64(schema),
// 			ValueRequiredSchema::Timestamp(schema) => PrimitiveRequiredSchema::Timestamp(schema),
// 			ValueRequiredSchema::Array(schema) => PrimitiveRequiredSchema::Array(schema),
// 			ValueRequiredSchema::String(schema) => PrimitiveRequiredSchema::String(schema),
// 			ValueRequiredSchema::List(_) | ValueRequiredSchema::Map(_) | ValueRequiredSchema::Group(_) => return Err(ParquetError::General(String::from("ccc"))),
// 		})
// 	}
// }

// impl TryFrom<PrimitiveSchema> for PrimitiveRequiredSchema {
// 	type Error = ParquetError;
// 	fn try_from(value: PrimitiveSchema) -> Result<Self, Self::Error> {
// 		Ok(match value {
// 			PrimitiveSchema::Bool(schema) => PrimitiveRequiredSchema::Bool(schema),
// 			PrimitiveSchema::U8(schema) => PrimitiveRequiredSchema::U8(schema),
// 			PrimitiveSchema::I8(schema) => PrimitiveRequiredSchema::I8(schema),
// 			PrimitiveSchema::U16(schema) => PrimitiveRequiredSchema::U16(schema),
// 			PrimitiveSchema::I16(schema) => PrimitiveRequiredSchema::I16(schema),
// 			PrimitiveSchema::U32(schema) => PrimitiveRequiredSchema::U32(schema),
// 			PrimitiveSchema::I32(schema) => PrimitiveRequiredSchema::I32(schema),
// 			PrimitiveSchema::U64(schema) => PrimitiveRequiredSchema::U64(schema),
// 			PrimitiveSchema::I64(schema) => PrimitiveRequiredSchema::I64(schema),
// 			PrimitiveSchema::F32(schema) => PrimitiveRequiredSchema::F32(schema),
// 			PrimitiveSchema::F64(schema) => PrimitiveRequiredSchema::F64(schema),
// 			PrimitiveSchema::Timestamp(schema) => PrimitiveRequiredSchema::Timestamp(schema),
// 			PrimitiveSchema::Array(schema) => PrimitiveRequiredSchema::Array(schema),
// 			PrimitiveSchema::String(schema) => PrimitiveRequiredSchema::String(schema),
// 			PrimitiveSchema::Option(_) => return Err(ParquetError::General(String::from("ccc"))),
// 		})
// 	}
// }

// impl TryFrom<PrimitiveSchema> for Option PrimitiveRequiredSchema {
// 	type Error = ParquetError;
// 	fn try_from(value: PrimitiveSchema) -> Result<Self, Self::Error> {
// 		Ok(match value {
// 			PrimitiveSchema::Bool(schema) => PrimitiveRequiredSchema::Bool(schema),
// 			PrimitiveSchema::U8(schema) => PrimitiveRequiredSchema::U8(schema),
// 			PrimitiveSchema::I8(schema) => PrimitiveRequiredSchema::I8(schema),
// 			PrimitiveSchema::U16(schema) => PrimitiveRequiredSchema::U16(schema),
// 			PrimitiveSchema::I16(schema) => PrimitiveRequiredSchema::I16(schema),
// 			PrimitiveSchema::U32(schema) => PrimitiveRequiredSchema::U32(schema),
// 			PrimitiveSchema::I32(schema) => PrimitiveRequiredSchema::I32(schema),
// 			PrimitiveSchema::U64(schema) => PrimitiveRequiredSchema::U64(schema),
// 			PrimitiveSchema::I64(schema) => PrimitiveRequiredSchema::I64(schema),
// 			PrimitiveSchema::F32(schema) => PrimitiveRequiredSchema::F32(schema),
// 			PrimitiveSchema::F64(schema) => PrimitiveRequiredSchema::F64(schema),
// 			PrimitiveSchema::Timestamp(schema) => PrimitiveRequiredSchema::Timestamp(schema),
// 			PrimitiveSchema::Array(schema) => PrimitiveRequiredSchema::Array(schema),
// 			PrimitiveSchema::String(schema) => PrimitiveRequiredSchema::String(schema),
// 			PrimitiveSchema::Option(_) => return Err(ParquetError::General(String::from("ccc"))),
// 		})
// 	}
// }

// impl TryFrom<OptionSchema<ValueRequiredSchema>> for OptionSchema<PrimitiveRequiredSchema> where U: TryFrom<T> {
// 	type Error = ParquetError;
// 	fn try_from(value: OptionSchema<ValueRequiredSchema>) -> Result<Self, Self::Error> {
// 		value.0.try_into().map(OptionSchema)
// 	}
// }

// #[derive(Clone, Hash, PartialEq, Eq, Debug)]
// enum Primitive {
// 	Bool(bool),
// 	U8(u8),
// 	I8(i8),
// 	U16(u16),
// 	I16(i16),
// 	U32(u32),
// 	I32(i32),
// 	U64(u64),
// 	I64(i64),
// 	// F32(f32),
// 	// F64(f64),
// 	Timestamp(Timestamp),
// 	Array(Vec<u8>),
// 	String(String),
// 	Option(Option<PrimitiveRequired>),
// }
// enum PrimitiveSchema {
// 	Bool(BoolSchema),
// 	U8(U8Schema),
// 	I8(I8Schema),
// 	U16(U16Schema),
// 	I16(I16Schema),
// 	U32(U32Schema),
// 	I32(I32Schema),
// 	U64(U64Schema),
// 	I64(I64Schema),
// 	F32(F32Schema),
// 	F64(F64Schema),
// 	Timestamp(TimestampSchema),
// 	Array(VecSchema),
// 	String(StringSchema),
// 	Option(OptionSchema<PrimitiveRequiredSchema>),
// }
// impl PrimitiveSchema {
// 	fn as_option(self) -> Option<OptionSchema<PrimitiveRequiredSchema>> {
// 		if let PrimitiveSchema::Option(schema) = self { Some(schema) } else { None }
// 	}
// }
// #[derive(Clone, Hash, PartialEq, Eq, Debug)]
// enum PrimitiveRequired {
// 	Bool(bool),
// 	U8(u8),
// 	I8(i8),
// 	U16(u16),
// 	I16(i16),
// 	U32(u32),
// 	I32(i32),
// 	U64(u64),
// 	I64(i64),
// 	// F32(f32),
// 	// F64(f64),
// 	Timestamp(Timestamp),
// 	Array(Vec<u8>),
// 	String(String),
// }
// enum PrimitiveRequiredSchema {
// 	Bool(BoolSchema),
// 	U8(U8Schema),
// 	I8(I8Schema),
// 	U16(U16Schema),
// 	I16(I16Schema),
// 	U32(U32Schema),
// 	I32(I32Schema),
// 	U64(U64Schema),
// 	I64(I64Schema),
// 	F32(F32Schema),
// 	F64(F64Schema),
// 	Timestamp(TimestampSchema),
// 	Array(VecSchema),
// 	String(StringSchema),
// }
#[derive(Clone, PartialEq, Debug)]
struct Group(pub Vec<Value>,pub Rc<HashMap<String,usize>>);
type Row = Group;
struct GroupSchema(Vec<ValueSchema>,HashMap<String,usize>);

impl Deserialize for Group {
	type Schema = GroupSchema;
	type Reader = GroupReader;

	fn placeholder() -> Self::Schema {
		unimplemented!()
	}
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
	fn render(name: &str, schema: &Self::Schema) -> Type {
		// Type::group_type_builder(name).with_repetition(Repetition::REQUIRED).with_fields(&mut vec![$(Rc::new(<$type_ as Deserialize>::render(stringify!($name), &schema.$name)),)*]).build().unwrap()
		unimplemented!()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
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

struct RootSchema<T,S>(String, S, PhantomData<fn(T)>);
struct VecSchema(Option<u32>);
struct ArraySchema<T>(PhantomData<fn(T)>);
struct TupleSchema<T>(T);
struct TupleReader<T>(T);
struct MapSchema<K,V>(K,V,Option<String>,Option<String>,Option<String>);
struct OptionSchema<T>(T);
struct ListSchema<T>(T,Option<(Option<String>,Option<String>)>);
struct BoolSchema;
struct U8Schema;
struct I8Schema;
struct U16Schema;
struct I16Schema;
struct U32Schema;
struct I32Schema;
struct U64Schema;
struct I64Schema;
struct F64Schema;
struct F32Schema;
struct StringSchema;
enum TimestampSchema {
	Int96,
	Millis,
	Micros,
}


// impl Deserialize for Root<Value> {
// 	type Schema = RootSchema<Value, ValueSchema>;
// 	type Reader = Reader;
// 	fn placeholder() -> Self::Schema {
// 		unimplemented!()
// 	}
// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		if schema.is_schema() {
// 			let mut schema2 = schema.clone();
// 			let basic_info = match schema2 {Type::PrimitiveType{ref mut basic_info,..} => basic_info, Type::GroupType{ref mut basic_info,..} => basic_info};
// 			basic_info.set_repetition(Some(Repetition::REQUIRED));
// 			return Value::parse(&schema2).map(|(name,schema)|(String::from(""),RootSchema(name, schema, PhantomData)))
// 		}
// 		Err(ParquetError::General(String::from("Root<Value>")))
// 	}
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		assert_eq!(name, "");
// 		let mut schema2 = Value::render(&schema.0, &schema.1);
// 		let basic_info = match schema2 {Type::PrimitiveType{ref mut basic_info,..} => basic_info, Type::GroupType{ref mut basic_info,..} => basic_info};
// 		basic_info.set_repetition(None);
// 		schema2
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		unimplemented!()
// 	}
// }


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

	fn placeholder() -> Self::Schema {
		unimplemented!()
	}
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
	fn render(name: &str, schema: &Self::Schema) -> Type {
		let (schema, repetition) = match schema {
			&ValueSchema::Option(box OptionSchema(ref schema)) => (schema, Repetition::OPTIONAL),
			&ValueSchema::List(box ListSchema(ref schema, None)) => (schema, Repetition::REPEATED),
			schema => (schema, Repetition::REQUIRED),
		};
		if let Some((physical,logical,length)) = match schema {
			ValueSchema::Bool(_) => Some((PhysicalType::BOOLEAN, LogicalType::NONE, None)),
			ValueSchema::U8(_) => Some((PhysicalType::INT32, LogicalType::UINT_8, None)),
			ValueSchema::I8(_) => Some((PhysicalType::INT32, LogicalType::INT_8, None)),
			ValueSchema::U16(_) => Some((PhysicalType::INT32, LogicalType::UINT_16, None)),
			ValueSchema::I16(_) => Some((PhysicalType::INT32, LogicalType::INT_16, None)),
			ValueSchema::U32(_) => Some((PhysicalType::INT32, LogicalType::UINT_32, None)),
			ValueSchema::I32(_) => Some((PhysicalType::INT32, LogicalType::INT_32, None)),
			ValueSchema::U64(_) => Some((PhysicalType::INT64, LogicalType::UINT_64, None)),
			ValueSchema::I64(_) => Some((PhysicalType::INT64, LogicalType::INT_64, None)),
			ValueSchema::F32(_) => Some((PhysicalType::FLOAT, LogicalType::NONE, None)),
			ValueSchema::F64(_) => Some((PhysicalType::DOUBLE, LogicalType::NONE, None)),
			ValueSchema::Timestamp(TimestampSchema::Int96) => Some((PhysicalType::INT96, LogicalType::NONE, None)),
			ValueSchema::Timestamp(TimestampSchema::Millis) => Some((PhysicalType::INT64, LogicalType::TIMESTAMP_MILLIS, None)),
			ValueSchema::Timestamp(TimestampSchema::Micros) => Some((PhysicalType::INT64, LogicalType::TIMESTAMP_MICROS, None)),
			ValueSchema::Array(VecSchema(None)) => Some((PhysicalType::BYTE_ARRAY, LogicalType::NONE, None)),
			ValueSchema::Array(VecSchema(Some(x))) => Some((PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::NONE, Some(*x))),
			ValueSchema::String(_) => Some((PhysicalType::BYTE_ARRAY, LogicalType::UTF8, None)),
			ValueSchema::List(_) | ValueSchema::Map(_) | ValueSchema::Group(_) => None,
			ValueSchema::Option(_) => unreachable!(),
		} {
			return Type::primitive_type_builder(name, physical)
				.with_repetition(repetition)
				.with_logical_type(logical)
				.with_length(length.map(|x| x as i32).unwrap_or(-1))
				.with_precision(-1)
				.with_scale(-1)
				.build()
				.unwrap()
		}
		if let Some((logical_type, mut fields)) = match schema {
			ValueSchema::List(box ListSchema(ref element_schema, Some((ref list_name, ref element_name)))) => {
				let list_name = list_name.as_ref().map(|x|&**x).unwrap_or("list");
				let element_name = element_name.as_ref().map(|x|&**x).unwrap_or("element");
				Some((LogicalType::LIST, vec![Rc::new(
						Type::group_type_builder(list_name)
							.with_repetition(Repetition::REPEATED)
							.with_logical_type(LogicalType::NONE)
							.with_fields(&mut vec![Rc::new(Value::render(element_name, element_schema))])
							.build()
							.unwrap(),
					)]))
			}
			ValueSchema::List(box ListSchema(ref element_schema, None)) => unreachable!(),
			ValueSchema::Map(box MapSchema(ref key_schema, ref value_schema, ref key_value_name, ref key_name, ref value_name)) => {
				let key_value_name = key_value_name.as_ref().map(|x|&**x).unwrap_or("key_value");
				let key_name = key_name.as_ref().map(|x|&**x).unwrap_or("key");
				let value_name = value_name.as_ref().map(|x|&**x).unwrap_or("value");
				Some((LogicalType::MAP, vec![Rc::new(
						Type::group_type_builder(key_value_name)
							.with_repetition(Repetition::REPEATED)
							.with_logical_type(LogicalType::NONE)
							.with_fields(&mut vec![Rc::new(Value::render(key_name, key_schema)),Rc::new(Value::render(value_name, value_schema))])
							.build()
							.unwrap(),
					)]))
			}
			ValueSchema::Group(GroupSchema(ref fields, ref names)) => {
				let mut names_ = vec![None; fields.len()];
				for (name,&index) in names {
					names_[index].replace(name);
				}
				Some((LogicalType::NONE, fields.iter().enumerate().map(|(index,field)|Rc::new(Value::render(names_[index].take().unwrap(), field))).collect()))
			}
			_ => None,
		} {
			return Type::group_type_builder(name)
				.with_repetition(repetition)
				.with_logical_type(logical_type)
				.with_fields(&mut fields)
				.build()
				.unwrap()
		}
		unimplemented!()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
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
	fn placeholder() -> Self::Schema {
		unimplemented!()
	}
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
	fn render(name: &str, schema: &Self::Schema) -> Type {
		assert_eq!(name, "");
		let name = &schema.0;
		let (schema, repetition) = match &schema.1 {
			&ValueSchema::Option(box OptionSchema(ref schema)) => (schema, Repetition::OPTIONAL),
			&ValueSchema::List(box ListSchema(ref schema, None)) => (schema, Repetition::REPEATED),
			schema => (schema, Repetition::REQUIRED),
		};
		if let Some((physical,logical)) = match schema {
			ValueSchema::Bool(_) => Some((PhysicalType::BOOLEAN, LogicalType::NONE)),
			ValueSchema::U8(_) => Some((PhysicalType::INT32, LogicalType::UINT_8)),
			ValueSchema::I8(_) => Some((PhysicalType::INT32, LogicalType::INT_8)),
			ValueSchema::U16(_) => Some((PhysicalType::INT32, LogicalType::UINT_16)),
			ValueSchema::I16(_) => Some((PhysicalType::INT32, LogicalType::INT_16)),
			ValueSchema::U32(_) => Some((PhysicalType::INT32, LogicalType::UINT_32)),
			ValueSchema::I32(_) => Some((PhysicalType::INT32, LogicalType::INT_32)),
			ValueSchema::U64(_) => Some((PhysicalType::INT64, LogicalType::UINT_64)),
			ValueSchema::I64(_) => Some((PhysicalType::INT64, LogicalType::INT_64)),
			ValueSchema::F32(_) => Some((PhysicalType::FLOAT, LogicalType::NONE)),
			ValueSchema::F64(_) => Some((PhysicalType::DOUBLE, LogicalType::NONE)),
			ValueSchema::Timestamp(_) => Some((PhysicalType::INT96, LogicalType::NONE)),
			ValueSchema::Array(_) => Some((PhysicalType::BYTE_ARRAY, LogicalType::NONE)),
			ValueSchema::String(_) => Some((PhysicalType::BYTE_ARRAY, LogicalType::UTF8)),
			ValueSchema::List(_) | ValueSchema::Map(_) | ValueSchema::Group(_) => None,
			ValueSchema::Option(_) => unreachable!(),
		} {
			return Type::primitive_type_builder(name, physical)
				.with_repetition(repetition)
				.with_logical_type(logical)
				.with_length(-1)
				.with_precision(-1)
				.with_scale(-1)
				.build()
				.unwrap()
		}
		if let Some((logical_type, mut fields)) = match schema {
			ValueSchema::List(box ListSchema(ref element_schema, Some((ref list_name, ref element_name)))) => {
				let list_name = list_name.as_ref().map(|x|&**x).unwrap_or("list");
				let element_name = element_name.as_ref().map(|x|&**x).unwrap_or("element");
				Some((LogicalType::LIST, vec![Rc::new(
						Type::group_type_builder(list_name)
							.with_repetition(Repetition::REPEATED)
							.with_logical_type(LogicalType::NONE)
							.with_fields(&mut vec![Rc::new(Value::render(element_name, element_schema))])
							.build()
							.unwrap(),
					)]))
			}
			ValueSchema::Group(GroupSchema(ref fields, ref names)) => {
				let mut names_ = vec![None; fields.len()];
				for (name,&index) in names {
					names_[index].replace(name);
				}
				Some((LogicalType::NONE, fields.iter().enumerate().map(|(index,field)|Rc::new(Value::render(names_[index].take().unwrap(), field))).collect()))
			}
			_ => None,
		} {
			return Type::group_type_builder(name)
				// .with_repetition(repetition)
				.with_logical_type(logical_type)
				.with_fields(&mut fields)
				.build()
				.unwrap()
		}
		unimplemented!()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		RootReader(match schema.1 {
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
		})
	}
}

impl Deserialize for Option<Value> {
	type Schema = OptionSchema<ValueSchema>;
	// type Reader = OptionReader<ValueReader>;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		unimplemented!()
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		<Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
			match schema {
				ValueSchema::Option(schema) => Ok((name, *schema)),
				_ => Err(ParquetError::General(String::from(""))),
			}
		})
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		unimplemented!()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		OptionReader{def_level: curr_def_level, reader: <Value as Deserialize>::reader(&schema.0, path, curr_def_level+1, curr_rep_level, paths)}//, |x:Value|x.as_option().ok_or(ParquetError::General(String::from("x"))))
	}
}

// enum Any<T> {
// 	Option(Option<T>),
// 	One(T),
// 	List(List<T>),
// }
// impl<T> Any<T> {
// 	fn as_one(self) -> Option<T> {
// 		match self {
// 			Any::One(t) => Some(t),
// 			Any::Option(_) | Any::List(_) => None,
// 		}
// 	}
// 	fn as_option(self) -> Option<Option<T>> {
// 		match self {
// 			Any::Option(t) => Some(t),
// 			Any::One(_) | Any::List(_) => None,
// 		}
// 	}
// 	fn as_list(self) -> Option<List<T>> {
// 		match self {
// 			Any::List(t) => Some(t),
// 			Any::Option(_) | Any::One(_) => None,
// 		}
// 	}
// }
// enum AnySchema<T> {
// 	Option(OptionSchema<T>),
// 	One(T),
// 	List(ListSchema<T>),
// }
// impl<T> AnySchema<T> {
// 	fn as_one(self) -> Option<T> {
// 		match self {
// 			AnySchema::One(t) => Some(t),
// 			AnySchema::Option(_) | AnySchema::List(_) => None,
// 		}
// 	}
// 	fn as_option(self) -> Option<OptionSchema<T>> {
// 		match self {
// 			AnySchema::Option(t) => Some(t),
// 			AnySchema::One(_) | AnySchema::List(_) => None,
// 		}
// 	}
// 	fn as_list(self) -> Option<ListSchema<T>> {
// 		match self {
// 			AnySchema::List(t) => Some(t),
// 			AnySchema::Option(_) | AnySchema::One(_) => None,
// 		}
// 	}
// }

// impl Deserialize for Any<ValueRequired> {
// 	type Schema = AnySchema<ValueRequiredSchema>;
// 	type Reader = Reader;
//
// 	fn placeholder() -> Self::Schema {
// 		unimplemented!()
// 	}
// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		Value::parse(schema).and_then(|(name, schema)| {
// 			match schema {
// 				ValueSchema::Bool(schema) => Ok(AnySchema::One(ValueRequiredSchema::Bool(schema))),
// 				ValueSchema::Option(OptionSchema(ValueRequiredSchema::Bool(schema))) => Ok(AnySchema::Option(OptionSchema(ValueRequiredSchema::Bool(schema)))),
// 				ValueSchema::List(box ListSchema(ValueSchema::Bool(schema), a)) => Ok(AnySchema::List(ListSchema(ValueRequiredSchema::Bool(schema), a))),
// 				_ => Err(ParquetError::General(String::from("")))
// 			}.map(|schema| (name, schema))
// 		})
// 	}
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		Value::render(name, &match schema {
// 			AnySchema::One(ValueRequiredSchema::Bool(schema)) => ValueSchema::Bool(*schema),
// 			AnySchema::Option(OptionSchema(ValueRequiredSchema::Bool(schema))) => ValueSchema::Option(OptionSchema(ValueRequiredSchema::Bool(*schema))),
// 			AnySchema::List(ListSchema(ValueRequiredSchema::Bool(schema), a)) => ValueSchema::List(Box::new(ListSchema(ValueSchema::Bool(*schema), a.clone()))),
// 			_ => unimplemented!()
// 		})
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		unimplemented!()
// 	}
// }
// // impl Deserialize for Option<ValueRequired> {
// // 	type Schema = OptionSchema<ValueRequiredSchema>;
// // 	type Reader = Reader;
// // 	fn placeholder() -> Self::Schema {
// // 		unimplemented!()
// // 	}
// // 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// // 		Any::<ValueRequired>::parse(schema).and_then(|(name,schema)|schema.as_option().ok_or(ParquetError::General(String::from("a"))).map(|schema|(name,schema)))
// // 	}
// // 	fn render(name: &str, schema: &Self::Schema) -> Type {
// // 		Any::<ValueRequired>::render(name, &AnySchema::Option(OptionSchema(schema.0)))
// // 	}
// // 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// // 		unimplemented!()
// // 	}
// // }
// // impl Deserialize for ValueRequired {
// // 	type Schema = ValueRequiredSchema;
// // 	type Reader = Reader;
// // 	fn placeholder() -> Self::Schema {
// // 		unimplemented!()
// // 	}
// // 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// // 		Any::<ValueRequired>::parse(schema).and_then(|(name,schema)|schema.as_one().ok_or(ParquetError::General(String::from("a"))).map(|schema|(name,schema)))
// // 	}
// // 	fn render(name: &str, schema: &Self::Schema) -> Type {
// // 		Any::<ValueRequired>::render(name, &AnySchema::One(*schema))
// // 	}
// // 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// // 		unimplemented!()
// // 	}
// // }


// impl Deserialize for Primitive {
// 	type Schema = PrimitiveSchema;
// 	type Reader = Reader;
// 	fn placeholder() -> Self::Schema {
// 		unimplemented!()
// 	}
// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		Value::parse(schema).and_then(|(name,schema)|schema.try_into().map(|schema|(name,schema)))
// 	}
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		unimplemented!()
// 		// let (physical,logical) = match schema {
// 		// 	PrimitiveSchema::Bool(_) => (PhysicalType::BOOLEAN, LogicalType::NONE),
// 		// 	PrimitiveSchema::U8(_) => (PhysicalType::INT32, LogicalType::UINT_8),
// 		// 	PrimitiveSchema::I8(_) => (PhysicalType::INT32, LogicalType::INT_8),
// 		// 	PrimitiveSchema::U16(_) => (PhysicalType::INT32, LogicalType::UINT_16),
// 		// 	PrimitiveSchema::I16(_) => (PhysicalType::INT32, LogicalType::INT_16),
// 		// 	PrimitiveSchema::U32(_) => (PhysicalType::INT32, LogicalType::UINT_32),
// 		// 	PrimitiveSchema::I32(_) => (PhysicalType::INT32, LogicalType::INT_32),
// 		// 	PrimitiveSchema::U64(_) => (PhysicalType::INT64, LogicalType::UINT_64),
// 		// 	PrimitiveSchema::I64(_) => (PhysicalType::INT64, LogicalType::INT_64),
// 		// 	PrimitiveSchema::F32(_) => (PhysicalType::FLOAT, LogicalType::NONE),
// 		// 	PrimitiveSchema::F64(_) => (PhysicalType::DOUBLE, LogicalType::NONE),
// 		// 	PrimitiveSchema::Timestamp(_) => (PhysicalType::INT96, LogicalType::NONE),
// 		// 	PrimitiveSchema::Array(_) => (PhysicalType::BYTE_ARRAY, LogicalType::NONE),
// 		// 	PrimitiveSchema::String(_) => (PhysicalType::BYTE_ARRAY, LogicalType::UTF8),
// 		// };
// 		// Type::primitive_type_builder(name, physical)
// 		// 	.with_repetition(Repetition::REQUIRED)
// 		// 	.with_logical_type(logical)
// 		// 	.with_length(-1)
// 		// 	.with_precision(-1)
// 		// 	.with_scale(-1)
// 		// 	.build()
// 		// 	.unwrap()
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		unimplemented!()
// 	}
// }
// impl Deserialize for PrimitiveRequired {
// 	type Schema = PrimitiveRequiredSchema;
// 	type Reader = Reader;
// 	fn placeholder() -> Self::Schema {
// 		unimplemented!()
// 	}
// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		Primitive::parse(schema).and_then(|(name,schema)|schema.try_into().map(|schema|(name,schema)))
// 	}
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		let (physical,logical) = match schema {
// 			PrimitiveRequiredSchema::Bool(_) => (PhysicalType::BOOLEAN, LogicalType::NONE),
// 			PrimitiveRequiredSchema::U8(_) => (PhysicalType::INT32, LogicalType::UINT_8),
// 			PrimitiveRequiredSchema::I8(_) => (PhysicalType::INT32, LogicalType::INT_8),
// 			PrimitiveRequiredSchema::U16(_) => (PhysicalType::INT32, LogicalType::UINT_16),
// 			PrimitiveRequiredSchema::I16(_) => (PhysicalType::INT32, LogicalType::INT_16),
// 			PrimitiveRequiredSchema::U32(_) => (PhysicalType::INT32, LogicalType::UINT_32),
// 			PrimitiveRequiredSchema::I32(_) => (PhysicalType::INT32, LogicalType::INT_32),
// 			PrimitiveRequiredSchema::U64(_) => (PhysicalType::INT64, LogicalType::UINT_64),
// 			PrimitiveRequiredSchema::I64(_) => (PhysicalType::INT64, LogicalType::INT_64),
// 			PrimitiveRequiredSchema::F32(_) => (PhysicalType::FLOAT, LogicalType::NONE),
// 			PrimitiveRequiredSchema::F64(_) => (PhysicalType::DOUBLE, LogicalType::NONE),
// 			PrimitiveRequiredSchema::Timestamp(_) => (PhysicalType::INT96, LogicalType::NONE),
// 			PrimitiveRequiredSchema::Array(_) => (PhysicalType::BYTE_ARRAY, LogicalType::NONE),
// 			PrimitiveRequiredSchema::String(_) => (PhysicalType::BYTE_ARRAY, LogicalType::UTF8),
// 		};
// 		Type::primitive_type_builder(name, physical)
// 			.with_repetition(Repetition::REQUIRED)
// 			.with_logical_type(logical)
// 			.with_length(-1)
// 			.with_precision(-1)
// 			.with_scale(-1)
// 			.build()
// 			.unwrap()
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		unimplemented!()
// 	}
// }
// impl Deserialize for Option<PrimitiveRequired> {
// 	type Schema = OptionSchema<PrimitiveRequiredSchema>;
// 	type Reader = Reader;
// 	fn placeholder() -> Self::Schema {
// 		unimplemented!()
// 	}
// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		Primitive::parse(schema).and_then(|(name,schema)|schema.as_option().ok_or(ParquetError::General(String::from("aA"))).map(|schema|(name,schema)))
// 	}
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		let (physical,logical) = match schema.0 {
// 			PrimitiveRequiredSchema::Bool(_) => (PhysicalType::BOOLEAN, LogicalType::NONE),
// 			PrimitiveRequiredSchema::U8(_) => (PhysicalType::INT32, LogicalType::UINT_8),
// 			PrimitiveRequiredSchema::I8(_) => (PhysicalType::INT32, LogicalType::INT_8),
// 			PrimitiveRequiredSchema::U16(_) => (PhysicalType::INT32, LogicalType::UINT_16),
// 			PrimitiveRequiredSchema::I16(_) => (PhysicalType::INT32, LogicalType::INT_16),
// 			PrimitiveRequiredSchema::U32(_) => (PhysicalType::INT32, LogicalType::UINT_32),
// 			PrimitiveRequiredSchema::I32(_) => (PhysicalType::INT32, LogicalType::INT_32),
// 			PrimitiveRequiredSchema::U64(_) => (PhysicalType::INT64, LogicalType::UINT_64),
// 			PrimitiveRequiredSchema::I64(_) => (PhysicalType::INT64, LogicalType::INT_64),
// 			PrimitiveRequiredSchema::F32(_) => (PhysicalType::FLOAT, LogicalType::NONE),
// 			PrimitiveRequiredSchema::F64(_) => (PhysicalType::DOUBLE, LogicalType::NONE),
// 			PrimitiveRequiredSchema::Timestamp(_) => (PhysicalType::INT96, LogicalType::NONE),
// 			PrimitiveRequiredSchema::Array(_) => (PhysicalType::BYTE_ARRAY, LogicalType::NONE),
// 			PrimitiveRequiredSchema::String(_) => (PhysicalType::BYTE_ARRAY, LogicalType::UTF8),
// 		};
// 		Type::primitive_type_builder(name, physical)
// 			.with_repetition(Repetition::REQUIRED)
// 			.with_logical_type(logical)
// 			.with_length(-1)
// 			.with_precision(-1)
// 			.with_scale(-1)
// 			.build()
// 			.unwrap()
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		unimplemented!()
// 	}
// }

trait Deserialize: Sized {
	type Schema;
	type Reader: RRReader<Item = Self>;
	fn placeholder() -> Self::Schema;
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError>;
	fn render(name: &str, schema: &Self::Schema) -> Type;
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader;
}
// fn read<'a, R: ParquetReader + 'static, T>(
// 	reader: R,
// ) -> Result<impl Iterator<Item = T> + 'a,ParquetError> where T: 'a, Root<T>: Deserialize, <Root<T> as Deserialize>::Schema: 'a {
// 	let reader = SerializedFileReader::new(reader).unwrap();
// 	read2(&reader)
// }
fn read<'a, R: ParquetReader + 'static, T>(
	reader: &'a SerializedFileReader<R>,
) -> Result<impl Iterator<Item = T> + 'a,ParquetError> where Root<T>: Deserialize, <Root<T> as Deserialize>::Schema: 'a, <Root<T> as Deserialize>::Reader: 'a {
	let file_schema = reader.metadata().file_metadata().schema_descr_ptr();
	let file_schema = file_schema.root_schema();
	let schema = <Root<T> as Deserialize>::parse(file_schema).map_err(|err| {
		let schema: Type = <Root<T> as Deserialize>::render("", &<Root<T> as Deserialize>::placeholder());
		let mut b = Vec::new();
		print_schema(&mut b, file_schema);
		let mut a = Vec::new();
		print_schema(&mut a, &schema);
		ParquetError::General(format!(
			"Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
			String::from_utf8(b).unwrap(),
			String::from_utf8(a).unwrap(),
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
	let schema = <Root<T>>::render("", &schema);
	print_schema(&mut std::io::stdout(), &schema);
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

	fn placeholder() -> Self::Schema {
		MapSchema(K::placeholder(), V::placeholder(), None, None, None)
	}
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
	fn render(name: &str, schema: &Self::Schema) -> Type {
		let key_value_name = schema.2.as_ref().map(|x|&**x).unwrap_or("key_value");
		let key_name = schema.3.as_ref().map(|x|&**x).unwrap_or("key");
		let value_name = schema.4.as_ref().map(|x|&**x).unwrap_or("value");
		Type::group_type_builder(name)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::MAP)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder(key_value_name)
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(K::render(key_name, &schema.0)),Rc::new(V::render(value_name, &schema.1))])
					.build()
					.unwrap(),
			)])
			.build()
			.unwrap()
	}

	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
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
impl<K,V> Deserialize for Option<Map<K,V>>
where
	K: Deserialize + Hash + Eq,
	V: Deserialize,
{
	type Schema = OptionSchema<MapSchema<K::Schema, V::Schema>>;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		OptionSchema(MapSchema(K::placeholder(), V::placeholder(), None, None, None))
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && (schema.get_basic_info().logical_type() == LogicalType::MAP || schema.get_basic_info().logical_type() == LogicalType::MAP_KEY_VALUE) && schema.get_fields().len() == 1 {
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 2 {
				let mut fields = sub_schema.get_fields().into_iter();
				let (key, value) = (fields.next().unwrap(), fields.next().unwrap());
				let key_value_name = if sub_schema.name() == "key_value" { None } else { Some(sub_schema.name().to_owned()) };
				let key_name = if key.name() == "key" { None } else { Some(key.name().to_owned()) };
				let value_name = if value.name() == "value" { None } else { Some(value.name().to_owned()) };
				return Ok((schema.name().to_owned(), OptionSchema(MapSchema(K::parse(&*key)?.1,V::parse(&*value)?.1, key_value_name, key_name, value_name))));
			}
		}
		Err(ParquetError::General(String::from("Option<Map<K,V>>")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		let key_value_name = (schema.0).2.as_ref().map(|x|&**x).unwrap_or("key_value");
		let key_name = (schema.0).3.as_ref().map(|x|&**x).unwrap_or("key");
		let value_name = (schema.0).4.as_ref().map(|x|&**x).unwrap_or("value");
		Type::group_type_builder(name)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::MAP)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder(key_value_name)
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(K::render(key_name, &(schema.0).0)),Rc::new(V::render(value_name, &(schema.0).1))])
					.build()
					.unwrap(),
			)])
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let key_value_name = (schema.0).2.as_ref().map(|x|&**x).unwrap_or("key_value");
		let key_name = (schema.0).3.as_ref().map(|x|&**x).unwrap_or("key");
		let value_name = (schema.0).4.as_ref().map(|x|&**x).unwrap_or("value");

		path.push(key_value_name.to_owned());
		path.push(key_name.to_owned());
		let keys_reader = K::reader(&(schema.0).0, path, curr_def_level + 1 + 1, curr_rep_level + 1, paths);
		path.pop().unwrap();
		path.push(value_name.to_owned());
		let values_reader = V::reader(&(schema.0).1, path, curr_def_level + 1 + 1, curr_rep_level + 1, paths);
		path.pop().unwrap();
		path.pop().unwrap();

		OptionReader{def_level: curr_def_level, reader: MapReader(KeyValueReader{
			def_level: curr_def_level + 1,
			rep_level: curr_rep_level,
			keys_reader,
			values_reader
		}, |x:Vec<_>|Ok(Map(x.into_iter().collect())))}
	}
}


impl<T> Deserialize for List<T>
where
	T: Deserialize,
{
	type Schema = ListSchema<T::Schema>;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		ListSchema(T::placeholder(), Some((None, None)))
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
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
	fn render(name: &str, schema: &Self::Schema) -> Type {
		if let Some((ref list_name,ref element_name)) = schema.1 {
			let list_name = list_name.as_ref().map(|x|&**x).unwrap_or("list");
			let element_name = element_name.as_ref().map(|x|&**x).unwrap_or("element");
			Type::group_type_builder(name)
				.with_repetition(Repetition::REQUIRED)
				.with_logical_type(LogicalType::LIST)
				.with_fields(&mut vec![Rc::new(
					Type::group_type_builder(list_name)
						.with_repetition(Repetition::REPEATED)
						.with_logical_type(LogicalType::NONE)
						.with_fields(&mut vec![Rc::new(T::render(element_name, &schema.0))])
						.build()
						.unwrap(),
				)])
				.build()
				.unwrap()
		} else {
			let mut ret = T::render(name, &schema.0);
			let basic_info = match ret {Type::PrimitiveType{ref mut basic_info,..} => basic_info, Type::GroupType{ref mut basic_info,..} => basic_info};
			assert_eq!(basic_info.repetition(), Repetition::REQUIRED);
			basic_info.set_repetition(Some(Repetition::REPEATED));
			ret
		}
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
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
impl<T> Deserialize for Option<List<T>>
where
	T: Deserialize,
{
	type Schema = OptionSchema<ListSchema<T::Schema>>;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		OptionSchema(ListSchema(T::placeholder(), Some((None, None))))
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_basic_info().logical_type() == LogicalType::LIST && schema.get_fields().len() == 1 {
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group() && !sub_schema.is_schema() && sub_schema.get_basic_info().repetition() == Repetition::REPEATED && sub_schema.get_fields().len() == 1 {
				let element = sub_schema.get_fields().into_iter().nth(0).unwrap();
				let list_name = if sub_schema.name() == "list" { None } else { Some(sub_schema.name().to_owned()) };
				let element_name = if element.name() == "element" { None } else { Some(element.name().to_owned()) };
				return Ok((schema.name().to_owned(), OptionSchema(ListSchema(T::parse(&*element)?.1, Some((list_name, element_name))))));
			}
		}
		Err(ParquetError::General(String::from("Option<List<T>>")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		if let Some((ref list_name,ref element_name)) = (schema.0).1 {
			let list_name = list_name.as_ref().map(|x|&**x).unwrap_or("list");
			let element_name = element_name.as_ref().map(|x|&**x).unwrap_or("element");
			Type::group_type_builder(name)
				.with_repetition(Repetition::OPTIONAL)
				.with_logical_type(LogicalType::LIST)
				.with_fields(&mut vec![Rc::new(
					Type::group_type_builder(list_name)
						.with_repetition(Repetition::REPEATED)
						.with_logical_type(LogicalType::NONE)
						.with_fields(&mut vec![Rc::new(T::render(element_name, &(schema.0).0))])
						.build()
						.unwrap(),
				)])
				.build()
				.unwrap()
		} else {
			unreachable!()
		}
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		if let Some((ref list_name,ref element_name)) = (schema.0).1 {
			let list_name = list_name.as_ref().map(|x|&**x).unwrap_or("list");
			let element_name = element_name.as_ref().map(|x|&**x).unwrap_or("element");

			path.push(list_name.to_owned());
			path.push(element_name.to_owned());
			let reader = T::reader(&(schema.0).0, path, curr_def_level + 1 + 1, curr_rep_level + 1, paths);
			path.pop().unwrap();
			path.pop().unwrap();

			OptionReader{def_level: curr_def_level, reader: MapReader(RepeatedReader{
				def_level: curr_def_level+1,
				rep_level: curr_rep_level,
				reader,
			}, |x|Ok(List(x)))}
		} else {
			unreachable!()
		}
	}
}

// impl Deserialize for Any<bool> {
// 	type Schema = AnySchema<BoolSchema>;
// 	type Reader = Reader;
// 	fn placeholder() -> Self::Schema {
// 		AnySchema::One(BoolSchema)
// 	}
// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		Value::parse(schema).and_then(|(name, schema)| {
// 			match schema {
// 				ValueSchema::Bool(schema) => Ok(AnySchema::One(schema)),
// 				ValueSchema::Option(OptionSchema(ValueRequiredSchema::Bool(schema))) => Ok(AnySchema::Option(OptionSchema(schema))),
// 				ValueSchema::List(box ListSchema(ValueSchema::Bool(schema), a)) => Ok(AnySchema::List(ListSchema(schema, a))),
// 				_ => Err(ParquetError::General(String::from("")))
// 			}.map(|schema| (name, schema))
// 		})
// 	}
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		Value::render(name, &match schema {
// 			AnySchema::One(schema) => ValueSchema::Bool(*schema),
// 			AnySchema::Option(OptionSchema(schema)) => ValueSchema::Option(OptionSchema(ValueRequiredSchema::Bool(*schema))),
// 			AnySchema::List(ListSchema(schema, a)) => ValueSchema::List(Box::new(ListSchema(ValueSchema::Bool(*schema), a.clone()))),
// 		})
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		unimplemented!()
// 	}
// }
// impl Deserialize for Option<bool> {
// 	type Schema = OptionSchema<BoolSchema>;
// 	type Reader = OptionReader<BoolReader>;
// 	fn placeholder() -> Self::Schema {
// 		OptionSchema(BoolSchema)
// 	}
// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		Any::<bool>::parse(schema).and_then(|(name,schema)|schema.as_option().ok_or(ParquetError::General(String::from("a"))).map(|schema|(name,schema)))
// 	}
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		Any::<bool>::render(name, &AnySchema::Option(OptionSchema(schema.0)))
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		unimplemented!()
// 	}
// }
// impl Deserialize for bool {
// 	type Schema = BoolSchema;
// 	type Reader = BoolReader;
// 	fn placeholder() -> Self::Schema {
// 		BoolSchema
// 	}
// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 		Any::<bool>::parse(schema).and_then(|(name,schema)|schema.as_one().ok_or(ParquetError::General(String::from("a"))).map(|schema|(name,schema)))
// 	}
// 	fn render(name: &str, schema: &Self::Schema) -> Type {
// 		Any::<bool>::render(name, &AnySchema::One(*schema))
// 	}
// 	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
// 		unimplemented!()
// 	}
// }

impl Deserialize for bool {
	type Schema = BoolSchema;
	type Reader = BoolReader;

	fn placeholder() -> Self::Schema {
		BoolSchema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::BOOLEAN && schema.get_basic_info().logical_type() == LogicalType::NONE {
			return Ok((schema.name().to_owned(), BoolSchema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BOOLEAN)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		BoolReader{column: TypedTripletIter::<BoolType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for Option<bool> {
	type Schema = OptionSchema<BoolSchema>;
	type Reader = OptionReader<BoolReader>;

	fn placeholder() -> Self::Schema {
		OptionSchema(BoolSchema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::BOOLEAN && schema.get_basic_info().logical_type() == LogicalType::NONE {
			return Ok((schema.name().to_owned(), OptionSchema(BoolSchema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BOOLEAN)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: BoolReader{column: TypedTripletIter::<BoolType>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}}
	}
}
impl Deserialize for f32 {
	type Schema = F32Schema;
	type Reader = F32Reader;

	fn placeholder() -> Self::Schema {
		F32Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::FLOAT && schema.get_basic_info().logical_type() == LogicalType::NONE {
			return Ok((schema.name().to_owned(), F32Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FLOAT)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		F32Reader{column: TypedTripletIter::<FloatType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for Option<f32> {
	type Schema = OptionSchema<F32Schema>;
	type Reader = OptionReader<F32Reader>;

	fn placeholder() -> Self::Schema {
		OptionSchema(F32Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::FLOAT && schema.get_basic_info().logical_type() == LogicalType::NONE {
			return Ok((schema.name().to_owned(), OptionSchema(F32Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FLOAT)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: F32Reader{column: TypedTripletIter::<FloatType>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}}
	}
}
impl Deserialize for f64 {
	type Schema = F64Schema;
	type Reader = F64Reader;

	fn placeholder() -> Self::Schema {
		F64Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::DOUBLE && schema.get_basic_info().logical_type() == LogicalType::NONE {
			return Ok((schema.name().to_owned(), F64Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::DOUBLE)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		F64Reader{column: TypedTripletIter::<DoubleType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for Option<f64> {
	type Schema = OptionSchema<F64Schema>;
	type Reader = OptionReader<F64Reader>;

	fn placeholder() -> Self::Schema {
		OptionSchema(F64Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::DOUBLE && schema.get_basic_info().logical_type() == LogicalType::NONE {
			return Ok((schema.name().to_owned(), OptionSchema(F64Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::DOUBLE)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: F64Reader{column: TypedTripletIter::<DoubleType>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}}
	}
}
impl Deserialize for i8 {
	type Schema = I8Schema;
	type Reader = TryIntoReader<I32Reader, i8>;

	fn placeholder() -> Self::Schema {
		I8Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 && schema.get_basic_info().logical_type() == LogicalType::INT_8 {
			return Ok((schema.name().to_owned(), I8Schema));
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::INT_8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)
	}
}
impl Deserialize for Option<i8> {
	type Schema = OptionSchema<I8Schema>;
	type Reader = OptionReader<TryIntoReader<I32Reader, i8>>;

	fn placeholder() -> Self::Schema {
		OptionSchema(I8Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT32 && schema.get_basic_info().logical_type() == LogicalType::INT_8 {
			return Ok((schema.name().to_owned(), OptionSchema(I8Schema)));
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::INT_8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)}
	}
}
impl Deserialize for u8 {
	type Schema = U8Schema;
	type Reader = TryIntoReader<I32Reader, u8>;

	fn placeholder() -> Self::Schema {
		U8Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 && schema.get_basic_info().logical_type() == LogicalType::UINT_8 {
			return Ok((schema.name().to_owned(), U8Schema));
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::UINT_8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)
	}
}

impl Deserialize for i16 {
	type Schema = I16Schema;
	type Reader = TryIntoReader<I32Reader, i16>;

	fn placeholder() -> Self::Schema {
		I16Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 && schema.get_basic_info().logical_type() == LogicalType::INT_16 {
			return Ok((schema.name().to_owned(), I16Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::INT_16)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)
	}
}
impl Deserialize for Option<i16> {
	type Schema = OptionSchema<I16Schema>;
	type Reader = OptionReader<TryIntoReader<I32Reader, i16>>;

	fn placeholder() -> Self::Schema {
		OptionSchema(I16Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT32 && schema.get_basic_info().logical_type() == LogicalType::INT_16 {
			return Ok((schema.name().to_owned(), OptionSchema(I16Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::INT_16)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)}
	}
}
impl Deserialize for u16 {
	type Schema = U16Schema;
	type Reader = TryIntoReader<I32Reader, u16>;

	fn placeholder() -> Self::Schema {
		U16Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 && schema.get_basic_info().logical_type() == LogicalType::UINT_16 {
			return Ok((schema.name().to_owned(), U16Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::UINT_16)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)
	}
}
impl Deserialize for Option<u16> {
	type Schema = OptionSchema<U16Schema>;
	type Reader = OptionReader<TryIntoReader<I32Reader, u16>>;

	fn placeholder() -> Self::Schema {
		OptionSchema(U16Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT32 && schema.get_basic_info().logical_type() == LogicalType::UINT_16 {
			return Ok((schema.name().to_owned(), OptionSchema(U16Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::UINT_16)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: TryIntoReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}, PhantomData)}
	}
}
impl Deserialize for i32 {
	type Schema = I32Schema;
	type Reader = I32Reader;

	fn placeholder() -> Self::Schema {
		I32Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 && (schema.get_basic_info().logical_type() == LogicalType::NONE || schema.get_basic_info().logical_type() == LogicalType::INT_32) {
			return Ok((schema.name().to_owned(), I32Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::INT_32)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for Option<i32> {
	type Schema = OptionSchema<I32Schema>;
	type Reader = OptionReader<I32Reader>;

	fn placeholder() -> Self::Schema {
		OptionSchema(I32Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT32 && (schema.get_basic_info().logical_type() == LogicalType::NONE || schema.get_basic_info().logical_type() == LogicalType::INT_32) {
			return Ok((schema.name().to_owned(), OptionSchema(I32Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::INT_32)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}}
	}
}
impl Deserialize for u32 {
	type Schema = U32Schema;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		U32Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 && schema.get_basic_info().logical_type() == LogicalType::UINT_32 {
			return Ok((schema.name().to_owned(), U32Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::UINT_32)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		MapReader(I32Reader{column: TypedTripletIter::<Int32Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |x|Ok(x as u32))
	}
}
impl Deserialize for i64 {
	type Schema = I64Schema;
	type Reader = I64Reader;

	fn placeholder() -> Self::Schema {
		I64Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT64 && (schema.get_basic_info().logical_type() == LogicalType::NONE || schema.get_basic_info().logical_type() == LogicalType::INT_64) {
			return Ok((schema.name().to_owned(), I64Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT64)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::INT_64)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for Option<i64> {
	type Schema = OptionSchema<I64Schema>;
	type Reader = OptionReader<I64Reader>;

	fn placeholder() -> Self::Schema {
		OptionSchema(I64Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT64 && (schema.get_basic_info().logical_type() == LogicalType::NONE || schema.get_basic_info().logical_type() == LogicalType::INT_64) {
			return Ok((schema.name().to_owned(), OptionSchema(I64Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT64)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::INT_64)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}}
	}
}
impl Deserialize for u64 {
	type Schema = U64Schema;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		U64Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT64 && schema.get_basic_info().logical_type() == LogicalType::UINT_64 {
			return Ok((schema.name().to_owned(), U64Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT64)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::UINT_64)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		MapReader(I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |x|Ok(x as u64))
	}
}
impl Deserialize for Option<u64> {
	type Schema = OptionSchema<U64Schema>;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		OptionSchema(U64Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT64 && schema.get_basic_info().logical_type() == LogicalType::UINT_64 {
			return Ok((schema.name().to_owned(), OptionSchema(U64Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT64)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::UINT_64)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: MapReader(I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}, |x|Ok(x as u64))}
	}
}
impl Deserialize for Timestamp {
	type Schema = TimestampSchema;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		TimestampSchema::Millis
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		<Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
			match schema {
				ValueSchema::Timestamp(schema) => Ok((name, schema)),
				_ => Err(ParquetError::General(String::from(""))),
			}
		})
		// if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED &&
		// (schema.get_physical_type() == PhysicalType::INT96 && schema.get_basic_info().logical_type() == LogicalType::NONE) || 
		// (schema.get_physical_type() == PhysicalType::INT64 && schema.get_basic_info().logical_type() == LogicalType::TIMESTAMP_MILLIS) || 
		// (schema.get_physical_type() == PhysicalType::INT64 && schema.get_basic_info().logical_type() == LogicalType::TIMESTAMP_MICROS)
		// {
		// 	return Ok((schema.name().to_owned(), match schema.get_basic_info().logical_type() { LogicalType::NONE => ))
		// }
		// Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		unimplemented!()
		// Value::render(name, &ValueSchema::Timestamp(schema))
		// Type::primitive_type_builder(name, PhysicalType::INT96)
		// 	.with_repetition(Repetition::REQUIRED)
		// 	.with_logical_type(LogicalType::NONE)
		// 	.with_length(-1)
		// 	.with_precision(-1)
		// 	.with_scale(-1)
		// 	.build()
		// 	.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
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
impl Deserialize for Option<Timestamp> {
	type Schema = OptionSchema<TimestampSchema>;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		OptionSchema(TimestampSchema::Millis)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		<Value as Deserialize>::parse(schema).and_then(|(name, schema)| {
			match schema {
				ValueSchema::Option(box OptionSchema(ValueSchema::Timestamp(schema))) => Ok((name, OptionSchema(schema))),
				_ => Err(ParquetError::General(String::from(""))),
			}
		})
		// if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT96 && schema.get_basic_info().logical_type() == LogicalType::NONE {
		// 	return Ok((schema.name().to_owned(), OptionSchema(TimestampSchema)))
		// }
		// Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT96)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		let reader = match schema.0 {
			TimestampSchema::Int96 => {
				sum::Sum3::A(MapReader(I96Reader{column: TypedTripletIter::<Int96Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}, |x|Ok(Timestamp(x))))
			}
			TimestampSchema::Millis => {
				sum::Sum3::B(MapReader(I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}, |millis| {
					let day: i64 = ((JULIAN_DAY_OF_EPOCH * SECONDS_PER_DAY * MILLIS_PER_SECOND) + millis) / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
					let nanoseconds: i64 = (millis - ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND)) * 1_000_000;

					Ok(Timestamp(Int96::new((nanoseconds & 0xffff).try_into().unwrap(),((nanoseconds as u64) >> 32).try_into().unwrap(),day.try_into().map_err(|err:TryFromIntError|ParquetError::General(err.description().to_owned()))?)))
				}))
			}
			TimestampSchema::Micros => {
				sum::Sum3::C(MapReader(I64Reader{column: TypedTripletIter::<Int64Type>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}, |micros| {
					let day: i64 = ((JULIAN_DAY_OF_EPOCH * SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI) + micros) / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
					let nanoseconds: i64 = (micros - ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI)) * 1_000;

					Ok(Timestamp(Int96::new((nanoseconds & 0xffff).try_into().unwrap(),((nanoseconds as u64) >> 32).try_into().unwrap(),day.try_into().map_err(|err:TryFromIntError|ParquetError::General(err.description().to_owned()))?)))
				}))
			}
		};
		OptionReader{def_level: curr_def_level, reader} 
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

	fn placeholder() -> Self::Schema {
		VecSchema(None)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::BYTE_ARRAY && schema.get_basic_info().logical_type() == LogicalType::NONE {
			return Ok((schema.name().to_owned(), VecSchema(None)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(schema.0.map(|x| x as i32).unwrap_or(-1))
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		ByteArrayReader{column: TypedTripletIter::<ByteArrayType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}
	}
}
impl Deserialize for Option<Vec<u8>> {
	type Schema = OptionSchema<VecSchema>;
	type Reader = OptionReader<ByteArrayReader>;

	fn placeholder() -> Self::Schema {
		OptionSchema(VecSchema(None))
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::BYTE_ARRAY && schema.get_basic_info().logical_type() == LogicalType::NONE {
			return Ok((schema.name().to_owned(), OptionSchema(VecSchema(None))))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: ByteArrayReader{column: TypedTripletIter::<ByteArrayType>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}}
	}
}
impl Deserialize for String {
	type Schema = StringSchema;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		StringSchema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::BYTE_ARRAY && schema.get_basic_info().logical_type() == LogicalType::UTF8 {
			return Ok((schema.name().to_owned(), StringSchema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::UTF8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		MapReader(ByteArrayReader{column: TypedTripletIter::<ByteArrayType>::new(curr_def_level, curr_rep_level, BATCH_SIZE, col_reader)}, |x|String::from_utf8(x).map_err(|err:FromUtf8Error|ParquetError::General(err.to_string())))
	}
}
impl Deserialize for Option<String> {
	type Schema = OptionSchema<StringSchema>;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		OptionSchema(StringSchema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::BYTE_ARRAY && schema.get_basic_info().logical_type() == LogicalType::UTF8 {
			return Ok((schema.name().to_owned(), OptionSchema(StringSchema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::UTF8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
		assert_eq!((curr_def_level+1, curr_rep_level), (col_descr.max_def_level(), col_descr.max_rep_level()));
		OptionReader{def_level: curr_def_level, reader: MapReader(ByteArrayReader{column: TypedTripletIter::<ByteArrayType>::new(curr_def_level+1, curr_rep_level, BATCH_SIZE, col_reader)}, |x|String::from_utf8(x).map_err(|err:FromUtf8Error|ParquetError::General(err.to_string())))}
	}
}
impl Deserialize for [u8; 1024] {
	type Schema = ArraySchema<Self>;
	existential type Reader: RRReader<Item = Self>;

	fn placeholder() -> Self::Schema {
		ArraySchema(PhantomData)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY && schema.get_basic_info().logical_type() == LogicalType::NONE && schema.get_type_length() == 1024 {
			return Ok((schema.name().to_owned(), ArraySchema(PhantomData)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn render(name: &str, schema: &Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(1024)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
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
					let x: Type = <Root<($($t,)*)> as Deserialize>::render("", &<Root<($($t,)*)> as Deserialize>::placeholder());
					let mut a = Vec::new();
					print_schema(&mut a, &x);
					ParquetError::General(format!(
						"Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
						s,
						String::from_utf8(a).unwrap(),
						err
					))
				})).map(|x|x.1)
			}
		}
		impl<$($t,)*> Deserialize for Root<($($t,)*)> where $($t: Deserialize,)* {
			type Schema = RootSchema<($($t,)*),TupleSchema<($((String,$t::Schema,),)*)>>;
			type Reader = RootReader<TupleReader<($($t::Reader,)*)>>;

			fn placeholder() -> Self::Schema {
				RootSchema(String::from("<name>"), TupleSchema(($((String::from("<name>"),$t::placeholder()),)*)), PhantomData)
			}
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
			fn render(name: &str, schema: &Self::Schema) -> Type {
				assert_eq!(name, "");
				Type::group_type_builder(&schema.0).with_fields(&mut vec![$(Rc::new($t::render(&((schema.1).0).$i.0, &((schema.1).0).$i.1)),)*]).build().unwrap()
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
				RootReader(<($($t,)*) as Deserialize>::reader(&schema.1, path, curr_def_level, curr_rep_level, paths))
			}
		}
		impl<$($t,)*> Deserialize for ($($t,)*) where $($t: Deserialize,)* {
			type Schema = TupleSchema<($((String,$t::Schema,),)*)>;
			type Reader = TupleReader<($($t::Reader,)*)>;

			fn placeholder() -> Self::Schema {
				TupleSchema(($((String::from("<name>"),$t::placeholder()),)*))
			}
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
			fn render(name: &str, schema: &Self::Schema) -> Type {
				Type::group_type_builder(name).with_repetition(Repetition::REQUIRED).with_fields(&mut vec![$(Rc::new($t::render(&(schema.0).$i.0, &(schema.0).$i.1)),)*]).build().unwrap()
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
				$(
					path.push((schema.0).$i.0.to_owned());
					let $t = <$t as Deserialize>::reader(&(schema.0).$i.1, path, curr_def_level, curr_rep_level, paths);
					path.pop().unwrap();
				)*;
				TupleReader(($($t,)*))
			}
		}
		impl<$($t,)*> Deserialize for Option<($($t,)*)> where $($t: Deserialize,)* {
			type Schema = OptionSchema<TupleSchema<($((String,$t::Schema,),)*)>>;
			type Reader = OptionReader<TupleReader<($($t::Reader,)*)>>;

			fn placeholder() -> Self::Schema {
				OptionSchema(TupleSchema(($((String::from("<name>"),$t::placeholder()),)*)))
			}
			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::OPTIONAL {
					let mut fields = schema.get_fields().iter();
					let schema_ = OptionSchema(TupleSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*)));
					if fields.next().is_none() {
						return Ok((schema.name().to_owned(), schema_))
					}
				}
				Err(ParquetError::General(String::from("")))
			}
			fn render(name: &str, schema: &Self::Schema) -> Type {
				Type::group_type_builder(name).with_repetition(Repetition::OPTIONAL).with_fields(&mut vec![$(Rc::new($t::render(&((schema.0).0).$i.0, &((schema.0).0).$i.1)),)*]).build().unwrap()
			}
			fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, mut curr_def_level: i16, mut curr_rep_level: i16, paths: &mut HashMap<ColumnPath, (ColumnDescPtr,ColumnReader)>) -> Self::Reader {
				$(
					path.push(((schema.0).0).$i.0.to_owned());
					let $t = <$t as Deserialize>::reader(&((schema.0).0).$i.1, path, curr_def_level+1, curr_rep_level, paths);
					path.pop().unwrap();
				)*;
				OptionReader{def_level: curr_def_level, reader: TupleReader(($($t,)*))}
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
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
