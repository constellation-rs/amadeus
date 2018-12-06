#![feature(maybe_uninit)]

use parquet::{
	basic::{LogicalType, Repetition, Type as PhysicalType}, errors::ParquetError, file::reader::{FileReader, ParquetReader, SerializedFileReader}, record::{reader::Reader, Field}, schema::{
		printer::{print_file_metadata, print_parquet_metadata, print_schema}, types::{BasicTypeInfo, SchemaDescPtr, SchemaDescriptor, Type}
	}
};
use std::{fs::File, path::Path, rc::Rc, str};
use std::collections::HashMap;
use std::hash::Hash;

#[rustfmt::skip]
fn main() {
	let file = File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap();
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
	>(file,
		("schema", (
			("bp1", ()),
			("bp2", ()),
			("bp3", ()),
			("bp4", ()),
			("bp5", ()),
			("bs1", ()),
			("bs2", ()),
			("bs3", ()),
			("bs4", ()),
			("bs5", ()),
			("ap1", ()),
			("ap2", ()),
			("ap3", ()),
			("ap4", ()),
			("ap5", ()),
			("as1", ()),
			("as2", ()),
			("as3", ()),
			("as4", ()),
			("as5", ()),
			("valid", ()),
			("__index_level_0__", ()),
		))
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<f64>,
			Option<i64>,
		)
	>(file,
		("schema", (
			("bs5", ()),
			("__index_level_0__", ()),
		))
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap();
	let rows = read::<_,
		(
		)
	>(file,
		("schema", (
		))
	);
	println!("{}", rows.count());


	let file = File::open(&Path::new("./parquet-rs/data/10k-v2.parquet")).unwrap();
	let rows = read::<_,
		(
			Vec<u8>,
			i32,
			i64,
			bool,
			f32,
			f64,
			[u8;1024],
			Timestamp<i96>,
		)
	>(file,
		("test", (
			("binary_field", ()),
			("int32_field", ()),
			("int64_field", ()),
			("boolean_field", ()),
			("float_field", ()),
			("double_field", ()),
			("flba_field", ()),
			("int96_field", ()),
		))
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/alltypes_dictionary.parquet")).unwrap();
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
			Option<Timestamp<i96>>,
		)
	>(file,
		("schema", (
			("id", ()),
			("bool_col", ()),
			("tinyint_col", ()),
			("smallint_col", ()),
			("int_col", ()),
			("bigint_col", ()),
			("float_col", ()),
			("double_col", ()),
			("date_string_col", ()),
			("string_col", ()),
			("timestamp_col", ()),
		))
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/alltypes_plain.parquet")).unwrap();
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
			Option<Timestamp<i96>>,
		)
	>(file,
		("schema", (
			("id", ()),
			("bool_col", ()),
			("tinyint_col", ()),
			("smallint_col", ()),
			("int_col", ()),
			("bigint_col", ()),
			("float_col", ()),
			("double_col", ()),
			("date_string_col", ()),
			("string_col", ()),
			("timestamp_col", ()),
		))
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/alltypes_plain.snappy.parquet")).unwrap();
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
			Option<Timestamp<i96>>,
		)
	>(file,
		("schema", (
			("id", ()),
			("bool_col", ()),
			("tinyint_col", ()),
			("smallint_col", ()),
			("int_col", ()),
			("bigint_col", ()),
			("float_col", ()),
			("double_col", ()),
			("date_string_col", ()),
			("string_col", ()),
			("timestamp_col", ()),
		))
	);
	println!("{}", rows.count());

	// let file = File::open(&Path::new("./parquet-rs/data/nation.dict-malformed.parquet")).unwrap();
	// let rows = read::<_,
	// 	(
	// 		Option<i32>,
	// 		Option<Vec<u8>>,
	// 		Option<i32>,
	// 		Option<Vec<u8>>,
	// 	)
	// >(file,
	// 	("m", (
	// 		("nation_key", ()),
	// 		("name", ()),
	// 		("region_key", ()),
	// 		("comment_col", ()),
	// 	))
	// );
	// println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/nested_lists.snappy.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<List<Option<List<Option<List<Option<String>>>>>>>,
			i32,
		)
	>(file,
		("spark_schema", (
			("a", ()),
			("b", ()),
		))
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/nested_maps.snappy.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<Map<String,Option<Map<i32,bool>>>>,
			i32,
			f64,
		)
	>(file,
		("spark_schema", (
			("a", ((), ((), ()))),
			("b", ()),
			("c", ()),
		))
	);
	println!("{}", rows.count());

	// let file = File::open(&Path::new("./parquet-rs/data/nonnullable.impala.parquet")).unwrap();
	// let rows = read::<_,
	// 	(
	// 		i64,
	// 		List<i32>,
	// 		List<List<i32>>,
	// 		Map<String,i32>,
	// 		List<Map<String,i32>>,
	// 		(
	// 			i32,
	// 			List<i32>,
	// 			(List<List<(i32,String)>>,),
	// 			Map<String,((List<f64>,),)>,
	// 		)
	// 	)
	// 	>(file,
	// 	("org.apache.impala.ComplexTypesTbl", (
	// 		("ID", ()),
	// 		("Int_Array", ()),
	// 		("int_array_array", ()),
	// 		("Int_Map", ((), ())),
	// 		("int_map_array", ((), ())),
	// 		("nested_Struct", (
	// 			("a", ()),
	// 			("B", ()),
	// 			("c", (("D", (("e", ()), ("f", ()))),)),
	// 			("G", ((), (("h", (("i", ()),)),))),
	// 		))
	// 	))
	// );
	// println!("{}", rows.count());

	// let file = File::open(&Path::new("./parquet-rs/data/nullable.impala.parquet")).unwrap();
	// let rows = read::<_,
	// 	(
	// 		Option<i64>,
	// 		Option<List<Option<i32>>>,
	// 		Option<List<Option<List<Option<i32>>>>>,
	// 		Option<Map<String,Option<i32>>>,
	// 		Option<List<Option<Map<String,Option<i32>>>>>,
	// 		Option<(
	// 			Option<i32>,
	// 			Option<List<Option<i32>>>,
	// 			Option<(Option<List<Option<List<Option<(Option<i32>,Option<String>)>>>>>,)>,
	// 			Option<Map<String,Option<(Option<(Option<List<Option<f64>>>,)>,)>>>,
	// 		)>
	// 	)
	// 	>(file,
	// 	("org.apache.impala.ComplexTypesTbl", (
	// 		("id", ()),
	// 		("int_array", ()),
	// 		("int_array_Array", ()),
	// 		("int_map", ((), ())),
	// 		("int_Map_Array", ((), ())),
	// 		("nested_struct", (
	// 			("A", ()),
	// 			("b", ()),
	// 			("C", (("d", (("E", ()), ("F", ()))),)),
	// 			("g", ((), (("H", (("i", ()),)),))),
	// 		))
	// 	))
	// );
	// println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/nulls.snappy.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<(Option<i32>,)>,
		)
		>(file,
		("spark_schema", (
			("b_struct", (("b_c_int", ()),)),
		))
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/repeated_no_annotation.parquet")).unwrap();
	let rows = read::<_,
		(
			i32,
			Option<(Repeat<(i64,Option<String>)>,)>,
		)
		>(file,
		("user", (
			("id", ()),
			("phoneNumbers", (("phone", (("number", ()), ("kind", ()))),)),
		))
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/test_datapage_v2.snappy.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<String>,
			i32,
			f64,
			bool,
			Option<List<i32>>,
		)
		>(file,
		("spark_schema", (
			("a", ()),
			("b", ()),
			("c", ()),
			("d", ()),
			("e", ())
		))
	);
	println!("{}", rows.count());
}


struct Timestamp<T>(T);
struct u96(u128);
struct i96(i128);
struct Repeat<T>(Vec<T>);
struct List<T>(Vec<T>);
struct Map<K,V>(HashMap<K,V>);

impl<K,V> ParquetDeserialize for Map<K,V>
where
	K: ParquetDeserialize + Hash + Eq,
	V: ParquetDeserialize,
{
	type Schema = (K::Schema, V::Schema);
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::group_type_builder(name)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::MAP)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder("key_value")
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(K::schema("key", schema.0)),Rc::new(V::schema("value", schema.1))])
					.build()
					.unwrap(),
			)])
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		let mut list = HashMap::new();
		reader.read_key_value(|keys_reader, values_reader| {
			list.insert(K::read(keys_reader), V::read(values_reader));
		});
		Map(list)
	}
}
impl<K,V> ParquetDeserialize for Option<Map<K,V>>
where
	K: ParquetDeserialize + Hash + Eq,
	V: ParquetDeserialize,
{
	type Schema = (K::Schema, V::Schema);
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::group_type_builder(name)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::MAP)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder("key_value")
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(K::schema("key", schema.0)),Rc::new(V::schema("value", schema.1))])
					.build()
					.unwrap(),
			)])
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		reader.read_option().map(|reader| {
			let mut list = HashMap::new();
			reader.read_key_value(|keys_reader, values_reader| {
				list.insert(K::read(keys_reader), V::read(values_reader));
			});
			Map(list)
		})
	}
}


impl<T> ParquetDeserialize for List<T>
where
	T: ParquetDeserialize,
{
	type Schema = T::Schema;
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::group_type_builder(name)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::LIST)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder("list")
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(T::schema("element", schema))])
					.build()
					.unwrap(),
			)])
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		let mut list = Vec::new();
		reader.read_repeated(|reader| {
			list.push(T::read(reader))
		});
		List(list)
	}
}
impl<T> ParquetDeserialize for Option<List<T>>
where
	T: ParquetDeserialize,
{
	type Schema = T::Schema;
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::group_type_builder(name)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::LIST)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder("list")
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(T::schema("element", schema))])
					.build()
					.unwrap(),
			)])
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		reader.read_option().map(|reader| {
			// if let Reader::GroupReader(_, _, readers) = reader {
			// 	assert_eq!(readers.len(), 1);
			// 	if let Reader::GroupReader(_, _, readers) = readers.into_iter().next().unwrap() {
			// 		assert_eq!(readers.len(), 1);
			// 		let reader = readers.into_iter().next().unwrap();
					let mut list = Vec::new();
					reader.read_repeated(|reader| {
						list.push(T::read(reader))
					});
					List(list)
			// 	} else {
			// 		unreachable!("{}", reader)
			// 	}
			// } else {
			// 	unreachable!("{}", reader)
			// }
		})
	}
}



fn read<R: ParquetReader + 'static, T>(
	reader: R, schema: <Root<T> as ParquetDeserialize>::Schema,
) -> impl Iterator<Item = T> where Root<T>: ParquetDeserialize {
	let schema = <Root<T>>::schema("", schema);
	print_schema(&mut std::io::stdout(), &schema);
	// println!("{:#?}", schema);
	let reader = SerializedFileReader::new(reader).unwrap();
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
	let projection = Some(schema);
	let descr = get_proj_descr(
		projection,
		reader.metadata().file_metadata().schema_descr_ptr(),
	)
	.unwrap();
	let tree_builder = parquet::record::reader::TreeBuilder::new();
	(0..reader.num_row_groups()).flat_map(move |i| {
		let row_group = reader.get_row_group(i).unwrap();
		let mut reader = tree_builder.build(descr.clone(), &*row_group);
		reader.advance_columns();
		// for row in tree_builder.as_iter(descr.clone(), &*row_group) {
		// 	println!("{:?}", row);
		// }
		// std::iter::empty()
		// println!("{:?}", reader.read());
		(0..row_group.metadata().num_rows()).map(move |_| {
			// println!("row");
			<Root<T>>::read(&mut reader).0
		})
	})
}

trait ParquetDeserialize {
	type Schema;//: str::FromStr;
	fn schema(name: &str, schema: Self::Schema) -> Type;
	fn read(reader: &mut Reader) -> Self;
}
impl ParquetDeserialize for bool {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BOOLEAN)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Bool(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for Option<bool> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BOOLEAN)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		match reader.read_field() {
			Field::Bool(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		}
	}
}
impl ParquetDeserialize for f64 {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::DOUBLE)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Double(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for Option<f32> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FLOAT)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		match reader.read_field() {
			Field::Float(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		}
	}
}
impl ParquetDeserialize for f32 {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FLOAT)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Float(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for Option<f64> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::DOUBLE)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		match reader.read_field() {
			Field::Double(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		}
	}
}
impl ParquetDeserialize for i32 {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::INT_32)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Int(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for Option<i32> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::INT_32)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		match reader.read_field() {
			Field::Int(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		}
	}
}
impl ParquetDeserialize for i64 {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT64)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::INT_64)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Long(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for u64 {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT64)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::UINT_64)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::ULong(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for Option<i64> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT64)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::INT_64)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		match reader.read_field() {
			Field::Long(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		}
	}
}
impl ParquetDeserialize for Option<u64> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT64)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::UINT_64)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		match reader.read_field() {
			Field::ULong(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		}
	}
}
impl ParquetDeserialize for Timestamp<i96> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT96)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Timestamp(field) = reader.read_field() {
			Timestamp(i96(field.into()))
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for Option<Timestamp<i96>> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT96)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		match reader.read_field() {
			Field::Timestamp(field) => Some(Timestamp(i96(field.into()))),
			Field::Null => None,
			_ => unreachable!(),
		}
	}
}
// impl ParquetDeserialize for u96 {
// 	type Schema = ();
// 	fn schema(name: &str, schema: Self::Schema) -> Type {
// 		Type::primitive_type_builder(name, PhysicalType::INT96)
// 			.with_repetition(Repetition::REQUIRED)
// 			.with_logical_type(LogicalType::NONE)
// 			.with_length(-1)
// 			.with_precision(-1)
// 			.with_scale(-1)
// 			.build().unwrap()
// 	}
// 	fn read(reader: &mut Reader) -> Self {
// 		if let Reader::PrimitiveReader(_,_) = reader { } else  { unreachable!() }
// 		panic!("x96??! {:?}", reader.read_field());
// 		// if let Field::ULong(field) = reader.read_field() { field } else { unreachable!() }
// 	}
// }

// impl ParquetDeserialize for parquet::data_type::Decimal {
// 	type Schema = DecimalSchema;
// 	fn schema(name: &str, schema: Self::Schema) -> Type {
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
impl ParquetDeserialize for Vec<u8> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Bytes(bytes) = reader.read_field() {
			bytes.data().to_owned()
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for Option<Vec<u8>> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::NONE)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!("{}", reader)
		}
		match reader.read_field() {
			Field::Bytes(bytes) => Some(bytes.data().to_owned()),
			Field::Null => None,
			_ => unreachable!(),
		}
	}
}
impl ParquetDeserialize for String {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::UTF8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Str(bytes) = reader.read_field() {
			bytes//String::from_utf8(bytes.data().to_owned()).unwrap()
		} else {
			unreachable!()
		}
	}
}
impl ParquetDeserialize for Option<String> {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::UTF8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!("{}", reader)
		}
		match reader.read_field() {
			Field::Str(bytes) => Some(bytes),//String::from_utf8(bytes.data().to_owned()).unwrap()),
			Field::Null => None,
			e => unreachable!("{:?}", e),
		}
	}
}
impl ParquetDeserialize for [u8; 0] {
	// is this valid?
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(0)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		unimplemented!()
	}
}
impl ParquetDeserialize for [u8; 1] {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		unimplemented!()
	}
}
impl ParquetDeserialize for [u8; 2] {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(2)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		unimplemented!()
	}
}
impl ParquetDeserialize for [u8; 1024] {
	type Schema = ();
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::NONE)
			.with_length(1024)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Self {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		if let Field::Bytes(bytes) = reader.read_field() {
			let mut ret = std::mem::MaybeUninit::<Self>::uninitialized();
			assert_eq!(bytes.len(), unsafe { ret.get_ref().len() });
			unsafe {
				std::ptr::copy_nonoverlapping(
					bytes.data().as_ptr(),
					ret.get_mut().as_mut_ptr(),
					bytes.len(),
				)
			};
			unsafe { ret.into_inner() }
		} else {
			unreachable!()
		}
	}
}

struct Root<T>(T);

macro_rules! impl_parquet_deserialize_tuple {
	($($t:ident $i:tt)*) => (
		// impl<$($t,)*> str::FromStr for ($($t,)*) where $($t: ParquetDeserialize,)* {
		// 	type Err = ();
		// 	fn from_str(s: &str) -> Result<Self, Self::Err> {
		// 		unimplemented!()
		// 	}
		// }
		impl<$($t,)*> ParquetDeserialize for Root<($($t,)*)> where $($t: ParquetDeserialize,)* {
			type Schema = (&'static str,($((&'static str,$t::Schema,),)*));
			fn schema(name: &str, schema: Self::Schema) -> Type {
				assert_eq!(name, "");
				Type::group_type_builder(schema.0).with_fields(&mut vec![$(Rc::new($t::schema((schema.1).$i.0, (schema.1).$i.1)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Self {
				Root(<($($t,)*) as ParquetDeserialize>::read(reader))
			}
		}
		impl<$($t,)*> ParquetDeserialize for ($($t,)*) where $($t: ParquetDeserialize,)* {
			type Schema = ($((&'static str,$t::Schema,),)*);
			fn schema(name: &str, schema: Self::Schema) -> Type {
				Type::group_type_builder(name).with_repetition(Repetition::REQUIRED).with_fields(&mut vec![$(Rc::new($t::schema(schema.$i.0, schema.$i.1)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Self {
				let mut readers = if let Reader::GroupReader(_,_,ref mut readers) = reader { readers } else  { unreachable!() }.into_iter();
				let ret = ($($t::read(readers.next().unwrap()),)*);
				assert!(readers.next().is_none());
				ret
			}
		}
		impl<$($t,)*> ParquetDeserialize for Option<($($t,)*)> where $($t: ParquetDeserialize,)* {
			type Schema = ($((&'static str,$t::Schema,),)*);
			fn schema(name: &str, schema: Self::Schema) -> Type {
				Type::group_type_builder(name).with_repetition(Repetition::OPTIONAL).with_fields(&mut vec![$(Rc::new($t::schema(schema.$i.0, schema.$i.1)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Self {
				reader.read_option().map(|reader| {
					let mut readers = if let Reader::GroupReader(_,_,ref mut readers) = reader { readers } else  { unreachable!() }.into_iter();
					let ret = ($($t::read(readers.next().unwrap()),)*);
					assert!(readers.next().is_none());
					ret
				})
			}
		}
		impl<$($t,)*> ParquetDeserialize for Repeat<($($t,)*)> where $($t: ParquetDeserialize,)* {
			type Schema = ($((&'static str,$t::Schema,),)*);
			fn schema(name: &str, schema: Self::Schema) -> Type {
				Type::group_type_builder(name).with_repetition(Repetition::REPEATED).with_fields(&mut vec![$(Rc::new($t::schema(schema.$i.0, schema.$i.1)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Self {
				let mut list = Vec::new();
				reader.read_repeated(|reader| {
					let mut readers = if let Reader::GroupReader(_,_,ref mut readers) = reader { readers } else  { unreachable!() }.into_iter();
					let ret = ($($t::read(readers.next().unwrap()),)*);
					assert!(readers.next().is_none());
					list.push(ret)
				});
				Repeat(list)
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

fn get_proj_descr(
	proj: Option<Type>, root_descr: SchemaDescPtr,
) -> Result<SchemaDescPtr, ParquetError> {
	match proj {
		Some(projection) => {
			// check if projection is part of file schema
			let root_schema = root_descr.root_schema();
			if !root_schema.check_contains(&projection) {
				let mut a = Vec::new();
				print_schema(&mut a, &root_schema);
				let mut b = Vec::new();
				print_schema(&mut b, &projection);
				return Err(ParquetError::General(format!(
					"Root schema does not contain projection:\n{}\n{}",
					String::from_utf8(a).unwrap(),
					String::from_utf8(b).unwrap()
				)));
			}
			Ok(Rc::new(SchemaDescriptor::new(Rc::new(projection))))
		}
		None => Ok(root_descr),
	}
}
