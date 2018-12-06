#![feature(maybe_uninit)]

use parquet::{
	basic::{LogicalType, Repetition, Type as PhysicalType}, errors::ParquetError, file::reader::{FileReader, ParquetReader, SerializedFileReader}, record::{reader::Reader, Field}, schema::{
		printer::{print_file_metadata, print_parquet_metadata, print_schema}, types::{BasicTypeInfo, SchemaDescPtr, SchemaDescriptor, Type}
	}
};
use std::{fs::File, path::Path, rc::Rc};
use std::collections::HashMap;
use std::hash::Hash;

#[rustfmt::skip]
fn main() {
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
	>(File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap(),
		("schema",
			"bp1", "bp2", "bp3", "bp4", "bp5", "bs1", "bs2", "bs3", "bs4", "bs5", "ap1", "ap2", "ap3", "ap4", "ap5", "as1", "as2", "as3", "as4", "as5", "valid", "__index_level_0__",
		)
	);
	println!("{}", rows.count());
	let rows = read::<_,
		(
			Option<f64>,
			Option<i64>,
		)
	>(File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap(),
		("schema",
			"bs5",
			"__index_level_0__",
		)
	);
	println!("{}", rows.count());
	let rows = read::<_,
		(
		)
	>(File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap(),
		("schema",
		)
	);
	println!("{}", rows.count());
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
	>(File::open(&Path::new("./parquet-rs/data/10k-v2.parquet")).unwrap(),
		("test",
			"binary_field",
			"int32_field",
			"int64_field",
			"boolean_field",
			"float_field",
			"double_field",
			"flba_field",
			"int96_field",
		)
	);
	println!("{}", rows.count());
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
	>(File::open(&Path::new("./parquet-rs/data/alltypes_dictionary.parquet")).unwrap(),
		("schema",
			"id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col", "date_string_col", "string_col", "timestamp_col",
		)
	);
	println!("{}", rows.count());
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
	>(File::open(&Path::new("./parquet-rs/data/alltypes_plain.parquet")).unwrap(),
		("schema",
			"id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col", "date_string_col", "string_col", "timestamp_col",
		)
	);
	println!("{}", rows.count());
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
	>(File::open(&Path::new("./parquet-rs/data/alltypes_plain.snappy.parquet")).unwrap(),
		("schema",
			"id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col", "date_string_col", "string_col", "timestamp_col",
		)
	);
	println!("{}", rows.count());
	// let rows = read::<_,
	// 	(
	// 		Option<i32>,
	// 		Option<Vec<u8>>,
	// 		Option<i32>,
	// 		Option<Vec<u8>>,
	// 	)
	// >(File::open(&Path::new("./parquet-rs/data/nation.dict-malformed.parquet")).unwrap(),
	// 	(
	// 		"m", "nation_key", "name", "region_key", "comment_col",
	// 	)
	// );
	// println!("{}", rows.count());
	let rows = read::<_,
		(
			Option<List<Option<List<Option<List<Option<String>>>>>>>,
			i32,
		)
	>(File::open(&Path::new("./parquet-rs/data/nested_lists.snappy.parquet")).unwrap(),
		("spark_schema",
			("a", ("element", ("element", "element"))),
			"b",
		)
	);
	println!("{}", rows.count());
	let rows = read::<_,
		(
			Option<Map<String,Option<Map<i32,bool>>>>,
			i32,
			f64,
		)
	>(File::open(&Path::new("./parquet-rs/data/nested_maps.snappy.parquet")).unwrap(),
		("spark_schema",
			("a", "key", ("value", "key", "value")),
			"b",
			"c",
		)
	);
	println!("{}", rows.count());
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
		>(File::open(&Path::new("./parquet-rs/data/nonnullable.impala.parquet")).unwrap(),
		("org.apache.impala.ComplexTypesTbl",
			"ID",
			("Int_Array", "element"),
			("int_array_array", ("element", "element")),
			("Int_Map", "key", "value"),
			("int_map_array", ("element", "key", "value")),
			("nested_Struct",
				"a",
				("B", "element"),
				("c", ("D", ("element", ("element", "e", "f")))),
				("G", "key", ("value", ("h", ("i", "element")))),
			)
		)
	);
	println!("{}", rows.count());
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
		>(File::open(&Path::new("./parquet-rs/data/nullable.impala.parquet")).unwrap(),
		("org.apache.impala.ComplexTypesTbl",
			"id",
			("int_array", "element"),
			("int_array_Array", ("element", "element")),
			("int_map", "key", "value"),
			("int_Map_Array", ("element", "key", "value")),
			("nested_struct",
				"A",
				("b", "element"),
				("C", ("d", ("element", ("element", "E", "F")))),
				("g", "key", ("value", ("H", ("i", "element")))),
			)
		)
	);
	println!("{}", rows.count());
	let rows = read::<_,
		(
			Option<(Option<i32>,)>,
		)
		>(File::open(&Path::new("./parquet-rs/data/nulls.snappy.parquet")).unwrap(),
		("spark_schema",
			("b_struct", "b_c_int"),
		)
	);
	println!("{}", rows.count());
	let rows = read::<_,
		(
			i32,
			Option<(Repeat<(i64,Option<String>)>,)>,
		)
		>(File::open(&Path::new("./parquet-rs/data/repeated_no_annotation.parquet")).unwrap(),
		("user",
			"id",
			("phoneNumbers", ("phone", "number", "kind")),
		)
	);
	println!("{}", rows.count());
	let rows = read::<_,
		(
			Option<String>,
			i32,
			f64,
			bool,
			Option<List<i32>>,
		)
		>(File::open(&Path::new("./parquet-rs/data/test_datapage_v2.snappy.parquet")).unwrap(),
		("spark_schema",
			"a",
			"b",
			"c",
			"d",
			("e", "element")
		)
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
	type Schema = (&'static str, K::Schema, V::Schema);
	fn schema(schema: Self::Schema) -> Type {
		Type::group_type_builder(schema.0)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::MAP)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder("key_value")
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(K::schema(schema.1)),Rc::new(V::schema(schema.2))])
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
	type Schema = (&'static str, K::Schema, V::Schema);
	fn schema(schema: Self::Schema) -> Type {
		Type::group_type_builder(schema.0)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::MAP)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder("key_value")
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(K::schema(schema.1)),Rc::new(V::schema(schema.2))])
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
	type Schema = (&'static str, T::Schema);
	fn schema(schema: Self::Schema) -> Type {
		Type::group_type_builder(schema.0)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::LIST)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder("list")
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(T::schema(schema.1))])
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
	type Schema = (&'static str, T::Schema);
	fn schema(schema: Self::Schema) -> Type {
		Type::group_type_builder(schema.0)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::LIST)
			.with_fields(&mut vec![Rc::new(
				Type::group_type_builder("list")
					.with_repetition(Repetition::REPEATED)
					.with_logical_type(LogicalType::NONE)
					.with_fields(&mut vec![Rc::new(T::schema(schema.1))])
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
	let schema = <Root<T>>::schema(schema);
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
	type Schema;
	fn schema(schema: Self::Schema) -> Type;
	fn read(reader: &mut Reader) -> Self;
}
impl ParquetDeserialize for bool {
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::BOOLEAN)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::BOOLEAN)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::DOUBLE)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::FLOAT)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::FLOAT)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::DOUBLE)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::INT32)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::INT32)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::INT64)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::INT64)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::INT64)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::INT64)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::INT96)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::INT96)
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
// 	type Schema = &'static str;
// 	fn schema(schema: Self::Schema) -> Type {
// 		Type::primitive_type_builder(schema, PhysicalType::INT96)
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
// 	fn schema(schema: Self::Schema) -> Type {
// 		Type::primitive_type_builder(schema, PhysicalType::DOUBLE)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::BYTE_ARRAY)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::BYTE_ARRAY)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::BYTE_ARRAY)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::BYTE_ARRAY)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::FIXED_LEN_BYTE_ARRAY)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::FIXED_LEN_BYTE_ARRAY)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::FIXED_LEN_BYTE_ARRAY)
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
	type Schema = &'static str;
	fn schema(schema: Self::Schema) -> Type {
		Type::primitive_type_builder(schema, PhysicalType::FIXED_LEN_BYTE_ARRAY)
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
		impl<$($t,)*> ParquetDeserialize for Root<($($t,)*)> where $($t: ParquetDeserialize,)* {
			type Schema = (&'static str,$($t::Schema,)*);
			fn schema(schema: Self::Schema) -> Type {
				Type::group_type_builder(schema.0).with_fields(&mut vec![$(Rc::new($t::schema(schema.$i)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Self {
				Root(<($($t,)*) as ParquetDeserialize>::read(reader))
			}
		}
		impl<$($t,)*> ParquetDeserialize for ($($t,)*) where $($t: ParquetDeserialize,)* {
			type Schema = (&'static str,$($t::Schema,)*);
			fn schema(schema: Self::Schema) -> Type {
				Type::group_type_builder(schema.0).with_repetition(Repetition::REQUIRED).with_fields(&mut vec![$(Rc::new($t::schema(schema.$i)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Self {
				let mut readers = if let Reader::GroupReader(_,_,ref mut readers) = reader { readers } else  { unreachable!() }.into_iter();
				let ret = ($($t::read(readers.next().unwrap()),)*);
				assert!(readers.next().is_none());
				ret
			}
		}
		impl<$($t,)*> ParquetDeserialize for Option<($($t,)*)> where $($t: ParquetDeserialize,)* {
			type Schema = (&'static str,$($t::Schema,)*);
			fn schema(schema: Self::Schema) -> Type {
				Type::group_type_builder(schema.0).with_repetition(Repetition::OPTIONAL).with_fields(&mut vec![$(Rc::new($t::schema(schema.$i)),)*]).build().unwrap()
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
			type Schema = (&'static str,$($t::Schema,)*);
			fn schema(schema: Self::Schema) -> Type {
				Type::group_type_builder(schema.0).with_repetition(Repetition::REPEATED).with_fields(&mut vec![$(Rc::new($t::schema(schema.$i)),)*]).build().unwrap()
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
impl_parquet_deserialize_tuple!(A 1);
impl_parquet_deserialize_tuple!(A 1 B 2);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17 R 18);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17 R 18 S 19);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17 R 18 S 19 T 20);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17 R 18 S 19 T 20 U 21);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17 R 18 S 19 T 20 U 21 V 22);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17 R 18 S 19 T 20 U 21 V 22 W 23);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17 R 18 S 19 T 20 U 21 V 22 W 23 X 24 Y 25);
impl_parquet_deserialize_tuple!(A 1 B 2 C 3 D 4 E 5 F 6 G 7 H 8 I 9 J 10 K 11 L 12 M 13 N 14 O 15 P 16 Q 17 R 18 S 19 T 20 U 21 V 22 W 23 X 24 Y 25 Z 26);

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
