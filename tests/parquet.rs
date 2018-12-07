#![feature(maybe_uninit,try_from)]

use parquet::{
	basic::{LogicalType, Repetition, Type as PhysicalType}, errors::ParquetError, file::reader::{FileReader, ParquetReader, SerializedFileReader}, record::{reader::Reader, Field}, schema::{
		printer::{print_file_metadata, print_parquet_metadata, print_schema}, types::{BasicTypeInfo, SchemaDescPtr, SchemaDescriptor, Type}
	}
};
use parquet::schema::parser::parse_message_type;
use std::{fs::File, path::Path, rc::Rc, str};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::convert::TryInto;
use std::num::TryFromIntError;

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
		"message schema {
			optional double bp1;
			optional double bp2;
			optional double bp3;
			optional double bp4;
			optional double bp5;
			optional double bs1;
			optional double bs2;
			optional double bs3;
			optional double bs4;
			optional double bs5;
			optional double ap1;
			optional double ap2;
			optional double ap3;
			optional double ap4;
			optional double ap5;
			optional double as1;
			optional double as2;
			optional double as3;
			optional double as4;
			optional double as5;
			optional double valid;
			optional int64 __index_level_0__;
		}".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<f64>,
			Option<i64>,
		)
	>(file,
		"message schema {
			optional double bs5;
			optional int64 __index_level_0__;
		}".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/stock_simulated.parquet")).unwrap();
	let rows = read::<_,
		(
		)
	>(file,
		"message schema {
		}".parse().unwrap()
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
		"message test {
			required byte_array binary_field;
			required int32 int32_field;
			required int64 int64_field;
			required boolean boolean_field;
			required float float_field;
			required double double_field;
			required fixed_len_byte_array (1024) flba_field;
			required int96 int96_field;
		}".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/alltypes_dictionary.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<i32>,
			Option<bool>,
			Option<i8>,
			Option<i16>,
			Option<i32>,
			Option<i64>,
			Option<f32>,
			Option<f64>,
			Option<Vec<u8>>,
			Option<Vec<u8>>,
			Option<Timestamp<i96>>,
		)
	>(file,
		"message schema {
			optional int32 id;
			optional boolean bool_col;
			optional int32 tinyint_col (int_8);
			optional int32 smallint_col (int_16);
			optional int32 int_col;
			optional int64 bigint_col;
			optional float float_col;
			optional double double_col;
			optional byte_array date_string_col;
			optional byte_array string_col;
			optional int96 timestamp_col;
		}".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/alltypes_plain.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<i32>,
			Option<bool>,
			Option<i8>,
			Option<i16>,
			Option<i32>,
			Option<i64>,
			Option<f32>,
			Option<f64>,
			Option<Vec<u8>>,
			Option<Vec<u8>>,
			Option<Timestamp<i96>>,
		)
	>(file,
		"message schema {
			optional int32 id;
			optional boolean bool_col;
			optional int32 tinyint_col (int_8);
			optional int32 smallint_col (int_16);
			optional int32 int_col;
			optional int64 bigint_col;
			optional float float_col;
			optional double double_col;
			optional byte_array date_string_col;
			optional byte_array string_col;
			optional int96 timestamp_col;
		}".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/alltypes_plain.snappy.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<i32>,
			Option<bool>,
			Option<i8>,
			Option<i16>,
			Option<i32>,
			Option<i64>,
			Option<f32>,
			Option<f64>,
			Option<Vec<u8>>,
			Option<Vec<u8>>,
			Option<Timestamp<i96>>,
		)
	>(file,
		"message schema {
			optional int32 id;
			optional boolean bool_col;
			optional int32 tinyint_col (int_8);
			optional int32 smallint_col (int_16);
			optional int32 int_col;
			optional int64 bigint_col;
			optional float float_col;
			optional double double_col;
			optional byte_array date_string_col;
			optional byte_array string_col;
			optional int96 timestamp_col;
		}".parse().unwrap()
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
	// 	"message m {
	// 		optional int32 nation_key;
	// 		optional byte_array name;
	// 		optional int32 region_key;
	// 		optional byte_array comment_col;
	// 	}".parse().unwrap()
	// );
	// println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/nested_lists.snappy.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<List<Option<List<Option<List<Option<String>>>>>>>,
			i32,
		)
	>(file,
		"message spark_schema {
			optional group a (LIST) {
				repeated group list {
					optional group element (LIST) {
						repeated group list {
							optional group element (LIST) {
								repeated group list {
									optional BYTE_ARRAY element (UTF8);
								}
							}
						}
					}
				}
			}
			required int32 b;
		}".parse().unwrap()
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
		"message spark_schema {
			optional group a (MAP) {
				repeated group key_value {
					required byte_array key (UTF8);
					optional group value (MAP) {
						repeated group key_value {
							required int32 key;
							required boolean value;
						}
					}
				}
			}
			required int32 b;
			required double c;
		".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/nonnullable.impala.parquet")).unwrap();
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
		>(file,
		"message org.apache.impala.ComplexTypesTbl {
			REQUIRED INT64 ID (INT_64);
			REQUIRED group Int_Array (LIST) {
				REPEATED group list {
					REQUIRED INT32 element (INT_32);
				}
			}
			REQUIRED group int_array_array (LIST) {
				REPEATED group list {
					REQUIRED group element (LIST) {
						REPEATED group list {
							REQUIRED INT32 element (INT_32);
						}
					}
				}
			}
			REQUIRED group Int_Map (MAP) {
				REPEATED group map {
					REQUIRED BYTE_ARRAY key (UTF8);
					REQUIRED INT32 value (INT_32);
				}
			}
			REQUIRED group int_map_array (LIST) {
				REPEATED group list {
					REQUIRED group element (MAP) {
						REPEATED group map {
							REQUIRED BYTE_ARRAY key (UTF8);
							REQUIRED INT32 value (INT_32);
						}
					}
				}
			}
			REQUIRED group nested_Struct {
				REQUIRED INT32 a (INT_32);
				REQUIRED group B (LIST) {
					REPEATED group list {
						REQUIRED INT32 element (INT_32);
					}
				}
				REQUIRED group c {
					REQUIRED group D (LIST) {
						REPEATED group list {
							REQUIRED group element (LIST) {
								REPEATED group list {
									REQUIRED group element {
										REQUIRED INT32 e (INT_32);
										REQUIRED BYTE_ARRAY f (UTF8);
									}
								}
							}
						}
					}
				}
				REQUIRED group G (MAP) {
					REPEATED group map {
						REQUIRED BYTE_ARRAY key (UTF8);
						REQUIRED group value {
							REQUIRED group h {
								REQUIRED group i (LIST) {
									REPEATED group list {
										REQUIRED DOUBLE element;
									}
								}
							}
						}
					}
				}
			}
		}".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/nullable.impala.parquet")).unwrap();
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
		>(file,
		"message org.apache.impala.ComplexTypesTbl {
			OPTIONAL INT64 id (INT_64);
			OPTIONAL group int_array (LIST) {
				REPEATED group list {
					OPTIONAL INT32 element (INT_32);
				}
			}
			OPTIONAL group int_array_Array (LIST) {
				REPEATED group list {
					OPTIONAL group element (LIST) {
						REPEATED group list {
							OPTIONAL INT32 element (INT_32);
						}
					}
				}
			}
			OPTIONAL group int_map (MAP) {
				REPEATED group map {
					REQUIRED BYTE_ARRAY key (UTF8);
					OPTIONAL INT32 value (INT_32);
				}
			}
			OPTIONAL group int_Map_Array (LIST) {
				REPEATED group list {
					OPTIONAL group element (MAP) {
						REPEATED group map {
							REQUIRED BYTE_ARRAY key (UTF8);
							OPTIONAL INT32 value (INT_32);
						}
					}
				}
			}
			OPTIONAL group nested_struct {
				OPTIONAL INT32 A (INT_32);
				OPTIONAL group b (LIST) {
					REPEATED group list {
						OPTIONAL INT32 element (INT_32);
					}
				}
				OPTIONAL group C {
					OPTIONAL group d (LIST) {
						REPEATED group list {
							OPTIONAL group element (LIST) {
								REPEATED group list {
									OPTIONAL group element {
										OPTIONAL INT32 E (INT_32);
										OPTIONAL BYTE_ARRAY F (UTF8);
									}
								}
							}
						}
					}
				}
				OPTIONAL group g (MAP) {
					REPEATED group map {
						REQUIRED BYTE_ARRAY key (UTF8);
						OPTIONAL group value {
							OPTIONAL group H {
								OPTIONAL group i (LIST) {
									REPEATED group list {
										OPTIONAL DOUBLE element;
									}
								}
							}
						}
					}
				}
			}
		}".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/nulls.snappy.parquet")).unwrap();
	let rows = read::<_,
		(
			Option<(Option<i32>,)>,
		)
		>(file,
		"message spark_schema {
			optional group b_struct {
				optional int32 b_c_int;
			}
		}".parse().unwrap()
	);
	println!("{}", rows.count());

	let file = File::open(&Path::new("./parquet-rs/data/repeated_no_annotation.parquet")).unwrap();
	let rows = read::<_,
		(
			i32,
			Option<(List<(i64,Option<String>)>,)>,
		)
		>(file,
		"message user {
			required int32 id;
			optional group phoneNumbers {
				repeated group phone {
					required int64 number;
					optional byte_array kind (UTF8);
				}
			}
		}".parse().unwrap()
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
		"message spark_schema {
			optional byte_array a (UTF8);
			required int32 b;
			required double c;
			required boolean d;
			optional group e (LIST) {
				repeated group list {
					required int32 element;
				}
			}
		}".parse().unwrap()
	);
	println!("{}", rows.count());
}


struct Root<T>(T);
struct Timestamp<T>(T);
struct u96(u128);
struct i96(i128);
struct List<T>(Vec<T>);
struct Map<K,V>(HashMap<K,V>);

struct RootSchema<T,S>(String, S, PhantomData<fn(T)>);
struct VecSchema;
struct ArraySchema<T>(PhantomData<fn(T)>);
struct GroupSchema<T>(T);
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
struct TimestampSchema<T>(PhantomData<fn(T)>);

trait Deserialize {
	type Schema;
	fn placeholder() -> Self::Schema;
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError>;
	fn schema(name: &str, schema: Self::Schema) -> Type;
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> where Self: Sized;
}
fn read<R: ParquetReader + 'static, T>(
	reader: R, schema: <Root<T> as Deserialize>::Schema,
) -> impl Iterator<Item = T> where Root<T>: Deserialize {
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
			<Root<T>>::read(&mut reader).unwrap().0
		})
	})
}

impl<K,V> Deserialize for Map<K,V>
where
	K: Deserialize + Hash + Eq,
	V: Deserialize,
{
	type Schema = MapSchema<K::Schema, V::Schema>;
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
	fn schema(name: &str, schema: Self::Schema) -> Type {
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
					.with_fields(&mut vec![Rc::new(K::schema(key_name, schema.0)),Rc::new(V::schema(value_name, schema.1))])
					.build()
					.unwrap(),
			)])
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		let mut list = HashMap::new();
		reader.read_key_value(|keys_reader, values_reader| {
			list.insert(K::read(keys_reader)?, V::read(values_reader)?);
			Ok(())
		})?;
		Ok(Map(list))
	}
}
impl<K,V> Deserialize for Option<Map<K,V>>
where
	K: Deserialize + Hash + Eq,
	V: Deserialize,
{
	type Schema = OptionSchema<MapSchema<K::Schema, V::Schema>>;
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
	fn schema(name: &str, schema: Self::Schema) -> Type {
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
					.with_fields(&mut vec![Rc::new(K::schema(key_name, (schema.0).0)),Rc::new(V::schema(value_name, (schema.0).1))])
					.build()
					.unwrap(),
			)])
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		let mut ret = None;
		reader.read_option(|reader| {
			let mut list = HashMap::new();
			reader.read_key_value(|keys_reader, values_reader| {
				list.insert(K::read(keys_reader)?, V::read(values_reader)?);
				Ok(())
			})?;
			ret = Some(Map(list));
			Ok(())
		})?;
		Ok(ret)
	}
}


impl<T> Deserialize for List<T>
where
	T: Deserialize,
{
	type Schema = ListSchema<T::Schema>;
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
			basic_info.set_repetition(Repetition::REQUIRED);
			return Ok((schema.name().to_owned(), ListSchema(T::parse(&schema2)?.1, None)));
		}
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
		Err(ParquetError::General(String::from("List<T>")))
	}
	fn schema(name: &str, schema: Self::Schema) -> Type {
		if let Some((list_name,element_name)) = schema.1 {
			let list_name = list_name.as_ref().map(|x|&**x).unwrap_or("list");
			let element_name = element_name.as_ref().map(|x|&**x).unwrap_or("element");
			Type::group_type_builder(name)
				.with_repetition(Repetition::REQUIRED)
				.with_logical_type(LogicalType::LIST)
				.with_fields(&mut vec![Rc::new(
					Type::group_type_builder(list_name)
						.with_repetition(Repetition::REPEATED)
						.with_logical_type(LogicalType::NONE)
						.with_fields(&mut vec![Rc::new(T::schema(element_name, schema.0))])
						.build()
						.unwrap(),
				)])
				.build()
				.unwrap()
		} else {
			let mut ret = T::schema(name, schema.0);
			let basic_info = match ret {Type::PrimitiveType{ref mut basic_info,..} => basic_info, Type::GroupType{ref mut basic_info,..} => basic_info};
			assert_eq!(basic_info.repetition(), Repetition::REQUIRED);
			basic_info.set_repetition(Repetition::REPEATED);
			ret
		}
	}
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		let mut list = Vec::new();
		reader.read_repeated(|reader| {
			list.push(T::read(reader)?);
			Ok(())
		})?;
		Ok(List(list))
	}
}
impl<T> Deserialize for Option<List<T>>
where
	T: Deserialize,
{
	type Schema = OptionSchema<ListSchema<T::Schema>>;
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
	fn schema(name: &str, schema: Self::Schema) -> Type {
		if let Some((list_name,element_name)) = (schema.0).1 {
			let list_name = list_name.as_ref().map(|x|&**x).unwrap_or("list");
			let element_name = element_name.as_ref().map(|x|&**x).unwrap_or("element");
			Type::group_type_builder(name)
				.with_repetition(Repetition::OPTIONAL)
				.with_logical_type(LogicalType::LIST)
				.with_fields(&mut vec![Rc::new(
					Type::group_type_builder(list_name)
						.with_repetition(Repetition::REPEATED)
						.with_logical_type(LogicalType::NONE)
						.with_fields(&mut vec![Rc::new(T::schema(element_name, (schema.0).0))])
						.build()
						.unwrap(),
				)])
				.build()
				.unwrap()
		} else {
			unreachable!()
		}
	}
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		let mut ret = None;
		reader.read_option(|reader| {
			// if let Reader::GroupReader(_, _, readers) = reader {
			// 	assert_eq!(readers.len(), 1);
			// 	if let Reader::GroupReader(_, _, readers) = readers.into_iter().next().unwrap() {
			// 		assert_eq!(readers.len(), 1);
			// 		let reader = readers.into_iter().next().unwrap();
					let mut list = Vec::new();
					reader.read_repeated(|reader| {
						list.push(T::read(reader)?);
						Ok(())
					})?;
					ret = Some(List(list));
					Ok(())
			// 	} else {
			// 		unreachable!("{}", reader)
			// 	}
			// } else {
			// 	unreachable!("{}", reader)
			// }
		})?;
		Ok(ret)
	}
}

impl Deserialize for bool {
	type Schema = BoolSchema;
	fn placeholder() -> Self::Schema {
		BoolSchema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::BOOLEAN {
			return Ok((schema.name().to_owned(), BoolSchema))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Bool(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<bool> {
	type Schema = OptionSchema<BoolSchema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(BoolSchema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::BOOLEAN {
			return Ok((schema.name().to_owned(), OptionSchema(BoolSchema)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::Bool(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for f32 {
	type Schema = F32Schema;
	fn placeholder() -> Self::Schema {
		F32Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::FLOAT {
			return Ok((schema.name().to_owned(), F32Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Float(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<f32> {
	type Schema = OptionSchema<F32Schema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(F32Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::FLOAT {
			return Ok((schema.name().to_owned(), OptionSchema(F32Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::Float(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for f64 {
	type Schema = F64Schema;
	fn placeholder() -> Self::Schema {
		F64Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::DOUBLE {
			return Ok((schema.name().to_owned(), F64Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Double(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<f64> {
	type Schema = OptionSchema<F64Schema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(F64Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::DOUBLE {
			return Ok((schema.name().to_owned(), OptionSchema(F64Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::Double(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for i8 {
	type Schema = I8Schema;
	fn placeholder() -> Self::Schema {
		I8Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 {
			return Ok((schema.name().to_owned(), I8Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::INT_8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Byte(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<i8> {
	type Schema = OptionSchema<I8Schema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(I8Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT32 {
			return Ok((schema.name().to_owned(), OptionSchema(I8Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::INT_8)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, reader) = reader {
			if let Reader::PrimitiveReader(type_ptr, _) = &**reader {
				// println!("{:?}", type_ptr);
			} else {
				unreachable!()
			}
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::Byte(field) => Some(field),
			Field::Int(field) => Some(field.try_into().map_err(|err:TryFromIntError|ParquetError::General(err.to_string()))?),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for i16 {
	type Schema = I16Schema;
	fn placeholder() -> Self::Schema {
		I16Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 {
			return Ok((schema.name().to_owned(), I16Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::REQUIRED)
			.with_logical_type(LogicalType::INT_16)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Short(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<i16> {
	type Schema = OptionSchema<I16Schema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(I16Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT32 {
			return Ok((schema.name().to_owned(), OptionSchema(I16Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
	fn schema(name: &str, schema: Self::Schema) -> Type {
		Type::primitive_type_builder(name, PhysicalType::INT32)
			.with_repetition(Repetition::OPTIONAL)
			.with_logical_type(LogicalType::INT_16)
			.with_length(-1)
			.with_precision(-1)
			.with_scale(-1)
			.build()
			.unwrap()
	}
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::Short(field) => Some(field),
			Field::Int(field) => Some(field.try_into().map_err(|err:TryFromIntError|ParquetError::General(err.to_string()))?),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for i32 {
	type Schema = I32Schema;
	fn placeholder() -> Self::Schema {
		I32Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT32 {
			return Ok((schema.name().to_owned(), I32Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Int(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<i32> {
	type Schema = OptionSchema<I32Schema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(I32Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT32 {
			return Ok((schema.name().to_owned(), OptionSchema(I32Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::Int(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for i64 {
	type Schema = I64Schema;
	fn placeholder() -> Self::Schema {
		I64Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT64 {
			return Ok((schema.name().to_owned(), I64Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Long(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<i64> {
	type Schema = OptionSchema<I64Schema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(I64Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT64 {
			return Ok((schema.name().to_owned(), OptionSchema(I64Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::Long(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for u64 {
	type Schema = U64Schema;
	fn placeholder() -> Self::Schema {
		U64Schema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT64 {
			return Ok((schema.name().to_owned(), U64Schema))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::ULong(field) = reader.read_field() {
			field
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<u64> {
	type Schema = OptionSchema<U64Schema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(U64Schema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT64 {
			return Ok((schema.name().to_owned(), OptionSchema(U64Schema)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::ULong(field) => Some(field),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for Timestamp<i96> {
	type Schema = TimestampSchema<i96>;
	fn placeholder() -> Self::Schema {
		TimestampSchema(PhantomData)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::INT96 {
			return Ok((schema.name().to_owned(), TimestampSchema(PhantomData)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Timestamp(field) = reader.read_field() {
			Timestamp(i96(field.into()))
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<Timestamp<i96>> {
	type Schema = OptionSchema<TimestampSchema<i96>>;
	fn placeholder() -> Self::Schema {
		OptionSchema(TimestampSchema(PhantomData))
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::INT96 {
			return Ok((schema.name().to_owned(), OptionSchema(TimestampSchema(PhantomData))))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(match reader.read_field() {
			Field::Timestamp(field) => Some(Timestamp(i96(field.into()))),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
// impl Deserialize for u96 {
// 	type Schema = ();
// fn placeholder() -> Self::Schema {
// 	
// }
// fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 	unimplemented!()
// }
// 	fn schema(name: &str, schema: Self::Schema) -> Type {
// 		Type::primitive_type_builder(name, PhysicalType::INT96)
// 			.with_repetition(Repetition::REQUIRED)
// 			.with_logical_type(LogicalType::NONE)
// 			.with_length(-1)
// 			.with_precision(-1)
// 			.with_scale(-1)
// 			.build().unwrap()
// 	}
// 	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
// 		if let Reader::PrimitiveReader(_,_) = reader { } else  { unreachable!() }
// 		panic!("x96??! {:?}", reader.read_field());
// 		// if let Field::ULong(field) = reader.read_field() { field } else { unreachable!() }
// 	}
// }

// impl Deserialize for parquet::data_type::Decimal {
// 	type Schema = DecimalSchema;
// fn placeholder() -> Self::Schema {
// 	
// }
// fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
// 	unimplemented!()
// }
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
impl Deserialize for Vec<u8> {
	type Schema = VecSchema;
	fn placeholder() -> Self::Schema {
		VecSchema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::BYTE_ARRAY {
			return Ok((schema.name().to_owned(), VecSchema))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Bytes(bytes) = reader.read_field() {
			bytes.data().to_owned()
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<Vec<u8>> {
	type Schema = OptionSchema<VecSchema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(VecSchema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::BYTE_ARRAY {
			return Ok((schema.name().to_owned(), OptionSchema(VecSchema)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!("{}", reader)
		}
		Ok(match reader.read_field() {
			Field::Bytes(bytes) => Some(bytes.data().to_owned()),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for String {
	type Schema = StringSchema;
	fn placeholder() -> Self::Schema {
		StringSchema
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::BYTE_ARRAY {
			return Ok((schema.name().to_owned(), StringSchema))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Str(bytes) = reader.read_field() {
			bytes//String::from_utf8(bytes.data().to_owned()).unwrap()
		} else {
			unreachable!()
		})
	}
}
impl Deserialize for Option<String> {
	type Schema = OptionSchema<StringSchema>;
	fn placeholder() -> Self::Schema {
		OptionSchema(StringSchema)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::OPTIONAL && schema.get_physical_type() == PhysicalType::BYTE_ARRAY {
			return Ok((schema.name().to_owned(), OptionSchema(StringSchema)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::OptionReader(_, _) = reader {
		} else {
			unreachable!("{}", reader)
		}
		Ok(match reader.read_field() {
			Field::Str(bytes) => Some(bytes),//String::from_utf8(bytes.data().to_owned()).unwrap()),
			Field::Null => None,
			_ => unreachable!(),
		})
	}
}
impl Deserialize for [u8; 0] {
	// is this valid?
	type Schema = ArraySchema<Self>;
	fn placeholder() -> Self::Schema {
		ArraySchema(PhantomData)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		unimplemented!()
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		unimplemented!()
	}
}
impl Deserialize for [u8; 1] {
	type Schema = ArraySchema<Self>;
	fn placeholder() -> Self::Schema {	
		ArraySchema(PhantomData)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		unimplemented!()
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		unimplemented!()
	}
}
impl Deserialize for [u8; 2] {
	type Schema = ArraySchema<Self>;
	fn placeholder() -> Self::Schema {
		ArraySchema(PhantomData)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		unimplemented!()
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		unimplemented!()
	}
}
impl Deserialize for [u8; 1024] {
	type Schema = ArraySchema<Self>;
	fn placeholder() -> Self::Schema {
		ArraySchema(PhantomData)
	}
	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		if schema.is_primitive() && schema.get_basic_info().repetition() == Repetition::REQUIRED && schema.get_physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY && schema.get_type_length() == 1024 {
			return Ok((schema.name().to_owned(), ArraySchema(PhantomData)))
		}
		Err(ParquetError::General(String::from("")))
	}
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
	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		if let Reader::PrimitiveReader(_, _) = reader {
		} else {
			unreachable!()
		}
		Ok(if let Field::Bytes(bytes) = reader.read_field() {
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
		})
	}
}


macro_rules! impl_parquet_deserialize_tuple {
	($($t:ident $i:tt)*) => (
		impl<$($t,)*> str::FromStr for RootSchema<($($t,)*),($((String,$t::Schema,),)*)> where $($t: Deserialize,)* {
			type Err = ParquetError;
			fn from_str(s: &str) -> Result<Self, Self::Err> {
				parse_message_type(s).and_then(|x|<Root<($($t,)*)> as Deserialize>::parse(&x).map_err(|err| {
					let x: Type = <Root<($($t,)*)> as Deserialize>::schema("", <Root<($($t,)*)> as Deserialize>::placeholder());
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
			type Schema = RootSchema<($($t,)*),($((String,$t::Schema,),)*)>;
			fn placeholder() -> Self::Schema {
				RootSchema(String::from("<name>"), ($((String::from("<name>"),$t::placeholder()),)*), PhantomData)
			}
			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_schema() {
					let mut fields = schema.get_fields().iter();
					let schema_ = RootSchema(schema.name().to_owned(), ($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*), PhantomData);
					if fields.next().is_none() {
						return Ok((String::from(""), schema_))
					}
				}
				Err(ParquetError::General(String::from("")))
			}
			fn schema(name: &str, schema: Self::Schema) -> Type {
				assert_eq!(name, "");
				Type::group_type_builder(&schema.0).with_fields(&mut vec![$(Rc::new($t::schema(&(schema.1).$i.0, (schema.1).$i.1)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
				Ok(Root(<($($t,)*) as Deserialize>::read(reader)?))
			}
		}
		impl<$($t,)*> Deserialize for ($($t,)*) where $($t: Deserialize,)* {
			type Schema = GroupSchema<($((String,$t::Schema,),)*)>;
			fn placeholder() -> Self::Schema {
				GroupSchema(($((String::from("<name>"),$t::placeholder()),)*))
			}
			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED {
					let mut fields = schema.get_fields().iter();
					let schema_ = GroupSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*));
					if fields.next().is_none() {
						return Ok((schema.name().to_owned(), schema_))
					}
				}
				Err(ParquetError::General(String::from("")))
			}
			fn schema(name: &str, schema: Self::Schema) -> Type {
				Type::group_type_builder(name).with_repetition(Repetition::REQUIRED).with_fields(&mut vec![$(Rc::new($t::schema(&(schema.0).$i.0, (schema.0).$i.1)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
				let mut readers = if let Reader::GroupReader(_,_,ref mut readers) = reader { readers } else  { unreachable!() }.into_iter();
				let ret = ($($t::read(readers.next().unwrap())?,)*);
				assert!(readers.next().is_none());
				Ok(ret)
			}
		}
		impl<$($t,)*> Deserialize for Option<($($t,)*)> where $($t: Deserialize,)* {
			type Schema = OptionSchema<GroupSchema<($((String,$t::Schema,),)*)>>;
			fn placeholder() -> Self::Schema {
				OptionSchema(GroupSchema(($((String::from("<name>"),$t::placeholder()),)*)))
			}
			fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
				if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::OPTIONAL {
					let mut fields = schema.get_fields().iter();
					let schema_ = OptionSchema(GroupSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*)));
					if fields.next().is_none() {
						return Ok((schema.name().to_owned(), schema_))
					}
				}
				Err(ParquetError::General(String::from("")))
			}
			fn schema(name: &str, schema: Self::Schema) -> Type {
				Type::group_type_builder(name).with_repetition(Repetition::OPTIONAL).with_fields(&mut vec![$(Rc::new($t::schema(&((schema.0).0).$i.0, ((schema.0).0).$i.1)),)*]).build().unwrap()
			}
			fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
				let mut ret = None;
				reader.read_option(|reader| {
					let mut readers = if let Reader::GroupReader(_,_,ref mut readers) = reader { readers } else  { unreachable!() }.into_iter();
					ret = Some(($($t::read(readers.next().unwrap())?,)*));
					assert!(readers.next().is_none());
					Ok(())
				})?;
				Ok(ret)
			}
		}
		// impl<$($t,)*> Deserialize for Repeat<($($t,)*)> where $($t: Deserialize,)* {
		// 	type Schema = RepeatSchema<GroupSchema<($((String,$t::Schema,),)*)>>;
		// 	fn placeholder() -> Self::Schema {
		// 		RepeatSchema(GroupSchema(($((String::from("<name>"),$t::placeholder()),)*)))
		// 	}
		// 	fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
		// 		if schema.is_group() && !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REPEATED {
		// 			let mut fields = schema.get_fields().iter();
		// 			let schema_ = RepeatSchema(GroupSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x))?,)*)));
		// 			if fields.next().is_none() {
		// 				return Ok((schema.name().to_owned(), schema_))
		// 			}
		// 		}
		// 		Err(ParquetError::General(String::from("")))
		// 	}
		// 	fn schema(name: &str, schema: Self::Schema) -> Type {
		// 		Type::group_type_builder(name).with_repetition(Repetition::REPEATED).with_fields(&mut vec![$(Rc::new($t::schema(&((schema.0).0).$i.0, ((schema.0).0).$i.1)),)*]).build().unwrap()
		// 	}
		// 	fn read(reader: &mut Reader) -> Result<Self,ParquetError> {
		// 		let mut list = Vec::new();
		// 		reader.read_repeated(|reader| {
		// 			let mut readers = if let Reader::GroupReader(_,_,ref mut readers) = reader { readers } else  { unreachable!() }.into_iter();
		// 			let ret = ($($t::read(readers.next().unwrap())?,)*);
		// 			assert!(readers.next().is_none());
		// 			list.push(ret);
		// 			Ok(())
		// 		})?;
		// 		Ok(Repeat(list))
		// 	}
		// }
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
