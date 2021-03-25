use hashlink::linked_hash_map::LinkedHashMap;
use std::{
	any::type_name, collections::HashMap, convert::{TryFrom, TryInto}, fmt, hash::{BuildHasher, Hash}, marker::PhantomData, string::FromUtf8Error, sync::Arc
};
use sum::{Sum2, Sum3};

use amadeus_types::{
	Bson, Data, Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, Decimal, Enum, Group, IpAddr, Json, List, Time, TimeWithoutTimezone, Timezone, Url, Value, Webpage
};

use crate::internal::{
	basic::{LogicalType, Repetition, Type as PhysicalType}, column::reader::ColumnReader, data_type::{
		BoolType, ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type, Int96, Int96Type
	}, errors::{ParquetError, Result}, record::{
		display::{DisplayFmt, DisplaySchemaGroup}, predicates::{GroupPredicate, MapPredicate, ValuePredicate}, reader::{
			BoolReader, BoxFixedLenByteArrayReader, BoxReader, ByteArrayReader, F32Reader, F64Reader, FixedLenByteArrayReader, GroupReader, I32Reader, I64Reader, I96Reader, KeyValueReader, MapReader, OptionReader, RepeatedReader, RootReader, TryIntoReader, TupleReader, ValueReader, VecU8Reader
		}, schemas::{
			BoolSchema, BoxSchema, BsonSchema, ByteArraySchema, DateSchema, DateTimeSchema, DecimalSchema, EnumSchema, F32Schema, F64Schema, FixedByteArraySchema, GroupSchema, I16Schema, I32Schema, I64Schema, I8Schema, JsonSchema, ListSchema, ListSchemaType, MapSchema, OptionSchema, RootSchema, StringSchema, TimeSchema, TupleSchema, U16Schema, U32Schema, U64Schema, U8Schema, ValueSchema, VecU8Schema
		}, triplet::TypedTripletIter, types::{downcast, Downcast, Root}, ParquetData, Predicate, Reader, Schema
	}, schema::types::{ColumnPath, Type}
};

////////////////////////////////////////////////////////////////////////////////

macro_rules! via_string {
	($($doc:tt $t:ty)*) => ($(
		impl ParquetData for $t {
			type Schema = StringSchema;
			type Reader = impl Reader<Item = Self>;
			type Predicate = Predicate;

			fn parse(schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>) -> Result<(String, Self::Schema)> {
				Value::parse(schema, None, repetition).and_then(downcast)
			}

			fn reader(
				schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
				paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
			) -> Self::Reader {
				MapReader(
					String::reader(schema, path, def_level, rep_level, paths, batch_size),
					|string: String| string.parse().map_err(Into::into),
				)
			}
		}
	)*)
}

////////////////////////////////////////////////////////////////////////////////

impl ParquetData for Bson {
	type Schema = BsonSchema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader(
			byte_array_reader(&schema.0, path, def_level, rep_level, paths, batch_size),
			|x| Ok(Bson::from(Vec::from(x))),
		)
	}
}

impl ParquetData for String {
	type Schema = StringSchema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader(
			byte_array_reader(&schema.0, path, def_level, rep_level, paths, batch_size),
			|x| {
				String::from_utf8(Vec::from(x))
					.map_err(|err: FromUtf8Error| ParquetError::General(err.to_string()))
			},
		)
	}
}

impl ParquetData for Json {
	type Schema = JsonSchema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader(
			String::reader(&schema.0, path, def_level, rep_level, paths, batch_size),
			|x| Ok(From::from(x)),
		)
	}
}

impl ParquetData for Enum {
	type Schema = EnumSchema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader(
			String::reader(&schema.0, path, def_level, rep_level, paths, batch_size),
			|x| Ok(From::from(x)),
		)
	}
}

// Implement ParquetData for common array lengths.
macro_rules! array {
	($($i:tt)*) => {$(
		impl ParquetData for [u8; $i] {
			type Schema = FixedByteArraySchema<Self>;
			type Reader = FixedLenByteArrayReader<Self>;
			type Predicate = Predicate;

			fn parse(
				schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
			) -> Result<(String, Self::Schema)> {
				if schema.is_primitive()
					&& repetition == Some(Repetition::Required)
					&& schema.get_physical_type() == PhysicalType::FixedLenByteArray
					&& schema.get_basic_info().logical_type() == LogicalType::None
					&& schema.get_type_length() == $i
				{
					return Ok((schema.name().to_owned(), FixedByteArraySchema(PhantomData)));
				}
				Err(ParquetError::General(format!(
					"Can't parse array {:?}",
					schema
				)))
			}

			fn reader(
				_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
				paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
			) -> Self::Reader {
				let col_path = ColumnPath::new(path.to_vec());
				let col_reader = paths.remove(&col_path).unwrap();
				FixedLenByteArrayReader::<[u8; $i]> {
					column: TypedTripletIter::<FixedLenByteArrayType>::new(
						def_level, rep_level, col_reader, batch_size,
					),
					marker: PhantomData,
				}
			}
		}

		// Specialize Box<[T; N]> to avoid passing a potentially large array around on the stack.
		#[cfg(nightly)]
		impl ParquetData for Box<[u8; $i]> {
			type Schema = FixedByteArraySchema<[u8; $i]>;
			type Reader = BoxFixedLenByteArrayReader<[u8; $i]>;

			fn parse(
				schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
			) -> Result<(String, Self::Schema)> {
				<[u8; $i]>::parse(schema, predicate, repetition)
			}

			fn reader(
				_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
				paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
			) -> Self::Reader {
				let col_path = ColumnPath::new(path.to_vec());
				let col_reader = paths.remove(&col_path).unwrap();
				BoxFixedLenByteArrayReader::<[u8; $i]> {
					column: TypedTripletIter::<FixedLenByteArrayType>::new(
						def_level, rep_level, col_reader, batch_size,
					),
					marker: PhantomData,
				}
			}
		}
	)*};
}
amadeus_types::array!(array);

////////////////////////////////////////////////////////////////////////////////

// Enables Rust types to be transparently boxed, for example to avoid overflowing the
// stack. This is marked as `default` so that `Box<[u8; N]>` can be specialized.
impl<T> ParquetData for Box<T>
where
	T: ParquetData,
{
	default type Schema = BoxSchema<T::Schema>;
	default type Reader = BoxReader<T::Reader>;
	type Predicate = T::Predicate;

	default fn parse(
		schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		T::parse(schema, predicate, repetition)
			.map(|(name, schema)| (name, type_coerce(BoxSchema(schema))))
	}

	default fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let schema = type_coerce::<&Self::Schema, &BoxSchema<T::Schema>>(schema);
		let ret = BoxReader(T::reader(
			&schema.0, path, def_level, rep_level, paths, batch_size,
		));
		type_coerce(ret)
	}
}

////////////////////////////////////////////////////////////////////////////////

impl ParquetData for Decimal {
	type Schema = DecimalSchema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		match *schema {
			DecimalSchema::Int32 { precision, scale } => DecimalReader::Int32 {
				reader: i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
				precision,
				scale,
			},
			DecimalSchema::Int64 { precision, scale } => DecimalReader::Int64 {
				reader: i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
				precision,
				scale,
			},
			DecimalSchema::Array {
				ref byte_array_schema,
				precision,
				scale,
			} => DecimalReader::Array {
				reader: byte_array_reader(
					byte_array_schema,
					path,
					def_level,
					rep_level,
					paths,
					batch_size,
				),
				precision,
				scale,
			},
		}
	}
}

pub enum DecimalReader {
	Int32 {
		reader: <i32 as ParquetData>::Reader,
		precision: u8,
		scale: u8,
	},
	Int64 {
		reader: <i64 as ParquetData>::Reader,
		precision: u8,
		scale: u8,
	},
	Array {
		reader: ByteArrayReader,
		precision: u32,
		scale: u32,
	},
}

impl Reader for DecimalReader {
	type Item = Decimal;

	#[inline]
	fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item> {
		match self {
			DecimalReader::Int32 {
				reader,
				precision,
				scale,
			} => reader.read(def_level, rep_level).map(|_bytes| {
				let _ = (precision, scale);
				unimplemented!()
				//Decimal::from_i32(bytes, *precision as i32, *scale as i32)),
			}),
			DecimalReader::Int64 {
				reader,
				precision,
				scale,
			} => reader.read(def_level, rep_level).map(|_bytes| {
				let _ = (precision, scale);
				unimplemented!()
				//Decimal::from_i64(bytes, *precision as i32, *scale as i32)),
			}),
			DecimalReader::Array {
				reader,
				precision,
				scale,
			} => reader
				.read(def_level, rep_level)
				.map(|bytes| Decimal::from_bytes(bytes.into(), *precision as i32, *scale as i32)),
		}
	}

	#[inline]
	fn advance_columns(&mut self) -> Result<()> {
		match self {
			DecimalReader::Int32 { reader, .. } => reader.advance_columns(),
			DecimalReader::Int64 { reader, .. } => reader.advance_columns(),
			DecimalReader::Array { reader, .. } => reader.advance_columns(),
		}
	}

	#[inline]
	fn has_next(&self) -> bool {
		match self {
			DecimalReader::Int32 { reader, .. } => reader.has_next(),
			DecimalReader::Int64 { reader, .. } => reader.has_next(),
			DecimalReader::Array { reader, .. } => reader.has_next(),
		}
	}

	#[inline]
	fn current_def_level(&self) -> i16 {
		match self {
			DecimalReader::Int32 { reader, .. } => reader.current_def_level(),
			DecimalReader::Int64 { reader, .. } => reader.current_def_level(),
			DecimalReader::Array { reader, .. } => reader.current_def_level(),
		}
	}

	#[inline]
	fn current_rep_level(&self) -> i16 {
		match self {
			DecimalReader::Int32 { reader, .. } => reader.current_rep_level(),
			DecimalReader::Int64 { reader, .. } => reader.current_rep_level(),
			DecimalReader::Array { reader, .. } => reader.current_rep_level(),
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

impl ParquetData for Group {
	type Schema = GroupSchema;
	type Reader = GroupReader;
	type Predicate = GroupPredicate;

	fn parse(
		schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		if schema.is_group() && repetition == Some(Repetition::Required) {
			let mut map = LinkedHashMap::with_capacity_and_hasher(
				schema.get_fields().len(),
				Default::default(),
			);
			let fields = schema.get_fields().iter();
			let fields = if let Some(predicate) = predicate {
				let fields = fields
					.map(|field| (field.name(), field))
					.collect::<HashMap<_, _>>();
				predicate
					.0
					.iter()
					.enumerate()
					.map(|(i, (field, predicate))| {
						let field = fields.get(&**field).ok_or_else(|| {
							ParquetError::General(format!(
								"Group has predicate field \"{}\" not in the schema",
								field
							))
						})?;
						let (name, schema) = <Value as ParquetData>::parse(
							&**field,
							predicate.as_ref(),
							Some(field.get_basic_info().repetition()),
						)?;
						let x = map.insert(name, i);
						assert!(x.is_none());
						Ok(schema)
					})
					.collect::<Result<Vec<ValueSchema>>>()?
			} else {
				fields
					.enumerate()
					.map(|(i, field)| {
						let (name, schema) = <Value as ParquetData>::parse(
							&**field,
							None,
							Some(field.get_basic_info().repetition()),
						)?;
						let x = map.insert(name, i);
						assert!(x.is_none());
						Ok(schema)
					})
					.collect::<Result<Vec<ValueSchema>>>()?
			};
			let schema_ = GroupSchema(fields, map);
			return Ok((schema.name().to_owned(), schema_));
		}
		Err(ParquetError::General(format!(
			"Can't parse Group {:?}",
			schema
		)))
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let readers = schema
			.1
			.iter()
			.map(|(name, _index)| name)
			.zip(schema.0.iter())
			.map(|(name, field)| {
				path.push(name.clone());
				let ret = Value::reader(field, path, def_level, rep_level, paths, batch_size);
				let _ = path.pop().unwrap();
				ret
			})
			.collect();
		GroupReader {
			readers,
			fields: Arc::new(schema.1.clone()),
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

/// Returns true if repeated type is an element type for the list.
/// Used to determine legacy list types.
/// This method is copied from Spark Parquet reader and is based on the reference:
/// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
fn parse_list<T: ParquetData>(
	schema: &Type, predicate: Option<&T::Predicate>,
) -> Result<ListSchema<T::Schema>> {
	if schema.is_group()
		&& schema.get_basic_info().logical_type() == LogicalType::List
		&& schema.get_fields().len() == 1
	{
		let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
		if sub_schema.get_basic_info().repetition() == Repetition::Repeated {
			return Ok(
				if sub_schema.is_group()
					&& sub_schema.get_fields().len() == 1
					&& sub_schema.name() != "array"
					&& sub_schema.name() != format!("{}_tuple", schema.name())
				{
					let element = sub_schema.get_fields().into_iter().nth(0).unwrap();
					let list_name = if sub_schema.name() == "list" {
						None
					} else {
						Some(sub_schema.name().to_owned())
					};
					let element_name = if element.name() == "element" {
						None
					} else {
						Some(element.name().to_owned())
					};

					ListSchema(
						T::parse(
							&*element,
							predicate,
							Some(element.get_basic_info().repetition()),
						)?
						.1,
						ListSchemaType::List(list_name, element_name),
					)
				} else {
					let element_name = sub_schema.name().to_owned();
					ListSchema(
						T::parse(&*sub_schema, predicate, Some(Repetition::Repeated))?.1,
						ListSchemaType::ListCompat(element_name),
					)
				},
			);
		}
	}
	Err(ParquetError::General(String::from("Couldn't parse Vec<T>")))
}

impl<T: Data> ParquetData for List<T>
where
	T: ParquetData,
{
	default type Schema = ListSchema<T::Schema>;
	default type Reader = RepeatedReader<T::Reader>;
	type Predicate = T::Predicate;

	default fn parse(
		schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		if repetition == Some(Repetition::Required) {
			return parse_list::<T>(schema, predicate)
				.map(|schema2| (schema.name().to_owned(), type_coerce(schema2)));
		}
		// A repeated field that is neither contained by a `LIST`- or `MAP`-annotated
		// group nor annotated by `LIST` or `MAP` should be interpreted as a
		// required list of required elements where the element type is the type
		// of the field.
		if repetition == Some(Repetition::Repeated) {
			return Ok((
				schema.name().to_owned(),
				type_coerce(ListSchema(
					T::parse(&schema, predicate, Some(Repetition::Required))?.1,
					ListSchemaType::Repeated,
				)),
			));
		}
		Err(ParquetError::General(String::from("Couldn't parse Vec<T>")))
	}

	default fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let schema: &ListSchema<T::Schema> = type_coerce(schema);
		type_coerce(list_reader::<T>(
			schema, path, def_level, rep_level, paths, batch_size,
		))
	}
}

fn list_reader<T>(
	schema: &ListSchema<T::Schema>, path: &mut Vec<String>, def_level: i16, rep_level: i16,
	paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
) -> RepeatedReader<T::Reader>
where
	T: ParquetData,
{
	match &schema.1 {
		ListSchemaType::List(ref list_name, ref element_name) => {
			let list_name = list_name.as_ref().map(|x| &**x).unwrap_or("list");
			let element_name = element_name.as_ref().map(|x| &**x).unwrap_or("element");

			path.push(list_name.to_owned());
			path.push(element_name.to_owned());
			let reader = T::reader(
				&schema.0,
				path,
				def_level + 1,
				rep_level + 1,
				paths,
				batch_size,
			);
			let _ = path.pop().unwrap();
			let _ = path.pop().unwrap();

			RepeatedReader { reader }
		}
		ListSchemaType::ListCompat(ref element_name) => {
			path.push(element_name.to_owned());
			let reader = T::reader(
				&schema.0,
				path,
				def_level + 1,
				rep_level + 1,
				paths,
				batch_size,
			);
			let _ = path.pop().unwrap();

			RepeatedReader { reader }
		}
		ListSchemaType::Repeated => {
			let reader = T::reader(
				&schema.0,
				path,
				def_level + 1,
				rep_level + 1,
				paths,
				batch_size,
			);
			RepeatedReader { reader }
		}
	}
}

fn byte_array_reader(
	_schema: &ByteArraySchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
	paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
) -> ByteArrayReader {
	let col_path = ColumnPath::new(path.to_vec());
	let col_reader = paths.remove(&col_path).unwrap();
	ByteArrayReader {
		column: TypedTripletIter::<ByteArrayType>::new(
			def_level, rep_level, col_reader, batch_size,
		),
	}
}

impl ParquetData for List<u8> {
	type Schema = VecU8Schema;
	type Reader = VecU8Reader;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		match schema {
			VecU8Schema::ByteArray(schema) => VecU8Reader::ByteArray(byte_array_reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			VecU8Schema::List(schema) => VecU8Reader::List(list_reader::<u8>(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

impl<K, V, S> ParquetData for HashMap<K, V, S>
where
	K: ParquetData + Hash + Eq,
	V: ParquetData,
	S: BuildHasher + Default + Clone + Send + 'static,
{
	type Schema = MapSchema<K::Schema, V::Schema>;
	type Reader = impl Reader<Item = Self>;
	type Predicate = MapPredicate<K::Predicate, V::Predicate>;

	fn parse(
		schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
		if schema.is_group()
			&& repetition == Some(Repetition::Required)
			&& (schema.get_basic_info().logical_type() == LogicalType::Map
				|| schema.get_basic_info().logical_type() == LogicalType::MapKeyValue)
			&& schema.get_fields().len() == 1
		{
			let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
			if sub_schema.is_group()
				&& sub_schema.get_basic_info().repetition() == Repetition::Repeated
				&& sub_schema.get_fields().len() == 2
			{
				let mut fields = sub_schema.get_fields().into_iter();
				let (key, value) = (fields.next().unwrap(), fields.next().unwrap());
				let key_value_name = if sub_schema.name() == "key_value" {
					None
				} else {
					Some(sub_schema.name().to_owned())
				};
				let key_name = if key.name() == "key" {
					None
				} else {
					Some(key.name().to_owned())
				};
				let value_name = if value.name() == "value" {
					None
				} else {
					Some(value.name().to_owned())
				};
				return Ok((
					schema.name().to_owned(),
					MapSchema(
						K::parse(
							&*key,
							predicate.and_then(|predicate| predicate.key.as_ref()),
							Some(key.get_basic_info().repetition()),
						)?
						.1,
						V::parse(
							&*value,
							predicate.and_then(|predicate| predicate.value.as_ref()),
							Some(value.get_basic_info().repetition()),
						)?
						.1,
						key_value_name,
						key_name,
						value_name,
					),
				));
			}
		}
		Err(ParquetError::General(String::from(
			"Couldn't parse HashMap<K,V>",
		)))
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let key_value_name = schema.2.as_ref().map(|x| &**x).unwrap_or("key_value");
		let key_name = schema.3.as_ref().map(|x| &**x).unwrap_or("key");
		let value_name = schema.4.as_ref().map(|x| &**x).unwrap_or("value");

		path.push(key_value_name.to_owned());
		path.push(key_name.to_owned());
		let keys_reader = K::reader(
			&schema.0,
			path,
			def_level + 1,
			rep_level + 1,
			paths,
			batch_size,
		);
		let _ = path.pop().unwrap();
		path.push(value_name.to_owned());
		let values_reader = V::reader(
			&schema.1,
			path,
			def_level + 1,
			rep_level + 1,
			paths,
			batch_size,
		);
		let _ = path.pop().unwrap();
		let _ = path.pop().unwrap();

		MapReader(
			KeyValueReader {
				keys_reader,
				values_reader,
			},
			|x: List<_>| {
				Ok(From::from(
					Vec::from(x).into_iter().collect::<HashMap<_, _, S>>(),
				))
			},
		)
	}
}

////////////////////////////////////////////////////////////////////////////////

// See [Numeric logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#numeric-types) for more details.

impl ParquetData for bool {
	type Schema = BoolSchema;
	type Reader = BoolReader;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let col_reader = paths.remove(&col_path).unwrap();
		BoolReader {
			column: TypedTripletIter::<BoolType>::new(def_level, rep_level, col_reader, batch_size),
		}
	}
}

impl ParquetData for i8 {
	type Schema = I8Schema;
	type Reader = TryIntoReader<I32Reader, i8>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		TryIntoReader(
			i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
			PhantomData,
		)
	}
}
impl ParquetData for u8 {
	type Schema = U8Schema;
	type Reader = TryIntoReader<I32Reader, u8>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		TryIntoReader(
			i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
			PhantomData,
		)
	}
}

impl ParquetData for i16 {
	type Schema = I16Schema;
	type Reader = TryIntoReader<I32Reader, i16>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		TryIntoReader(
			i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
			PhantomData,
		)
	}
}
impl ParquetData for u16 {
	type Schema = U16Schema;
	type Reader = TryIntoReader<I32Reader, u16>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		TryIntoReader(
			i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
			PhantomData,
		)
	}
}

impl ParquetData for i32 {
	type Schema = I32Schema;
	type Reader = I32Reader;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let col_reader = paths.remove(&col_path).unwrap();
		I32Reader {
			column: TypedTripletIter::<Int32Type>::new(
				def_level, rep_level, col_reader, batch_size,
			),
		}
	}
}
impl ParquetData for u32 {
	type Schema = U32Schema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader(
			i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
			|x| Ok(x as u32),
		)
	}
}

impl ParquetData for i64 {
	type Schema = I64Schema;
	type Reader = I64Reader;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let col_reader = paths.remove(&col_path).unwrap();
		I64Reader {
			column: TypedTripletIter::<Int64Type>::new(
				def_level, rep_level, col_reader, batch_size,
			),
		}
	}
}
impl ParquetData for u64 {
	type Schema = U64Schema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader(
			i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
			|x| Ok(x as u64),
		)
	}
}

impl ParquetData for f32 {
	type Schema = F32Schema;
	type Reader = F32Reader;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let col_reader = paths.remove(&col_path).unwrap();
		F32Reader {
			column: TypedTripletIter::<FloatType>::new(
				def_level, rep_level, col_reader, batch_size,
			),
		}
	}
}
impl ParquetData for f64 {
	type Schema = F64Schema;
	type Reader = F64Reader;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		let col_path = ColumnPath::new(path.to_vec());
		let col_reader = paths.remove(&col_path).unwrap();
		F64Reader {
			column: TypedTripletIter::<DoubleType>::new(
				def_level, rep_level, col_reader, batch_size,
			),
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

// `Option<T>` corresponds to Parquet fields marked as "optional".
impl<T> ParquetData for Option<T>
where
	T: ParquetData,
{
	type Schema = OptionSchema<T::Schema>;
	type Reader = OptionReader<T::Reader>;
	type Predicate = T::Predicate;

	fn parse(
		schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		if repetition == Some(Repetition::Optional) {
			return T::parse(&schema, predicate, Some(Repetition::Required))
				.map(|(name, schema)| (name, OptionSchema(schema)));
		}
		Err(ParquetError::General(String::from(
			"Couldn't parse Option<T>",
		)))
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		OptionReader {
			reader: <T as ParquetData>::reader(
				&schema.0,
				path,
				def_level + 1,
				rep_level,
				paths,
				batch_size,
			),
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

impl<T> ParquetData for Root<T>
where
	T: ParquetData,
{
	type Schema = RootSchema<T>;
	type Reader = RootReader<T::Reader>;
	type Predicate = T::Predicate;

	fn parse(
		schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		assert!(repetition.is_none());
		if schema.is_schema() {
			T::parse(schema, predicate, Some(Repetition::Required))
				.map(|(name, schema_)| (String::from(""), RootSchema(name, schema_, PhantomData)))
				.map_err(|err| {
					let actual_schema = Value::parse(schema, None, Some(Repetition::Required))
						.map(|(name, schema_)| RootSchema(name, schema_, PhantomData));
					let actual_schema = match actual_schema {
						Ok(actual_schema) => actual_schema,
						Err(err) => return err,
					};
					let actual_schema = DisplayFmt::new(|fmt| {
						<<Root<Value> as ParquetData>::Schema>::fmt(
							Some(&actual_schema),
							None,
							None,
							fmt,
						)
					});
					let schema_ = DisplayFmt::new(|fmt| {
						<<Root<T> as ParquetData>::Schema>::fmt(None, None, None, fmt)
					});
					ParquetError::General(format!(
						"Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
						actual_schema,
						schema_,
						err
					))
				})
				.map(|(name, schema_)| {
					#[cfg(debug_assertions)]
					{
						use crate::internal::schema::parser::parse_message_type;

						// Check parsing and printing by round-tripping both typed and untyped and checking correctness.
						// TODO: do with predicates also

						let printed = format!("{}", schema_);
						let schema_2 = parse_message_type(&printed).unwrap();

						let schema2 = T::parse(&schema_2, None, Some(Repetition::Required))
							.map(|(name, schema_)| RootSchema::<T>(name, schema_, PhantomData))
							.unwrap();
						let printed2 = format!("{}", schema2);
						assert_eq!(printed, printed2, "{:#?}", schema);

						let schema3 = Value::parse(&schema_2, None, Some(Repetition::Required))
							.map(|(name, schema_)| RootSchema::<Value>(name, schema_, PhantomData))
							.unwrap();
						let printed3 = format!("{}", schema3);
						assert_eq!(printed, printed3, "{:#?}", schema);
					}
					(name, schema_)
				})
		} else {
			Err(ParquetError::General(format!(
				"Not a valid root schema {:?}",
				schema
			)))
		}
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		RootReader(T::reader(
			&schema.1, path, def_level, rep_level, paths, batch_size,
		))
	}
}

////////////////////////////////////////////////////////////////////////////////

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const SECONDS_PER_DAY: u64 = 86_400;
const MILLIS_PER_SECOND: u64 = 1_000;
const MICROS_PER_MILLI: u64 = 1_000;
const NANOS_PER_MICRO: u64 = 1_000;

via_string!(
	"Corresponds to string `%:z OR name`" Timezone
	"Corresponds to string `%Y-%m-%d`" DateWithoutTimezone
	"Corresponds to RFC 3339 and ISO 8601 string `%H:%M:%S%.9f%:z`" TimeWithoutTimezone
	"Corresponds to RFC 3339 and ISO 8601 string `%Y-%m-%dT%H:%M:%S%.9f%:z`" DateTimeWithoutTimezone
);

fn date_from_parquet(days: i32) -> Result<Date> {
	Ok(Date::from_days(i64::from(days), Timezone::UTC).unwrap())
}

/// Corresponds to the UTC [Date logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date).
impl ParquetData for Date {
	type Schema = DateSchema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		_schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		MapReader(
			i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
			|days| date_from_parquet(days),
		)
	}
}

fn time_from_parquet(time: Sum2<i64, i32>) -> Result<Time> {
	match time {
		Sum2::A(micros) => {
			let err = || ParquetError::General(format!("Invalid Time Micros {}", micros));
			let micros: u64 = micros.try_into().ok().ok_or_else(err)?;
			let divisor = MICROS_PER_MILLI * MILLIS_PER_SECOND;
			let seconds = (micros / divisor).try_into().ok().ok_or_else(err)?;
			let nanos = u32::try_from(micros % divisor).unwrap() * NANOS_PER_MICRO as u32;
			Time::from_seconds(seconds, nanos, Timezone::UTC).ok_or_else(err)
		}
		Sum2::B(millis) => {
			let err = || ParquetError::General(format!("Invalid Time Millis {}", millis));
			let millis: u32 = millis.try_into().ok().ok_or_else(err)?;
			let divisor = MILLIS_PER_SECOND as u32;
			let seconds = (millis / divisor).try_into().ok().ok_or_else(err)?;
			let nanos = (millis % divisor) * (MICROS_PER_MILLI * NANOS_PER_MICRO) as u32;
			Time::from_seconds(seconds, nanos, Timezone::UTC).ok_or_else(err)
		}
	}
}

/// Corresponds to the UTC [Time logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time).
impl ParquetData for Time {
	type Schema = TimeSchema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		match schema {
			TimeSchema::Micros => Sum2::A(MapReader(
				i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
				|micros: i64| time_from_parquet(Sum2::A(micros)),
			)),
			TimeSchema::Millis => Sum2::B(MapReader(
				i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
				|millis: i32| time_from_parquet(Sum2::B(millis)),
			)),
		}
	}
}

fn date_time_from_parquet(date_time: Sum3<Int96, i64, i64>) -> Result<DateTime> {
	match date_time {
		Sum3::A(date_time) => {
			let mut day = i64::from(date_time.data()[2]);
			let nanoseconds =
				(i64::from(date_time.data()[1]) << 32) + i64::from(date_time.data()[0]);
			let nanos_per_second = NANOS_PER_MICRO * MICROS_PER_MILLI * MILLIS_PER_SECOND;
			let nanos_per_day = (nanos_per_second * SECONDS_PER_DAY) as i64;
			day += nanoseconds.div_euclid(nanos_per_day);
			let nanoseconds: u64 = nanoseconds.rem_euclid(nanos_per_day).try_into().unwrap();
			let date =
				Date::from_days(day.checked_sub(JULIAN_DAY_OF_EPOCH).unwrap(), Timezone::UTC)
					.unwrap();
			let time = Time::from_seconds(
				(nanoseconds / nanos_per_second).try_into().unwrap(),
				(nanoseconds % nanos_per_second).try_into().unwrap(),
				Timezone::UTC,
			)
			.unwrap();
			Ok(DateTime::from_date_time(date, time).unwrap())
		}
		Sum3::B(millis) => {
			let millis_per_day = (MILLIS_PER_SECOND * SECONDS_PER_DAY) as i64;
			let days = millis.div_euclid(millis_per_day);
			let millis: u32 = millis.rem_euclid(millis_per_day).try_into().unwrap();
			let date = Date::from_days(days, Timezone::UTC).unwrap();
			let time = Time::from_seconds(
				(millis / MILLIS_PER_SECOND as u32).try_into().unwrap(),
				(millis % MILLIS_PER_SECOND as u32).try_into().unwrap(),
				Timezone::UTC,
			)
			.unwrap();
			Ok(DateTime::from_date_time(date, time).unwrap())
		}
		Sum3::C(micros) => {
			let micros_per_day = (MICROS_PER_MILLI * MILLIS_PER_SECOND * SECONDS_PER_DAY) as i64;
			let micros_per_second = (MICROS_PER_MILLI * MILLIS_PER_SECOND) as u64;
			let days = micros.div_euclid(micros_per_day);
			let micros: u64 = micros.rem_euclid(micros_per_day).try_into().unwrap();
			let date = Date::from_days(days, Timezone::UTC).unwrap();
			let time = Time::from_seconds(
				(micros / micros_per_second).try_into().unwrap(),
				(micros % micros_per_second).try_into().unwrap(),
				Timezone::UTC,
			)
			.unwrap();
			Ok(DateTime::from_date_time(date, time).unwrap())
		}
	}
}

/// Corresponds to the UTC [DateTime logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp).
impl ParquetData for DateTime {
	type Schema = DateTimeSchema;
	type Reader = impl Reader<Item = Self>;
	type Predicate = Predicate;

	fn parse(
		schema: &Type, _predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		Value::parse(schema, None, repetition).and_then(downcast)
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		match schema {
			DateTimeSchema::Int96 => Sum3::A(MapReader(
				{
					let col_path = ColumnPath::new(path.to_vec());
					let col_reader = paths.remove(&col_path).unwrap();
					I96Reader {
						column: TypedTripletIter::<Int96Type>::new(
							def_level, rep_level, col_reader, batch_size,
						),
					}
				},
				|date_time: Int96| date_time_from_parquet(Sum3::A(date_time)),
			)),
			DateTimeSchema::Millis => Sum3::B(MapReader(
				i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
				|millis: i64| date_time_from_parquet(Sum3::B(millis)),
			)),
			DateTimeSchema::Micros => Sum3::C(MapReader(
				i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
				|micros: i64| date_time_from_parquet(Sum3::C(micros)),
			)),
		}
	}
}

// impl From<chrono::Date<Utc>> for Date {
// 	fn from(date: chrono::Date<Utc>) -> Self {
// 		Date(
// 			(date
// 				.and_time(NaiveTime::from_hms(0, 0, 0))
// 				.unwrap()
// 				.timestamp() / SECONDS_PER_DAY)
// 				.try_into()
// 				.unwrap(),
// 		)
// 	}
// }
// impl From<Date> for chrono::Date<Utc> {
// 	fn from(date: Date) -> Self {
// 		let x = Utc.timestamp(i64::from(date.0) * SECONDS_PER_DAY, 0);
// 		assert_eq!(x.time(), NaiveTime::from_hms(0, 0, 0));
// 		x.date()
// 	}
// }
// impl Display for Date {
// 	// Input is a number of days since the epoch in UTC.
// 	// Date is displayed in local timezone.
// 	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
// 		let dt = Local
// 			.timestamp(i64::from(self.0) * SECONDS_PER_DAY, 0)
// 			.date();
// 		dt.format("%Y-%m-%d %:z").fmt(f)
// 	}
// }
// #[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
// pub struct Time(pub(super) u64);
// impl Time {
// 	/// Create a Time from the number of milliseconds since midnight
// 	pub fn from_millis(millis: u32) -> Option<Self> {
// 		if millis < (SECONDS_PER_DAY * MILLIS_PER_SECOND) as u32 {
// 			Some(Time(millis as u64 * MICROS_PER_MILLI as u64))
// 		} else {
// 			None
// 		}
// 	}
// 	/// Create a Time from the number of microseconds since midnight
// 	pub fn from_micros(micros: u64) -> Option<Self> {
// 		if micros < (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI) as u64 {
// 			Some(Time(micros as u64))
// 		} else {
// 			None
// 		}
// 	}
// 	/// Get the number of milliseconds since midnight
// 	pub fn as_millis(&self) -> u32 {
// 		(self.0 / MICROS_PER_MILLI as u64) as u32
// 	}
// 	/// Get the number of microseconds since midnight
// 	pub fn as_micros(&self) -> u64 {
// 		self.0
// 	}
// }
// impl From<NaiveTime> for Time {
// 	fn from(time: NaiveTime) -> Self {
// 		Time(
// 			time.num_seconds_from_midnight() as u64 * (MILLIS_PER_SECOND * MICROS_PER_MILLI) as u64
// 				+ time.nanosecond() as u64 / NANOS_PER_MICRO as u64,
// 		)
// 	}
// }
// impl From<Time> for NaiveTime {
// 	fn from(time: Time) -> Self {
// 		NaiveTime::from_num_seconds_from_midnight(
// 			(time.0 / (MICROS_PER_MILLI * MILLIS_PER_SECOND) as u64) as u32,
// 			(time.0 % (MICROS_PER_MILLI * MILLIS_PER_SECOND) as u64 * NANOS_PER_MICRO as u64)
// 				as u32,
// 		)
// 	}
// }
// impl Display for Time {
// 	// Input is a number of microseconds since midnight.
// 	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
// 		let dt = NaiveTime::from_num_seconds_from_midnight(
// 			(self.0 as i64 / MICROS_PER_MILLI / MILLIS_PER_SECOND)
// 				.try_into()
// 				.unwrap(),
// 			(self.0 as i64 % MICROS_PER_MILLI / MILLIS_PER_SECOND)
// 				.try_into()
// 				.unwrap(),
// 		);
// 		dt.format("%H:%M:%S%.9f").fmt(f)
// 	}
// }

// #[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
// pub struct DateTime(pub(super) Int96);
// impl DateTime {
// 	/// Create a DateTime from the number of days and nanoseconds since the Julian epoch
// 	pub fn from_day_nanos(days: i64, nanos: i64) -> Self {
// 		DateTime(Int96::new(
// 			(nanos & 0xffffffff).try_into().unwrap(),
// 			((nanos as u64) >> 32).try_into().unwrap(),
// 			days.try_into().unwrap(),
// 		))
// 	}

// 	/// Create a DateTime from the number of milliseconds since the Unix epoch
// 	pub fn from_millis(millis: i64) -> Self {
// 		let day: i64 = JULIAN_DAY_OF_EPOCH + millis / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
// 		let nanoseconds: i64 = (millis
// 			- ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND))
// 			* MICROS_PER_MILLI
// 			* NANOS_PER_MICRO;

// 		DateTime(Int96::new(
// 			(nanoseconds & 0xffffffff).try_into().unwrap(),
// 			((nanoseconds as u64) >> 32).try_into().unwrap(),
// 			day.try_into().unwrap(),
// 		))
// 	}

// 	/// Create a DateTime from the number of microseconds since the Unix epoch
// 	pub fn from_micros(micros: i64) -> Self {
// 		let day: i64 =
// 			JULIAN_DAY_OF_EPOCH + micros / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
// 		let nanoseconds: i64 = (micros
// 			- ((day - JULIAN_DAY_OF_EPOCH)
// 				* SECONDS_PER_DAY
// 				* MILLIS_PER_SECOND
// 				* MICROS_PER_MILLI))
// 			* NANOS_PER_MICRO;

// 		DateTime(Int96::new(
// 			(nanoseconds & 0xffffffff).try_into().unwrap(),
// 			((nanoseconds as u64) >> 32).try_into().unwrap(),
// 			day.try_into().unwrap(),
// 		))
// 	}

// 	/// Create a DateTime from the number of nanoseconds since the Unix epoch
// 	pub fn from_nanos(nanos: i64) -> Self {
// 		let day: i64 = JULIAN_DAY_OF_EPOCH
// 			+ nanos / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO);
// 		let nanoseconds: i64 =
// 			nanos
// 				- ((day - JULIAN_DAY_OF_EPOCH)
// 					* SECONDS_PER_DAY * MILLIS_PER_SECOND
// 					* MICROS_PER_MILLI * NANOS_PER_MICRO);

// 		DateTime(Int96::new(
// 			(nanoseconds & 0xffffffff).try_into().unwrap(),
// 			((nanoseconds as u64) >> 32).try_into().unwrap(),
// 			day.try_into().unwrap(),
// 		))
// 	}

// 	/// Get the number of days and nanoseconds since the Julian epoch
// 	pub fn as_day_nanos(&self) -> (i64, i64) {
// 		let day = i64::from(self.0.data()[2]);
// 		let nanoseconds = (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
// 		(day, nanoseconds)
// 	}

// 	/// Get the number of milliseconds since the Unix epoch
// 	pub fn as_millis(&self) -> Option<i64> {
// 		let day = i64::from(self.0.data()[2]);
// 		let nanoseconds = (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
// 		let seconds = day
// 			.checked_sub(JULIAN_DAY_OF_EPOCH)?
// 			.checked_mul(SECONDS_PER_DAY)?;
// 		Some(
// 			seconds.checked_mul(MILLIS_PER_SECOND)?.checked_add(
// 				nanoseconds
// 					.checked_div(NANOS_PER_MICRO)?
// 					.checked_div(MICROS_PER_MILLI)?,
// 			)?,
// 		)
// 	}

// 	/// Get the number of microseconds since the Unix epoch
// 	pub fn as_micros(&self) -> Option<i64> {
// 		let day = i64::from(self.0.data()[2]);
// 		let nanoseconds = (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
// 		let seconds = day
// 			.checked_sub(JULIAN_DAY_OF_EPOCH)?
// 			.checked_mul(SECONDS_PER_DAY)?;
// 		Some(
// 			seconds
// 				.checked_mul(MILLIS_PER_SECOND)?
// 				.checked_mul(MICROS_PER_MILLI)?
// 				.checked_add(nanoseconds.checked_div(NANOS_PER_MICRO)?)?,
// 		)
// 	}

// 	/// Get the number of nanoseconds since the Unix epoch
// 	pub fn as_nanos(&self) -> Option<i64> {
// 		let day = i64::from(self.0.data()[2]);
// 		let nanoseconds = (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
// 		let seconds = day
// 			.checked_sub(JULIAN_DAY_OF_EPOCH)?
// 			.checked_mul(SECONDS_PER_DAY)?;
// 		Some(
// 			seconds
// 				.checked_mul(MILLIS_PER_SECOND)?
// 				.checked_mul(MICROS_PER_MILLI)?
// 				.checked_mul(NANOS_PER_MICRO)?
// 				.checked_add(nanoseconds)?,
// 		)
// 	}
// }
// impl From<chrono::DateTime<Utc>> for DateTime {
// 	fn from(timestamp: chrono::DateTime<Utc>) -> Self {
// 		DateTime::from_nanos(timestamp.timestamp_nanos())
// 	}
// }
// impl TryFrom<DateTime> for chrono::DateTime<Utc> {
// 	type Error = ();

// 	fn try_from(timestamp: DateTime) -> result::Result<Self, Self::Error> {
// 		Ok(Utc.timestamp(
// 			timestamp.as_millis().unwrap() / MILLIS_PER_SECOND,
// 			(timestamp.as_day_nanos().1 % (MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO))
// 				as u32,
// 		))
// 	}
// }
// impl Display for DateTime {
// 	// Datetime is displayed in local timezone.
// 	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
// 		let dt = Local.timestamp(self.as_millis().unwrap() / MILLIS_PER_SECOND, 0);
// 		dt.format("%Y-%m-%d %H:%M:%S%.9f %:z").fmt(f)
// 	}
// }

#[cfg(test)]
mod tests {
	use super::*;

	use amadeus_types::Timezone;
	use chrono::{offset::TimeZone, NaiveDate, NaiveTime, Utc};

	#[test]
	fn test_int96() {
		let value = date_time_from_parquet(Sum3::A(Int96::new(0, 0, 2454923))).unwrap();
		assert_eq!(value.as_chrono().unwrap().timestamp_millis(), 1238544000000);

		let value = date_time_from_parquet(Sum3::A(Int96::new(4165425152, 13, 2454923))).unwrap();
		assert_eq!(value.as_chrono().unwrap().timestamp_millis(), 1238544060000);

		let value = date_time_from_parquet(Sum3::A(Int96::new(0, 0, 0))).unwrap();
		assert_eq!(
			value.as_chrono().unwrap().timestamp_millis(),
			-210866803200000
		);
	}

	#[test]
	fn test_convert_date_to_string() {
		fn check_date_conversion(y: i32, m: u32, d: u32) {
			let datetime = NaiveDate::from_ymd(y, m, d).and_hms(0, 0, 0);
			let dt = Utc.from_utc_datetime(&datetime);
			let date = date_from_parquet((dt.timestamp() / SECONDS_PER_DAY as i64) as i32).unwrap();
			assert_eq!(date.to_string(), dt.format("%Y-%m-%d %:z").to_string());
		}

		check_date_conversion(2010, 01, 02);
		check_date_conversion(2014, 05, 01);
		check_date_conversion(2016, 02, 29);
		check_date_conversion(2017, 09, 12);
		check_date_conversion(2018, 03, 31);
	}

	#[test]
	fn test_convert_time_to_string() {
		fn check_time_conversion(h: u32, mi: u32, s: u32) {
			let chrono_time = NaiveTime::from_hms(h, mi, s);
			let time = TimeWithoutTimezone::from_chrono(&chrono_time).with_timezone(Timezone::UTC);
			assert_eq!(
				time.to_string(),
				format!("{}+00:00", chrono_time.format("%H:%M:%S%.9f").to_string())
			);
		}

		check_time_conversion(13, 12, 54);
		check_time_conversion(08, 23, 01);
		check_time_conversion(11, 06, 32);
		check_time_conversion(16, 38, 00);
		check_time_conversion(21, 15, 12);
	}

	#[test]
	fn test_convert_timestamp_to_string() {
		#[allow(clippy::many_single_char_names)]
		fn check_datetime_conversion(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) {
			let datetime = NaiveDate::from_ymd(y, m, d).and_hms(h, mi, s);
			let dt = Utc.from_utc_datetime(&datetime);
			let res = DateTime::from_chrono(&dt).to_string();
			let exp = dt.format("%Y-%m-%d %H:%M:%S%.9f %:z").to_string();
			assert_eq!(res, exp);
		}

		check_datetime_conversion(2010, 01, 02, 13, 12, 54);
		check_datetime_conversion(2011, 01, 03, 08, 23, 01);
		check_datetime_conversion(2012, 04, 05, 11, 06, 32);
		check_datetime_conversion(2013, 05, 12, 16, 38, 00);
		check_datetime_conversion(2014, 11, 28, 21, 15, 12);
	}
}

////////////////////////////////////////////////////////////////////////////////

/// Macro to implement [`Reader`] on tuples up to length 12.
macro_rules! tuple {
	($len:tt $($t:ident $i:tt)*) => (
		// Tuples correspond to Parquet groups with an equal number of fields with corresponding types.
		impl<$($t,)*> Reader for TupleReader<($($t,)*)> where $($t: Reader,)* {
			type Item = ($($t::Item,)*);

			#[allow(unused_variables, non_snake_case)]
			fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item> {
				$(
					let $t = (self.0).$i.read(def_level, rep_level);
				)*
				if $($t.is_err() ||)* false { // TODO: unlikely
					$(let _ = $t?;)*
					unreachable!()
				}
				Ok((
					$($t.unwrap(),)*
				))
			}
			fn advance_columns(&mut self) -> Result<()> {
				#[allow(unused_mut)]
				let mut res = Ok(());
				$(
					res = res.and((self.0).$i.advance_columns());
				)*
				res
			}
			#[inline]
			fn has_next(&self) -> bool {
				$(if true { (self.0).$i.has_next() } else)*
				{
					true
				}
			}
			#[inline]
			fn current_def_level(&self) -> i16 {
				$(if true { (self.0).$i.current_def_level() } else)*
				{
					panic!("Current definition level: empty group reader")
				}
			}
			#[inline]
			fn current_rep_level(&self) -> i16 {
				$(if true { (self.0).$i.current_rep_level() } else)*
				{
					panic!("Current repetition level: empty group reader")
				}
			}
		}
		impl<$($t,)*> Default for TupleSchema<($((String,$t,),)*)> where $($t: Default,)* {
			fn default() -> Self {
				Self(($((format!("field_{}", $i), Default::default()),)*))
			}
		}
		impl<$($t,)*> fmt::Debug for TupleSchema<($((String,$t,),)*)> where $($t: fmt::Debug,)* {
			fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
				f.debug_tuple("TupleSchema")
					$(.field(&(self.0).$i))*
					.finish()
			}
		}
		impl<$($t,)*> Schema for TupleSchema<($((String,$t,),)*)> where $($t: Schema,)* {
			#[allow(unused_variables)]
			fn fmt(self_: Option<&Self>, r: Option<Repetition>, name: Option<&str>, f: &mut fmt::Formatter) -> fmt::Result {
				DisplaySchemaGroup::new(r, name, None, f)
					$(.field(self_.map(|self_|&*(self_.0).$i.0), self_.map(|self_|&(self_.0).$i.1)))*
					.finish()
			}
		}
		impl<$($t,)*> ParquetData for ($($t,)*) where $($t: ParquetData,)* {
			type Schema = TupleSchema<($((String,$t::Schema,),)*)>;
			type Reader = TupleReader<($($t::Reader,)*)>;
			type Predicate = ($(Option<$t::Predicate>,)*);

			#[allow(unused_variables)]
			fn parse(schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>) -> Result<(String, Self::Schema)> {
				if schema.is_group() && repetition == Some(Repetition::Required) {
					let mut fields = schema.get_fields().iter();
					let schema_ = TupleSchema(($(fields.next().ok_or_else(|| ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x, predicate.and_then(|predicate| predicate.$i.as_ref()), Some(x.get_basic_info().repetition())))?,)*));
					if fields.next().is_none() {
						return Ok((schema.name().to_owned(), schema_))
					}
				}
				Err(ParquetError::General(format!("Can't parse Tuple {:?}", schema)))
			}
			#[allow(unused_variables)]
			fn reader(schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16, paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize) -> Self::Reader {
				$(
					path.push((schema.0).$i.0.to_owned());
					#[allow(non_snake_case)]
					let $t = <$t as ParquetData>::reader(&(schema.0).$i.1, path, def_level, rep_level, paths, batch_size);
					let _ = path.pop().unwrap();
				)*
				TupleReader(($($t,)*))
			}
		}
		impl<$($t,)*> Downcast<TupleSchema<($((String,$t,),)*)>> for ValueSchema where ValueSchema: $(Downcast<$t> +)* {
			fn downcast(self) -> Result<TupleSchema<($((String,$t,),)*)>> {
				let group = self.into_group()?;
				#[allow(unused_mut,unused_variables)]
				let mut fields = group.0.into_iter();
				#[allow(unused_mut,unused_variables)]
				let mut names = group.1.into_iter().map(|(name,_index)|name);
				Ok(TupleSchema(($({let _ = $i;(names.next().unwrap(),fields.next().unwrap().downcast()?)},)*)))
			}
		}
	);
}
amadeus_types::tuple!(tuple);

////////////////////////////////////////////////////////////////////////////////

impl ParquetData for Value {
	type Schema = ValueSchema;
	type Reader = ValueReader;
	type Predicate = ValuePredicate;

	/// This is reused by many of the other `ParquetData` implementations. It is the canonical
	/// encoding of the mapping from [`Type`]s to Schemas.
	fn parse(
		schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)> {
		let mut value = None;

		// Try parsing as a primitive. See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md for details.
		if repetition.is_some() && schema.is_primitive() {
			value = Some(
				match (
					schema.get_physical_type(),
					schema.get_basic_info().logical_type(),
				) {
					(PhysicalType::Boolean, LogicalType::None) => ValueSchema::Bool(BoolSchema),
					(PhysicalType::Int32, LogicalType::Uint8) => ValueSchema::U8(U8Schema),
					(PhysicalType::Int32, LogicalType::Int8) => ValueSchema::I8(I8Schema),
					(PhysicalType::Int32, LogicalType::Uint16) => ValueSchema::U16(U16Schema),
					(PhysicalType::Int32, LogicalType::Int16) => ValueSchema::I16(I16Schema),
					(PhysicalType::Int32, LogicalType::Uint32) => ValueSchema::U32(U32Schema),
					(PhysicalType::Int32, LogicalType::Int32)
					| (PhysicalType::Int32, LogicalType::None) => ValueSchema::I32(I32Schema),
					(PhysicalType::Int32, LogicalType::Date) => ValueSchema::Date(DateSchema),
					(PhysicalType::Int32, LogicalType::TimeMillis) => {
						ValueSchema::Time(TimeSchema::Millis)
					}
					(PhysicalType::Int32, LogicalType::Decimal) => {
						let (precision, scale) = (schema.get_precision(), schema.get_scale());
						let (precision, scale) =
							(precision.try_into().unwrap(), scale.try_into().unwrap());
						ValueSchema::Decimal(DecimalSchema::Int32 { precision, scale })
					}
					(PhysicalType::Int64, LogicalType::Uint64) => ValueSchema::U64(U64Schema),
					(PhysicalType::Int64, LogicalType::Int64)
					| (PhysicalType::Int64, LogicalType::None) => ValueSchema::I64(I64Schema),
					(PhysicalType::Int64, LogicalType::TimeMicros) => {
						ValueSchema::Time(TimeSchema::Micros)
					}
					(PhysicalType::Int64, LogicalType::TimestampMillis) => {
						ValueSchema::DateTime(DateTimeSchema::Millis)
					}
					(PhysicalType::Int64, LogicalType::TimestampMicros) => {
						ValueSchema::DateTime(DateTimeSchema::Micros)
					}
					(PhysicalType::Int64, LogicalType::Decimal) => {
						let (precision, scale) = (schema.get_precision(), schema.get_scale());
						let (precision, scale) =
							(precision.try_into().unwrap(), scale.try_into().unwrap());
						ValueSchema::Decimal(DecimalSchema::Int64 { precision, scale })
					}
					(PhysicalType::Int96, LogicalType::None) => {
						ValueSchema::DateTime(DateTimeSchema::Int96)
					}
					(PhysicalType::Float, LogicalType::None) => ValueSchema::F32(F32Schema),
					(PhysicalType::Double, LogicalType::None) => ValueSchema::F64(F64Schema),
					(PhysicalType::ByteArray, LogicalType::Utf8)
					| (PhysicalType::FixedLenByteArray, LogicalType::Utf8) => {
						ValueSchema::String(StringSchema(ByteArraySchema(
							if schema.get_physical_type() == PhysicalType::FixedLenByteArray {
								Some(schema.get_type_length().try_into().unwrap())
							} else {
								None
							},
						)))
					}
					(PhysicalType::ByteArray, LogicalType::Json)
					| (PhysicalType::FixedLenByteArray, LogicalType::Json) => {
						ValueSchema::Json(JsonSchema(StringSchema(ByteArraySchema(
							if schema.get_physical_type() == PhysicalType::FixedLenByteArray {
								Some(schema.get_type_length().try_into().unwrap())
							} else {
								None
							},
						))))
					}
					(PhysicalType::ByteArray, LogicalType::Enum)
					| (PhysicalType::FixedLenByteArray, LogicalType::Enum) => {
						ValueSchema::Enum(EnumSchema(StringSchema(ByteArraySchema(
							if schema.get_physical_type() == PhysicalType::FixedLenByteArray {
								Some(schema.get_type_length().try_into().unwrap())
							} else {
								None
							},
						))))
					}
					(PhysicalType::ByteArray, LogicalType::None)
					| (PhysicalType::FixedLenByteArray, LogicalType::None) => {
						ValueSchema::ByteArray(ByteArraySchema(
							if schema.get_physical_type() == PhysicalType::FixedLenByteArray {
								Some(schema.get_type_length().try_into().unwrap())
							} else {
								None
							},
						))
					}
					(PhysicalType::ByteArray, LogicalType::Bson)
					| (PhysicalType::FixedLenByteArray, LogicalType::Bson) => {
						ValueSchema::Bson(BsonSchema(ByteArraySchema(
							if schema.get_physical_type() == PhysicalType::FixedLenByteArray {
								Some(schema.get_type_length().try_into().unwrap())
							} else {
								None
							},
						)))
					}
					(PhysicalType::ByteArray, LogicalType::Decimal)
					| (PhysicalType::FixedLenByteArray, LogicalType::Decimal) => {
						let byte_array_schema = ByteArraySchema(
							if schema.get_physical_type() == PhysicalType::FixedLenByteArray {
								Some(schema.get_type_length().try_into().unwrap())
							} else {
								None
							},
						);
						let (precision, scale) = (schema.get_precision(), schema.get_scale());
						let (precision, scale) =
							(precision.try_into().unwrap(), scale.try_into().unwrap());
						ValueSchema::Decimal(DecimalSchema::Array {
							byte_array_schema,
							precision,
							scale,
						})
					}
					(PhysicalType::ByteArray, LogicalType::Interval)
					| (PhysicalType::FixedLenByteArray, LogicalType::Interval) => {
						unimplemented!("Interval logical type not yet implemented")
					}

					// Fallbacks for unrecognised LogicalType
					(PhysicalType::Boolean, _) => ValueSchema::Bool(BoolSchema),
					(PhysicalType::Int32, _) => ValueSchema::I32(I32Schema),
					(PhysicalType::Int64, _) => ValueSchema::I64(I64Schema),
					(PhysicalType::Int96, _) => ValueSchema::DateTime(DateTimeSchema::Int96),
					(PhysicalType::Float, _) => ValueSchema::F32(F32Schema),
					(PhysicalType::Double, _) => ValueSchema::F64(F64Schema),
					(PhysicalType::ByteArray, _) | (PhysicalType::FixedLenByteArray, _) => {
						ValueSchema::ByteArray(ByteArraySchema(
							if schema.get_physical_type() == PhysicalType::FixedLenByteArray {
								Some(schema.get_type_length().try_into().unwrap())
							} else {
								None
							},
						))
					}
				},
			);
		}

		// Try parsing as a list (excluding unannotated repeated fields)
		if repetition.is_some()
			&& value.is_none()
			&& predicate
				.map(|predicate| predicate.is_list())
				.unwrap_or(true)
		{
			value = parse_list::<Value>(
				schema,
				predicate.and_then(|predicate| predicate.as_list().unwrap().as_ref()),
			)
			.ok()
			.map(|value| ValueSchema::List(Box::new(value)));
		}

		// Try parsing as a map
		if repetition.is_some()
			&& value.is_none()
			&& predicate
				.map(|predicate| predicate.is_map())
				.unwrap_or(true)
		{
			let predicate = predicate.and_then(|predicate| predicate.as_map().unwrap().as_ref());
			value = HashMap::<Value, Value>::parse(schema, predicate, Some(Repetition::Required))
				.ok()
				.map(|(_, value)| ValueSchema::Map(Box::new(value)));
		}

		// Try parsing as a group
		if repetition.is_some()
			&& value.is_none()
			&& schema.is_group()
			&& predicate
				.map(|predicate| predicate.is_group())
				.unwrap_or(true)
		{
			let predicate = predicate.and_then(|predicate| predicate.as_group().unwrap().as_ref());
			value = Some(ValueSchema::Group(
				Group::parse(schema, predicate, Some(Repetition::Required))?.1,
			));
		}

		// If we haven't parsed it by now it's not valid
		let mut value = value
			.ok_or_else(|| ParquetError::General(format!("Can't parse value {:?}", schema)))?;

		// Account for the repetition level
		match repetition.unwrap() {
			Repetition::Optional => {
				value = ValueSchema::Option(Box::new(OptionSchema(value)));
			}
			Repetition::Repeated => {
				value = ValueSchema::List(Box::new(ListSchema(value, ListSchemaType::Repeated)));
			}
			Repetition::Required => (),
		}

		Ok((schema.name().to_owned(), value))
	}

	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader {
		// Map the ValueSchema to the corresponding ValueReader
		match *schema {
			ValueSchema::Bool(ref schema) => ValueReader::Bool(<bool as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::U8(ref schema) => ValueReader::U8(<u8 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::I8(ref schema) => ValueReader::I8(<i8 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::U16(ref schema) => ValueReader::U16(<u16 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::I16(ref schema) => ValueReader::I16(<i16 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::U32(ref schema) => ValueReader::U32(<u32 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::I32(ref schema) => ValueReader::I32(<i32 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::U64(ref schema) => ValueReader::U64(<u64 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::I64(ref schema) => ValueReader::I64(<i64 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::F32(ref schema) => ValueReader::F32(<f32 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::F64(ref schema) => ValueReader::F64(<f64 as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::Date(ref schema) => ValueReader::Date(<Date as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::Time(ref schema) => ValueReader::Time(<Time as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::DateTime(ref schema) => {
				ValueReader::DateTime(<DateTime as ParquetData>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				))
			}
			ValueSchema::Decimal(ref schema) => {
				ValueReader::Decimal(<Decimal as ParquetData>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				))
			}
			ValueSchema::ByteArray(ref schema) => ValueReader::ByteArray(byte_array_reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::Bson(ref schema) => ValueReader::Bson(<Bson as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::String(ref schema) => {
				ValueReader::String(<String as ParquetData>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				))
			}
			ValueSchema::Json(ref schema) => ValueReader::Json(<Json as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::Enum(ref schema) => ValueReader::Enum(<Enum as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::List(ref schema) => {
				ValueReader::List(Box::new(<List<Value> as ParquetData>::reader(
					type_coerce(&**schema),
					path,
					def_level,
					rep_level,
					paths,
					batch_size,
				)))
			}
			ValueSchema::Map(ref schema) => {
				ValueReader::Map(Box::new(<HashMap<Value, Value> as ParquetData>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				)))
			}
			ValueSchema::Group(ref schema) => ValueReader::Group(<Group as ParquetData>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			)),
			ValueSchema::Option(ref schema) => {
				ValueReader::Option(Box::new(<Option<Value> as ParquetData>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				)))
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

via_string!(
	"" Webpage<'static>
	"" Url
	"" IpAddr
);

////////////////////////////////////////////////////////////////////////////////

impl From<amadeus_types::ParseDateError> for ParquetError {
	fn from(err: amadeus_types::ParseDateError) -> Self {
		ParquetError::General(err.to_string())
	}
}
impl From<amadeus_types::ParseAddrError> for ParquetError {
	fn from(err: amadeus_types::ParseAddrError) -> Self {
		ParquetError::General(err.to_string())
	}
}
impl From<amadeus_types::ParseUrlError> for ParquetError {
	fn from(err: amadeus_types::ParseUrlError) -> Self {
		ParquetError::General(err.to_string())
	}
}
impl From<amadeus_types::ParseWebpageError> for ParquetError {
	fn from(err: amadeus_types::ParseWebpageError) -> Self {
		ParquetError::General(err.to_string())
	}
}

////////////////////////////////////////////////////////////////////////////////

fn type_coerce<A, B>(a: A) -> B {
	try_type_coerce(a)
		.unwrap_or_else(|| panic!("can't coerce {} to {}", type_name::<A>(), type_name::<B>()))
}
fn try_type_coerce<A, B>(a: A) -> Option<B> {
	trait Eq<B> {
		fn eq(self) -> Option<B>;
	}

	struct Foo<A, B>(A, PhantomData<fn() -> B>);

	impl<A, B> Eq<B> for Foo<A, B> {
		default fn eq(self) -> Option<B> {
			None
		}
	}
	impl<A> Eq<A> for Foo<A, A> {
		fn eq(self) -> Option<A> {
			Some(self.0)
		}
	}

	Foo::<A, B>(a, PhantomData).eq()
}
