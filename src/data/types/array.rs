//! Implement [`Record`] for `Vec<u8>` (byte_array/fixed_len_byte_array), [`Bson`] (bson),
//! `String` (utf8), [`Json`] (json), [`Enum`] (enum), and `[u8; N]`
//! (fixed_len_byte_array).

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
	collections::HashMap, error::Error, fmt::{self, Display}
};

use super::{super::Data, IntoReader, SchemaIncomplete};
use amadeus_parquet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, schema::types::{ColumnPath, Type}
};

// `Vec<u8>` corresponds to the `binary`/`byte_array` and `fixed_len_byte_array` physical
// types.
impl Data for Vec<u8> {
	type ParquetSchema = <Self as amadeus_parquet::record::Record>::Schema;
	type ParquetReader = <Self as amadeus_parquet::record::Record>::Reader;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn postgres_decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.serialize(serializer)
	}
	fn serde_deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::deserialize(deserializer)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<Self as amadeus_parquet::record::Record>::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		<Self as amadeus_parquet::record::Record>::reader(
			schema, path, def_level, rep_level, paths, batch_size,
		)
	}
}

/// A Rust type corresponding to the [Bson logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#bson).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
pub struct Bson(Vec<u8>);
impl Data for Bson {
	type ParquetSchema =
		<amadeus_parquet::record::types::Bson as amadeus_parquet::record::Record>::Schema;
	type ParquetReader = IntoReader<
		<amadeus_parquet::record::types::Bson as amadeus_parquet::record::Record>::Reader,
		Self,
	>;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn postgres_decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.serialize(serializer)
	}
	fn serde_deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::deserialize(deserializer)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<amadeus_parquet::record::types::Bson as amadeus_parquet::record::Record>::parse(
			schema, repetition,
		)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		IntoReader::new(
			<amadeus_parquet::record::types::Bson as amadeus_parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}
impl From<Bson> for amadeus_parquet::record::types::Bson {
	fn from(bson: Bson) -> Self {
		bson.0.into()
	}
}
impl From<amadeus_parquet::record::types::Bson> for Bson {
	fn from(bson: amadeus_parquet::record::types::Bson) -> Self {
		Self(bson.into())
	}
}
impl From<Bson> for Vec<u8> {
	fn from(json: Bson) -> Self {
		json.0
	}
}
impl From<Vec<u8>> for Bson {
	fn from(string: Vec<u8>) -> Self {
		Self(string)
	}
}

// `String` corresponds to the [UTF8/String logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string)

/// A Rust type corresponding to the [Json logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#json).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
pub struct Json(String);
impl Data for Json {
	type ParquetSchema =
		<amadeus_parquet::record::types::Json as amadeus_parquet::record::Record>::Schema;
	type ParquetReader = IntoReader<
		<amadeus_parquet::record::types::Json as amadeus_parquet::record::Record>::Reader,
		Self,
	>;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn postgres_decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.serialize(serializer)
	}
	fn serde_deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::deserialize(deserializer)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<amadeus_parquet::record::types::Json as amadeus_parquet::record::Record>::parse(
			schema, repetition,
		)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		IntoReader::new(
			<amadeus_parquet::record::types::Json as amadeus_parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}
impl From<Json> for amadeus_parquet::record::types::Json {
	fn from(json: Json) -> Self {
		json.0.into()
	}
}
impl From<amadeus_parquet::record::types::Json> for Json {
	fn from(json: amadeus_parquet::record::types::Json) -> Self {
		Self(json.into())
	}
}
impl Display for Json {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.0.fmt(f)
	}
}
impl From<Json> for String {
	fn from(json: Json) -> Self {
		json.0
	}
}
impl From<String> for Json {
	fn from(string: String) -> Self {
		Self(string)
	}
}

/// A Rust type corresponding to the [Enum logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#enum).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
pub struct Enum(String);
impl Data for Enum {
	type ParquetSchema =
		<amadeus_parquet::record::types::Enum as amadeus_parquet::record::Record>::Schema;
	type ParquetReader = IntoReader<
		<amadeus_parquet::record::types::Enum as amadeus_parquet::record::Record>::Reader,
		Self,
	>;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn postgres_decode(
		_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.serialize(serializer)
	}
	fn serde_deserialize<'de, D>(
		deserializer: D, _schema: Option<SchemaIncomplete>,
	) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::deserialize(deserializer)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<amadeus_parquet::record::types::Enum as amadeus_parquet::record::Record>::parse(
			schema, repetition,
		)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		IntoReader::new(
			<amadeus_parquet::record::types::Enum as amadeus_parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}
impl From<Enum> for amadeus_parquet::record::types::Enum {
	fn from(enum_: Enum) -> Self {
		enum_.0.into()
	}
}
impl From<amadeus_parquet::record::types::Enum> for Enum {
	fn from(enum_: amadeus_parquet::record::types::Enum) -> Self {
		Self(enum_.into())
	}
}
impl Display for Enum {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.0.fmt(f)
	}
}
impl From<Enum> for String {
	fn from(enum_: Enum) -> Self {
		enum_.0
	}
}
impl From<String> for Enum {
	fn from(string: String) -> Self {
		Self(string)
	}
}

macro_rules! impl_parquet_record_array {
	($i:tt) => {
		impl Data for [u8; $i] {
			type ParquetReader = <Self as amadeus_parquet::record::Record>::Reader;
			type ParquetSchema = <Self as amadeus_parquet::record::Record>::Schema;

			fn postgres_query(
				f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
			) -> fmt::Result {
				name.unwrap().fmt(f)
			}
			fn postgres_decode(
				_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
			) -> Result<Self, Box<dyn Error + Sync + Send>> {
				unimplemented!()
			}

			fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
			where
				S: Serializer,
			{
				self.serialize(serializer)
			}
			fn serde_deserialize<'de, D>(
				deserializer: D, _schema: Option<SchemaIncomplete>,
			) -> Result<Self, D::Error>
			where
				D: Deserializer<'de>,
			{
				Self::deserialize(deserializer)
			}

			fn parquet_parse(
				schema: &Type, repetition: Option<Repetition>,
			) -> Result<(String, Self::ParquetSchema), ParquetError> {
				<Self as amadeus_parquet::record::Record>::parse(schema, repetition)
			}

			fn parquet_reader(
				schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16,
				rep_level: i16, paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
			) -> Self::ParquetReader {
				<Self as amadeus_parquet::record::Record>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				)
			}
		}

		// Specialize the implementation to avoid passing a potentially large array around
		// on the stack.
		impl Data for Box<[u8; $i]> {
			type ParquetReader = <Self as amadeus_parquet::record::Record>::Reader;
			type ParquetSchema = <Self as amadeus_parquet::record::Record>::Schema;

			fn postgres_query(
				f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
			) -> fmt::Result {
				name.unwrap().fmt(f)
			}
			fn postgres_decode(
				_type_: &::postgres::types::Type, _buf: Option<&[u8]>,
			) -> Result<Self, Box<dyn Error + Sync + Send>> {
				unimplemented!()
			}

			fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
			where
				S: Serializer,
			{
				self.serialize(serializer)
			}
			fn serde_deserialize<'de, D>(
				deserializer: D, _schema: Option<SchemaIncomplete>,
			) -> Result<Self, D::Error>
			where
				D: Deserializer<'de>,
			{
				Self::deserialize(deserializer)
			}

			fn parquet_parse(
				schema: &Type, repetition: Option<Repetition>,
			) -> Result<(String, Self::ParquetSchema), ParquetError> {
				<Self as amadeus_parquet::record::Record>::parse(schema, repetition)
			}

			fn parquet_reader(
				schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16,
				rep_level: i16, paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
			) -> Self::ParquetReader {
				<Self as amadeus_parquet::record::Record>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				)
			}
		}
	};
}

// Implement Record for common array lengths, copied from arrayvec
impl_parquet_record_array!(0);
impl_parquet_record_array!(1);
impl_parquet_record_array!(2);
impl_parquet_record_array!(3);
impl_parquet_record_array!(4);
impl_parquet_record_array!(5);
impl_parquet_record_array!(6);
impl_parquet_record_array!(7);
impl_parquet_record_array!(8);
impl_parquet_record_array!(9);
impl_parquet_record_array!(10);
impl_parquet_record_array!(11);
impl_parquet_record_array!(12);
impl_parquet_record_array!(13);
impl_parquet_record_array!(14);
impl_parquet_record_array!(15);
impl_parquet_record_array!(16);
impl_parquet_record_array!(17);
impl_parquet_record_array!(18);
impl_parquet_record_array!(19);
impl_parquet_record_array!(20);
impl_parquet_record_array!(21);
impl_parquet_record_array!(22);
impl_parquet_record_array!(23);
impl_parquet_record_array!(24);
impl_parquet_record_array!(25);
impl_parquet_record_array!(26);
impl_parquet_record_array!(27);
impl_parquet_record_array!(28);
impl_parquet_record_array!(29);
impl_parquet_record_array!(30);
impl_parquet_record_array!(31);
impl_parquet_record_array!(32);
// impl_parquet_record_array!(40);
// impl_parquet_record_array!(48);
// impl_parquet_record_array!(50);
// impl_parquet_record_array!(56);
// impl_parquet_record_array!(64);
// impl_parquet_record_array!(72);
// impl_parquet_record_array!(96);
// impl_parquet_record_array!(100);
// impl_parquet_record_array!(128);
// impl_parquet_record_array!(160);
// impl_parquet_record_array!(192);
// impl_parquet_record_array!(200);
// impl_parquet_record_array!(224);
// impl_parquet_record_array!(256);
// impl_parquet_record_array!(384);
// impl_parquet_record_array!(512);
// impl_parquet_record_array!(768);
// impl_parquet_record_array!(1024);
// impl_parquet_record_array!(2048);
// impl_parquet_record_array!(4096);
// impl_parquet_record_array!(8192);
// impl_parquet_record_array!(16384);
// impl_parquet_record_array!(32768);
// impl_parquet_record_array!(65536);
