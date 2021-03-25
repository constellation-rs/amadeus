//! Contains record-based API for reading Parquet files.
//!
//! Example usage of reading data untyped:
//!
//! ```ignore
//! use std::fs::File;
//! use std::path::Path;
//! use amadeus_parquet::internal::file::reader::{FileReader, SerializedFileReader};
//! use amadeus_types::Group;
//!
//! let file = File::open(&Path::new("/path/to/file")).unwrap();
//! let reader = SerializedFileReader::new(file).unwrap();
//! let iter = reader.get_row_iter::<Group>(None).unwrap();
//! for record in iter.map(Result::unwrap) {
//!     println!("{:?}", record);
//! }
//! ```
//!
//! Example usage of reading data strongly-typed:
//!
//! ```ignore
//! use std::fs::File;
//! use std::path::Path;
//! use amadeus_parquet::internal::file::reader::{FileReader, SerializedFileReader};
//! use amadeus_parquet::internal::record::{ParquetData, types::Timestamp};
//!
//! #[derive(Data, Debug)]
//! struct MyRow {
//!     id: u64,
//!     time: Timestamp,
//!     event: String,
//! }
//!
//! let file = File::open(&Path::new("/path/to/file")).unwrap();
//! let reader = SerializedFileReader::new(file).unwrap();
//! let iter = reader.get_row_iter::<MyRow>(None).unwrap();
//! for record in iter.map(Result::unwrap) {
//!     println!("{}: {}", record.time, record.event);
//! }
//! ```

mod display;
mod impls;
pub mod predicates;
mod reader;
mod schemas;
mod triplet;
pub mod types;

use serde::{Deserialize, Serialize};
use std::{
	collections::HashMap, fmt::{self, Debug}
};

use amadeus_types::Data;

use crate::internal::{
	basic::Repetition, column::reader::ColumnReader, errors::Result, schema::types::{ColumnPath, Type}
};

/// This is used by `#[derive(Data)]`
pub use display::DisplaySchemaGroup;
pub use reader::RowIter;
pub use schemas::RootSchema;

mod predicate {
	/// This is for forward compatibility when Predicate pushdown and dynamic schemas are
	/// implemented.
	use serde::{Deserialize, Serialize};
	#[derive(Clone, Debug, Serialize, Deserialize)]
	pub struct Predicate;
}
pub(crate) use self::predicate::Predicate;

/// This trait is implemented on all types that can be read from/written to Parquet files.
///
/// It is implemented on the following types:
///
/// | Rust type | Parquet Physical Type | Parquet Logical Type |
/// |---|---|---|
/// | `bool` | boolean | none |
/// | `u8` | int32 | uint_8 |
/// | `i8` | int32 | int_8 |
/// | `u16` | int32 | uint_16 |
/// | `i16` | int32 | int_16 |
/// | `u32` | int32 | uint_32 |
/// | `i32` | int32 | int_32 |
/// | `u64` | int64 | uint_64 |
/// | `i64` | int64 | int_64 |
/// | `f32` | float | none |
/// | `f64` | double | none |
/// | [`Date`](self::types::Date) | int32 | [date](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date) |
/// | [`Time`](self::types::Time) | int32 | [time_millis](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) |
/// | [`Time`](self::types::Time) | int64 | [time_micros](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) |
/// | [`Timestamp`](self::types::Timestamp) | int64 | [timestamp_millis](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp) |
/// | [`Timestamp`](self::types::Timestamp) | int64 | [timestamp_micros](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp) |
/// | [`Timestamp`](self::types::Timestamp) | int96 | [none](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp) |
/// | [`Decimal`](crate::internal::data_type::Decimal) | int32 | [decimal](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) |
/// | [`Decimal`](crate::internal::data_type::Decimal) | int64 | [decimal](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) |
/// | [`Decimal`](crate::internal::data_type::Decimal) | byte_array | [decimal](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) |
/// | `Vec<u8>` | byte_array | none |
/// | `[u8; N]` | fixed_len_byte_array | none |
/// | [`Bson`](self::types::Bson) | byte_array | [bson](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#bson) |
/// | `String` | byte_array | [utf8](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string) |
/// | [`Json`](self::types::Json) | byte_array | [json](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#json) |
/// | [`Enum`](self::types::Enum) | byte_array | [enum](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#enum) |
/// | [`List<T>`](self::types::List) | group | [list](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists) |
/// | [`Map<K,V>`](self::types::Map) | group | [map](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps) |
/// | [`Group`](self::types::Group) | group | none |
/// | `(T, U, …)` | group | none |
/// | `Option<T>` | – | – |
/// | [`Value`](self::types::Value) | * | * |
///
/// `Option<T>` corresponds to a field marked as "optional".
///
/// [`List<T>`](self::types::List) corresponds to either [annotated List logical types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists), or unannotated fields marked as "repeated".
///
/// [`Value`](self::types::Value) corresponds to any valid Parquet type, and is useful
/// when the type is not known at compile time.
///
/// "byte_array" is interchangeable with "fixed_len_byte_array" for the purposes of the
/// above correspondance.
///
/// The implementation for tuples is only for those up to length 12. The implementation
/// for arrays is only for common array lengths. See [`ParquetData`] for more details.
///
/// ## `#[derive(Data)]`
///
/// The easiest way to implement `ParquetData` on a new type is using `#[derive(Data)]`:
///
/// ```ignore
/// use amadeus_parquet::internal;
/// use internal::record::{types::Timestamp, ParquetData};
///
/// #[derive(Data, Debug)]
/// struct MyRow {
///     id: u64,
///     time: Timestamp,
///     event: String,
/// }
/// ```
///
/// If the Rust field name and the Parquet field name differ, say if the latter is not an idiomatic or valid identifier in Rust, then an automatic rename can be made like so:
///
/// ```ignore
/// # use amadeus_parquet::internal::record::{types::Timestamp, ParquetData};
/// #[derive(Data, Debug)]
/// struct MyRow {
///     #[amadeus(name = "ID")]
///     id: u64,
///     time: Timestamp,
///     event: String,
/// }
/// ```
pub trait ParquetData: Data + Sized {
	// Clone + PartialEq + Debug + 'static
	type Schema: Schema;
	type Reader: Reader<Item = Self>;
	type Predicate: Clone + Debug + Serialize + for<'de> Deserialize<'de> + Send + 'static;

	/// Parse a [`Type`] into `Self::Schema`, using `repetition` instead of
	/// `Type::get_basic_info().repetition()`. A `repetition` of `None` denotes a root
	/// schema.
	fn parse(
		schema: &Type, predicate: Option<&Self::Predicate>, repetition: Option<Repetition>,
	) -> Result<(String, Self::Schema)>;

	/// Builds tree of [`Reader`]s for the specified [`Schema`] recursively.
	fn reader(
		schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::Reader;
}

/// This trait is implemented by Schemas so that they can be printed as Parquet schema
/// strings.
pub trait Schema: Debug {
	fn fmt(
		self_: Option<&Self>, r: Option<Repetition>, name: Option<&str>, f: &mut fmt::Formatter,
	) -> fmt::Result;
}

/// This trait is implemented by Readers so the values of one or more columns can be read
/// while taking into account the definition and repetition levels for optional and
/// repeated values.
pub trait Reader {
	/// Type returned by the Reader.
	type Item: Data;

	/// Read a value.
	fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item>;
	/// Advance the columns; this is used instead of `read` for optional values when
	/// `current_def_level <= max_def_level`.
	fn advance_columns(&mut self) -> Result<()>;
	/// Check if there's another value readable.
	fn has_next(&self) -> bool;
	/// Get the current definition level.
	fn current_def_level(&self) -> i16;
	/// Get the current repetition level.
	fn current_rep_level(&self) -> i16;
}
