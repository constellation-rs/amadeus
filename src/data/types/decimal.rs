//! Implement [`Record`] for [`Decimal`].

use std::{
	collections::HashMap, fmt::{self, Display}
};
// use num_bigint::{BigInt, Sign};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{super::Data, IntoReader, Schema};
use parquet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, schema::types::{ColumnPath, Type}
};

// use parquet::{
//     basic::Repetition,
//     column::reader::ColumnReader,
//     // data_type::{Vec<u8>, Decimal},
//     errors::Result,
//     record::{
//         reader::MapReader,
//         schemas::{DecimalSchema, I32Schema, I64Schema},
//         types::{downcast, Value},
//         Reader, Record,
//     },
//     schema::types::{ColumnPath, Type},
// };

// [`Decimal`] corresponds to the [Decimal logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal).
/// Rust representation for Decimal values.
///
/// This is not a representation of Parquet physical type, but rather a wrapper for
/// DECIMAL logical type, and serves as container for raw parts of decimal values:
/// unscaled value in bytes, precision and scale.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub enum Decimal {
	/// Decimal backed by `i32`.
	Int32 {
		value: [u8; 4],
		precision: i32,
		scale: i32,
	},
	/// Decimal backed by `i64`.
	Int64 {
		value: [u8; 8],
		precision: i32,
		scale: i32,
	},
	/// Decimal backed by byte array.
	Bytes {
		value: Vec<u8>,
		precision: i32,
		scale: i32,
	},
}

impl Decimal {
	// /// Creates new decimal value from `i32`.
	// pub fn from_i32(value: i32, precision: i32, scale: i32) -> Self {
	//     let mut bytes = [0; 4];
	//     BigEndian::write_i32(&mut bytes, value);
	//     Decimal::Int32 {
	//         value: bytes,
	//         precision,
	//         scale,
	//     }
	// }

	// /// Creates new decimal value from `i64`.
	// pub fn from_i64(value: i64, precision: i32, scale: i32) -> Self {
	//     let mut bytes = [0; 8];
	//     BigEndian::write_i64(&mut bytes, value);
	//     Decimal::Int64 {
	//         value: bytes,
	//         precision,
	//         scale,
	//     }
	// }

	/// Creates new decimal value from `Vec<u8>`.
	pub fn from_bytes(value: Vec<u8>, precision: i32, scale: i32) -> Self {
		Decimal::Bytes {
			value,
			precision,
			scale,
		}
	}

	/// Returns bytes of unscaled value.
	pub fn data(&self) -> &[u8] {
		match *self {
			Decimal::Int32 { ref value, .. } => value,
			Decimal::Int64 { ref value, .. } => value,
			Decimal::Bytes { ref value, .. } => value,
		}
	}

	/// Returns decimal precision.
	pub fn precision(&self) -> i32 {
		match *self {
			Decimal::Int32 { precision, .. } => precision,
			Decimal::Int64 { precision, .. } => precision,
			Decimal::Bytes { precision, .. } => precision,
		}
	}

	/// Returns decimal scale.
	pub fn scale(&self) -> i32 {
		match *self {
			Decimal::Int32 { scale, .. } => scale,
			Decimal::Int64 { scale, .. } => scale,
			Decimal::Bytes { scale, .. } => scale,
		}
	}
}

impl Data for Decimal {
	type ParquetSchema = <parquet::data_type::Decimal as parquet::record::Record>::Schema;
	type ParquetReader =
		IntoReader<<parquet::data_type::Decimal as parquet::record::Record>::Reader, Self>;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn postgres_decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<std::error::Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.serialize(serializer)
	}
	fn serde_deserialize<'de, D>(deserializer: D, schema: Option<Schema>) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::deserialize(deserializer)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<parquet::data_type::Decimal as parquet::record::Record>::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		IntoReader::new(
			<parquet::data_type::Decimal as parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}
impl From<Decimal> for parquet::data_type::Decimal {
	fn from(decimal: Decimal) -> Self {
		unimplemented!()
	}
}
impl From<parquet::data_type::Decimal> for Decimal {
	fn from(decimal: parquet::data_type::Decimal) -> Self {
		unimplemented!()
	}
}

// impl Display for Decimal {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         // We assert that `scale >= 0` and `precision > scale`, but this will be enforced
//         // when constructing Parquet schema.
//         assert!(self.scale() >= 0 && self.precision() > self.scale());

//         // Specify as signed bytes to resolve sign as part of conversion.
//         let num = BigInt::from_signed_bytes_be(self.data());

//         // Offset of the first digit in a string.
//         let negative = if num.sign() == Sign::Minus { 1 } else { 0 };
//         let mut num_str = num.to_string();
//         let mut point = num_str.len() as i32 - self.scale() - negative;

//         // Convert to string form without scientific notation.
//         if point <= 0 {
//             // Zeros need to be prepended to the unscaled value.
//             while point < 0 {
//                 num_str.insert(negative as usize, '0');
//                 point += 1;
//             }
//             num_str.insert_str(negative as usize, "0.");
//         } else {
//             // No zeroes need to be prepended to the unscaled value, simply insert decimal
//             // point.
//             num_str.insert((point + negative) as usize, '.');
//         }

//         num_str.fmt(f)
//     }
// }

// impl Default for Decimal {
//     fn default() -> Self {
//         Self::from_i32(0, 0, 0)
//     }
// }

// impl PartialEq for Decimal {
// 	fn eq(&self, other: &Decimal) -> bool {
// 		self.precision() == other.precision()
// 			&& self.scale() == other.scale()
// 			&& self.data() == other.data()
// 	}
// }
