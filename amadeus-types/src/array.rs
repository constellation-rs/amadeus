//! Implement [`Record`] for `Vec<u8>` (byte_array/fixed_len_byte_array), [`Bson`] (bson),
//! `String` (utf8), [`Json`] (json), [`Enum`] (enum), and `[u8; N]`
//! (fixed_len_byte_array).

use serde::{Deserialize, Serialize};
use std::{
	cmp::Ordering, fmt::{self, Display}
};

use super::AmadeusOrd;

/// A Rust type corresponding to the [Bson logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#bson).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
pub struct Bson(Vec<u8>);
impl Bson {
	pub fn capacity(&self) -> usize {
		self.0.capacity()
	}
}
impl AmadeusOrd for Bson {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
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
impl Json {
	pub fn capacity(&self) -> usize {
		self.0.capacity()
	}
}
impl AmadeusOrd for Json {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
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
impl Enum {
	pub fn capacity(&self) -> usize {
		self.0.capacity()
	}
}
impl AmadeusOrd for Enum {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
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
