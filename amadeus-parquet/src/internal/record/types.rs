//! Implementations of Rust types that correspond to Parquet logical types.
//! [`ParquetData`](super::ParquetData) is implemented for each of them.

use amadeus_types::Data;

use super::schemas::ValueSchema;
use crate::internal::errors::Result;

// pub use self::{
// 	array::{Bson, Enum, Json}, group::{Group, Row}, list::List, map::Map, root::Root, time::{Date, Time, Timestamp}, value::Value, value_required::ValueRequired
// };

/// `Root<T>` corresponds to the root of the schema, i.e. what is marked as "message" in a
/// Parquet schema string.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Root<T>(pub T);

impl<T: Data> Data for Root<T> {
	type Vec = Vec<Self>;
	type DynamicType = T::DynamicType;

	fn new_vec(_type: Self::DynamicType) -> Self::Vec {
		unimplemented!()
	}
}

/// This trait lets one downcast a generic type like [`Value`] to a specific type like
/// `u64`.
///
/// It exists, rather than for example using [`TryInto`](std::convert::TryInto), due to
/// coherence issues with downcasting to foreign types like `Option<T>`.
pub trait Downcast<T> {
	fn downcast(self) -> Result<T>;
}

pub(crate) fn downcast<T>((name, schema): (String, ValueSchema)) -> Result<(String, T)>
where
	ValueSchema: Downcast<T>,
{
	schema.downcast().map(|schema| (name, schema))
}
