//! Implementations of Rust types that correspond to Parquet logical types.
//! [`Record`](super::Record) is implemented for each of them.

mod array;
// mod boxed;
mod decimal;
mod group;
mod list;
mod map;
// mod numbers;
// mod option;
mod time;
// mod tuple;
mod value;
mod value_required;

use std::{
	error::Error, fmt::{self, Display}, marker::PhantomData
};

pub use self::{
	array::{Bson, Enum, Json}, decimal::Decimal, group::Group, list::List, map::Map, time::{Date, Time, Timestamp}, value::{Schema, SchemaIncomplete, Value}, value_required::ValueRequired
};

/// This trait lets one downcast a generic type like [`Value`] to a specific type like
/// `u64`.
///
/// It exists, rather than for example using [`TryInto`](std::convert::TryInto), due to
/// coherence issues with downcasting to foreign types like `Option<T>`.
pub trait Downcast<T> {
	fn downcast(self) -> Result<T, DowncastError>;
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct DowncastError {
	pub from: &'static str,
	pub to: &'static str,
}
impl Error for DowncastError {
	fn description(&self) -> &str {
		"invalid downcast"
	}
}
impl Display for DowncastError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "could not downcast \"{}\" to \"{}\"", self.from, self.to)
	}
}

/// A convenience Reader that maps the read value using [`TryInto`].
pub struct IntoReader<R: amadeus_parquet::record::Reader, T>(R, PhantomData<fn(T)>);
impl<R: amadeus_parquet::record::Reader, T> IntoReader<R, T> {
	fn new(reader: R) -> Self {
		IntoReader(reader, PhantomData)
	}
}
impl<R: amadeus_parquet::record::Reader, T> amadeus_parquet::record::Reader for IntoReader<R, T>
where
	R::Item: Into<T>,
{
	type Item = T;

	#[inline]
	fn read(
		&mut self, def_level: i16, rep_level: i16,
	) -> Result<Self::Item, amadeus_parquet::errors::ParquetError> {
		self.0.read(def_level, rep_level).map(Into::into)
	}

	#[inline]
	fn advance_columns(&mut self) -> Result<(), amadeus_parquet::errors::ParquetError> {
		self.0.advance_columns()
	}

	#[inline]
	fn has_next(&self) -> bool {
		self.0.has_next()
	}

	#[inline]
	fn current_def_level(&self) -> i16 {
		self.0.current_def_level()
	}

	#[inline]
	fn current_rep_level(&self) -> i16 {
		self.0.current_rep_level()
	}
}

/// A convenience Reader that maps the read value using the supplied closure.
pub struct MapReader<R: amadeus_parquet::record::Reader, F>(R, F);
impl<R: amadeus_parquet::record::Reader, F> MapReader<R, F> {
	fn new(reader: R, f: F) -> Self {
		MapReader(reader, f)
	}
}
impl<R: amadeus_parquet::record::Reader, F, T> amadeus_parquet::record::Reader for MapReader<R, F>
where
	F: FnMut(R::Item) -> Result<T, amadeus_parquet::errors::ParquetError>,
{
	type Item = T;

	#[inline]
	fn read(
		&mut self, def_level: i16, rep_level: i16,
	) -> Result<Self::Item, amadeus_parquet::errors::ParquetError> {
		self.0.read(def_level, rep_level).and_then(&mut self.1)
	}

	#[inline]
	fn advance_columns(&mut self) -> Result<(), amadeus_parquet::errors::ParquetError> {
		self.0.advance_columns()
	}

	#[inline]
	fn has_next(&self) -> bool {
		self.0.has_next()
	}

	#[inline]
	fn current_def_level(&self) -> i16 {
		self.0.current_def_level()
	}

	#[inline]
	fn current_rep_level(&self) -> i16 {
		self.0.current_rep_level()
	}
}

// /// A convenience Reader that maps the read value using [`TryInto`].
// pub struct TryIntoReader<R: amadeus_parquet::record::Reader, T>(R, PhantomData<fn(T)>);
// impl<R: amadeus_parquet::record::Reader, T> TryIntoReader<R, T> {
//     fn new(reader: R) -> Self {
//         TryIntoReader(reader, PhantomData)
//     }
// }
// impl<R: amadeus_parquet::record::Reader, T> amadeus_parquet::record::Reader for TryIntoReader<R, T>
// where
//     R::Item: TryInto<T>,
//     <R::Item as TryInto<T>>::Error: Error,
// {
//     type Item = T;

//     #[inline]
//     fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, amadeus_parquet::errors::ParquetError> {
//         self.0.read(def_level, rep_level).and_then(|x| {
//             x.try_into()
//                 .map_err(|err| ParquetError::General(err.description().to_owned()))
//         })
//     }

//     #[inline]
//     fn advance_columns(&mut self) -> Result<(), amadeus_parquet::errors::ParquetError> {
//         self.0.advance_columns()
//     }

//     #[inline]
//     fn has_next(&self) -> bool {
//         self.0.has_next()
//     }

//     #[inline]
//     fn current_def_level(&self) -> i16 {
//         self.0.current_def_level()
//     }

//     #[inline]
//     fn current_rep_level(&self) -> i16 {
//         self.0.current_rep_level()
//     }
// }
