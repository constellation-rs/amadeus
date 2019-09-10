#![feature(specialization)]

//! Implementations of Rust types that correspond to Parquet logical types.
//! [`Record`](super::Record) is implemented for each of them.

mod array;
mod decimal;
mod group;
mod http;
mod list;
mod map;
mod time;
mod value;
mod value_required;

use std::{
	error::Error, fmt::{self, Debug, Display}
};

pub use self::{
	array::{Bson, Enum, Json}, decimal::Decimal, group::Group, http::Webpage, list::List, map::Map, time::{Date, Time, Timestamp}, value::{Schema, SchemaIncomplete, Value}, value_required::ValueRequired
};

/// This trait lets one downcast a generic type like [`Value`] to a specific type like
/// `u64`.
///
/// It exists, rather than for example using [`TryInto`](std::convert::TryInto), due to
/// coherence issues with downcasting to foreign types like `Option<T>`.
pub trait DowncastImpl<T> {
	fn downcast_impl(t: T) -> Result<Self, DowncastError>
	where
		Self: Sized;
}
pub trait Downcast<T> {
	fn downcast(self) -> Result<T, DowncastError>;
}
impl<A, B> Downcast<A> for B
where
	A: DowncastImpl<B>,
{
	fn downcast(self) -> Result<A, DowncastError> {
		A::downcast_impl(self)
	}
}

impl<A, B> DowncastImpl<A> for Box<B>
where
	B: DowncastImpl<A>,
{
	fn downcast_impl(t: A) -> Result<Self, DowncastError>
	where
		Self: Sized,
	{
		t.downcast().map(Box::new)
	}
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
