#![doc(html_root_url = "https://docs.rs/amadeus-types/0.1.3")]
#![feature(specialization)]

//! Implementations of Rust types that correspond to Parquet logical types.
//! [`Record`](super::Record) is implemented for each of them.

#[macro_export]
macro_rules! array {
	($array_macro:ident) => (
		$array_macro!(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32); // 40 48 50 56 64 72 96 100 128 160 192 200 224 256 384 512 768 1024 2048 4096 8192 16384 32768 65536);
	)
}
#[macro_export]
macro_rules! tuple {
	($tuple_macro:ident) => (
		$tuple_macro!(0);
		$tuple_macro!(1 A 0);
		$tuple_macro!(2 A 0 B 1);
		$tuple_macro!(3 A 0 B 1 C 2);
		$tuple_macro!(4 A 0 B 1 C 2 D 3);
		$tuple_macro!(5 A 0 B 1 C 2 D 3 E 4);
		$tuple_macro!(6 A 0 B 1 C 2 D 3 E 4 F 5);
		$tuple_macro!(7 A 0 B 1 C 2 D 3 E 4 F 5 G 6);
		$tuple_macro!(8 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
		$tuple_macro!(9 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
		$tuple_macro!(10 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
		$tuple_macro!(11 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
		$tuple_macro!(12 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);
		// $tuple_macro!(13 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
		// $tuple_macro!(14 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
		// $tuple_macro!(15 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
		// $tuple_macro!(16 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
		// $tuple_macro!(17 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
		// $tuple_macro!(18 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
		// $tuple_macro!(19 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
		// $tuple_macro!(20 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
		// $tuple_macro!(21 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
		// $tuple_macro!(22 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
		// $tuple_macro!(23 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
		// $tuple_macro!(24 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
		// $tuple_macro!(25 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
		// $tuple_macro!(26 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
		// $tuple_macro!(27 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
		// $tuple_macro!(28 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
		// $tuple_macro!(29 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
		// $tuple_macro!(30 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
		// $tuple_macro!(31 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
		// $tuple_macro!(32 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);
	)
}

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
	array::{Bson, Enum, Json}, decimal::Decimal, group::Group, http::{IpAddr, ParseAddrError, ParseUrlError, ParseWebpageError, Url, Webpage}, list::List, map::Map, time::{
		Date, DateTime, DateTimeWithoutTimezone, DateWithoutTimezone, ParseDateError, Time, TimeWithoutTimezone, Timezone
	}, value::{Schema, SchemaIncomplete, Value}, value_required::ValueRequired
};

/// This trait lets one downcast a generic type like [`Value`] to a specific type like
/// `u64`.
///
/// It exists, rather than for example using [`TryInto`](std::convert::TryInto), due to
/// coherence issues with downcasting to foreign types like `Option<T>`.
pub trait DowncastFrom<T>
where
	T: Downcast<Self>,
	Self: Sized,
{
	fn downcast_from(t: T) -> Result<Self, DowncastError>
	where
		Self: Sized;
}
pub trait Downcast<T> {
	fn downcast(self) -> Result<T, DowncastError>;
}
impl<A, B> Downcast<A> for B
where
	A: DowncastFrom<B>,
{
	fn downcast(self) -> Result<A, DowncastError> {
		A::downcast_from(self)
	}
}

impl<A, B> DowncastFrom<A> for Box<B>
where
	B: DowncastFrom<A>,
{
	fn downcast_from(t: A) -> Result<Self, DowncastError>
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
