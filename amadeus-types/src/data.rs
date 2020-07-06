// TODO associated_type_defaults https://github.com/rust-lang/rust/issues/29661

use std::{
	collections::HashMap, hash::{BuildHasher, Hash}
};

use super::*;

// + AmadeusOrd + DowncastFrom<Value> + Into<Value>
pub trait Data: Clone + Debug + Send + Sized + 'static {
	type Vec: ListVec<Self>;
	type DynamicType;
	fn new_vec(_type: Self::DynamicType) -> Self::Vec;
}

impl<T> Data for Option<T>
where
	T: Data,
{
	type Vec = Vec<Self>;
	type DynamicType = ();

	fn new_vec(_type: Self::DynamicType) -> Self::Vec {
		Vec::new()
	}
}
impl<T> Data for Box<T>
where
	T: Data,
{
	type Vec = Vec<Self>;
	type DynamicType = ();

	fn new_vec(_type: Self::DynamicType) -> Self::Vec {
		Vec::new()
	}
}
impl<T> Data for List<T>
where
	T: Data,
{
	type Vec = Vec<Self>;
	type DynamicType = ();

	fn new_vec(_type: Self::DynamicType) -> Self::Vec {
		Vec::new()
	}
}
impl<K, V, S> Data for HashMap<K, V, S>
where
	K: Hash + Eq + Data,
	V: Data,
	S: BuildHasher + Clone + Default + Send + 'static,
{
	type Vec = Vec<Self>;
	type DynamicType = ();

	fn new_vec(_type: Self::DynamicType) -> Self::Vec {
		Vec::new()
	}
}

macro_rules! impl_data {
	($($t:ty)*) => ($(
		impl Data for $t {
			type Vec = Vec<Self>;
			type DynamicType = ();

			fn new_vec(_type: Self::DynamicType) -> Self::Vec {
				Vec::new()
			}
		}
	)*);
}
impl_data!(bool u8 i8 u16 i16 u32 i32 u64 i64 f32 f64 String Bson Json Enum Decimal Group Date DateWithoutTimezone Time TimeWithoutTimezone DateTime DateTimeWithoutTimezone Timezone Webpage<'static> Url IpAddr);

// Implement Record for common array lengths.
macro_rules! array {
	($($i:tt)*) => {$(
		impl Data for [u8; $i] {
			type Vec = Vec<Self>;
			type DynamicType = ();

			fn new_vec(_type: Self::DynamicType) -> Self::Vec {
				Vec::new()
			}
		}
		// TODO: impl<T> Data for [T; $i] where T: Data {}
	)*};
}
super::array!(array);

// Implement Record on tuples up to length 12.
macro_rules! tuple {
	($len:tt $($t:ident $i:tt)*) => {
		impl<$($t,)*> Data for ($($t,)*) where $($t: Data,)* {
			type Vec = Vec<Self>;
			type DynamicType = ();

			fn new_vec(_type: Self::DynamicType) -> Self::Vec {
				Vec::new()
			}
		}
	};
}
super::tuple!(tuple);
