//! Implement [`Record`] for [`Group`] aka [`Row`].

use fxhash::FxBuildHasher;
use hashlink::linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
	cmp::Ordering, fmt::{self, Debug}, ops::Index, slice::SliceIndex, str, sync::Arc
};

use super::{util::IteratorExt, AmadeusOrd, Downcast, DowncastError, DowncastFrom, Value};

/// Corresponds to Parquet groups of named fields.
///
/// Its fields can be accessed by name via
/// [`get()`](Group::get)/[`get_mut()`](Self::get_mut) and via name or ordinal with
/// [`group[index]`](#impl-Index<usize>).
#[derive(Clone, PartialEq)]
pub struct Group {
	fields: Vec<Value>,
	field_names: Option<Arc<LinkedHashMap<String, usize, FxBuildHasher>>>,
}

impl Group {
	#[doc(hidden)]
	pub fn new(
		fields: Vec<Value>, field_names: Option<Arc<LinkedHashMap<String, usize, FxBuildHasher>>>,
	) -> Self {
		Self {
			fields,
			field_names,
		}
	}
	#[doc(hidden)]
	pub fn fields(&self) -> &[Value] {
		&self.fields
	}
	#[doc(hidden)]
	pub fn field_names(&self) -> Option<&Arc<LinkedHashMap<String, usize, FxBuildHasher>>> {
		self.field_names.as_ref()
	}
	/// Get a reference to the value belonging to a particular field name. Returns `None`
	/// if the field name doesn't exist.
	pub fn get(&self, k: &str) -> Option<&Value> {
		self.field_names
			.as_ref()?
			.get(k)
			.map(|&offset| &self.fields[offset])
	}
	#[doc(hidden)]
	pub fn into_fields(self) -> Vec<Value> {
		self.fields
	}
}

impl Serialize for Group {
	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		// <Self as SerdeData>::serialize(self, serializer)
		unimplemented!()
	}
}
impl<'de> Deserialize<'de> for Group {
	fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		// <Self as SerdeData>::deserialize(deserializer, None)
		unimplemented!()
	}
}

// impl From<Group> for internal::record::types::Group {
// 	fn from(group: Group) -> Self {
// 		let field_names = group.field_names();
// 		Self::new(group.into_fields().map(Into::into), field_names)
// 	}
// }
impl<I> Index<I> for Group
where
	I: SliceIndex<[Value]>,
{
	type Output = <I as SliceIndex<[Value]>>::Output;

	fn index(&self, index: I) -> &Self::Output {
		self.fields.index(index)
	}
}
impl PartialOrd for Group {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		if match (self.field_names.as_ref(), other.field_names.as_ref()) {
			(Some(a), Some(b)) => a == b,
			_ => self.fields.len() == other.fields.len(),
		} {
			self.fields.partial_cmp(&other.fields)
		} else {
			None
		}
	}
}
impl AmadeusOrd for Group {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		match (self.field_names.as_ref(), other.field_names.as_ref()) {
			(Some(a), Some(b)) => a
				.iter()
				.map(|(name, _index)| name)
				.zip(&self.fields)
				.cmp_by_(
					b.iter().map(|(name, _index)| name).zip(&other.fields),
					|a, b| a.amadeus_cmp(&b),
				),
			(None, None) => self
				.fields
				.iter()
				.cmp_by_(&other.fields, AmadeusOrd::amadeus_cmp),
			(Some(_), None) => Ordering::Less,
			(None, Some(_)) => Ordering::Greater,
		}
	}
}

impl Debug for Group {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if let Some(field_names) = self.field_names.as_ref() {
			let mut printer = f.debug_struct("Group");
			for (name, field) in field_names
				.iter()
				.map(|(name, _index)| name)
				.zip(self.fields.iter())
			{
				let _ = printer.field(name, field);
			}
			printer.finish()
		} else {
			let mut printer = f.debug_tuple("Group");
			for field in &self.fields {
				let _ = printer.field(field);
			}
			printer.finish()
		}
	}
}

impl From<LinkedHashMap<String, Value, FxBuildHasher>> for Group {
	fn from(hashmap: LinkedHashMap<String, Value, FxBuildHasher>) -> Self {
		let mut keys = LinkedHashMap::with_capacity_and_hasher(hashmap.len(), Default::default());
		Self::new(
			hashmap
				.into_iter()
				.map(|(key, value)| {
					if keys.insert(key, keys.len()).is_some() {
						panic!("duplicate key");
					}
					value
				})
				.collect(),
			Some(Arc::new(keys)),
		)
	}
}

macro_rules! tuple_downcast {
	($len:tt $($t:ident $i:tt)*) => (
		impl<$($t,)*> DowncastFrom<Group> for ($($t,)*) where $($t: DowncastFrom<Value>,)* {
			fn downcast_from(self_: Group) -> Result<Self, DowncastError> {
				#[allow(unused_mut, unused_variables)]
				let mut fields = self_.into_fields().into_iter();
				if fields.len() != $len {
					return Err(DowncastError{from:"",to:""});
				}
				Ok(($({let _ = $i;fields.next().unwrap().downcast()?},)*))
			}
		}
	);
}
tuple!(tuple_downcast);

macro_rules! tuple_from {
	($len:tt $($t:ident $i:tt)*) => (
		impl<$($t,)*> From<($($t,)*)> for Group where $($t: Into<Value>,)* {
			#[allow(unused_variables)]
			fn from(value: ($($t,)*)) -> Self {
				Group::new(vec![$(value.$i.into(),)*], None)
			}
		}
	);
}
tuple!(tuple_from);

// impl From<Group> for LinkedHashMap<String, Value, FxBuildHasher> {
// 	fn from(group: Group) -> Self {
// 		group
// 			.field_names
// 			.iter()
// 			.map(|(name, _index)| name.clone())
// 			.zip(group.fields)
// 			.collect()
// 	}
// }
