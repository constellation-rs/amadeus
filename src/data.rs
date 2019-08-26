pub use serde as _serde;

pub mod types;

mod data {
	use super::types::SchemaIncomplete;
	use amadeus_parquet::{
		basic::Repetition, column::reader::ColumnReader, errors::ParquetError, record::{Reader as ParquetReader, Record as ParquetRecord, Schema as ParquetSchema}, schema::types::{ColumnPath, Type}
	};
	use serde::{Deserialize, Deserializer, Serialize, Serializer};
	use std::{
		collections::HashMap, fmt::{self, Debug, Display}
	};

	pub trait Data
	where
		Self: Clone + PartialEq + Debug + 'static,
	{
		//, super::types::Value: super::types::Downcast<Self> {
		type ParquetSchema: ParquetSchema;
		type ParquetReader: ParquetReader<Item = Self>;

		// fn for_each(impl FnMut(&str,

		fn upcast(self) -> super::types::Value {
			unimplemented!()
		}

		fn postgres_query(
			f: &mut fmt::Formatter, name: Option<&super::super::source::postgres::Names<'_>>,
		) -> fmt::Result;
		fn postgres_decode(
			type_: &::postgres::types::Type, buf: Option<&[u8]>,
		) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>;

		fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer;
		fn serde_deserialize<'de, D>(
			deserializer: D, schema: Option<SchemaIncomplete>,
		) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>;

		fn parquet_parse(
			schema: &Type, repetition: Option<Repetition>,
		) -> Result<(String, Self::ParquetSchema), ParquetError>;
		fn parquet_reader(
			schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
			paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
		) -> Self::ParquetReader;
	}

	macro_rules! impl_data_for_record {
		($($t:ty : $pt:ty),*) => (
			$(
			#[allow(clippy::use_self)]
			impl Data for $t {
				type ParquetSchema = <Self as ParquetRecord>::Schema;
				type ParquetReader = <Self as ParquetRecord>::Reader;

				fn postgres_query(f: &mut fmt::Formatter, name: Option<&super::super::source::postgres::Names<'_>>) -> fmt::Result {
					name.unwrap().fmt(f)
				}
				fn postgres_decode(type_: &::postgres::types::Type, buf: Option<&[u8]>) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
					if !<$pt as ::postgres::types::FromSql>::accepts(type_) {
						return Err(Into::into("invalid type"));
					}
					#[allow(trivial_numeric_casts)]
					<$pt as ::postgres::types::FromSql>::from_sql(type_, buf.ok_or_else(||Box::new(::postgres::types::WasNull))?).map(|x|x as Self)
				}

				fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
				where
					S: Serializer {
					self.serialize(serializer)
				}
				fn serde_deserialize<'de, D>(deserializer: D, _schema: Option<SchemaIncomplete>) -> Result<Self, D::Error>
				where
					D: Deserializer<'de> {
					Self::deserialize(deserializer)
				}

				fn parquet_parse(
					schema: &Type, repetition: Option<Repetition>,
				) -> Result<(String, Self::ParquetSchema), ParquetError> {
					<Self as ParquetRecord>::parse(schema, repetition)
				}
				fn parquet_reader(
					schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
					paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
				) -> Self::ParquetReader {
					<Self as ParquetRecord>::reader(schema, path, def_level, rep_level, paths, batch_size)
				}
			}
			)*
		);
	}
	impl_data_for_record!(
		bool: bool,
		u8: i8,
		i8: i8,
		u16: i16,
		i16: i16,
		u32: i32,
		i32: i32,
		u64: i64,
		i64: i64,
		f32: f32,
		f64: f64,
		String: String
	);
	// use super::types::{Date,Time,Timestamp,Decimal};

	impl<T> Data for Option<T>
	where
		T: Data,
	{
		type ParquetSchema = <Option<crate::source::parquet::Record<T>> as ParquetRecord>::Schema;
		type ParquetReader =
			XxxReader<<Option<crate::source::parquet::Record<T>> as ParquetRecord>::Reader>;

		fn postgres_query(
			f: &mut fmt::Formatter, name: Option<&super::super::source::postgres::Names<'_>>,
		) -> fmt::Result {
			T::postgres_query(f, name)
		}
		fn postgres_decode(
			type_: &::postgres::types::Type, buf: Option<&[u8]>,
		) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
			match buf {
				Some(buf) => T::postgres_decode(type_, Some(buf)).map(Some),
				None => Ok(None),
			}
		}

		fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			self.as_ref().map(SerdeSerialize).serialize(serializer)
		}
		fn serde_deserialize<'de, D>(
			deserializer: D, _schema: Option<SchemaIncomplete>,
		) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			<Option<SerdeDeserialize<T>>>::deserialize(deserializer).map(|x| x.map(|x| x.0))
		}

		fn parquet_parse(
			schema: &Type, repetition: Option<Repetition>,
		) -> Result<(String, Self::ParquetSchema), ParquetError> {
			<Option<crate::source::parquet::Record<T>> as ParquetRecord>::parse(schema, repetition)
		}
		fn parquet_reader(
			schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
			paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
		) -> Self::ParquetReader {
			XxxReader(
				<Option<crate::source::parquet::Record<T>> as ParquetRecord>::reader(
					schema, path, def_level, rep_level, paths, batch_size,
				),
			)
		}
	}

	#[repr(transparent)]
	pub struct SerdeSerialize<'a, T: Data>(pub &'a T);
	impl<'a, T> Serialize for SerdeSerialize<'a, T>
	where
		T: Data,
	{
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			self.0.serde_serialize(serializer)
		}
	}

	#[repr(transparent)]
	pub struct SerdeDeserialize<T: Data>(pub T);
	impl<'de, T> Deserialize<'de> for SerdeDeserialize<T>
	where
		T: Data,
	{
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			T::serde_deserialize(deserializer, None).map(Self)
		}
	}
	#[repr(transparent)]
	pub struct SerdeDeserializeGroup<T: Data>(pub T);
	impl<'de, T> Deserialize<'de> for SerdeDeserializeGroup<T>
	where
		T: Data,
	{
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			T::serde_deserialize(deserializer, Some(SchemaIncomplete::Group(None))).map(Self)
		}
	}

	/// A Reader that wraps a Reader, wrapping the read value in a `Record`.
	pub struct XxxReader<T>(T);
	impl<T, U> amadeus_parquet::record::Reader for XxxReader<T>
	where
		T: amadeus_parquet::record::Reader<Item = Option<crate::source::parquet::Record<U>>>,
		U: Data,
	{
		type Item = Option<U>;

		#[inline]
		fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, ParquetError> {
			self.0.read(def_level, rep_level).map(|x| x.map(|x| x.0))
		}

		#[inline]
		fn advance_columns(&mut self) -> Result<(), ParquetError> {
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
}
pub use amadeus_derive::Data;
pub use data::{Data, SerdeDeserialize, SerdeDeserializeGroup, SerdeSerialize};

pub mod serde_data {
	use super::Data;
	use serde::{Deserializer, Serializer};

	pub fn serialize<T, S>(self_: &T, serializer: S) -> Result<S::Ok, S::Error>
	where
		T: Data + ?Sized,
		S: Serializer,
	{
		self_.serde_serialize(serializer)
	}
	pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
	where
		T: Data,
		D: Deserializer<'de>,
	{
		T::serde_deserialize(deserializer, None)
	}
}

// impl<T: ?Sized> Data for T where T: PartialEq + Eq + Clone + 'static {}

// pub trait DataSource: crate::dist_iter::DistributedIterator<Item = <Self as DataSource>::Itemm> {
// 	type Itemm: Data;
// }

// impl<T: ?Sized> DataSource for T where T: crate::dist_iter::DistributedIterator, <T as crate::dist_iter::DistributedIterator>::Item: Data {
// 	type Itemm = <T as crate::dist_iter::DistributedIterator>::Item;
// }

pub trait DataSource
where
	Self: crate::dist_iter::DistributedIterator<Item = <Self as DataSource>::Item>,
	<Self as crate::dist_iter::DistributedIterator>::Item: Data,
{
	type Item;
}

// impl<T: ?Sized> DataSource for T where T: crate::dist_iter::DistributedIterator, <T as crate::dist_iter::DistributedIterator>::Item: Data {
// 	// type Itemm = <T as crate::dist_iter::DistributedIterator>::Item;
// 	type Item = <T as crate::dist_iter::DistributedIterator>::Item;
// }

mod tuple {
	use super::{
		data::{SerdeDeserialize, SerdeSerialize}, types::SchemaIncomplete, Data
	};
	use std::{collections::HashMap, fmt, marker::PhantomData};

	use amadeus_parquet::{
		basic::Repetition, column::reader::ColumnReader, errors::Result as ParquetResult, record::Record as ParquetRecord, schema::types::{ColumnPath, Type}
	};
	use serde::{Deserialize, Deserializer, Serialize, Serializer};

	/// A Reader that wraps a Reader, wrapping the read value in a `Record`.
	pub struct TupleXxxReader<T, U>(U, PhantomData<fn(T)>);

	// impl<T, U> amadeus_parquet::record::Reader for TupleXxxReader<T>
	// where
	// 	T: amadeus_parquet::record::Reader<Item = (U,)>,
	// 	U: Data,
	// {
	// 	type Item = (U,);

	// 	#[inline]
	// 	fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item> {
	// 		self.0.read(def_level, rep_level).map(|x| (x.0,))
	// 	}

	// 	#[inline]
	// 	fn advance_columns(&mut self) -> Result<()> {
	// 		self.0.advance_columns()
	// 	}

	// 	#[inline]
	// 	fn has_next(&self) -> bool {
	// 		self.0.has_next()
	// 	}

	// 	#[inline]
	// 	fn current_def_level(&self) -> i16 {
	// 		self.0.current_def_level()
	// 	}

	// 	#[inline]
	// 	fn current_rep_level(&self) -> i16 {
	// 		self.0.current_rep_level()
	// 	}
	// }
	// impl<T, U, V> amadeus_parquet::record::Reader for TupleXxxReader<T>
	// where
	// 	T: amadeus_parquet::record::Reader<Item = (U,V,)>,
	// 	U: Data,
	// 	V: Data,
	// {
	// 	type Item = (U,V);

	// 	#[inline]
	// 	fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item> {
	// 		self.0.read(def_level, rep_level).map(|x| (x.0,x.1,))
	// 	}

	// 	#[inline]
	// 	fn advance_columns(&mut self) -> Result<()> {
	// 		self.0.advance_columns()
	// 	}

	// 	#[inline]
	// 	fn has_next(&self) -> bool {
	// 		self.0.has_next()
	// 	}

	// 	#[inline]
	// 	fn current_def_level(&self) -> i16 {
	// 		self.0.current_def_level()
	// 	}

	// 	#[inline]
	// 	fn current_rep_level(&self) -> i16 {
	// 		self.0.current_rep_level()
	// 	}
	// }

	/// Macro to implement [`Reader`] on tuples up to length 32.
	macro_rules! impl_parquet_record_tuple {
		($len:tt $($t:ident $i:tt)*) => (
			// // Tuples correspond to Parquet groups with an equal number of fields with corresponding types.
			// impl<$($t,)*> Reader for TupleReader<($($t,)*)> where $($t: Reader,)* {
			// 	type Item = ($($t::Item,)*);

			// 	#[allow(unused_variables, non_snake_case)]
			// 	fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item> {
			// 		$(
			// 			let $t = (self.0).$i.read(def_level, rep_level);
			// 		)*
			// 		if unsafe{unlikely($($t.is_err() ||)* false)} {
			// 			$($t?;)*
			// 			unreachable!()
			// 		}
			// 		Ok((
			// 			$($t.unwrap(),)*
			// 		))
			// 	}
			// 	fn advance_columns(&mut self) -> Result<()> {
			// 		#[allow(unused_mut)]
			// 		let mut res = Ok(());
			// 		$(
			// 			res = res.and((self.0).$i.advance_columns());
			// 		)*
			// 		res
			// 	}
			// 	#[inline]
			// 	fn has_next(&self) -> bool {
			// 		$(if true { (self.0).$i.has_next() } else)*
			// 		{
			// 			true
			// 		}
			// 	}
			// 	#[inline]
			// 	fn current_def_level(&self) -> i16 {
			// 		$(if true { (self.0).$i.current_def_level() } else)*
			// 		{
			// 			panic!("Current definition level: empty group reader")
			// 		}
			// 	}
			// 	#[inline]
			// 	fn current_rep_level(&self) -> i16 {
			// 		$(if true { (self.0).$i.current_rep_level() } else)*
			// 		{
			// 			panic!("Current repetition level: empty group reader")
			// 		}
			// 	}
			// }
			// impl<$($t,)*> Default for TupleSchema<($((String,$t,),)*)> where $($t: Default,)* {
			// 	fn default() -> Self {
			// 		Self(($((format!("field_{}", $i), Default::default()),)*))
			// 	}
			// }
			// impl<$($t,)*> Debug for TupleSchema<($((String,$t,),)*)> where $($t: Debug,)* {
			// 	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
			// 		f.debug_tuple("TupleSchema")
			// 			$(.field(&(self.0).$i))*
			// 			.finish()
			// 	}
			// }
			// impl<$($t,)*> Schema for TupleSchema<($((String,$t,),)*)> where $($t: ParquetSchema,)* {
			// 	#[allow(unused_variables)]
			// 	fn fmt(self_: Option<&Self>, r: Option<Repetition>, name: Option<&str>, f: &mut fmt::Formatter) -> fmt::Result {
			// 		let mut printer = DisplaySchemaGroup::new(r, name, None, f);
			// 		$(
			// 			printer.field(self_.map(|self_|&*(self_.0).$i.0), self_.map(|self_|&(self_.0).$i.1));
			// 		)*
			// 		printer.finish()
			// 	}
			// }

			impl<$($t,)*> amadeus_parquet::record::Reader for TupleXxxReader<($($t,)*),<($(crate::source::parquet::Record<$t>,)*) as ParquetRecord>::Reader>
			where
				$($t: Data,)*
			{
				type Item = ($($t,)*);

				#[allow(unused_variables)]
				#[inline]
				fn read(&mut self, def_level: i16, rep_level: i16) -> ParquetResult<Self::Item> {
					self.0.read(def_level, rep_level).map(|self_|($((self_.$i).0,)*))
				}

				#[inline]
				fn advance_columns(&mut self) -> ParquetResult<()> {
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

			impl<$($t,)*> Data for ($($t,)*) where $($t: Data,)* {
				type ParquetSchema = <($(crate::source::parquet::Record<$t>,)*) as ParquetRecord>::Schema; //TupleSchema<($((String,$t::Schema,),)*)>;
				type ParquetReader = TupleXxxReader<($($t,)*),<($(crate::source::parquet::Record<$t>,)*) as ParquetRecord>::Reader>; //TupleReader<($($t::Reader,)*)>;

				fn postgres_query(_f: &mut fmt::Formatter, _name: Option<&super::super::source::postgres::Names<'_>>) -> fmt::Result {
					unimplemented!()
				}
				fn postgres_decode(_type_: &::postgres::types::Type, _buf: Option<&[u8]>) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
					unimplemented!()
				}

				fn serde_serialize<S_>(&self, serializer: S_) -> Result<S_::Ok, S_::Error>
				where
					S_: Serializer {
					($(SerdeSerialize(&self.$i),)*).serialize(serializer)
				}
				#[allow(unused_variables)]
				fn serde_deserialize<'de,D_>(deserializer: D_, _schema: Option<SchemaIncomplete>) -> Result<Self, D_::Error>
				where
					D_: Deserializer<'de> {
					<($(SerdeDeserialize<$t>,)*)>::deserialize(deserializer).map(|self_|($((self_.$i).0,)*))
				}

				fn parquet_parse(schema: &Type, repetition: Option<Repetition>) -> ParquetResult<(String, Self::ParquetSchema)> {
					<($(crate::source::parquet::Record<$t>,)*) as ParquetRecord>::parse(schema, repetition)
				}
				#[allow(unused_variables)]
				fn parquet_reader(schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16, paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize) -> Self::ParquetReader {
					TupleXxxReader(<($(crate::source::parquet::Record<$t>,)*) as ParquetRecord>::reader(schema, path, def_level, rep_level, paths, batch_size), PhantomData)
				}
			}
			impl<$($t,)*> super::types::Downcast<($($t,)*)> for super::types::Value where super::types::Value: $(super::types::Downcast<$t> +)* {
				fn downcast(self) -> Result<($($t,)*), super::types::DowncastError> {
					#[allow(unused_mut,unused_variables)]
					let mut fields = self.into_group()?.0.into_iter();
					if fields.len() != $len {
						return Err(super::types::DowncastError{from:"group",to:concat!("tuple of length ", $len)});
					}
					Ok(($({let _ = $i;fields.next().unwrap().downcast()?},)*))
				}
			}
			// impl<$($t,)*> Downcast<($($t,)*)> for Group where Value: $(Downcast<$t> +)* {
			// 	fn downcast(self) -> Result<($($t,)*)> {
			// 		#[allow(unused_mut,unused_variables)]
			// 		let mut fields = self.0.into_iter();
			// 		if fields.len() != $len {
			// 			return Err(ParquetError::General(format!("Can't downcast group of length {} to tuple of length {}", fields.len(), $len)));
			// 		}
			// 		Ok(($({let _ = $i;fields.next().unwrap().downcast()?},)*))
			// 	}
			// }
			// impl<$($t,)*> PartialEq<($($t,)*)> for Value where Value: $(PartialEq<$t> +)* {
			// 	#[allow(unused_variables)]
			// 	fn eq(&self, other: &($($t,)*)) -> bool {
			// 		self.is_group() $(&& self.as_group().unwrap()[$i] == other.$i)*
			// 	}
			// }
			// impl<$($t,)*> PartialEq<($($t,)*)> for Group where Value: $(PartialEq<$t> +)* {
			// 	#[allow(unused_variables)]
			// 	fn eq(&self, other: &($($t,)*)) -> bool {
			// 		$(self[$i] == other.$i && )* true
			// 	}
			// }
			// impl<$($t,)*> Downcast<TupleSchema<($((String,$t,),)*)>> for ValueSchema where ValueSchema: $(Downcast<$t> +)* {
			// 	fn downcast(self) -> Result<TupleSchema<($((String,$t,),)*)>> {
			// 		let group = self.into_group()?;
			// 		#[allow(unused_mut,unused_variables)]
			// 		let mut fields = group.0.into_iter();
			// 		let mut names = vec![None; group.1.len()];
			// 		for (name,&index) in group.1.iter() {
			// 			names[index].replace(name.to_owned());
			// 		}
			// 		#[allow(unused_mut,unused_variables)]
			// 		let mut names = names.into_iter().map(Option::unwrap);
			// 		Ok(TupleSchema(($({let _ = $i;(names.next().unwrap(),fields.next().unwrap().downcast()?)},)*)))
			// 	}
			// }
		);
	}

	impl_parquet_record_tuple!(0);
	impl_parquet_record_tuple!(1 A 0);
	impl_parquet_record_tuple!(2 A 0 B 1);
	impl_parquet_record_tuple!(3 A 0 B 1 C 2);
	impl_parquet_record_tuple!(4 A 0 B 1 C 2 D 3);
	impl_parquet_record_tuple!(5 A 0 B 1 C 2 D 3 E 4);
	impl_parquet_record_tuple!(6 A 0 B 1 C 2 D 3 E 4 F 5);
	impl_parquet_record_tuple!(7 A 0 B 1 C 2 D 3 E 4 F 5 G 6);
	impl_parquet_record_tuple!(8 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
	impl_parquet_record_tuple!(9 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
	impl_parquet_record_tuple!(10 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
	impl_parquet_record_tuple!(11 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
	impl_parquet_record_tuple!(12 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);
	// impl_parquet_record_tuple!(13 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
	// impl_parquet_record_tuple!(14 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
	// impl_parquet_record_tuple!(15 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
	// impl_parquet_record_tuple!(16 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
	// impl_parquet_record_tuple!(17 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
	// impl_parquet_record_tuple!(18 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
	// impl_parquet_record_tuple!(19 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
	// impl_parquet_record_tuple!(20 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
	// impl_parquet_record_tuple!(21 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
	// impl_parquet_record_tuple!(22 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
	// impl_parquet_record_tuple!(23 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
	// impl_parquet_record_tuple!(24 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
	// impl_parquet_record_tuple!(25 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
	// impl_parquet_record_tuple!(26 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
	// impl_parquet_record_tuple!(27 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
	// impl_parquet_record_tuple!(28 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
	// impl_parquet_record_tuple!(29 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
	// impl_parquet_record_tuple!(30 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
	// impl_parquet_record_tuple!(31 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
	// impl_parquet_record_tuple!(32 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);
}
