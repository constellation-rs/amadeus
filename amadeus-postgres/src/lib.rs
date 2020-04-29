#![doc(html_root_url = "https://docs.rs/amadeus-postgres/0.1.7")]
#![feature(specialization)]
#![feature(type_alias_impl_trait)]

mod impls;

#[doc(hidden)]
pub use postgres as _internal;

use postgres::{params::IntoConnectParams, Error as PostgresError};
use serde::{Deserialize, Serialize};
use serde_closure::*;
use std::{
	convert::TryFrom, error, fmt::{self, Debug, Display}, io, iter, marker::PhantomData, ops::Fn, path::PathBuf, str
};

use amadeus_core::{
	dist_iter::DistributedIterator, into_dist_iter::IntoDistributedIterator, util::IoError, Source as DSource
};

pub trait PostgresData
where
	Self: Clone + PartialEq + Debug + 'static,
{
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result;
	fn decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>;
}

impl<T> PostgresData for Box<T>
where
	T: PostgresData,
{
	fn query(f: &mut fmt::Formatter, name: Option<&Names<'_>>) -> fmt::Result {
		T::query(f, name)
	}
	default fn decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		T::decode(type_, buf).map(Box::new)
	}
}

#[derive(Serialize, Deserialize)]
pub enum Source {
	Table(Table),
	Query(String),
}

#[derive(Serialize, Deserialize)]
pub struct Table {
	schema: Option<String>,
	table: String,
}
impl Table {
	pub fn new(schema: Option<String>, table: String) -> Self {
		Self { schema, table }
	}
}

impl Display for Table {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if let Some(ref schema) = self.schema {
			EscapeIdentifier(schema).fmt(f)?;
			f.write_str(".")?;
		}
		EscapeIdentifier(&self.table).fmt(f)
	}
}
impl str::FromStr for Table {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.contains(&['"', '.', '\0'] as &[char]) {
			unimplemented!(
				"Table parsing not yet implemented. Construct it with Table::new instead"
			);
		}
		Ok(Self {
			schema: None,
			table: s.to_string(),
		})
	}
}

// postgres::params::ConnectParams
#[derive(Serialize, Deserialize)]
pub struct ConnectParams {
	#[serde(with = "Host")]
	host: postgres::params::Host,
	port: u16,
	#[serde(with = "misc_serde")]
	user: Option<postgres::params::User>,
	database: Option<String>,
	options: Vec<(String, String)>,
	connect_timeout: Option<std::time::Duration>,
}
impl IntoConnectParams for ConnectParams {
	fn into_connect_params(
		self,
	) -> Result<postgres::params::ConnectParams, Box<dyn std::error::Error + 'static + Send + Sync>>
	{
		let mut builder = postgres::params::ConnectParams::builder();
		let _ = builder.port(self.port);
		if let Some(user) = self.user {
			let _ = builder.user(user.name(), user.password());
		}
		if let Some(database) = self.database {
			let _ = builder.database(&database);
		}
		for (name, value) in self.options {
			let _ = builder.option(&name, &value);
		}
		let _ = builder.connect_timeout(self.connect_timeout);
		Ok(builder.build(self.host))
	}
}
impl str::FromStr for ConnectParams {
	type Err = Box<dyn std::error::Error + 'static + Send + Sync>;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let params = s.into_connect_params()?;
		Ok(Self {
			host: params.host().clone(),
			port: params.port(),
			user: params.user().map(Clone::clone),
			database: params.database().map(ToOwned::to_owned),
			options: params.options().to_owned(),
			connect_timeout: params.connect_timeout(),
		})
	}
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "postgres::params::Host")]
enum Host {
	Tcp(String),
	Unix(PathBuf),
}

pub struct Postgres<Row>
where
	Row: PostgresData,
{
	files: Vec<(ConnectParams, Vec<Source>)>,
	marker: PhantomData<fn() -> Row>,
}
impl<Row> Postgres<Row>
where
	Row: PostgresData,
{
	pub fn new<I>(files: I) -> Self
	where
		I: IntoIterator<Item = (ConnectParams, Vec<Source>)>,
	{
		Self {
			files: files.into_iter().collect(),
			marker: PhantomData,
		}
	}
}

impl<Row> DSource for Postgres<Row>
where
	Row: PostgresData,
{
	type Item = Row;
	type Error = Error;

	#[cfg(not(feature = "doc"))]
	type DistIter = impl DistributedIterator<Item = Result<Self::Item, Self::Error>>;
	#[cfg(feature = "doc")]
	type DistIter = amadeus_core::util::ImplDistributedIterator<Result<Self::Item, Self::Error>>;
	type Iter = iter::Empty<Result<Self::Item, Self::Error>>;

	#[allow(clippy::let_and_return)]
	fn dist_iter(self) -> Self::DistIter {
		let ret = self
			.files
			.into_dist_iter()
			.flat_map(FnMut!(|(connect, tables)| {
				let (connect, tables): (ConnectParams, Vec<Source>) = (connect, tables);
				let connection =
					postgres::Connection::connect(connect, postgres::TlsMode::None).unwrap();
				tables.into_iter().flat_map(
					(move |table: Source| {
						// let stmt = connection.prepare("SELECT $1::\"public\".\"weather\"").unwrap();
						// let type_ = stmt.param_types()[0].clone();
						// let stmt = connection.prepare("SELECT ROW(city, temp_lo, temp_hi, prcp, date, CASE WHEN invent IS NOT NULL THEN ROW((invent).name, (invent).supplier_id, (invent).price) ELSE NULL END) FROM \"public\".\"weather\"").unwrap();
						// let type_ = stmt.columns()[0].type_().clone();
						// println!("{:?}", type_);
						// println!("{:?}", type_.kind());
						// let stmt = connection.execute("SELECT $1::weather", &[&A::default()]).unwrap();

						let mut vec = Vec::new();

						let writer = |row: Option<&[u8]>, _: &postgres::stmt::CopyInfo| {
							println!("{:?}", row);
							let row = Row::decode(&postgres::types::RECORD, row).unwrap();
							println!("{:?}", row);
							vec.push(Ok(row));
							Ok(())
						};

						let mut writer = postgres_binary_copy::BinaryCopyWriter::new(writer);

						let table = match table {
							Source::Table(table) => table.to_string(),
							Source::Query(query) => format!("({}) _", query),
						};
						let query = format!(
							"COPY (SELECT {} FROM {}) TO STDOUT (FORMAT BINARY)",
							DisplayFmt::new(|f| Row::query(f, None)),
							table
						);
						println!("{}", query);
						let stmt = connection.prepare(&query).unwrap();
						let _ = stmt.copy_out(&[], &mut writer).unwrap();
						vec.into_iter()
					}),
				)
			}));
		#[cfg(feature = "doc")]
		let ret = amadeus_core::util::ImplDistributedIterator::new(ret);
		ret
	}
	fn iter(self) -> Self::Iter {
		iter::empty()
	}
}

use std::io::Read;
pub fn read_be_i32(buf: &mut &[u8]) -> ::std::io::Result<i32> {
	let mut bytes = [0; 4];
	buf.read_exact(&mut bytes)?;
	let num = ((i32::from(bytes[0])) << 24)
		| ((i32::from(bytes[1])) << 16)
		| ((i32::from(bytes[2])) << 8)
		| (i32::from(bytes[3]));
	Ok(num)
}

pub fn read_value<T>(
	type_: &::postgres::types::Type, buf: &mut &[u8],
) -> Result<T, Box<dyn std::error::Error + Sync + Send>>
where
	T: PostgresData,
{
	let len = read_be_i32(buf)?;
	let value = if len < 0 {
		None
	} else {
		let len = usize::try_from(len)?;
		if len > buf.len() {
			return Err(Into::into("invalid buffer size"));
		}
		let (head, tail) = buf.split_at(len);
		*buf = tail;
		Some(&head[..])
	};
	T::decode(type_, value)
}

// https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
struct EscapeIdentifier<T>(T);
impl<T: Display> Display for EscapeIdentifier<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("\"")
			.and_then(|()| f.write_str(&self.0.to_string().replace('"', "\"\"")))
			.and_then(|()| f.write_str("\""))
	}
}

pub struct Names<'a>(pub Option<&'a Names<'a>>, pub &'static str);
impl<'a> Display for Names<'a> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if let Some(prev) = self.0 {
			f.write_str("(")
				.and_then(|()| prev.fmt(f))
				.and_then(|()| f.write_str(")."))?;
		}
		EscapeIdentifier(self.1).fmt(f)
	}
}

// select column_name, is_nullable, data_type, character_maximum_length, * from information_schema.columns where table_name = 'weather' order by ordinal_position;
// select attname, atttypid, atttypmod, attnotnull, attndims from pg_attribute where attrelid = 'public.weather'::regclass and attnum > 0 and not attisdropped;

mod misc_serde {
	use postgres::Error as PostgresError;
	use serde::{Deserialize, Deserializer, Serialize, Serializer};

	pub struct Serde<T>(T);

	impl Serialize for Serde<&postgres::params::User> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			(self.0.name(), self.0.password()).serialize(serializer)
		}
	}
	impl<'de> Deserialize<'de> for Serde<postgres::params::User> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			<(String, Option<String>)>::deserialize(deserializer).map(|(name, password)| {
				Self(
					postgres::params::ConnectParams::builder()
						.user(&name, password.as_deref())
						.build(postgres::params::Host::Tcp(String::new()))
						.user()
						.unwrap()
						.clone(),
				)
			})
		}
	}

	impl Serialize for Serde<&Option<postgres::params::User>> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			self.0.as_ref().map(Serde).serialize(serializer)
		}
	}
	impl<'de> Deserialize<'de> for Serde<Option<postgres::params::User>> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			<Option<Serde<postgres::params::User>>>::deserialize(deserializer)
				.map(|x| Self(x.map(|x| x.0)))
		}
	}

	impl Serialize for Serde<&PostgresError> {
		fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			panic!()
		}
	}
	impl<'de> Deserialize<'de> for Serde<PostgresError> {
		fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			panic!()
		}
	}

	pub fn serialize<T, S>(t: &T, serializer: S) -> Result<S::Ok, S::Error>
	where
		for<'a> Serde<&'a T>: Serialize,
		S: Serializer,
	{
		Serde(t).serialize(serializer)
	}
	pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
	where
		Serde<T>: Deserialize<'de>,
		D: Deserializer<'de>,
	{
		Serde::<T>::deserialize(deserializer).map(|x| x.0)
	}
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Error {
	Io(IoError),
	Postgres(#[serde(with = "misc_serde")] PostgresError),
}
impl PartialEq for Error {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Io(a), Self::Io(b)) => a.to_string() == b.to_string(),
			(Self::Postgres(a), Self::Postgres(b)) => a.to_string() == b.to_string(),
			_ => false,
		}
	}
}
impl error::Error for Error {}
impl Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Io(err) => Display::fmt(err, f),
			Self::Postgres(err) => Display::fmt(err, f),
		}
	}
}
impl From<io::Error> for Error {
	fn from(err: io::Error) -> Self {
		Self::Io(err.into())
	}
}
impl From<PostgresError> for Error {
	fn from(err: PostgresError) -> Self {
		Self::Postgres(err)
	}
}

struct DisplayFmt<F>(F)
where
	F: Fn(&mut fmt::Formatter) -> fmt::Result;
impl<F> DisplayFmt<F>
where
	F: Fn(&mut fmt::Formatter) -> fmt::Result,
{
	pub fn new(f: F) -> Self {
		Self(f)
	}
}
impl<F> Display for DisplayFmt<F>
where
	F: Fn(&mut fmt::Formatter) -> fmt::Result,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.0(f)
	}
}
