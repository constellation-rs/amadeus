use super::ResultExpand;
use crate::{
	data::Data, dist_iter::Consumer, into_dist_iter::IntoDistributedIterator, DistributedIterator, IteratorExt
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
	borrow::Cow, convert::identity, error, fmt::{self, Display}, fs::File, io::{self, BufRead, BufReader}, iter, marker::PhantomData, path::PathBuf, str, sync::Arc, time, vec
};
use walkdir::WalkDir;

use postgres::Error as PostgresError;

use crate::data::SerdeDeserialize;

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

type PostgresInner<Row> = crate::dist_iter::FlatMap<
	crate::into_dist_iter::IterIter<vec::IntoIter<(ConnectParams, Vec<Source>)>>,
	Closure<
		(),
		((ConnectParams, Vec<Source>),),
		iter::FlatMap<
			vec::IntoIter<Source>,
			vec::IntoIter<Result<Row, Error>>,
			Closure<(postgres::Connection,), (Source,), vec::IntoIter<Result<Row, Error>>>,
		>,
	>,
>;

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
		Table { schema, table }
	}
}

impl fmt::Display for Table {
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

use postgres::params::IntoConnectParams;

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
impl postgres::params::IntoConnectParams for ConnectParams {
	fn into_connect_params(
		self,
	) -> Result<postgres::params::ConnectParams, Box<std::error::Error + 'static + Send + Sync>> {
		let mut builder = postgres::params::ConnectParams::builder();
		builder.port(self.port);
		if let Some(user) = self.user {
			builder.user(user.name(), user.password());
		}
		if let Some(database) = self.database {
			builder.database(&database);
		}
		for (name, value) in self.options {
			builder.option(&name, &value);
		}
		builder.connect_timeout(self.connect_timeout);
		Ok(builder.build(self.host))
	}
}
impl str::FromStr for ConnectParams {
	type Err = Box<std::error::Error + 'static + Send + Sync>;

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
	Row: Data,
{
	i: PostgresInner<Row>,
}
impl<Row> Postgres<Row>
where
	Row: Data,
{
	pub fn new<I>(files: I) -> Result<Self, ()>
	where
		I: iter::IntoIterator<Item = (ConnectParams, Vec<Source>)>,
	{
		let i = files
			.into_iter()
			.collect::<Vec<_>>()
			.into_dist_iter()
			.flat_map(FnMut!(|(connect, tables)| {
				let (connect, tables): (ConnectParams,Vec<Source>) = (connect, tables);
				let connection = postgres::Connection::connect(connect, postgres::TlsMode::None).unwrap();
				tables.into_iter().flat_map(FnMut!([connection] move |table:Source| {
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
						let row = Row::postgres_decode(&postgres::types::RECORD, row).unwrap();
						println!("{:?}", row);
						vec.push(Ok(row));
						Ok(())
					};

					let mut writer = postgres_binary_copy::BinaryCopyWriter::new(writer);

					pub struct DisplayFmt<F>(F)
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

					let table = match table {
						Source::Table(table) => table.to_string(),
						Source::Query(query) => format!("({}) _", query),
					};
					let query = format!("COPY (SELECT {} FROM {}) TO STDOUT (FORMAT BINARY)", DisplayFmt::new(|f| Row::postgres_query(f, None)), table);
					println!("{}", query);
					let stmt = connection.prepare(&query).unwrap();
					stmt.copy_out(&[], &mut writer).unwrap();
					vec.into_iter()
				}))
			}));
		Ok(Postgres { i })
	}
}

use std::io::Read;
pub fn read_be_i32(buf: &mut &[u8]) -> ::std::io::Result<i32> {
	let mut bytes = [0; 4];
	buf.read_exact(&mut bytes)?;
	let num = ((bytes[0] as i32) << 24)
		| ((bytes[1] as i32) << 16)
		| ((bytes[2] as i32) << 8)
		| (bytes[3] as i32);
	Ok(num)
}

pub fn read_value<T>(
	type_: &::postgres::types::Type, buf: &mut &[u8],
) -> Result<T, Box<::std::error::Error + Sync + Send>>
where
	T: Data,
{
	let len = read_be_i32(buf)?;
	let value = if len < 0 {
		None
	} else {
		if len as usize > buf.len() {
			return Err(Into::into("invalid buffer size"));
		}
		let (head, tail) = buf.split_at(len as usize);
		*buf = tail;
		Some(&head[..])
	};
	T::postgres_decode(type_, value)
}

// https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
struct EscapeIdentifier<T>(T);
impl<T: Display> fmt::Display for EscapeIdentifier<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("\"")
			.and_then(|()| f.write_str(&self.0.to_string().replace('"', "\"\"")))
			.and_then(|()| f.write_str("\""))
	}
}

pub struct Names<'a>(pub Option<&'a Names<'a>>, pub &'static str);
impl<'a> fmt::Display for Names<'a> {
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
	use std::{io, sync::Arc};

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
				Serde(
					postgres::params::ConnectParams::builder()
						.user(&name, password.as_ref().map(|x| &**x))
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
				.map(|x| Serde(x.map(|x| x.0)))
		}
	}

	impl Serialize for Serde<&io::ErrorKind> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			usize::serialize(
				&match self.0 {
					io::ErrorKind::NotFound => 0,
					io::ErrorKind::PermissionDenied => 1,
					io::ErrorKind::ConnectionRefused => 2,
					io::ErrorKind::ConnectionReset => 3,
					io::ErrorKind::ConnectionAborted => 4,
					io::ErrorKind::NotConnected => 5,
					io::ErrorKind::AddrInUse => 6,
					io::ErrorKind::AddrNotAvailable => 7,
					io::ErrorKind::BrokenPipe => 8,
					io::ErrorKind::AlreadyExists => 9,
					io::ErrorKind::WouldBlock => 10,
					io::ErrorKind::InvalidInput => 11,
					io::ErrorKind::InvalidData => 12,
					io::ErrorKind::TimedOut => 13,
					io::ErrorKind::WriteZero => 14,
					io::ErrorKind::Interrupted => 15,
					io::ErrorKind::UnexpectedEof => 17,
					_ => 16,
				},
				serializer,
			)
		}
	}
	impl<'de> Deserialize<'de> for Serde<io::ErrorKind> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			usize::deserialize(deserializer)
				.map(|kind| match kind {
					0 => io::ErrorKind::NotFound,
					1 => io::ErrorKind::PermissionDenied,
					2 => io::ErrorKind::ConnectionRefused,
					3 => io::ErrorKind::ConnectionReset,
					4 => io::ErrorKind::ConnectionAborted,
					5 => io::ErrorKind::NotConnected,
					6 => io::ErrorKind::AddrInUse,
					7 => io::ErrorKind::AddrNotAvailable,
					8 => io::ErrorKind::BrokenPipe,
					9 => io::ErrorKind::AlreadyExists,
					10 => io::ErrorKind::WouldBlock,
					11 => io::ErrorKind::InvalidInput,
					12 => io::ErrorKind::InvalidData,
					13 => io::ErrorKind::TimedOut,
					14 => io::ErrorKind::WriteZero,
					15 => io::ErrorKind::Interrupted,
					17 => io::ErrorKind::UnexpectedEof,
					_ => io::ErrorKind::Other,
				})
				.map(Serde)
		}
	}

	impl Serialize for Serde<&Arc<io::Error>> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			<(Serde<&io::ErrorKind>, String)>::serialize(
				&(Serde(&self.0.kind()), self.0.to_string()),
				serializer,
			)
		}
	}
	impl<'de> Deserialize<'de> for Serde<Arc<io::Error>> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			<(Serde<io::ErrorKind>, String)>::deserialize(deserializer)
				.map(|(kind, message)| Arc::new(io::Error::new(kind.0, message)))
				.map(Serde)
		}
	}

	impl Serialize for Serde<&PostgresError> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			panic!()
		}
	}
	impl<'de> Deserialize<'de> for Serde<PostgresError> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
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
	Io(#[serde(with = "misc_serde")] Arc<io::Error>),
	Postgres(#[serde(with = "misc_serde")] PostgresError),
}
impl PartialEq for Error {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Error::Io(a), Error::Io(b)) => a.to_string() == b.to_string(),
			(Error::Postgres(a), Error::Postgres(b)) => a.to_string() == b.to_string(),
			_ => false,
		}
	}
}
impl error::Error for Error {}
impl Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Error::Io(err) => err.fmt(f),
			Error::Postgres(err) => err.fmt(f),
		}
	}
}
impl From<io::Error> for Error {
	fn from(err: io::Error) -> Self {
		Error::Io(Arc::new(err))
	}
}
impl From<PostgresError> for Error {
	fn from(err: PostgresError) -> Self {
		Error::Postgres(err)
	}
}

impl<Row> DistributedIterator for Postgres<Row>
where
	Row: Data,
{
	type Item = Result<Row, Error>;
	type Task = PostgresConsumer<Row>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| PostgresConsumer::<Row> {
			task,
			marker: PhantomData,
		})
	}
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct PostgresConsumer<Row>
where
	Row: Data,
{
	task: <PostgresInner<Row> as DistributedIterator>::Task,
	marker: PhantomData<fn() -> Row>,
}

impl<Row> Consumer for PostgresConsumer<Row>
where
	Row: Data,
{
	type Item = Result<Row, Error>;

	fn run(self, i: &mut impl FnMut(Result<Row, Error>) -> bool) -> bool {
		self.task.run(i)
	}
}
