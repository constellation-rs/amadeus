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
impl fmt::Display for Table {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// unsafe{asm!("nop;int3;nop")};
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
		// TODO
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
				// let connect = connect.into_connect_params().unwrap();
				let connection = postgres::Connection::connect(connect, postgres::TlsMode::None).unwrap();
				tables.into_iter().flat_map(FnMut!([connection] move |table:Source| {
					let mut i = 0;

					use postgres::to_sql_checked;
					use postgres::types::{FromSql,ToSql};
					#[derive(Debug)]//, postgres_derive::FromSql, postgres_derive::ToSql)]
					// #[postgres(name = "row")]
					struct A {
						city: Option<String>,
						temp_lo: Option<i32>,
						temp_hi: Option<i32>,
						prcp: Option<f32>,
						date: Option<chrono::NaiveDate>,
						invent: Option<InventoryItem>,
					}
					#[derive(Default, Debug)]//, postgres_derive::FromSql, postgres_derive::ToSql)]
					// #[postgres(name = "inventory_item")]
					struct InventoryItem {
						name: Option<String>,
						supplier_id: Option<i32>,
						price: Option<f64>,
					}

					fn read_be_i32(buf: &mut &[u8]) -> ::std::io::Result<i32> {
						let mut bytes = [0; 4];
						::std::io::Read::read_exact(buf, &mut bytes)?;
						let num = ((bytes[0] as i32) << 24) |
							((bytes[1] as i32) << 16) |
							((bytes[2] as i32) << 8) |
							(bytes[3] as i32);
						Ok(num)
					}

					fn read_value<T>(type_: &::postgres::types::Type,
									 buf: &mut &[u8])
									 -> Result<T,
										 ::std::boxed::Box<::std::error::Error +
										 ::std::marker::Sync +
										 ::std::marker::Send>>
									 where T: ::postgres::types::FromSql
					{
						if !T::accepts(type_) {
							return Err(
								Into::into("invalid type"));
						}
						let len = read_be_i32(buf)?;
						let value = if len < 0 {
							None
						} else {
							if len as usize > buf.len() {
								return Err(
									Into::into("invalid buffer size"));
							}
							let (head, tail) = buf.split_at(len as usize);
							*buf = tail;
							Some(&head[..])
						};
						::postgres::types::FromSql::from_sql_nullable(type_, value)
					}
					impl ::postgres::types::FromSql for A {
						fn from_sql(type_: &::postgres::types::Type,
									buf: &[u8])
									-> Result<Self,
															 ::std::boxed::Box<::std::error::Error +
																			   ::std::marker::Sync +
																			   ::std::marker::Send>> {
							assert!(Self::accepts(type_));
							// let fields = match *_type.kind() {
							// 	::postgres::types::Kind::Composite(ref fields) => fields,
							// 	_ => unreachable!(),
							// };

							let mut buf = buf;
							let num_fields = read_be_i32(&mut buf)?;
							if num_fields as usize != 6 {
								return Err(
									Into::into(format!("invalid field count: {} vs {}", num_fields,
																	   6)));
							}

							// #(
							// 	let mut #temp_vars = None;
							// )*

							// for _ in 0..6 {
							// 	let oid = read_be_i32(&mut buf)? as u32;
							// 	if oid != field.type_().oid() {
							// 		return Err(Into::into("unexpected OID"));
							// 	}

							// 	match field.name() {
							// 		#(
							// 			#field_names => {
							// 				#temp_vars = Some(
							// 					read_value(field.type_(), &mut buf)?);
							// 			}
							// 		)*
							// 		_ => unreachable!(),
							// 	}
							// }

							Ok(Self {
								// #(
									city: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
									temp_lo: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
									temp_hi: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
									prcp: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
									date: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
									invent: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
								// )*
							})
						}
						fn accepts(type_: &::postgres::types::Type) -> bool {
							type_ == &postgres::types::RECORD
						}
					}
					impl ::postgres::types::FromSql for InventoryItem {
						fn from_sql(type_: &::postgres::types::Type,
									buf: &[u8])
									-> Result<Self,
															 ::std::boxed::Box<::std::error::Error +
																			   ::std::marker::Sync +
																			   ::std::marker::Send>> {
							assert!(Self::accepts(type_));

							let mut buf = buf;
							let num_fields = read_be_i32(&mut buf)?;
							if num_fields as usize != 3 {
								return Err(
									Into::into(format!("invalid field count: {} vs {}", num_fields,
																	   3)));
							}

							Ok(Self {
								// #(
									name: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
									supplier_id: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
									price: {
										let oid = read_be_i32(&mut buf)? as u32;
										read_value(&postgres::types::Type::from_oid(oid).unwrap_or(postgres::types::OPAQUE), &mut buf)?
									},
								// )*
							})
						}
						fn accepts(type_: &::postgres::types::Type) -> bool {
							type_ == &postgres::types::RECORD
						}
					}
					// let stmt = connection.prepare("SELECT $1::\"public\".\"weather\"").unwrap();
					// let type_ = stmt.param_types()[0].clone();
					// let stmt = connection.prepare("SELECT ROW(city, temp_lo, temp_hi, prcp, date, CASE WHEN invent IS NOT NULL THEN ROW((invent).name, (invent).supplier_id, (invent).price) ELSE NULL END) FROM \"public\".\"weather\"").unwrap();
					// let type_ = stmt.columns()[0].type_().clone();
					// println!("{:?}", type_);
					// println!("{:?}", type_.kind());
					// let stmt = connection.execute("SELECT $1::weather", &[&A::default()]).unwrap();
					// let mut vec = Vec::new();
					let writer = |r: Option<&[u8]>, a: &postgres::stmt::CopyInfo| {
						// vec.extend
						// i += 1;
						// if i == 6 {
						// 	;
						// }
						println!("{:?}", r);
						// let mut vec = Vec::new();
						// A{
						// 	city: Some(String::from("london")),
						// 	temp_lo: Some(10),
						// 	temp_hi: Some(10),
						// 	prcp: Some(10.0),
						// 	date: None,
						// 	invent: None,
						// }.to_sql(&type_,&mut vec);
						// println!("{:?}", vec);
						let type_ = postgres::types::RECORD;
						let row = A::from_sql(&type_, r.unwrap()).unwrap();
						println!("{:?}", row);
						// match r {
						// 	Some(r) => out.push(Option::<i32>::from_sql(&INT4, r).unwrap()),
						// 	None => out.push(Option::<i32>::from_sql_null(&INT4).unwrap()),
						// }
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 255, 255, 255, 255, 0, 0, 64, 15, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 255, 255, 255, 255, 0, 0, 64, 15, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 0, 0, 0, 4, 0, 0, 17, 31, 0, 0, 64, 15, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 0, 0, 0, 4, 0, 0, 17, 31, 0, 0, 64, 15, 0, 0, 0, 41, 0, 0, 0, 3, 0, 0, 0, 25, 0, 0, 0, 1, 97, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 189, 0, 0, 0, 8, 64, 36, 0, 0, 0, 0, 0, 0])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 255, 255, 255, 255, 0, 0, 8, 201, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 255, 255, 255, 255, 0, 0, 8, 201, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 0, 0, 0, 4, 0, 0, 17, 31, 0, 0, 8, 201, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 0, 0, 0, 4, 0, 0, 17, 31, 0, 0, 8, 201, 0, 0, 0, 41, 0, 0, 0, 3, 0, 0, 0, 25, 0, 0, 0, 1, 97, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 189, 0, 0, 0, 8, 64, 36, 0, 0, 0, 0, 0, 0])

// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 255, 255, 255, 255, 0, 0, 8, 201, 0, 0, 0, 28, 0, 0, 0, 3, 0, 0, 0, 25, 255, 255, 255, 255, 0, 0, 0, 23, 255, 255, 255, 255, 0, 0, 2, 189, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 255, 255, 255, 255, 0, 0, 8, 201, 0, 0, 0, 28, 0, 0, 0, 3, 0, 0, 0, 25, 255, 255, 255, 255, 0, 0, 0, 23, 255, 255, 255, 255, 0, 0, 2, 189, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 0, 0, 0, 4, 0, 0, 17, 31, 0, 0, 8, 201, 0, 0, 0, 28, 0, 0, 0, 3, 0, 0, 0, 25, 255, 255, 255, 255, 0, 0, 0, 23, 255, 255, 255, 255, 0, 0, 2, 189, 255, 255, 255, 255])
// Some([0, 0, 0, 6, 0, 0, 4, 19, 0, 0, 0, 6, 108, 111, 110, 100, 111, 110, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 188, 0, 0, 0, 4, 65, 32, 0, 0, 0, 0, 4, 58, 0, 0, 0, 4, 0, 0, 17, 31, 0, 0, 8, 201, 0, 0, 0, 41, 0, 0, 0, 3, 0, 0, 0, 25, 0, 0, 0, 1, 97, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 2, 189, 0, 0, 0, 8, 64, 36, 0, 0, 0, 0, 0, 0])
						Ok(())
					};

					let mut writer = postgres_binary_copy::BinaryCopyWriter::new(writer);

					impl Abc for A {
						fn abc(buf: &mut String, name: Option<&Names<'_>>) {
							if let Some(name) = name {
								buf.push_str("CASE WHEN ");
								name.print(buf);
								buf.push_str(" IS NOT NULL THEN ROW(");
								<Option<String> as Abc>::abc(buf, Some(&Names(Some(name), "city")));
								buf.push_str(",");
								<Option<i32> as Abc>::abc(buf, Some(&Names(Some(name), "temp_lo")));
								buf.push_str(",");
								<Option<i32> as Abc>::abc(buf, Some(&Names(Some(name), "temp_hi")));
								buf.push_str(",");
								<Option<f32> as Abc>::abc(buf, Some(&Names(Some(name), "prcp")));
								buf.push_str(",");
								<Option<chrono::NaiveDate> as Abc>::abc(buf, Some(&Names(Some(name), "date")));
								buf.push_str(",");
								<Option<InventoryItem> as Abc>::abc(buf, Some(&Names(Some(name), "invent")));
								buf.push_str(") ELSE NULL END");
							} else {
								buf.push_str("ROW(");
								<Option<String> as Abc>::abc(buf, Some(&Names(name, "city")));
								buf.push_str(",");
								<Option<i32> as Abc>::abc(buf, Some(&Names(name, "temp_lo")));
								buf.push_str(",");
								<Option<i32> as Abc>::abc(buf, Some(&Names(name, "temp_hi")));
								buf.push_str(",");
								<Option<f32> as Abc>::abc(buf, Some(&Names(name, "prcp")));
								buf.push_str(",");
								<Option<chrono::NaiveDate> as Abc>::abc(buf, Some(&Names(name, "date")));
								buf.push_str(",");
								<Option<InventoryItem> as Abc>::abc(buf, Some(&Names(name, "invent")));
								buf.push_str(")");
							}
						}
					}
					impl<T> Abc for Option<T> where T: ::postgres::types::FromSql {
						fn abc(buf: &mut String, name: Option<&Names<'_>>) {
							T::abc(buf, name)
						}
					}
					impl Abc for InventoryItem {
						fn abc(buf: &mut String, name: Option<&Names<'_>>) {
							if let Some(name) = name {
								buf.push_str("CASE WHEN ");
								name.print(buf);
								buf.push_str(" IS NOT NULL THEN ROW(");
								<Option<String> as Abc>::abc(buf, Some(&Names(Some(name), "name")));
								buf.push_str(",");
								<Option<i32> as Abc>::abc(buf, Some(&Names(Some(name), "supplier_id")));
								buf.push_str(",");
								<Option<f64> as Abc>::abc(buf, Some(&Names(Some(name), "price")));
								buf.push_str(") ELSE NULL END");
							} else {
								buf.push_str("ROW(");
								<Option<String> as Abc>::abc(buf, Some(&Names(name, "name")));
								buf.push_str(",");
								<Option<i32> as Abc>::abc(buf, Some(&Names(name, "supplier_id")));
								buf.push_str(",");
								<Option<f64> as Abc>::abc(buf, Some(&Names(name, "price")));
								buf.push_str(")");
							}
						}
					}

					let mut string = String::new();
					A::abc(&mut string, None);
					let table = match table {
						Source::Table(table) => table.to_string(),
						Source::Query(query) => format!("({}) _", query),
					};
					let query = format!("COPY (SELECT {} FROM {}) TO STDOUT (FORMAT BINARY)", string, table);
					println!("{}", query);
					let stmt = connection.prepare(&query).unwrap();
					stmt.copy_out(&[], &mut writer).unwrap();
					Vec::<Result<Row,Error>>::new().into_iter()
				}))
			}));
		Ok(Postgres { i })
	}
}

// https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
struct EscapeIdentifier<T>(T);
impl<T: Print> Print for EscapeIdentifier<T> {
	fn print(&self, f: &mut impl fmt::Write) -> fmt::Result {
		let mut inner = String::new();
		self.0.print(&mut inner)?;
		f.write_str("\"")
			.and_then(|()| f.write_str(&inner.replace('"', "\"\"")))
			.and_then(|()| f.write_str("\""))
	}
}
impl<T: Display> fmt::Display for EscapeIdentifier<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("\"")
			.and_then(|()| f.write_str(&self.0.to_string().replace('"', "\"\"")))
			.and_then(|()| f.write_str("\""))
	}
}
trait Print {
	fn print(&self, f: &mut impl fmt::Write) -> fmt::Result;
}

impl<'a, T: Print> Print for &'a T {
	fn print(&self, f: &mut impl fmt::Write) -> fmt::Result {
		(**self).print(f)
	}
}

impl<'a> Print for &'a str {
	fn print(&self, f: &mut impl fmt::Write) -> fmt::Result {
		f.write_str(self)
	}
}

struct Names<'a>(Option<&'a Names<'a>>, &'static str);
impl<'a> Print for Names<'a> {
	fn print(&self, mut f: &mut impl fmt::Write) -> fmt::Result {
		if let Some(prev) = self.0 {
			f.write_str("(")
				.and_then(|()| prev.print(f))
				.and_then(|()| f.write_str(")."))?;
		}
		EscapeIdentifier(self.1).print(f)
	}
}
trait Abc: postgres::types::FromSql {
	fn abc(buf: &mut String, name: Option<&Names<'_>>) {
		name.unwrap().print(buf).unwrap();
	}
}
impl<T> Abc for T where T: postgres::types::FromSql {}

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
