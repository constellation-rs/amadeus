//! Implement [`Record`] for [`Time`], [`Date`], and [`Timestamp`].

use chrono::{Datelike, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use parquet::{
	basic::Repetition, column::reader::ColumnReader, errors::ParquetError, schema::types::{ColumnPath, Type}
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
	collections::HashMap, convert::{TryFrom, TryInto}, fmt::{self, Display}
};

use super::{super::Data, IntoReader, Schema};

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const SECONDS_PER_DAY: i64 = 86_400;
const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;
const NANOS_PER_MICRO: i64 = 1_000;
const GREGORIAN_DAY_OF_EPOCH: i64 = 719_163;

// Parquet's [Date logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date) is i32 days from Unix epoch
// Postgres https://www.postgresql.org/docs/11/datatype-datetime.html is 4713 BC to 5874897 AD
// MySQL https://dev.mysql.com/doc/refman/8.0/en/datetime.html is 1000-01-01 to 9999-12-31
// Chrono https://docs.rs/chrono/0.4.6/chrono/naive/struct.NaiveDate.html Jan 1, 262145 BCE to Dec 31, 262143 CE
// TODO: i33
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Date(i64);
impl Date {
	/// Create a Date from the number of days since the Unix epoch
	pub fn from_days(days: i64) -> Option<Self> {
		if JULIAN_DAY_OF_EPOCH + i32::min_value() as i64 <= days && days <= i32::max_value() as i64
		{
			Some(Date(days))
		} else {
			None
		}
	}
	/// Get the number of days since the Unix epoch
	pub fn as_days(&self) -> i64 {
		self.0
	}
}
impl Data for Date {
	type ParquetSchema = <parquet::record::types::Date as parquet::record::Record>::Schema;
	type ParquetReader =
		IntoReader<<parquet::record::types::Date as parquet::record::Record>::Reader, Self>;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn postgres_decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<std::error::Error + Sync + Send>> {
		if !<NaiveDate as ::postgres::types::FromSql>::accepts(type_) {
			return Err(Into::into("invalid type"));
		}
		<NaiveDate as ::postgres::types::FromSql>::from_sql(
			type_,
			buf.ok_or_else(|| Box::new(::postgres::types::WasNull))?,
		)
		.map(Into::into)
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		chrono::NaiveDate::try_from(*self)
			.expect("not implemented yet")
			.serialize(serializer)
	}
	fn serde_deserialize<'de, D>(deserializer: D, schema: Option<Schema>) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		chrono::NaiveDate::deserialize(deserializer).map(Into::into)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<parquet::record::types::Date as parquet::record::Record>::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		IntoReader::new(
			<parquet::record::types::Date as parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}
impl From<parquet::record::types::Date> for Date {
	fn from(date: parquet::record::types::Date) -> Self {
		Date::from_days(date.as_days().into()).unwrap()
	}
}
impl From<chrono::NaiveDate> for Date {
	fn from(date: chrono::NaiveDate) -> Self {
		Date::from_days(i64::from(date.num_days_from_ce()) - GREGORIAN_DAY_OF_EPOCH).unwrap()
	}
}
impl TryFrom<Date> for parquet::record::types::Date {
	type Error = ();

	fn try_from(date: Date) -> Result<Self, ()> {
		Ok(parquet::record::types::Date::from_days(
			date.0.try_into().map_err(|_| ())?,
		))
	}
}
impl TryFrom<Date> for chrono::NaiveDate {
	type Error = ();

	fn try_from(date: Date) -> Result<Self, ()> {
		NaiveDate::from_num_days_from_ce_opt(
			(date.0 + GREGORIAN_DAY_OF_EPOCH)
				.try_into()
				.map_err(|_| ())?,
		)
		.ok_or(())
	}
}
impl Serialize for Date {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		Self::serde_serialize(self, serializer)
	}
}
impl<'de> Deserialize<'de> for Date {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::serde_deserialize(deserializer, None)
	}
}
impl Display for Date {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		chrono::NaiveDate::try_from(*self)
			.expect("not implemented yet")
			.fmt(f)
	}
}

// Parquet [Time logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) number of microseconds since midnight
// Postgres https://www.postgresql.org/docs/11/datatype-datetime.html 00:00:00 to 24:00:00, no :60
// MySQL https://dev.mysql.com/doc/refman/8.0/en/time.html https://dev.mysql.com/doc/refman/5.7/en/time-zone-leap-seconds.html -838:59:59 to 838:59:59, no :60
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Time(u64);
impl Time {
	/// Create a Time from the number of milliseconds since midnight
	pub fn from_millis(millis: u32) -> Option<Self> {
		if millis < (SECONDS_PER_DAY * MILLIS_PER_SECOND) as u32 {
			Some(Time(
				millis as u64 * MICROS_PER_MILLI as u64 * NANOS_PER_MICRO as u64,
			))
		} else {
			None
		}
	}
	/// Create a Time from the number of microseconds since midnight
	pub fn from_micros(micros: u64) -> Option<Self> {
		if micros < (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI) as u64 {
			Some(Time(micros as u64 * NANOS_PER_MICRO as u64))
		} else {
			None
		}
	}
	/// Create a Time from the number of nanoseconds since midnight
	pub fn from_nanos(nanos: u64) -> Option<Self> {
		if nanos < (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO) as u64
		{
			Some(Time(nanos as u64))
		} else {
			None
		}
	}
	/// Get the number of milliseconds since midnight
	pub fn as_millis(&self) -> u32 {
		(self.0 / NANOS_PER_MICRO as u64 / MICROS_PER_MILLI as u64) as u32
	}
	/// Get the number of microseconds since midnight
	pub fn as_micros(&self) -> u64 {
		self.0 / NANOS_PER_MICRO as u64
	}
	/// Get the number of microseconds since midnight
	pub fn as_nanos(&self) -> u64 {
		self.0
	}
}
impl Data for Time {
	type ParquetSchema = <parquet::record::types::Time as parquet::record::Record>::Schema;
	type ParquetReader =
		IntoReader<<parquet::record::types::Time as parquet::record::Record>::Reader, Self>;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn postgres_decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<std::error::Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		chrono::NaiveTime::try_from(*self)
			.expect("not implemented yet")
			.serialize(serializer)
	}
	fn serde_deserialize<'de, D>(deserializer: D, schema: Option<Schema>) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		chrono::NaiveTime::deserialize(deserializer).map(Into::into)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<parquet::record::types::Time as parquet::record::Record>::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		IntoReader::new(
			<parquet::record::types::Time as parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}
impl From<parquet::record::types::Time> for Time {
	fn from(time: parquet::record::types::Time) -> Self {
		Time::from_nanos(time.as_micros() * NANOS_PER_MICRO as u64).unwrap()
	}
}
impl From<NaiveTime> for Time {
	fn from(time: NaiveTime) -> Self {
		Time::from_nanos(
			time.num_seconds_from_midnight() as u64
				* (MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO) as u64
				+ time.nanosecond() as u64,
		)
		.unwrap()
	}
}
impl TryFrom<Time> for parquet::record::types::Time {
	type Error = ();

	fn try_from(time: Time) -> Result<Self, Self::Error> {
		Ok(parquet::record::types::Time::from_micros(time.0 / NANOS_PER_MICRO as u64).unwrap())
	}
}
impl TryFrom<Time> for NaiveTime {
	type Error = ();

	fn try_from(time: Time) -> Result<Self, Self::Error> {
		Ok(NaiveTime::from_num_seconds_from_midnight(
			(time.as_nanos() / (NANOS_PER_MICRO * MICROS_PER_MILLI * MILLIS_PER_SECOND) as u64)
				as u32,
			(time.as_nanos() % (NANOS_PER_MICRO * MICROS_PER_MILLI * MILLIS_PER_SECOND) as u64)
				as u32,
		))
	}
}
impl Serialize for Time {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		Self::serde_serialize(self, serializer)
	}
}
impl<'de> Deserialize<'de> for Time {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::serde_deserialize(deserializer, None)
	}
}
impl Display for Time {
	// Input is a number of microseconds since midnight.
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// let dt = NaiveTime::from_num_seconds_from_midnight(
		// 	(self.0 as i64 / MICROS_PER_MILLI / MILLIS_PER_SECOND)
		// 		.try_into()
		// 		.unwrap(),
		// 	(self.0 as i64 % MICROS_PER_MILLI / MILLIS_PER_SECOND)
		// 		.try_into()
		// 		.unwrap(),
		// );
		// dt.format("%H:%M:%S").fmt(f)
		chrono::NaiveTime::try_from(*self)
			.expect("not implemented yet")
			.fmt(f)
	}
}

// [`Timestamp`] corresponds to the [Timestamp logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp).
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Timestamp {
	date: Date,
	time: Time,
}
impl Timestamp {
	/// Create a Timestamp from a [`Date`] and [`Time`].
	pub fn from_date_time(date: Date, time: Time) -> Option<Self> {
		Some(Self { date, time })
	}

	/// Create a Timestamp from the number of milliseconds since the Unix epoch
	pub fn from_millis(millis: i64) -> Self {
		let mut days = millis / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
		let mut millis = millis % (SECONDS_PER_DAY * MILLIS_PER_SECOND);
		if millis < 0 {
			days -= 1;
			millis += SECONDS_PER_DAY * MILLIS_PER_SECOND;
		}
		Self::from_date_time(
			Date::from_days(days).unwrap(),
			Time::from_millis(millis as u32).unwrap(),
		)
		.unwrap()
		// let day: i64 =
		//     JULIAN_DAY_OF_EPOCH + millis / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
		// let nanoseconds: i64 = (millis
		//     - ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND))
		//     * MICROS_PER_MILLI
		//     * NANOS_PER_MICRO;

		// Timestamp(Int96::from(vec![
		//     (nanoseconds & 0xffffffff).try_into().unwrap(),
		//     ((nanoseconds as u64) >> 32).try_into().unwrap(),
		//     day.try_into().unwrap(),
		// ]))
	}

	/// Create a Timestamp from the number of microseconds since the Unix epoch
	pub fn from_micros(micros: i64) -> Self {
		let mut days = micros / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
		let mut micros = micros % (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
		if micros < 0 {
			days -= 1;
			micros += SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI;
		}
		Self::from_date_time(
			Date::from_days(days).unwrap(),
			Time::from_micros(micros as u64).unwrap(),
		)
		.unwrap()
		// let day: i64 = JULIAN_DAY_OF_EPOCH
		//     + micros / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
		// let nanoseconds: i64 = (micros
		//     - ((day - JULIAN_DAY_OF_EPOCH)
		//         * SECONDS_PER_DAY
		//         * MILLIS_PER_SECOND
		//         * MICROS_PER_MILLI))
		//     * NANOS_PER_MICRO;

		// Timestamp(Int96::from(vec![
		//     (nanoseconds & 0xffffffff).try_into().unwrap(),
		//     ((nanoseconds as u64) >> 32).try_into().unwrap(),
		//     day.try_into().unwrap(),
		// ]))
	}

	/// Create a Timestamp from the number of nanoseconds since the Unix epoch
	pub fn from_nanos(nanos: i64) -> Self {
		let mut days =
			nanos / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO);
		let mut nanos =
			nanos % (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO);
		if nanos < 0 {
			days -= 1;
			nanos += SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO;
		}
		Self::from_date_time(
			Date::from_days(days).unwrap(),
			Time::from_nanos(nanos as u64).unwrap(),
		)
		.unwrap()
		// let day: i64 = JULIAN_DAY_OF_EPOCH
		//     + nanos
		//         / (SECONDS_PER_DAY
		//             * MILLIS_PER_SECOND
		//             * MICROS_PER_MILLI
		//             * NANOS_PER_MICRO);
		// let nanoseconds: i64 = nanos
		//     - ((day - JULIAN_DAY_OF_EPOCH)
		//         * SECONDS_PER_DAY
		//         * MILLIS_PER_SECOND
		//         * MICROS_PER_MILLI
		//         * NANOS_PER_MICRO);

		// Timestamp(Int96::from(vec![
		//     (nanoseconds & 0xffffffff).try_into().unwrap(),
		//     ((nanoseconds as u64) >> 32).try_into().unwrap(),
		//     day.try_into().unwrap(),
		// ]))
	}

	/// Get the [`Date`] and [`Time`] from this Timestamp.
	pub fn as_date_time(&self) -> (Date, Time) {
		(self.date, self.time)
	}

	// /// Get the number of days and nanoseconds since the Julian epoch
	// pub fn as_day_nanos(&self) -> (i64, i64) {
	//     let day = i64::from(self.0.data()[2]);
	//     let nanoseconds =
	//         (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
	//     (day, nanoseconds)
	// }

	/// Get the number of milliseconds since the Unix epoch
	pub fn as_millis(&self) -> Option<i64> {
		Some(
			self.date
				.as_days()
				.checked_mul(SECONDS_PER_DAY * MILLIS_PER_SECOND)?
				.checked_add(self.time.as_millis() as i64)?,
		)
		// let day = i64::from(self.0.data()[2]);
		// let nanoseconds =
		//     (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
		// let seconds = day
		//     .checked_sub(JULIAN_DAY_OF_EPOCH)?
		//     .checked_mul(SECONDS_PER_DAY)?;
		// Some(
		//     seconds.checked_mul(MILLIS_PER_SECOND)?.checked_add(
		//         nanoseconds
		//             .checked_div(NANOS_PER_MICRO)?
		//             .checked_div(MICROS_PER_MILLI)?,
		//     )?,
		// )
	}

	/// Get the number of microseconds since the Unix epoch
	pub fn as_micros(&self) -> Option<i64> {
		Some(
			self.date
				.as_days()
				.checked_mul(SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI)?
				.checked_add(self.time.as_micros() as i64)?,
		)
		// let day = i64::from(self.0.data()[2]);
		// let nanoseconds =
		//     (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
		// let seconds = day
		//     .checked_sub(JULIAN_DAY_OF_EPOCH)?
		//     .checked_mul(SECONDS_PER_DAY)?;
		// Some(
		//     seconds
		//         .checked_mul(MILLIS_PER_SECOND)?
		//         .checked_mul(MICROS_PER_MILLI)?
		//         .checked_add(nanoseconds.checked_div(NANOS_PER_MICRO)?)?,
		// )
	}

	/// Get the number of nanoseconds since the Unix epoch
	pub fn as_nanos(&self) -> Option<i64> {
		Some(
			self.date
				.as_days()
				.checked_mul(
					SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO,
				)?
				.checked_add(self.time.as_nanos() as i64)?,
		)
		// let day = i64::from(self.0.data()[2]);
		// let nanoseconds =
		//     (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
		// let seconds = day
		//     .checked_sub(JULIAN_DAY_OF_EPOCH)?
		//     .checked_mul(SECONDS_PER_DAY)?;
		// Some(
		//     seconds
		//         .checked_mul(MILLIS_PER_SECOND)?
		//         .checked_mul(MICROS_PER_MILLI)?
		//         .checked_mul(NANOS_PER_MICRO)?
		//         .checked_add(nanoseconds)?,
		// )
	}
}
impl Data for Timestamp {
	type ParquetSchema = <parquet::record::types::Timestamp as parquet::record::Record>::Schema;
	type ParquetReader =
		IntoReader<<parquet::record::types::Timestamp as parquet::record::Record>::Reader, Self>;

	fn postgres_query(
		f: &mut fmt::Formatter, name: Option<&crate::source::postgres::Names<'_>>,
	) -> fmt::Result {
		name.unwrap().fmt(f)
	}
	fn postgres_decode(
		type_: &::postgres::types::Type, buf: Option<&[u8]>,
	) -> Result<Self, Box<std::error::Error + Sync + Send>> {
		unimplemented!()
	}

	fn serde_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		chrono::NaiveDateTime::try_from(*self)
			.expect("not implemented yet")
			.serialize(serializer)
	}
	fn serde_deserialize<'de, D>(deserializer: D, schema: Option<Schema>) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		chrono::NaiveDateTime::deserialize(deserializer).map(Into::into)
	}

	fn parquet_parse(
		schema: &Type, repetition: Option<Repetition>,
	) -> Result<(String, Self::ParquetSchema), ParquetError> {
		<parquet::record::types::Timestamp as parquet::record::Record>::parse(schema, repetition)
	}
	fn parquet_reader(
		schema: &Self::ParquetSchema, path: &mut Vec<String>, def_level: i16, rep_level: i16,
		paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize,
	) -> Self::ParquetReader {
		IntoReader::new(
			<parquet::record::types::Timestamp as parquet::record::Record>::reader(
				schema, path, def_level, rep_level, paths, batch_size,
			),
		)
	}
}
impl From<parquet::record::types::Timestamp> for Timestamp {
	fn from(timestamp: parquet::record::types::Timestamp) -> Self {
		// Self::from_day_nanos(timestamp.as_nanos().unwrap())
		let (date, time) = timestamp.as_day_nanos();
		let date = Date::from_days(date - JULIAN_DAY_OF_EPOCH).unwrap();
		let time = Time::from_nanos(time.try_into().unwrap()).unwrap();
		Timestamp::from_date_time(date, time).unwrap()
	}
}
impl From<chrono::NaiveDateTime> for Timestamp {
	fn from(timestamp: chrono::NaiveDateTime) -> Self {
		Timestamp::from_nanos(timestamp.timestamp_nanos())
	}
}
impl TryFrom<Timestamp> for parquet::record::types::Timestamp {
	type Error = ();

	fn try_from(timestamp: Timestamp) -> Result<Self, Self::Error> {
		// parquet::record::types::Timestamp::from_days(timestamp.0)
		unimplemented!()
	}
}
impl TryFrom<Timestamp> for chrono::NaiveDateTime {
	type Error = ();

	fn try_from(timestamp: Timestamp) -> Result<Self, Self::Error> {
		Ok(NaiveDate::try_from(timestamp.date)?.and_time(timestamp.time.try_into()?))
	}
}
// impl From<chrono::DateTime<Utc>> for Timestamp {
// 	fn from(timestamp: chrono::DateTime<Utc>) -> Self {
// 		Timestamp::from_nanos(timestamp.timestamp_nanos())
// 	}
// }
// impl TryFrom<Timestamp> for chrono::DateTime<Utc> {
// 	type Error = ();

// 	fn try_from(timestamp: Timestamp) -> Result<Self, Self::Error> {
// 		Ok(Utc.timestamp(
// 			timestamp.as_millis().unwrap() / MILLIS_PER_SECOND,
// 			(timestamp.as_date_time().1.as_nanos() % (MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO) as u64)
// 				as u32,
// 		))
// 	}
// }
impl Serialize for Timestamp {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		Self::serde_serialize(self, serializer)
	}
}
impl<'de> Deserialize<'de> for Timestamp {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Self::serde_deserialize(deserializer, None)
	}
}
impl Display for Timestamp {
	// Datetime is displayed in local timezone.
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// let dt = Local.timestamp(self.as_millis().unwrap() / MILLIS_PER_SECOND, 0);
		// dt.format("%Y-%m-%d %H:%M:%S %:z").fmt(f)
		chrono::NaiveDateTime::try_from(*self)
			.expect("not implemented yet")
			.fmt(f)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use chrono::NaiveDate;

	// #[test]
	// fn test_int96() {
	// 	let value = Timestamp(Int96::from(vec![0, 0, 2454923]));
	// 	assert_eq!(value.as_millis().unwrap(), 1238544000000);

	// 	let value = Timestamp(Int96::from(vec![4165425152, 13, 2454923]));
	// 	assert_eq!(value.as_millis().unwrap(), 1238544060000);

	// 	let value = Timestamp(Int96::from(vec![0, 0, 0]));
	// 	assert_eq!(value.as_millis().unwrap(), -210866803200000);
	// }

	#[test]
	fn test_convert_date_to_string() {
		fn check_date_conversion(y: i32, m: u32, d: u32) {
			let chrono_date = NaiveDate::from_ymd(y, m, d);
			let chrono_datetime = chrono_date.and_hms(0, 0, 0);
			assert_eq!(chrono_datetime.timestamp() % SECONDS_PER_DAY, 0);
			let date = Date::from_days(chrono_datetime.timestamp() / SECONDS_PER_DAY).unwrap();
			assert_eq!(date.to_string(), chrono_date.to_string());
			let date2 = Date::from(chrono::NaiveDate::try_from(date).unwrap());
			assert_eq!(date, date2);
		}

		check_date_conversion(-262144, 01, 01);
		check_date_conversion(1969, 12, 31);
		check_date_conversion(1970, 01, 01);
		check_date_conversion(2010, 01, 02);
		check_date_conversion(2014, 05, 01);
		check_date_conversion(2016, 02, 29);
		check_date_conversion(2017, 09, 12);
		check_date_conversion(2018, 03, 31);
		check_date_conversion(262143, 12, 31);
	}

	#[test]
	fn test_convert_time_to_string() {
		fn check_time_conversion(h: u32, mi: u32, s: u32) {
			let chrono_time = NaiveTime::from_hms(h, mi, s);
			let time = Time::from(chrono_time);
			assert_eq!(time.to_string(), chrono_time.to_string());
		}

		check_time_conversion(13, 12, 54);
		check_time_conversion(08, 23, 01);
		check_time_conversion(11, 06, 32);
		check_time_conversion(16, 38, 00);
		check_time_conversion(21, 15, 12);
	}

	#[test]
	fn test_convert_timestamp_to_string() {
		fn check_datetime_conversion(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) {
			let dt = NaiveDate::from_ymd(y, m, d).and_hms(h, mi, s);
			// let dt = Local.from_utc_datetime(&datetime);
			let res = Timestamp::from_millis(dt.timestamp_millis()).to_string();
			let exp = dt.to_string();
			assert_eq!(res, exp);
		}

		check_datetime_conversion(2010, 01, 02, 13, 12, 54);
		check_datetime_conversion(2011, 01, 03, 08, 23, 01);
		check_datetime_conversion(2012, 04, 05, 11, 06, 32);
		check_datetime_conversion(2013, 05, 12, 16, 38, 00);
		check_datetime_conversion(2014, 11, 28, 21, 15, 12);
	}
}
