//! Implement [`Record`] for [`Time`], [`Date`], and [`Timestamp`].

#![allow(clippy::trivially_copy_pass_by_ref)]

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
	convert::{TryFrom, TryInto}, fmt::{self, Display}
};

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
		if JULIAN_DAY_OF_EPOCH + i64::from(i32::min_value()) <= days
			&& days <= i64::from(i32::max_value())
		{
			Some(Self(days))
		} else {
			None
		}
	}
	/// Get the number of days since the Unix epoch
	pub fn as_days(&self) -> i64 {
		self.0
	}
}
impl From<NaiveDate> for Date {
	fn from(date: NaiveDate) -> Self {
		Self::from_days(i64::from(date.num_days_from_ce()) - GREGORIAN_DAY_OF_EPOCH).unwrap()
	}
}
// impl TryFrom<Date> for parchet::record::types::Date {
// 	type Error = ();

// 	fn try_from(date: Date) -> Result<Self, ()> {
// 		Ok(Self::from_days(date.0.try_into().map_err(|_| ())?))
// 	}
// }
impl TryFrom<Date> for NaiveDate {
	type Error = ();

	fn try_from(date: Date) -> Result<Self, ()> {
		Self::from_num_days_from_ce_opt(
			(date.0 + GREGORIAN_DAY_OF_EPOCH)
				.try_into()
				.map_err(|_| ())?,
		)
		.ok_or(())
	}
}
impl Serialize for Date {
	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		// <Self as SerdeData>::serialize(self, serializer)
		unimplemented!()
	}
}
impl<'de> Deserialize<'de> for Date {
	fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		// <Self as SerdeData>::deserialize(deserializer, None)
		unimplemented!()
	}
}
impl Display for Date {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		NaiveDate::try_from(*self)
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
		if millis < u32::try_from(SECONDS_PER_DAY * MILLIS_PER_SECOND).unwrap() {
			Some(Self(
				u64::from(millis)
					* u64::try_from(MICROS_PER_MILLI).unwrap()
					* u64::try_from(NANOS_PER_MICRO).unwrap(),
			))
		} else {
			None
		}
	}
	/// Create a Time from the number of microseconds since midnight
	pub fn from_micros(micros: u64) -> Option<Self> {
		if micros < u64::try_from(SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI).unwrap() {
			Some(Self(micros * u64::try_from(NANOS_PER_MICRO).unwrap()))
		} else {
			None
		}
	}
	/// Create a Time from the number of nanoseconds since midnight
	pub fn from_nanos(nanos: u64) -> Option<Self> {
		if nanos
			< u64::try_from(
				SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO,
			)
			.unwrap()
		{
			Some(Self(nanos))
		} else {
			None
		}
	}
	/// Get the number of milliseconds since midnight
	pub fn as_millis(&self) -> u32 {
		(self.0
			/ u64::try_from(NANOS_PER_MICRO).unwrap()
			/ u64::try_from(MICROS_PER_MILLI).unwrap())
		.try_into()
		.unwrap()
	}
	/// Get the number of microseconds since midnight
	pub fn as_micros(&self) -> u64 {
		self.0 / u64::try_from(NANOS_PER_MICRO).unwrap()
	}
	/// Get the number of microseconds since midnight
	pub fn as_nanos(&self) -> u64 {
		self.0
	}
}
impl From<NaiveTime> for Time {
	fn from(time: NaiveTime) -> Self {
		Self::from_nanos(
			u64::try_from(time.num_seconds_from_midnight()).unwrap()
				* u64::try_from(MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO).unwrap()
				+ u64::from(time.nanosecond()),
		)
		.unwrap()
	}
}
// impl TryFrom<Time> for parchet::record::types::Time {
// 	type Error = ();

// 	fn try_from(time: Time) -> Result<Self, Self::Error> {
// 		Ok(Self::from_micros(time.0 / u64::try_from(NANOS_PER_MICRO).unwrap()).unwrap())
// 	}
// }
impl TryFrom<Time> for NaiveTime {
	type Error = ();

	fn try_from(time: Time) -> Result<Self, Self::Error> {
		Ok(Self::from_num_seconds_from_midnight(
			(time.as_nanos()
				/ u64::try_from(NANOS_PER_MICRO * MICROS_PER_MILLI * MILLIS_PER_SECOND).unwrap())
			.try_into()
			.unwrap(),
			(time.as_nanos()
				% u64::try_from(NANOS_PER_MICRO * MICROS_PER_MILLI * MILLIS_PER_SECOND).unwrap())
			.try_into()
			.unwrap(),
		))
	}
}
impl Serialize for Time {
	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		// <Self as SerdeData>::serialize(self, serializer)
		unimplemented!()
	}
}
impl<'de> Deserialize<'de> for Time {
	fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		// <Self as SerdeData>::deserialize(deserializer, None)
		unimplemented!()
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
			Time::from_millis(millis.try_into().unwrap()).unwrap(),
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
			Time::from_micros(micros.try_into().unwrap()).unwrap(),
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
			Time::from_nanos(nanos.try_into().unwrap()).unwrap(),
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
				.checked_add(i64::try_from(self.time.as_millis()).unwrap())?,
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
				.checked_add(i64::try_from(self.time.as_micros()).unwrap())?,
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
				.checked_add(i64::try_from(self.time.as_nanos()).unwrap())?,
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
impl From<NaiveDateTime> for Timestamp {
	fn from(timestamp: NaiveDateTime) -> Self {
		Self::from_nanos(timestamp.timestamp_nanos())
	}
}
// impl TryFrom<Timestamp> for parchet::record::types::Timestamp {
// 	type Error = ();

// 	fn try_from(_timestamp: Timestamp) -> Result<Self, Self::Error> {
// 		// parchet::record::types::Timestamp::from_days(timestamp.0)
// 		unimplemented!()
// 	}
// }
impl TryFrom<Timestamp> for NaiveDateTime {
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
	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		// <Self as SerdeData>::serialize(self, serializer)
		unimplemented!()
	}
}
impl<'de> Deserialize<'de> for Timestamp {
	fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		// <Self as SerdeData>::deserialize(deserializer, None)
		unimplemented!()
	}
}
impl Display for Timestamp {
	// Datetime is displayed in local timezone.
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// let dt = Local.timestamp(self.as_millis().unwrap() / MILLIS_PER_SECOND, 0);
		// dt.format("%Y-%m-%d %H:%M:%S %:z").fmt(f)
		NaiveDateTime::try_from(*self)
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
			let date2 = Date::from(NaiveDate::try_from(date).unwrap());
			assert_eq!(date, date2);
		}

		check_date_conversion(-262_144, 1, 1);
		check_date_conversion(1969, 12, 31);
		check_date_conversion(1970, 1, 1);
		check_date_conversion(2010, 1, 2);
		check_date_conversion(2014, 5, 1);
		check_date_conversion(2016, 2, 29);
		check_date_conversion(2017, 9, 12);
		check_date_conversion(2018, 3, 31);
		check_date_conversion(262_143, 12, 31);
	}

	#[test]
	fn test_convert_time_to_string() {
		fn check_time_conversion(h: u32, mi: u32, s: u32) {
			let chrono_time = NaiveTime::from_hms(h, mi, s);
			let time = Time::from(chrono_time);
			assert_eq!(time.to_string(), chrono_time.to_string());
		}

		check_time_conversion(13, 12, 54);
		check_time_conversion(8, 23, 1);
		check_time_conversion(11, 6, 32);
		check_time_conversion(16, 38, 0);
		check_time_conversion(21, 15, 12);
	}

	#[test]
	fn test_convert_timestamp_to_string() {
		#[allow(clippy::many_single_char_names)]
		fn check_datetime_conversion(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) {
			let dt = NaiveDate::from_ymd(y, m, d).and_hms(h, mi, s);
			// let dt = Local.from_utc_datetime(&datetime);
			let res = Timestamp::from_millis(dt.timestamp_millis()).to_string();
			let exp = dt.to_string();
			assert_eq!(res, exp);
		}

		check_datetime_conversion(2010, 1, 2, 13, 12, 54);
		check_datetime_conversion(2011, 1, 3, 8, 23, 1);
		check_datetime_conversion(2012, 4, 5, 11, 6, 32);
		check_datetime_conversion(2013, 5, 12, 16, 38, 0);
		check_datetime_conversion(2014, 11, 28, 21, 15, 12);
	}
}
