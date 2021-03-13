//! Implement [`Record`] for [`Time`], [`Date`], and [`DateTime`].

#![allow(clippy::trivially_copy_pass_by_ref)]

use chrono::{
	offset::{Offset, TimeZone}, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc
};
use serde::{Deserialize, Serialize};
use std::{
	cmp::Ordering, convert::TryInto, error::Error, fmt::{self, Display}, str::FromStr
};

use super::AmadeusOrd;

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const GREGORIAN_DAY_OF_EPOCH: i64 = 719_163;

const TODO: &str = "not implemented yet";

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ParseDateError;
impl Display for ParseDateError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "error parsing date")
	}
}
impl Error for ParseDateError {}

/// A timezone. It can have a varying offset, like `Europe/London`, or fixed like `GMT+1`.
// Tz with a FixedOffset: https://github.com/chronotope/chrono-tz/issues/11
#[derive(Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct Timezone {
	inner: TimezoneInner,
}
#[derive(Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, Debug)]
enum TimezoneInner {
	Variable(chrono_tz::Tz), // variable offsets
	Fixed(i32), // fixed offsets // can wrap FixedOffset on https://github.com/chronotope/chrono/issues/309
}
impl Timezone {
	pub const UTC: Self = Self {
		inner: TimezoneInner::Fixed(0),
	};
	pub const GMT: Self = Self {
		inner: TimezoneInner::Fixed(0),
	};
	pub const GMT_MINUS_1: Self = Self {
		inner: TimezoneInner::Fixed(-60 * 60),
	};
	pub const GMT_MINUS_2: Self = Self {
		inner: TimezoneInner::Fixed(-2 * 60 * 60),
	};
	pub const GMT_MINUS_3: Self = Self {
		inner: TimezoneInner::Fixed(-3 * 60 * 60),
	};
	pub const GMT_MINUS_4: Self = Self {
		inner: TimezoneInner::Fixed(-4 * 60 * 60),
	};
	pub const GMT_MINUS_5: Self = Self {
		inner: TimezoneInner::Fixed(-5 * 60 * 60),
	};
	pub const GMT_MINUS_6: Self = Self {
		inner: TimezoneInner::Fixed(-6 * 60 * 60),
	};
	pub const GMT_MINUS_7: Self = Self {
		inner: TimezoneInner::Fixed(-7 * 60 * 60),
	};
	pub const GMT_MINUS_8: Self = Self {
		inner: TimezoneInner::Fixed(-8 * 60 * 60),
	};
	pub const GMT_MINUS_9: Self = Self {
		inner: TimezoneInner::Fixed(-9 * 60 * 60),
	};
	pub const GMT_MINUS_10: Self = Self {
		inner: TimezoneInner::Fixed(-10 * 60 * 60),
	};
	pub const GMT_MINUS_11: Self = Self {
		inner: TimezoneInner::Fixed(-11 * 60 * 60),
	};
	pub const GMT_MINUS_12: Self = Self {
		inner: TimezoneInner::Fixed(-12 * 60 * 60),
	};
	pub const GMT_PLUS_1: Self = Self {
		inner: TimezoneInner::Fixed(60 * 60),
	};
	pub const GMT_PLUS_2: Self = Self {
		inner: TimezoneInner::Fixed(2 * 60 * 60),
	};
	pub const GMT_PLUS_3: Self = Self {
		inner: TimezoneInner::Fixed(3 * 60 * 60),
	};
	pub const GMT_PLUS_4: Self = Self {
		inner: TimezoneInner::Fixed(4 * 60 * 60),
	};
	pub const GMT_PLUS_5: Self = Self {
		inner: TimezoneInner::Fixed(4 * 60 * 60),
	};
	pub const GMT_PLUS_6: Self = Self {
		inner: TimezoneInner::Fixed(5 * 60 * 60),
	};
	pub const GMT_PLUS_7: Self = Self {
		inner: TimezoneInner::Fixed(6 * 60 * 60),
	};
	pub const GMT_PLUS_8: Self = Self {
		inner: TimezoneInner::Fixed(7 * 60 * 60),
	};
	pub const GMT_PLUS_9: Self = Self {
		inner: TimezoneInner::Fixed(8 * 60 * 60),
	};
	pub const GMT_PLUS_10: Self = Self {
		inner: TimezoneInner::Fixed(10 * 60 * 60),
	};
	pub const GMT_PLUS_11: Self = Self {
		inner: TimezoneInner::Fixed(11 * 60 * 60),
	};
	pub const GMT_PLUS_12: Self = Self {
		inner: TimezoneInner::Fixed(12 * 60 * 60),
	};
	pub const GMT_PLUS_13: Self = Self {
		inner: TimezoneInner::Fixed(13 * 60 * 60),
	};
	pub const GMT_PLUS_14: Self = Self {
		inner: TimezoneInner::Fixed(14 * 60 * 60),
	};

	/// Create a new Timezone from a name in the [IANA Database](https://www.iana.org/time-zones).
	pub fn from_name(name: &str) -> Option<Self> {
		use chrono_tz::*;
		#[rustfmt::skip]
		#[allow(non_upper_case_globals)] // https://github.com/rust-lang/rust/issues/25207
		let inner = match name.parse().ok()? {
			UTC | UCT | Universal | Zulu | GMT | GMT0 | GMTMinus0 | GMTPlus0 | Greenwich |
			Etc::UTC | Etc::UCT | Etc::Universal | Etc::Zulu | Etc::GMT | Etc::GMT0 | Etc::GMTMinus0 | Etc::GMTPlus0 | Etc::Greenwich => {
				TimezoneInner::Fixed(0)
			}
			Etc::GMTPlus1 => TimezoneInner::Fixed(-60 * 60),
			Etc::GMTPlus2 => TimezoneInner::Fixed(-2 * 60 * 60),
			Etc::GMTPlus3 => TimezoneInner::Fixed(-3 * 60 * 60),
			Etc::GMTPlus4 => TimezoneInner::Fixed(-4 * 60 * 60),
			Etc::GMTPlus5 => TimezoneInner::Fixed(-5 * 60 * 60),
			Etc::GMTPlus6 => TimezoneInner::Fixed(-6 * 60 * 60),
			Etc::GMTPlus7 => TimezoneInner::Fixed(-7 * 60 * 60),
			Etc::GMTPlus8 => TimezoneInner::Fixed(-8 * 60 * 60),
			Etc::GMTPlus9 => TimezoneInner::Fixed(-9 * 60 * 60),
			Etc::GMTPlus10 => TimezoneInner::Fixed(-10 * 60 * 60),
			Etc::GMTPlus11 => TimezoneInner::Fixed(-11 * 60 * 60),
			Etc::GMTPlus12 => TimezoneInner::Fixed(-12 * 60 * 60),
			Etc::GMTMinus1 => TimezoneInner::Fixed(60 * 60),
			Etc::GMTMinus2 => TimezoneInner::Fixed(2 * 60 * 60),
			Etc::GMTMinus3 => TimezoneInner::Fixed(3 * 60 * 60),
			Etc::GMTMinus4 => TimezoneInner::Fixed(4 * 60 * 60),
			Etc::GMTMinus5 => TimezoneInner::Fixed(5 * 60 * 60),
			Etc::GMTMinus6 => TimezoneInner::Fixed(6 * 60 * 60),
			Etc::GMTMinus7 => TimezoneInner::Fixed(7 * 60 * 60),
			Etc::GMTMinus8 => TimezoneInner::Fixed(8 * 60 * 60),
			Etc::GMTMinus9 => TimezoneInner::Fixed(9 * 60 * 60),
			Etc::GMTMinus10 => TimezoneInner::Fixed(10 * 60 * 60),
			Etc::GMTMinus11 => TimezoneInner::Fixed(11 * 60 * 60),
			Etc::GMTMinus12 => TimezoneInner::Fixed(12 * 60 * 60),
			Etc::GMTMinus13 => TimezoneInner::Fixed(13 * 60 * 60),
			Etc::GMTMinus14 => TimezoneInner::Fixed(14 * 60 * 60),
			tz => TimezoneInner::Variable(tz),
		};
		Some(Self { inner })
	}
	/// Get the name of the timezone as in the [IANA Database](https://www.iana.org/time-zones). It might differ from (although still be equivalent to) the name given to `from_name`.
	pub fn as_name(&self) -> Option<&'static str> {
		use chrono_tz::*;
		match self.inner {
			TimezoneInner::Variable(tz) => Some(tz),
			TimezoneInner::Fixed(offset) => match offset {
				0 => Some(Etc::GMT),
				-3600 => Some(Etc::GMTPlus1),
				-7200 => Some(Etc::GMTPlus2),
				-10800 => Some(Etc::GMTPlus3),
				-14400 => Some(Etc::GMTPlus4),
				-18000 => Some(Etc::GMTPlus5),
				-21600 => Some(Etc::GMTPlus6),
				-25200 => Some(Etc::GMTPlus7),
				-28800 => Some(Etc::GMTPlus8),
				-32400 => Some(Etc::GMTPlus9),
				-36000 => Some(Etc::GMTPlus10),
				-39600 => Some(Etc::GMTPlus11),
				-43200 => Some(Etc::GMTPlus12),
				3600 => Some(Etc::GMTMinus1),
				7200 => Some(Etc::GMTMinus2),
				10800 => Some(Etc::GMTMinus3),
				14400 => Some(Etc::GMTMinus4),
				18000 => Some(Etc::GMTMinus5),
				21600 => Some(Etc::GMTMinus6),
				25200 => Some(Etc::GMTMinus7),
				28800 => Some(Etc::GMTMinus8),
				32400 => Some(Etc::GMTMinus9),
				36000 => Some(Etc::GMTMinus10),
				39600 => Some(Etc::GMTMinus11),
				43200 => Some(Etc::GMTMinus12),
				46800 => Some(Etc::GMTMinus13),
				50400 => Some(Etc::GMTMinus14),
				_ => None,
			},
		}
		.map(Tz::name)
	}
	/// Makes a new Timezone for the Eastern Hemisphere with given timezone difference. The negative seconds means the Western Hemisphere.
	pub fn from_offset(seconds: i32) -> Option<Self> {
		FixedOffset::east_opt(seconds).map(|offset| Self {
			inner: TimezoneInner::Fixed(offset.local_minus_utc()),
		})
	}
	/// Returns the number of seconds to add to convert from UTC to the local time.
	pub fn as_offset(&self) -> Option<i32> {
		match self.inner {
			TimezoneInner::Variable(_tz) => None,
			TimezoneInner::Fixed(seconds) => Some(seconds),
		}
	}
	/// Returns the number of seconds to add to convert from UTC to the local time.
	pub fn as_offset_at(&self, utc_date_time: &DateTime) -> i32 {
		assert_eq!(utc_date_time.timezone, Self::UTC);
		match self.inner {
			TimezoneInner::Variable(tz) => tz
				.offset_from_utc_datetime(&utc_date_time.date_time.as_chrono().expect(TODO))
				.fix()
				.local_minus_utc(),
			TimezoneInner::Fixed(seconds) => seconds,
		}
	}
	#[doc(hidden)]
	pub fn from_chrono<Tz>(_timezone: &Tz, offset: &Tz::Offset) -> Self
	where
		Tz: TimeZone,
	{
		Self::from_offset(offset.fix().local_minus_utc()).unwrap() // TODO: this loses variable timezone
	}
	#[doc(hidden)]
	pub fn as_chrono(&self) -> ChronoTimezone {
		ChronoTimezone(*self)
	}
}
impl PartialOrd for Timezone {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(Ord::cmp(self, other))
	}
}
impl Ord for Timezone {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self.inner, other.inner) {
			(TimezoneInner::Variable(a), TimezoneInner::Variable(b)) => (a as u32).cmp(&(b as u32)),
			(TimezoneInner::Fixed(a), TimezoneInner::Fixed(b)) => a.cmp(&b),
			(TimezoneInner::Variable(_), _) => Ordering::Less,
			(TimezoneInner::Fixed(_), _) => Ordering::Greater,
		}
	}
}
impl AmadeusOrd for Timezone {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
	}
}
impl Display for Timezone {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.as_chrono().fmt(f)
	}
}
impl FromStr for Timezone {
	type Err = ParseDateError;

	fn from_str(_s: &str) -> Result<Self, Self::Err> {
		unimplemented!()
	}
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ChronoTimezone(Timezone);
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ChronoTimezoneOffset(Timezone, FixedOffset);
impl TimeZone for ChronoTimezone {
	type Offset = ChronoTimezoneOffset;

	fn from_offset(offset: &Self::Offset) -> Self {
		ChronoTimezone(offset.0)
	}
	fn offset_from_local_date(&self, _local: &NaiveDate) -> chrono::LocalResult<Self::Offset> {
		unimplemented!()
	}
	fn offset_from_local_datetime(
		&self, _local: &NaiveDateTime,
	) -> chrono::LocalResult<Self::Offset> {
		unimplemented!()
	}
	fn offset_from_utc_date(&self, _utc: &NaiveDate) -> Self::Offset {
		unimplemented!()
	}
	fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
		ChronoTimezoneOffset(
			self.0,
			FixedOffset::east(self.0.as_offset_at(&DateTime::from_chrono(
				&chrono::DateTime::<Utc>::from_utc(*utc, Utc),
			))),
		)
	}
}
impl Offset for ChronoTimezoneOffset {
	fn fix(&self) -> FixedOffset {
		self.1
	}
}
impl Display for ChronoTimezone {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.0.inner {
			TimezoneInner::Variable(tz) => f.write_str(tz.name()),
			TimezoneInner::Fixed(offset) => Display::fmt(&FixedOffset::east(offset), f),
		}
	}
}
impl Display for ChronoTimezoneOffset {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(&ChronoTimezone(self.0), f)
	}
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct Date {
	date: DateWithoutTimezone, // UTC
	timezone: Timezone,
}
impl Date {
	pub fn new(year: i64, month: u8, day: u8, timezone: Timezone) -> Option<Self> {
		DateWithoutTimezone::new(year, month, day).map(|date| Self { date, timezone })
	}
	pub fn from_ordinal(year: i64, day: u16, timezone: Timezone) -> Option<Self> {
		DateWithoutTimezone::from_ordinal(year, day).map(|date| Self { date, timezone })
	}
	pub fn year(&self) -> i64 {
		self.date.year()
	}
	pub fn month(&self) -> u8 {
		self.date.month()
	}
	pub fn day(&self) -> u8 {
		self.date.day()
	}
	pub fn ordinal(&self) -> u16 {
		self.date.ordinal()
	}
	pub fn without_timezone(&self) -> DateWithoutTimezone {
		self.date
	}
	pub fn timezone(&self) -> Timezone {
		self.timezone
	}
	/// Create a DateWithoutTimezone from the number of days since the Unix epoch
	pub fn from_days(days: i64, timezone: Timezone) -> Option<Self> {
		DateWithoutTimezone::from_days(days).map(|date| Self { date, timezone })
	}
	/// Get the number of days since the Unix epoch
	pub fn as_days(&self) -> i64 {
		self.date.as_days()
	}
	#[doc(hidden)]
	pub fn from_chrono<Tz>(date: &chrono::Date<Tz>) -> Self
	where
		Tz: TimeZone,
	{
		Self::new(
			date.year().into(),
			date.month().try_into().unwrap(),
			date.day().try_into().unwrap(),
			Timezone::from_chrono(&date.timezone(), date.offset()),
		)
		.unwrap()
	}
	#[doc(hidden)]
	pub fn as_chrono(&self) -> Option<chrono::Date<ChronoTimezone>> {
		Some(
			Utc.ymd(
				self.year().try_into().ok()?,
				self.month().into(),
				self.day().into(),
			)
			.with_timezone(&ChronoTimezone(self.timezone)),
		)
	}
}
impl AmadeusOrd for Date {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
	}
}
/// Corresponds to RFC 3339 and ISO 8601 string `%Y-%m-%d%:z`
impl Display for Date {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{:04}-{:02}-{:02} {}",
			self.year(),
			self.month(),
			self.day(),
			self.timezone()
		)
	}
}
impl FromStr for Date {
	type Err = ParseDateError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		chrono::DateTime::parse_from_str(s, "%Y-%m-%d%:z")
			.map(|date| Self::from_chrono(&date.date()))
			.map_err(|_| ParseDateError)
	}
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct Time {
	time: TimeWithoutTimezone, // UTC
	timezone: Timezone,
}
impl Time {
	/// Create a TimeWithoutTimezone from hour, minute, second and nanosecond.
	///
	/// The nanosecond part can exceed 1,000,000,000 in order to represent the leap second.
	///
	/// Returns None on invalid hour, minute, second and/or nanosecond.
	pub fn new(
		hour: u8, minute: u8, second: u8, nanosecond: u32, timezone: Timezone,
	) -> Option<Self> {
		TimeWithoutTimezone::new(hour, minute, second, nanosecond)
			.map(|time| Self { time, timezone })
	}
	/// Create a TimeWithoutTimezone from the number of seconds since midnight and nanosecond.
	///
	/// The nanosecond part can exceed 1,000,000,000 in order to represent the leap second.
	///
	/// Returns None on invalid number of seconds and/or nanosecond.
	pub fn from_seconds(seconds: u32, nanosecond: u32, timezone: Timezone) -> Option<Self> {
		TimeWithoutTimezone::from_seconds(seconds, nanosecond).map(|time| Self { time, timezone })
	}
	pub fn hour(&self) -> u8 {
		self.time.hour()
	}
	pub fn minute(&self) -> u8 {
		self.time.minute()
	}
	pub fn second(&self) -> u8 {
		self.time.second()
	}
	pub fn nanosecond(&self) -> u32 {
		self.time.nanosecond()
	}
	pub fn without_timezone(&self) -> TimeWithoutTimezone {
		self.time
	}
	pub fn timezone(&self) -> Timezone {
		self.timezone
	}
	pub fn truncate_minutes(&self, minutes: u8) -> Self {
		Self {
			time: self.time.truncate_minutes(minutes),
			timezone: self.timezone,
		}
	}
}
impl AmadeusOrd for Time {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
	}
}
/// Corresponds to RFC 3339 and ISO 8601 string `%H:%M:%S%.9f%:z`
impl Display for Time {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{}{}",
			self.time.as_chrono().expect(TODO).format("%H:%M:%S%.9f"),
			ChronoTimezone(self.timezone)
		)
	}
}
impl FromStr for Time {
	type Err = ParseDateError;

	fn from_str(_s: &str) -> Result<Self, Self::Err> {
		unimplemented!()
	}
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct DateTime {
	date_time: DateTimeWithoutTimezone, // UTC
	timezone: Timezone,
}
impl DateTime {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		year: i64, month: u8, day: u8, hour: u8, minute: u8, second: u8, nanosecond: u32,
		timezone: Timezone,
	) -> Option<Self> {
		DateTimeWithoutTimezone::new(year, month, day, hour, minute, second, nanosecond).map(
			|date_time| Self {
				date_time,
				timezone,
			},
		)
	}
	/// Create a DateTimeWithoutTimezone from a [`DateWithoutTimezone`] and [`TimeWithoutTimezone`].
	pub fn from_date_time(date: Date, time: Time) -> Option<Self> {
		let timezone = date.timezone();
		if timezone != time.timezone() {
			return None;
		}
		let date_time = DateTimeWithoutTimezone::from_date_time(
			date.without_timezone(),
			time.without_timezone(),
		)?;
		Some(Self {
			date_time,
			timezone,
		})
	}
	pub fn date(&self) -> DateWithoutTimezone {
		self.date_time.date()
	}
	pub fn time(&self) -> TimeWithoutTimezone {
		self.date_time.time()
	}
	pub fn year(&self) -> i64 {
		self.date_time.year()
	}
	pub fn month(&self) -> u8 {
		self.date_time.month()
	}
	pub fn day(&self) -> u8 {
		self.date_time.day()
	}
	pub fn hour(&self) -> u8 {
		self.date_time.hour()
	}
	pub fn minute(&self) -> u8 {
		self.date_time.minute()
	}
	pub fn second(&self) -> u8 {
		self.date_time.second()
	}
	pub fn nanosecond(&self) -> u32 {
		self.date_time.nanosecond()
	}
	#[doc(hidden)]
	pub fn from_chrono<Tz>(date_time: &chrono::DateTime<Tz>) -> Self
	where
		Tz: TimeZone,
	{
		Self::new(
			date_time.year().into(),
			date_time.month().try_into().unwrap(),
			date_time.day().try_into().unwrap(),
			date_time.hour().try_into().unwrap(),
			date_time.minute().try_into().unwrap(),
			date_time.second().try_into().unwrap(),
			date_time.nanosecond(),
			Timezone::from_chrono(&date_time.timezone(), date_time.offset()),
		)
		.unwrap()
	}
	#[doc(hidden)]
	pub fn as_chrono(&self) -> Option<chrono::DateTime<ChronoTimezone>> {
		Some(
			chrono::DateTime::<Utc>::from_utc(self.date_time.as_chrono()?, Utc)
				.with_timezone(&ChronoTimezone(self.timezone)),
		)
	}
	pub fn truncate_minutes(&self, minutes: u8) -> Self {
		Self {
			date_time: self.date_time.truncate_minutes(minutes),
			timezone: self.timezone,
		}
	}
}
impl AmadeusOrd for DateTime {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
	}
}
/// Corresponds to RFC 3339 and ISO 8601 string `%Y-%m-%dT%H:%M:%S%.9f%:z`
impl Display for DateTime {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{}",
			self.as_chrono()
				.expect(TODO)
				.format("%Y-%m-%d %H:%M:%S%.9f %:z")
		)
	}
}
impl FromStr for DateTime {
	type Err = ParseDateError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		chrono::DateTime::<FixedOffset>::from_str(s)
			.map(|date| Self::from_chrono(&date))
			.map_err(|_| ParseDateError)
	}
}

// https://github.com/chronotope/chrono/issues/52
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct Duration {
	months: i64,
	days: i64,
	nanos: i64,
}
impl AmadeusOrd for Duration {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
	}
}

// Parquet's [Date logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date) is i32 days from Unix epoch
// Postgres https://www.postgresql.org/docs/11/datatype-datetime.html is 4713 BC to 5874897 AD
// MySQL https://dev.mysql.com/doc/refman/8.0/en/datetime.html is 1000-01-01 to 9999-12-31
// Chrono https://docs.rs/chrono/0.4.6/chrono/naive/struct.NaiveDate.html Jan 1, 262145 BCE to Dec 31, 262143 CE
// TODO: i33
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct DateWithoutTimezone(i64);
impl DateWithoutTimezone {
	pub fn new(year: i64, month: u8, day: u8) -> Option<Self> {
		NaiveDate::from_ymd_opt(
			year.try_into().ok()?,
			month.try_into().ok()?,
			day.try_into().ok()?,
		)
		.as_ref()
		.map(Self::from_chrono)
	}
	pub fn from_ordinal(year: i64, day: u16) -> Option<Self> {
		NaiveDate::from_yo_opt(year.try_into().ok()?, day.try_into().ok()?)
			.as_ref()
			.map(Self::from_chrono)
	}
	pub fn year(&self) -> i64 {
		i64::from(self.as_chrono().expect(TODO).year())
	}
	pub fn month(&self) -> u8 {
		self.as_chrono().expect(TODO).month().try_into().unwrap()
	}
	pub fn day(&self) -> u8 {
		self.as_chrono().expect(TODO).day().try_into().unwrap()
	}
	pub fn ordinal(&self) -> u16 {
		self.as_chrono().expect(TODO).ordinal().try_into().unwrap()
	}
	pub fn with_timezone(self, timezone: Timezone) -> Date {
		Date {
			date: self,
			timezone,
		}
	}
	/// Create a DateWithoutTimezone from the number of days since the Unix epoch
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
	#[doc(hidden)]
	pub fn from_chrono(date: &NaiveDate) -> Self {
		Self::from_days(i64::from(date.num_days_from_ce()) - GREGORIAN_DAY_OF_EPOCH).unwrap()
	}
	#[doc(hidden)]
	pub fn as_chrono(&self) -> Option<NaiveDate> {
		NaiveDate::from_num_days_from_ce_opt((self.0 + GREGORIAN_DAY_OF_EPOCH).try_into().ok()?)
	}
}
impl AmadeusOrd for DateWithoutTimezone {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
	}
}
impl Display for DateWithoutTimezone {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.as_chrono().expect(TODO).fmt(f)
	}
}
impl FromStr for DateWithoutTimezone {
	type Err = ParseDateError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		NaiveDate::from_str(s)
			.map(|date| Self::from_chrono(&date))
			.map_err(|_| ParseDateError)
	}
}

// Parquet [Time logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) number of microseconds since midnight
// Postgres https://www.postgresql.org/docs/11/datatype-datetime.html 00:00:00 to 24:00:00, no :60
// MySQL https://dev.mysql.com/doc/refman/8.0/en/time.html https://dev.mysql.com/doc/refman/5.7/en/time-zone-leap-seconds.html -838:59:59 to 838:59:59, no :60
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct TimeWithoutTimezone(NaiveTime);
impl TimeWithoutTimezone {
	/// Create a TimeWithoutTimezone from hour, minute, second and nanosecond.
	///
	/// The nanosecond part can exceed 1,000,000,000 in order to represent the leap second.
	///
	/// Returns None on invalid hour, minute, second and/or nanosecond.
	pub fn new(hour: u8, minute: u8, second: u8, nanosecond: u32) -> Option<Self> {
		NaiveTime::from_hms_nano_opt(hour.into(), minute.into(), second.into(), nanosecond)
			.map(Self)
	}
	/// Create a TimeWithoutTimezone from the number of seconds since midnight and nanosecond.
	///
	/// The nanosecond part can exceed 1,000,000,000 in order to represent the leap second.
	///
	/// Returns None on invalid number of seconds and/or nanosecond.
	pub fn from_seconds(seconds: u32, nanosecond: u32) -> Option<Self> {
		NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanosecond).map(Self)
	}
	pub fn hour(&self) -> u8 {
		self.0.hour().try_into().unwrap()
	}
	pub fn minute(&self) -> u8 {
		self.0.minute().try_into().unwrap()
	}
	pub fn second(&self) -> u8 {
		self.0.second().try_into().unwrap()
	}
	pub fn nanosecond(&self) -> u32 {
		self.0.nanosecond()
	}
	pub fn with_timezone(self, timezone: Timezone) -> Time {
		Time {
			time: self,
			timezone,
		}
	}
	#[doc(hidden)]
	pub fn from_chrono(time: &NaiveTime) -> Self {
		Self(*time)
	}
	#[doc(hidden)]
	pub fn as_chrono(&self) -> Option<NaiveTime> {
		Some(self.0)
	}
	pub fn truncate_minutes(&self, minutes: u8) -> Self {
		assert!(
			minutes != 0 && 60 % minutes == 0,
			"minutes must be a divisor of 60"
		);
		Self::new(self.hour(), self.minute() / minutes * minutes, 0, 0).unwrap()
	}
	// /// Create a TimeWithoutTimezone from the number of milliseconds since midnight
	// pub fn from_millis(millis: u32) -> Option<Self> {
	// 	if millis < u32::try_from(SECONDS_PER_DAY * MILLIS_PER_SECOND).unwrap() {
	// 		Some(Self(
	// 			u64::from(millis)
	// 				* u64::try_from(MICROS_PER_MILLI).unwrap()
	// 				* u64::try_from(NANOS_PER_MICRO).unwrap(),
	// 		))
	// 	} else {
	// 		None
	// 	}
	// }
	// /// Create a TimeWithoutTimezone from the number of microseconds since midnight
	// pub fn from_micros(micros: u64) -> Option<Self> {
	// 	if micros < u64::try_from(SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI).unwrap() {
	// 		Some(Self(micros * u64::try_from(NANOS_PER_MICRO).unwrap()))
	// 	} else {
	// 		None
	// 	}
	// }
	// /// Create a TimeWithoutTimezone from the number of nanoseconds since midnight
	// pub fn from_nanos(nanos: u64) -> Option<Self> {
	// 	if nanos
	// 		< u64::try_from(
	// 			SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO,
	// 		)
	// 		.unwrap()
	// 	{
	// 		Some(Self(nanos))
	// 	} else {
	// 		None
	// 	}
	// }
	// /// Get the number of milliseconds since midnight
	// pub fn as_millis(&self) -> u32 {
	// 	(self.0
	// 		/ u64::try_from(NANOS_PER_MICRO).unwrap()
	// 		/ u64::try_from(MICROS_PER_MILLI).unwrap())
	// 	.try_into()
	// 	.unwrap()
	// }
	// /// Get the number of microseconds since midnight
	// pub fn as_micros(&self) -> u64 {
	// 	self.0 / u64::try_from(NANOS_PER_MICRO).unwrap()
	// }
	// /// Get the number of microseconds since midnight
	// pub fn as_nanos(&self) -> u64 {
	// 	self.0
	// }
}
impl AmadeusOrd for TimeWithoutTimezone {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
	}
}
impl Display for TimeWithoutTimezone {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.as_chrono().expect(TODO).fmt(f)
	}
}
impl FromStr for TimeWithoutTimezone {
	type Err = ParseDateError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		NaiveTime::from_str(s)
			.map(|date| Self::from_chrono(&date))
			.map_err(|_| ParseDateError)
	}
}

// [`DateTimeWithoutTimezone`] corresponds to the [DateTimeWithoutTimezone logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp).
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct DateTimeWithoutTimezone {
	date: DateWithoutTimezone,
	time: TimeWithoutTimezone,
}
impl DateTimeWithoutTimezone {
	pub fn new(
		year: i64, month: u8, day: u8, hour: u8, minute: u8, second: u8, nanosecond: u32,
	) -> Option<Self> {
		let date = DateWithoutTimezone::new(year, month, day)?;
		let time = TimeWithoutTimezone::new(hour, minute, second, nanosecond)?;
		Some(Self { date, time })
	}
	/// Create a DateTimeWithoutTimezone from a [`DateWithoutTimezone`] and [`TimeWithoutTimezone`].
	pub fn from_date_time(date: DateWithoutTimezone, time: TimeWithoutTimezone) -> Option<Self> {
		Some(Self { date, time })
	}
	pub fn date(&self) -> DateWithoutTimezone {
		self.date
	}
	pub fn time(&self) -> TimeWithoutTimezone {
		self.time
	}
	pub fn year(&self) -> i64 {
		self.date.year()
	}
	pub fn month(&self) -> u8 {
		self.date.month()
	}
	pub fn day(&self) -> u8 {
		self.date.day()
	}
	pub fn hour(&self) -> u8 {
		self.time.hour()
	}
	pub fn minute(&self) -> u8 {
		self.time.minute()
	}
	pub fn second(&self) -> u8 {
		self.time.second()
	}
	pub fn nanosecond(&self) -> u32 {
		self.time.nanosecond()
	}
	pub fn with_timezone(self, timezone: Timezone) -> DateTime {
		DateTime {
			date_time: self,
			timezone,
		}
	}
	#[doc(hidden)]
	pub fn from_chrono(date_time: &NaiveDateTime) -> Self {
		Self::new(
			date_time.year().into(),
			date_time.month().try_into().unwrap(),
			date_time.day().try_into().unwrap(),
			date_time.hour().try_into().unwrap(),
			date_time.minute().try_into().unwrap(),
			date_time.second().try_into().unwrap(),
			date_time.nanosecond(),
		)
		.unwrap()
	}
	#[doc(hidden)]
	pub fn as_chrono(&self) -> Option<NaiveDateTime> {
		Some(self.date.as_chrono()?.and_time(self.time.as_chrono()?))
	}
	pub fn truncate_minutes(&self, minutes: u8) -> Self {
		Self {
			date: self.date,
			time: self.time.truncate_minutes(minutes),
		}
	}
	// /// Create a DateTimeWithoutTimezone from the number of milliseconds since the Unix epoch
	// pub fn from_millis(millis: i64) -> Self {
	// 	let mut days = millis / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
	// 	let mut millis = millis % (SECONDS_PER_DAY * MILLIS_PER_SECOND);
	// 	if millis < 0 {
	// 		days -= 1;
	// 		millis += SECONDS_PER_DAY * MILLIS_PER_SECOND;
	// 	}
	// 	Self::from_date_time(
	// 		DateWithoutTimezone::from_days(days).unwrap(),
	// 		TimeWithoutTimezone::from_millis(millis.try_into().unwrap()).unwrap(),
	// 	)
	// 	.unwrap()
	// }
	// /// Create a DateTimeWithoutTimezone from the number of microseconds since the Unix epoch
	// pub fn from_micros(micros: i64) -> Self {
	// 	let mut days = micros / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
	// 	let mut micros = micros % (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
	// 	if micros < 0 {
	// 		days -= 1;
	// 		micros += SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI;
	// 	}
	// 	Self::from_date_time(
	// 		DateWithoutTimezone::from_days(days).unwrap(),
	// 		TimeWithoutTimezone::from_micros(micros.try_into().unwrap()).unwrap(),
	// 	)
	// 	.unwrap()
	// }
	// /// Create a DateTimeWithoutTimezone from the number of nanoseconds since the Unix epoch
	// pub fn from_nanos(nanos: i64) -> Self {
	// 	let mut days =
	// 		nanos / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO);
	// 	let mut nanos =
	// 		nanos % (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO);
	// 	if nanos < 0 {
	// 		days -= 1;
	// 		nanos += SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO;
	// 	}
	// 	Self::from_date_time(
	// 		DateWithoutTimezone::from_days(days).unwrap(),
	// 		TimeWithoutTimezone::from_nanos(nanos.try_into().unwrap()).unwrap(),
	// 	)
	// 	.unwrap()
	// }
	// /// Get the [`DateWithoutTimezone`] and [`TimeWithoutTimezone`] from this DateTimeWithoutTimezone.
	// pub fn as_date_time(&self) -> (DateWithoutTimezone, TimeWithoutTimezone) {
	// 	(self.date, self.time)
	// }
	// /// Get the number of milliseconds since the Unix epoch
	// pub fn as_millis(&self) -> Option<i64> {
	// 	Some(
	// 		self.date
	// 			.as_days()
	// 			.checked_mul(SECONDS_PER_DAY * MILLIS_PER_SECOND)?
	// 			.checked_add(i64::try_from(self.time.as_millis()).unwrap())?,
	// 	)
	// }
	// /// Get the number of microseconds since the Unix epoch
	// pub fn as_micros(&self) -> Option<i64> {
	// 	Some(
	// 		self.date
	// 			.as_days()
	// 			.checked_mul(SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI)?
	// 			.checked_add(i64::try_from(self.time.as_micros()).unwrap())?,
	// 	)
	// }
	// /// Get the number of nanoseconds since the Unix epoch
	// pub fn as_nanos(&self) -> Option<i64> {
	// 	Some(
	// 		self.date
	// 			.as_days()
	// 			.checked_mul(
	// 				SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO,
	// 			)?
	// 			.checked_add(i64::try_from(self.time.as_nanos()).unwrap())?,
	// 	)
	// }
}
impl AmadeusOrd for DateTimeWithoutTimezone {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(self, other)
	}
}
impl Display for DateTimeWithoutTimezone {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.as_chrono().expect(TODO).fmt(f)
	}
}
impl FromStr for DateTimeWithoutTimezone {
	type Err = ParseDateError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		NaiveDateTime::from_str(s)
			.map(|date| Self::from_chrono(&date))
			.map_err(|_| ParseDateError)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use chrono::NaiveDate;

	const SECONDS_PER_DAY: i64 = 86_400;

	// #[test]
	// fn test_int96() {
	// 	let value = DateTimeWithoutTimezone(Int96::from(vec![0, 0, 2454923]));
	// 	assert_eq!(value.as_millis().unwrap(), 1238544000000);

	// 	let value = DateTimeWithoutTimezone(Int96::from(vec![4165425152, 13, 2454923]));
	// 	assert_eq!(value.as_millis().unwrap(), 1238544060000);

	// 	let value = DateTimeWithoutTimezone(Int96::from(vec![0, 0, 0]));
	// 	assert_eq!(value.as_millis().unwrap(), -210866803200000);
	// }

	#[test]
	fn timezone() {
		assert_eq!(
			Timezone::from_name("Etc/GMT-14")
				.unwrap()
				.as_offset()
				.unwrap(),
			14 * 60 * 60
		);
	}

	#[test]
	fn test_convert_date_to_string() {
		fn check_date_conversion(y: i32, m: u32, d: u32) {
			let chrono_date = NaiveDate::from_ymd(y, m, d);
			let chrono_datetime = chrono_date.and_hms(0, 0, 0);
			assert_eq!(chrono_datetime.timestamp() % SECONDS_PER_DAY, 0);
			let date =
				DateWithoutTimezone::from_days(chrono_datetime.timestamp() / SECONDS_PER_DAY)
					.unwrap();
			assert_eq!(date.to_string(), chrono_date.to_string());
			let date2 = DateWithoutTimezone::from_chrono(&date.as_chrono().unwrap());
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
			let time = TimeWithoutTimezone::from_chrono(&chrono_time);
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
			// let res = DateTimeWithoutTimezone::from_millis(dt.timestamp_millis()).to_string();
			let res = DateTimeWithoutTimezone::from_chrono(&dt).to_string();
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
