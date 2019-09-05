use serde::{Deserialize, Deserializer, Serialize, Serializer};
// use serde_json::Error as JsonError;
use std::{io, sync::Arc};

pub struct Serde<T>(T);

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
			.map(Self)
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
			.map(Self)
	}
}

impl Serialize for Serde<&io::Error> {
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
impl<'de> Deserialize<'de> for Serde<io::Error> {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		<(Serde<io::ErrorKind>, String)>::deserialize(deserializer)
			.map(|(kind, message)| io::Error::new(kind.0, message))
			.map(Self)
	}
}

// impl Serialize for Serde<&JsonError> {
// 	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
// 	where
// 		S: Serializer,
// 	{
// 		panic!()
// 	}
// }
// impl<'de> Deserialize<'de> for Serde<JsonError> {
// 	fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
// 	where
// 		D: Deserializer<'de>,
// 	{
// 		panic!()
// 	}
// }

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
