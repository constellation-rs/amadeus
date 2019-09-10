use serde::{Deserialize, Serialize};
use std::{borrow::Cow, net};
use url::Url;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct Webpage<'a> {
	pub ip: net::IpAddr,
	pub url: Url,
	pub contents: Cow<'a, [u8]>,
}
impl<'a> Webpage<'a> {
	pub fn to_owned(&self) -> Webpage<'static> {
		Webpage {
			ip: self.ip,
			url: self.url.clone(),
			contents: Cow::Owned(self.contents.clone().into_owned()),
		}
	}
}
