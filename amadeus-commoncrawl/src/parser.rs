//! Web ARChive format parser
//!
//! Takes data and separates records in headers and content.
use nom::{complete, do_parse, many1, map_res, named, opt, space, tag, Err, IResult, Needed};
use std::{fmt, str};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordType {
	WARCInfo,
	Response,
	Resource,
	Request,
	Metadata,
	Revisit,
	Conversion,
	Continuation,
}
impl RecordType {
	pub(crate) fn parse(x: &str) -> RecordType {
		match x {
			"warcinfo" => RecordType::WARCInfo,
			"response" => RecordType::Response,
			"resource" => RecordType::Resource,
			"request" => RecordType::Request,
			"metadata" => RecordType::Metadata,
			"revisit" => RecordType::Revisit,
			"conversion" => RecordType::Conversion,
			"continuation" => RecordType::Continuation,
			_ => panic!("bad RecordType"),
		}
	}
}

/// The WArc `Record` struct
// #[derive(Clone)]
pub(crate) struct Record<'a> {
	// lazy design should not use pub(crate)
	/// WArc headers
	pub(crate) type_: RecordType,
	pub(crate) target_uri: Option<&'a str>,
	pub(crate) ip_address: Option<&'a str>,
	/// Content for call in a raw format
	pub(crate) content: &'a [u8],
}

impl<'a> fmt::Debug for Record<'a> {
	fn fmt(&self, form: &mut fmt::Formatter) -> fmt::Result {
		writeln!(form, "\nHeaders:").unwrap();
		// for (name, value) in &self.headers {
		//     write!(form, "{}", name).unwrap();
		//     write!(form, ": ").unwrap();
		//     write!(form, "{}", value).unwrap();
		//     write!(form, "\n").unwrap();
		// }
		writeln!(form, "Content Length:{}", self.content.len()).unwrap();
		let s = str::from_utf8(self.content).unwrap_or("Could not convert");
		writeln!(form, "Content :{:?}", s).unwrap();
		writeln!(form)
	}
}

fn version_number(input: &[u8]) -> IResult<&[u8], &[u8]> {
	for (idx, chr) in input.iter().enumerate() {
		match *chr {
			46 | 48..=57 => continue,
			_ => return Ok((&input[idx..], &input[..idx])),
		}
	}
	Err(Err::Incomplete(Needed::Size(1)))
}

fn utf8_allowed(input: &[u8]) -> IResult<&[u8], &[u8]> {
	for (idx, chr) in input.iter().enumerate() {
		match *chr {
			0..=31 => return Ok((&input[idx..], &input[..idx])),
			_ => continue,
		}
	}
	Err(Err::Incomplete(Needed::Size(1)))
}

fn token(input: &[u8]) -> IResult<&[u8], &[u8]> {
	for (idx, chr) in input.iter().enumerate() {
		match *chr {
			33 | 35..=39 | 42 | 43 | 45 | 48..=57 | 65..=90 | 94..=122 | 124 => continue,
			_ => return Ok((&input[idx..], &input[..idx])),
		}
	}
	Err(Err::Incomplete(Needed::Size(1)))
}

named!(init_line <&[u8], (&str, &str)>,
	do_parse!(
		opt!(tag!("\r"))            >>
		opt!(tag!("\n"))            >>
		tag!("WARC")                >>
		tag!("/")                   >>
		opt!(space)                 >>
		version: map_res!(version_number, str::from_utf8) >>
		opt!(tag!("\r"))            >>
		tag!("\n")                  >>
		(("WARCVERSION", version))
	)
);

named!(header_match <&[u8], (&str, &str)>,
	do_parse!(
		name: map_res!(token, str::from_utf8) >>
		opt!(space)                 >>
		tag!(":")                   >>
		opt!(space)                 >>
		value: map_res!(utf8_allowed, str::from_utf8) >>
		opt!(tag!("\r"))            >>
		tag!("\n")                  >>
		((name, value))
	)
);

named!(header_aggregator<&[u8], Vec<(&str,&str)> >, many1!(header_match));

named!(warc_header<&[u8], ((&str, &str), Vec<(&str,&str)>) >,
	do_parse!(
		version: init_line          >>
		headers: header_aggregator  >>
		opt!(tag!("\r"))            >>
		tag!("\n")                  >>
		((version, headers))
	)
);

/// Parses one record and returns an IResult from nom
///
/// IResult<&[u8], Record>
///
/// See records for processing more then one. The documentation is not displaying.
///
/// # Examples
/// ```ignore
///  extern crate warc_parser;
///  extern crate nom;
///  use nom::{IResult};
///  let parsed = warc_parser::record(&bbc);
///  match parsed{
///      IResult::Error(_) => assert!(false),
///      Err::Incomplete(_) => assert!(false),
///      Ok((i, entry)) => {
///          let empty: Vec<u8> =  Vec::new();
///          assert_eq!(empty, i);
///          assert_eq!(13, entry.headers.len());
///      }
///  }
/// ```
#[inline(always)]
pub(crate) fn record(input: &[u8]) -> IResult<&[u8], Record> {
	// TODO if the stream parser does not get all the header it fails .
	// like a default size of 10 doesnt for for a producer
	warc_header(input).and_then(|(mut i, tuple_vec)| {
		let (_name, _version) = tuple_vec.0;
		let headers = tuple_vec.1; // not need figure it out
		let mut content = None;
		let mut bytes_needed = 1;
		let mut type_ = None;
		let mut target_uri = None;
		let mut ip_address = None;
		for &(k, v) in &headers {
			match k {
				"Content-Length" => {
					let length_number = v.parse::<usize>().unwrap();
					if length_number <= i.len() {
						content = Some(&i[0..length_number]);
						i = &i[length_number..];
						bytes_needed = 0;
					} else {
						bytes_needed = length_number - i.len();
					}
				}
				"WARC-Type" => {
					type_ = Some(v);
				}
				"WARC-Target-URI" => target_uri = Some(v),
				"WARC-IP-Address" => ip_address = Some(v),
				_ => (),
			}
		}
		match content {
			Some(content) => {
				let entry = Record {
					// headers: h,
					type_: RecordType::parse(type_.unwrap()),
					target_uri,
					ip_address,
					content,
				};
				Ok((i, entry))
			}
			None => Err(Err::Incomplete(Needed::Size(bytes_needed))),
		}
	})
}

named!(record_complete <&[u8], Record >,
	complete!(
		do_parse!(
			entry: record              >>
			opt!(tag!("\r"))           >>
			tag!("\n")                 >>
			opt!(tag!("\r"))           >>
			tag!("\n")                 >>
			(entry)
		)
	)
);

// Parses many record and returns an IResult with a Vec of Record
//
// IResult<&[u8], Vec<Record>>
//
// # Examples
// ```ignore
//  extern crate warc_parser;
//  extern crate nom;
//  use nom::{IResult};
//  let parsed = warc_parser::records(&bbc);
//  match parsed{
//      IResult::Error(_) => assert!(false),
//      IResult::Incomplete(_) => assert!(false),
//      IResult::Done(i, records) => {
//          assert_eq!(8, records.len());
//      }
//  }
// ```
named!(pub(crate) records<&[u8], Vec<Record> >, many1!(record_complete));
