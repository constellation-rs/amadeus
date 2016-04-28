//! Web ARChive format parser
//!
//! Takes data and separates records in headers and content.
#[macro_use]
extern crate nom;
use nom::{IResult, space, Needed};
use std::str;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Result};

/// The WArc `Record` struct
pub struct Record {
    /// WArc headers
    pub headers: HashMap<String, String>,
    /// Content for call in a raw format
    pub content: Vec<u8>,
}
impl<'a> Debug for Record {
    fn fmt(&self, form: &mut Formatter) -> Result {
        write!(form, "\nHeaders:\n").unwrap();
        for (name, value) in &self.headers {
            write!(form, "{}", name).unwrap();
            write!(form, ": ").unwrap();
            write!(form, "{}", value).unwrap();
            write!(form, "\n").unwrap();
        }
        write!(form, "Content Length:{}\n", self.content.len()).unwrap();
        let s = match String::from_utf8(self.content.clone()) {
            Ok(s) => s,
            Err(_) => "Could not convert".to_string(),
        };
        write!(form, "Content :{:?}\n", s).unwrap();
        write!(form, "\n")
    }
}

fn version_number(input: &[u8]) -> IResult<&[u8], &[u8]> {
    for (idx, chr) in input.iter().enumerate() {
        match *chr {
            46 | 48...57 => continue,
            _ => return IResult::Done(&input[idx..], &input[..idx]),
        }
    }
    IResult::Incomplete(Needed::Size(1))
}

fn utf8_allowed(input: &[u8]) -> IResult<&[u8], &[u8]> {
    for (idx, chr) in input.iter().enumerate() {
        match *chr {
            0...31 => return IResult::Done(&input[idx..], &input[..idx]),
            _ => continue,
        }
    }
    IResult::Incomplete(Needed::Size(1))
}

fn token(input: &[u8]) -> IResult<&[u8], &[u8]> {
    for (idx, chr) in input.iter().enumerate() {
        match *chr {
            33 | 35...39 | 42 | 43 | 45 | 48...57 | 65...90 | 94...122 | 124 => continue,
            _ => return IResult::Done(&input[idx..], &input[..idx]),
        }
    }
    IResult::Incomplete(Needed::Size(1))
}

named!(init_line <&[u8], (&str, &str)>,
    chain!(
        tag!("\r")?                 ~
        tag!("\n")?                 ~
        tag!("WARC")                ~
        tag!("/")                   ~
        space?                      ~
        version: map_res!(version_number, str::from_utf8)~
        tag!("\r")?                 ~
        tag!("\n")                  ,
        || {("WARCVERSION", version)}
    )
);

named!(header_match <&[u8], (&str, &str)>,
    chain!(
        name: map_res!(token, str::from_utf8)~
        space?                      ~
        tag!(":")                   ~
        space?                      ~
        value: map_res!(utf8_allowed, str::from_utf8)~
        tag!("\r")?                 ~
        tag!("\n")                  ,
        || {(name, value)}
    )
);

named!(header_aggregator<&[u8], Vec<(&str,&str)> >, many1!(header_match));

named!(warc_header<&[u8], ((&str, &str), Vec<(&str,&str)>) >,
    chain!(
        version: init_line          ~
        headers: header_aggregator  ~
        tag!("\r")?                 ~
        tag!("\n")                  ,
        move ||{(version, headers)}
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
///      IResult::Incomplete(_) => assert!(false),
///      IResult::Done(i, record) => {
///          let empty: Vec<u8> =  Vec::new();
///          assert_eq!(empty, i);
///          assert_eq!(13, record.headers.len());
///      }
///  }
/// ```
pub fn record(input: &[u8]) -> IResult<&[u8], Record> {
    let mut h: HashMap<String, String> = HashMap::new();
    match warc_header(input) {
        IResult::Done(mut i, tuple_vec) => {
            let (name, version) = tuple_vec.0;
            h.insert(name.to_string(), version.to_string());
            let headers = tuple_vec.1; // not need figure it out
            for &(k, ref v) in headers.iter() {
                h.insert(k.to_string(), v.clone().to_string());
            }
            let mut content = None;
            match h.get("Content-Length") {
                Some(length) => {
                    let mut length_number = length.parse::<usize>().unwrap();
                    match h.get("WARC-Truncated") {
                        Some(_) => {
                            length_number = std::cmp::min(length_number, i.len());
                        }
                        _ => {}
                    }
                    content = Some(&i[0..length_number as usize]);
                    i = &i[length_number as usize..];
                }
                _ => {}
            }
            match content {
                Some(content) => {
                    let record = Record {
                        headers: h,
                        content: content.to_vec(),
                    };
                    IResult::Done(i, record)
                }
                None => IResult::Incomplete(Needed::Size(1)),
            }
        }
        IResult::Incomplete(a) => IResult::Incomplete(a),
        IResult::Error(a) => IResult::Error(a),
    }
}

named!(record_complete <&[u8], Record >,
    chain!(
        record: record              ~
        tag!("\r")?                 ~
        tag!("\n")                  ~
        tag!("\r")?                 ~
        tag!("\n")                  ,
        move ||{record}
    )
);

/// Parses many record and returns an IResult with a Vec of Record
///
/// IResult<&[u8], Vec<Record>>
///
/// # Examples
/// ```ignore
///  extern crate warc_parser;
///  extern crate nom;
///  use nom::{IResult};
///  let parsed = warc_parser::records(&bbc);
///  match parsed{
///      IResult::Error(_) => assert!(false),
///      IResult::Incomplete(_) => assert!(false),
///      IResult::Done(i, records) => {
///          assert_eq!(8, records.len());
///      }
///  }
/// ```
named!(pub records<&[u8], Vec<Record> >, many1!(record_complete));
