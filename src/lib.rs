//! Web ARChive format parser
//!
//! Takes data and separates records in headers and content.
#[macro_use]
extern crate nom;
use std::str;
use std::collections::HashMap;
use std::fmt;
use nom::{Offset, space, Needed, IResult, Err};

/// The WArc `Record` struct
#[derive(Clone)]
pub struct Record {
    // lazy design should not use pub
    /// WArc headers
    pub headers: HashMap<String, String>,
    /// Content for call in a raw format
    pub content: Vec<u8>,
}

impl<'a> fmt::Debug for Record {
    fn fmt(&self, form: &mut fmt::Formatter) -> fmt::Result {
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
            _ => return Ok((&input[idx..], &input[..idx])),
        }
    }
    Err(Err::Incomplete(Needed::Size(1)))
}

fn utf8_allowed(input: &[u8]) -> IResult<&[u8], &[u8]> {
    for (idx, chr) in input.iter().enumerate() {
        match *chr {
            0...31 => return Ok((&input[idx..], &input[..idx])),
            _ => continue,
        }
    }
    Err(Err::Incomplete(Needed::Size(1)))
}

fn token(input: &[u8]) -> IResult<&[u8], &[u8]> {
    for (idx, chr) in input.iter().enumerate() {
        match *chr {
            33 | 35...39 | 42 | 43 | 45 | 48...57 | 65...90 | 94...122 | 124 => continue,
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
pub fn record(input: &[u8]) -> IResult<&[u8], Record> {
    let mut h: HashMap<String, String> = HashMap::new();
    // TODO if the stream parser does not get all the header it fails .
    // like a default size of 10 doesnt for for a producer
    warc_header(input).and_then(|(mut i, tuple_vec)| {
            let (name, version) = tuple_vec.0;
            h.insert(name.to_string(), version.to_string());
            let headers = tuple_vec.1; // not need figure it out
            for &(k, ref v) in headers.iter() {
                h.insert(k.to_string(), v.clone().to_string());
            }
            let mut content = None;
            let mut bytes_needed = 1;
            match h.get("Content-Length") {
                Some(length) => {
                    let length_number = length.parse::<usize>().unwrap();
                    if length_number <= i.len() {
                        content = Some(&i[0..length_number as usize]);
                        i = &i[length_number as usize..];
                        bytes_needed = 0;
                    } else {
                        bytes_needed = length_number - i.len();
                    }
                }
                _ => {
                    // TODO: Custom error type, this field is mandatory
                }
            }
            match content {
                Some(content) => {
                    let entry = Record {
                        headers: h,
                        content: content.to_vec(),
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
