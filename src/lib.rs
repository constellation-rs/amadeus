#[macro_use]
extern crate nom;
use nom::{IResult, space, multispace, Needed, Err};
use nom::IResult::*;
use std::str;
use std::collections::HashMap;

fn version_number(input: &[u8]) -> IResult<&[u8], &[u8]> {
    for (idx, chr) in input.iter().enumerate() {
        match *chr {
            46 | 48...57  => continue,
            _ => return IResult::Done(&input[idx..], &input[..idx]),
        }
    }
    IResult::Incomplete(Needed::Size(1))
}

fn just_about_everything(input: &[u8]) -> IResult<&[u8], &[u8]> {
    for (idx, chr) in input.iter().enumerate() {
        match *chr {
            32...126 => continue,
            _ => return IResult::Done(&input[idx..], &input[..idx]),
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
        multispace?                  ~
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
        value: map_res!(just_about_everything, str::from_utf8)~
        tag!("\r")?                 ~
        tag!("\n")                  ,
        || {(name, value)}
        )
    );

named!(header_aggregator<&[u8], Vec<(&str,&str)> >, many1!(header_match));

named!(warc_header<&[u8], ((&str, &str), Vec<(&str,&str)>) >,
chain!(
    version: init_line              ~
    headers: header_aggregator      ,
    move ||{(version, headers)}
    )
);

pub fn record(input: &[u8]) -> IResult<&[u8], HashMap<&str,&str>>{
    let mut h: HashMap<&str,  &str> = HashMap::new();
    match warc_header(input) {
        IResult::Done(mut i, tuple_vec) => {
            let (name, version) = tuple_vec.0;
            h.insert(name, version);
            let headers =  tuple_vec.1;
            for &(k,ref v) in headers.iter() {
                h.insert(k, v.clone());
            }
            println!("{:?}", h);
            match h.get(&"Content-Length"){
                Some(&length) => {
                    let mut length_number = length.parse::<usize>().unwrap();
                    println!("{:?} :: {:?}", length_number, i.len());
                    match h.get(&"WARC-Truncated"){
                        Some(_) =>{
                            length_number = std::cmp::min(length_number, i.len());
                        }
                        _ => {}
                    }
                    println!("{:?} :: {:?}", length_number, i.len());
                    let content = &i[0..length_number as usize];
                    let content_str = str::from_utf8(content).unwrap();
                    i = &i[length_number as usize ..];
                    h.insert(&"CONTENT", content_str);
                    IResult::Done(i, h)
                }
                None => {
                    IResult::Incomplete(Needed::Size(1))
                }
            }

        },
        IResult::Incomplete(a)     => IResult::Incomplete(a),
        IResult::Error(a)          => IResult::Error(a)
    }
}

named!(pub records<&[u8], Vec<HashMap<&str,&str>> >, many1!(record));
