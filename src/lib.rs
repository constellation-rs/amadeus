#[macro_use]
extern crate nom;
extern crate url;
extern crate serde;

mod nom_parser;

use std::{
    io::{self, Read},
    iter, net,
};
use url::Url;
use serde::{Serialize, Deserialize};

pub struct Webpage<'a> {
    pub ip: net::IpAddr,
    pub url: Url,
    pub contents: &'a [u8],
}
// impl<'a> borrow::Borrow<Webpage<'a>> for WebpageOwned {
//  fn borrow(&self) -> &Webpage<'a> {
//      &Webpage{ip:self.ip,url:self.url,contents:&self.contents}
//  }
// }
// impl<'a> borrow::ToOwned for Webpage<'a> {
// type Owned = WebpageOwned;
impl<'a> Webpage<'a> {
    pub fn to_owned(&self) -> WebpageOwned {
        WebpageOwned {
            ip: self.ip,
            url: self.url.clone(),
            contents: self.contents.to_owned().into_boxed_slice(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WebpageOwned {
    pub ip: net::IpAddr,
    pub url: Url,
    pub contents: Box<[u8]>,
}

const BUF: usize = 2 << 26; // 64 MiB
const CHOMP: usize = 2 << 13; // 8 KiB

#[derive(Clone, Debug)]
pub struct WarcParser<I> {
    input: I,
    state: WarcParserState,
    res: Vec<u8>,
    offset: usize,
}
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum WarcParserState {
    Info,
    Request,
    Response,
    Metadata,
    Done,
}
impl<I> WarcParser<I> {
    pub fn new(input: I) -> WarcParser<I> {
        WarcParser {
            input,
            state: WarcParserState::Info,
            res: Vec::with_capacity(BUF),
            offset: 0,
        }
    }
}
impl<I> WarcParser<I>
where
    I: Read,
{
    pub fn next_borrowed<'a>(&'a mut self) -> Result<Option<Webpage<'a>>, io::Error> {
        if let WarcParserState::Done = self.state {
            return Ok(None);
        }
        'chomp: loop {
            assert!(
                self.res.len() < BUF,
                "Individual record > configured BUF {:?}",
                BUF
            );
            let n = io::copy(
                &mut self
                    .input
                    .by_ref()
                    .take(CHOMP.min(BUF - self.res.len()) as u64),
                &mut self.res,
            )?;
            assert_eq!(self.res.capacity(), BUF);
            if n == 0 && self.offset == self.res.len() {
                assert_eq!(self.state, WarcParserState::Request);
                self.state = WarcParserState::Done;
                return Ok(None);
            }

            loop {
                self.res.splice(..self.offset, iter::empty());
                self.offset = 0;
                if self.offset == self.res.len() {
                    continue 'chomp;
                }
                let record = match nom_parser::record(&self.res[self.offset..]) {
                    Ok((rem, record)) => {
                        let record_len = self.res.len() - self.offset - rem.len() + 4; // 4 is \r\n\r\n
                        if self.offset + record_len > self.res.len() {
                            continue 'chomp;
                        }
                        self.offset += record_len;
                        record
                    }
                    Err(nom::Err::Incomplete(_)) => {
                        continue 'chomp;
                    }
                    _ => panic!(),
                };
                self.state = match self.state {
                    WarcParserState::Info => {
                        assert!(record.type_ == nom_parser::RecordType::WARCInfo);
                        WarcParserState::Request
                    }
                    WarcParserState::Request => {
                        assert!(record.type_ == nom_parser::RecordType::Request);
                        WarcParserState::Response
                    }
                    WarcParserState::Response => {
                        assert!(record.type_ == nom_parser::RecordType::Response);
                        self.state = WarcParserState::Metadata;

                        let content: *const u8 = record.content.as_ptr();
                        let buffer: *const u8 = self.res.as_slice().as_ptr();
                        let start = (content as usize) - (buffer as usize);
                        let end = start + record.content.len();
                        return Ok(Some(Webpage {
                            ip: record.ip_address.unwrap().parse().unwrap(),
                            url: Url::parse(record.target_uri.unwrap()).unwrap(),
                            contents: &self.res[start..end],
                        }));
                    }
                    WarcParserState::Metadata => {
                        assert!(record.type_ == nom_parser::RecordType::Metadata);
                        WarcParserState::Request
                    }
                    WarcParserState::Done => unreachable!(),
                }
            }
        }
    }
}
impl<I> Iterator for WarcParser<I>
where
    I: Read,
{
    type Item = Result<WebpageOwned, io::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_borrowed()
            .transpose()
            .map(|x| x.map(|x| x.to_owned()))
    }
}
