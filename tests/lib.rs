extern crate warc_parser;
extern crate nom;

mod tests {
    use std::fs::File;
    use std::io::prelude::*;
    use nom::{Err, IResult, Needed};

    fn read_sample_file(sample_name: &str) -> Vec<u8> {
        let full_path = "sample/".to_string() + sample_name;
        let mut f = File::open(full_path).unwrap();
        let mut s = Vec::new();
        f.read_to_end(&mut s).unwrap();
        s
    }

    use warc_parser;

    #[test]
    fn it_parses_a_plethora() {
        let examples = read_sample_file("plethora.warc");
        let parsed = warc_parser::records(&examples);
        assert!(parsed.is_ok());
        match parsed {
            Err(_) => assert!(false),
            Ok((i, records)) => {
                let empty: Vec<u8> = Vec::new();
                assert_eq!(empty, i);
                assert_eq!(8, records.len());
            }
        }
    }

    #[test]
    fn it_parses_single() {
        let bbc = read_sample_file("bbc.warc");
        let parsed = warc_parser::record(&bbc);
        assert!(parsed.is_ok());
        match parsed {
            Err(_) => assert!(false),
            Ok((i, record)) => {
                let empty: Vec<u8> = Vec::new();
                assert_eq!(empty, i);
                assert_eq!(13, record.headers.len());
            }
        }
    }

    #[test]
    fn it_parses_incomplete() {
        let bbc = read_sample_file("bbc.warc");
        let parsed = warc_parser::record(&bbc[..bbc.len() - 10]);
        assert!(!parsed.is_ok());
        match parsed {
            Err(Err::Incomplete(needed)) => assert_eq!(Needed::Size(10), needed),
            Err(_) => assert!(false),
            Ok((_, _)) => assert!(false),
        }
    }
}
