extern crate warc_parser;
extern crate nom;

#[cfg(tests)]
mod tests{
    use std::fs::File;
    use std::io::prelude::*;
    use nom::{IResult};

    fn read_sample_file(sample_name: &str) -> Vec<u8> {
        let full_path = "sample/".to_string()+sample_name;
        let mut f = File::open(full_path).unwrap();
        let mut s = Vec::new();
        f.read_to_end(&mut s).unwrap();
        s
    }
    use warc_parser;
    #[test]
    fn it_parses_a_plethora(){
        let examples = read_sample_file("plethora.warc");
        let parsed = warc_parser::records(&examples);
        assert!(parsed.is_done());
        match parsed{
            IResult::Error(_) => assert!(false),
            IResult::Incomplete(_) => assert!(false),
            IResult::Done(i, records) => {
                let empty: Vec<u8> =  Vec::new();
                assert_eq!(empty, i);
                assert_eq!(8, records.len());
            }
        }
    }

    #[test]
    fn it_parses_single(){
        let bbc = read_sample_file("bbc.warc");
        let parsed = warc_parser::record(&bbc);
        assert!(parsed.is_done());
        match parsed{
            IResult::Error(_) => assert!(false),
            IResult::Incomplete(_) => assert!(false),
            IResult::Done(i, record) => {
                let empty: Vec<u8> =  Vec::new();
                assert_eq!(empty, i);
                assert_eq!(13, record.headers.len());
            }
        }
    }
}
