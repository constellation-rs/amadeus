#![feature(test)]
extern crate test;
extern crate warc_parser;
mod tests{
    use std::error::Error;
    use std::fs::File;
    use std::io::prelude::*;
    use std::path::Path;

    fn read_sample_file(sample_name: &str) -> String{
        let full_path = "sample/".to_string()+sample_name;
        let mut f = File::open(full_path).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        s
    }
    use warc_parser;
    #[test]
    fn it_parses_a_plethora(){
        let examples = read_sample_file("picplz.warc");
        let parsed = warc_parser::records(examples.as_bytes());
        println!("{:?}",parsed);
        assert!(parsed.is_done());
    }

    #[test]
    fn it_parses_single(){
        let bbc = read_sample_file("bbc.warc");
        let parsed = warc_parser::record(bbc.as_bytes());
        assert!(parsed.is_done());
    }
}
