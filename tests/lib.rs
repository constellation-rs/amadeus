#![feature(test)]
extern crate test;
extern crate warc_parser;
mod tests{
    use std::error::Error;
    use std::fs::File;
    use std::io::prelude::*;
    use std::path::Path;

    fn read_sample_file(sample_name: &str) -> String{
        let full_path = "../sample/".to_string()+sample_name;
        let path = Path::new(&full_path);
        let mut file = match File::open(&path) {
            Err(why) => panic!("couldn't open {}: {}", sample_name,
                               Error::description(&why)),
            Ok(file) => file,
        };

        // Read the file contents into a string, returns `io::Result<usize>`
        let mut s = String::new();
        file.read_to_string(&mut s).unwrap();
        s
    }
    use warc_parser::*;

    #[test]
    fn it_parses_single(){
        assert!(false);
        let bbc = "";
    }
}
