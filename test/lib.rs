#![feature(test)]
extern crate test;
extern crate warc_parser;
mod tests{
    use std::error::Error;
    use std::fs::File;
    use std::io::prelude::*;
    use std::path::Path;

    fn read_sample_file(sample_name: &str) -> String{
        let path = Path::new("../sample/"+sample_name);
        let mut file = match File::open(&path) {
            Err(why) => panic!("couldn't open {}: {}", display,
                               Error::description(&why)),
            Ok(file) => file,
        };

        // Read the file contents into a string, returns `io::Result<usize>`
        let mut s = String::new();
        match file.read_to_string(&mut s) {
            Err(why) => panic!("couldn't read {}: {}", display,
                               Error::description(&why)),
                               Ok(_) => print!("{} contains:\n{}", display, s),
        }
    }
    use warc_parser::*;

    #[test]
    fn it_parses_single(){
        let bbc = "";
    }
}
