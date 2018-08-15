extern crate nom;
extern crate warc_parser;

mod tests {
    use nom::{ConsumerState, Move, Producer};
    use nom::{FileProducer, IResult, Needed};
    use std::fs::File;
    use std::io::prelude::*;
    fn read_sample_file(sample_name: &str) -> Vec<u8> {
        let full_path = "sample/".to_string() + sample_name;
        let mut f = File::open(full_path).unwrap();
        let mut s = Vec::new();
        f.read_to_end(&mut s).unwrap();
        s
    }
    use warc_parser;
    // TODO organize this mess
    #[test]
    fn it_iterators() {
        let warc_streamer = warc_parser::WarcStreamer::new("sample/plethora.warc").unwrap();
        let mut count = 0;
        for record in warc_streamer {
            println!("record::{:?}", record);
            count += 1;
        }
        assert_eq!(count, 8);
    }
    #[test]
    fn it_stream_parses_incomplete_file() {
        let mut producer = FileProducer::new("sample/bbc.warc", 5000).unwrap();
        let mut consumer = warc_parser::WarcConsumer {
            state: warc_parser::State::Beginning,
            c_state: ConsumerState::Continue(Move::Consume(0)),
            counter: 0,
            last_record: None,
        };
        while let &ConsumerState::Continue(_) = producer.apply(&mut consumer) {}
        assert_eq!(consumer.counter, 0);
        assert_eq!(consumer.state, warc_parser::State::Error);
    }

    #[test]
    fn it_stream_parses_file() {
        let mut producer = FileProducer::new("sample/plethora.warc", 50000).unwrap();
        let mut consumer = warc_parser::WarcConsumer {
            state: warc_parser::State::Beginning,
            c_state: ConsumerState::Continue(Move::Consume(0)),
            counter: 0,
            last_record: None,
        };
        while let &ConsumerState::Continue(_) = producer.apply(&mut consumer) {}
        assert_eq!(consumer.counter, 8);
        assert_eq!(consumer.state, warc_parser::State::Done);
    }

    #[test]
    fn it_parses_a_plethora() {
        let examples = read_sample_file("plethora.warc");
        let parsed = warc_parser::records(&examples);
        assert!(parsed.is_done());
        match parsed {
            IResult::Error(_) => assert!(false),
            IResult::Incomplete(_) => assert!(false),
            IResult::Done(i, records) => {
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
        assert!(parsed.is_done());
        match parsed {
            IResult::Error(_) => assert!(false),
            IResult::Incomplete(_) => assert!(false),
            IResult::Done(i, record) => {
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
        assert!(!parsed.is_done());
        match parsed {
            IResult::Error(_) => assert!(false),
            IResult::Incomplete(needed) => assert_eq!(Needed::Size(10), needed),
            IResult::Done(_, _) => assert!(false),
        }
    }
}
