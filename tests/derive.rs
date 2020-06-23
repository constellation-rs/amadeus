use amadeus::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Data, Clone, PartialEq, Debug)]
struct GenericRow<G> {
	t: G,
}

#[derive(Data, Clone, PartialEq, Serialize, Deserialize, Debug)]
struct Row {
	a: String,
	b: u64,
	c: f64,
	// d: List<Row>,
	e: List<Value>,
}

#[test]
fn list() {
	let rows: List<Row> = vec![Row {
		a: String::from("a"),
		b: 1,
		c: 2.0,
		// d: vec![].into(),
		e: vec![Value::U8(0)].into(),
	}]
	.into();
	assert_eq!(
		"[Row { a: \"a\", b: 1, c: 2.0, e: [U8(0)] }]",
		format!("{:?}", rows)
	);
	let json = serde_json::to_string(&rows).unwrap();
	let rows2 = serde_json::from_str(&*json).unwrap();
	assert_eq!(rows, rows2);
}

mod no_prelude {
	#![no_implicit_prelude]

	#[derive(::amadeus::prelude::Data, Clone, PartialEq, Debug)]
	struct GenericRow<G> {
		t: G,
	}
}
