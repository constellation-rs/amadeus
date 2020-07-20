#![cfg(target_arch = "wasm32")]
#![allow(clippy::suspicious_map)]

use std::path::PathBuf;
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use amadeus::prelude::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen]
extern "C" {
	#[wasm_bindgen(js_namespace = console)]
	fn log(s: &str);
}
macro_rules! print {
	($($t:tt)*) => (log(&format_args!($($t)*).to_string()));
}
macro_rules! println {
	($fmt:expr) => (print!(concat!($fmt, "\n")));
	($fmt:expr, $($t:tt)*) => (print!(concat!($fmt, "\n"), $($t)*));
}

#[no_mangle]
pub extern "C" fn malloc(_size: usize) -> *mut std::ffi::c_void {
	panic!()
}
#[no_mangle]
pub extern "C" fn free(_ptr: *mut std::ffi::c_void) {
	panic!()
}
#[no_mangle]
pub extern "C" fn calloc(_nmemb: usize, _size: usize) -> *mut std::ffi::c_void {
	panic!()
}
#[no_mangle]
pub extern "C" fn realloc(_ptr: *mut std::ffi::c_void, _size: usize) -> *mut std::ffi::c_void {
	panic!()
}

#[wasm_bindgen_test]
async fn csv() {
	let timer = web_sys::window().unwrap().performance().unwrap();
	let start = timer.now();

	let pool = &ThreadPool::new(None).unwrap();

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct GameDerived {
		a: String,
		b: String,
		c: String,
		d: String,
		e: u32,
		f: String,
	}

	let rows = Csv::<_, GameDerived>::new(vec![PathBuf::from("amadeus-testing/csv/game.csv")])
		.await
		.unwrap();
	assert_eq!(
		rows.par_stream()
			.map(|row: Result<_, _>| row.unwrap())
			.count(pool)
			.await,
		100_000
	);

	#[derive(Data, Clone, PartialEq, PartialOrd, Debug)]
	struct GameDerived2 {
		a: String,
		b: String,
		c: String,
		d: String,
		e: u64,
		f: String,
	}

	let rows = Csv::<_, Value>::new(vec![PathBuf::from("amadeus-testing/csv/game.csv")])
		.await
		.unwrap();
	assert_eq!(
		rows.par_stream()
			.map(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				// println!("{:?}", value);
				let _: GameDerived2 = value.clone().downcast().unwrap();
				value
			})
			.count(pool)
			.await,
		100_000
	);

	let elapsed = timer.now() - start;
	println!("in {}s", elapsed / 1000.0);
}
