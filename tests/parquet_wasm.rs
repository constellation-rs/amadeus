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
async fn parquet() {
	let timer = web_sys::window().unwrap().performance().unwrap();
	let start = timer.now();

	let pool = &ThreadPool::new(None).unwrap();

	let rows = Parquet::<_, Value>::new(vec![
		PathBuf::from("amadeus-testing/parquet/cf-accesslogs/year=2018/month=11/day=02/part-00176-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet"),
		PathBuf::from("amadeus-testing/parquet/cf-accesslogs/year=2018/month=11/day=02/part-00176-ed461019-4a12-46fa-a3f3-246d58f0ee06.c000.snappy.parquet"),
		PathBuf::from("amadeus-testing/parquet/cf-accesslogs/year=2018/month=11/day=03/part-00137-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet"),
		PathBuf::from("amadeus-testing/parquet/cf-accesslogs/year=2018/month=11/day=04/part-00173-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet"),
		PathBuf::from("amadeus-testing/parquet/cf-accesslogs/year=2018/month=11/day=05/part-00025-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet"),
		PathBuf::from("amadeus-testing/parquet/cf-accesslogs/year=2018/month=11/day=05/part-00025-96c249f4-3a10-4509-b6b8-693a5d90dbf3.c000.snappy.parquet"),
		PathBuf::from("amadeus-testing/parquet/cf-accesslogs/year=2018/month=11/day=06/part-00185-96c249f4-3a10-4509-b6b8-693a5d90dbf3.c000.snappy.parquet"),
		PathBuf::from("amadeus-testing/parquet/cf-accesslogs/year=2018/month=11/day=07/part-00151-96c249f4-3a10-4509-b6b8-693a5d90dbf3.c000.snappy.parquet"),
	]).await.unwrap();
	assert_eq!(
		rows.par_stream()
			.map(|row: Result<_, _>| row.unwrap())
			.count(pool)
			.await,
		207_535
	);

	let elapsed = timer.now() - start;
	println!("in {}s", elapsed / 1000.0);
}
