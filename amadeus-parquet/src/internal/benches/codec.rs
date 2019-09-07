// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![feature(test)]

extern crate test;

#[allow(dead_code)]
#[path = "common.rs"]
mod common;
use crate::common::*;

use std::fs::File;
use test::Bencher;

use parchet::{basic::Compression, compression::*, file::reader::*};

// 10k rows written in page v2 with type:
//
//   message test {
//     required binary binary_field,
//     required int32 int32_field,
//     required int64 int64_field,
//     required boolean boolean_field,
//     required float float_field,
//     required double double_field,
//     required fixed_len_byte_array(1024) flba_field,
//     required int96 int96_field
//   }
//
// filled with random values.

fn get_rg_reader() -> parchet::file::reader::SerializedRowGroupReader<File> {
	let file = get_test_file("10k-v2.parquet");
	let f_reader = SerializedFileReader::new(file).unwrap();
	f_reader.get_row_group(0).unwrap()
}

fn get_pages_bytes(col_idx: usize) -> Vec<u8> {
	let mut data: Vec<u8> = Vec::new();
	let rg_reader = get_rg_reader();
	let mut pg_reader = rg_reader.get_column_page_reader(col_idx).unwrap();
	while let Some(p) = pg_reader.get_next_page().unwrap() {
		data.extend_from_slice(p.buffer().data());
	}
	data
}

macro_rules! compress {
	($fname:ident, $codec:expr, $col_idx:expr) => {
		#[bench]
		fn $fname(bench: &mut Bencher) {
			let mut codec = create_codec($codec).unwrap().unwrap();
			let data = get_pages_bytes($col_idx);
			bench.bytes = data.len() as u64;
			let mut v = Vec::new();
			bench.iter(|| {
				codec.compress(&data[..], &mut v).unwrap();
				v.clear();
			})
		}
	};
}

macro_rules! decompress {
	($fname:ident, $codec:expr, $col_idx:expr) => {
		#[bench]
		fn $fname(bench: &mut Bencher) {
			let compressed_pages = {
				let mut codec = create_codec($codec).unwrap().unwrap();
				let raw_data = get_pages_bytes($col_idx);
				let mut v = Vec::new();
				codec.compress(&raw_data[..], &mut v).unwrap();
				v
			};

			let mut codec = create_codec($codec).unwrap().unwrap();
			let rg_reader = get_rg_reader();
			bench.bytes = rg_reader.metadata().total_byte_size() as u64;
			let mut v = Vec::new();
			bench.iter(|| {
				let _ = codec.decompress(&compressed_pages[..], &mut v).unwrap();
				v.clear();
			})
		}
	};
}

compress!(compress_brotli_binary, Compression::Brotli, 0);
compress!(compress_brotli_int32, Compression::Brotli, 1);
compress!(compress_brotli_int64, Compression::Brotli, 2);
compress!(compress_brotli_boolean, Compression::Brotli, 3);
compress!(compress_brotli_float, Compression::Brotli, 4);
compress!(compress_brotli_double, Compression::Brotli, 5);
compress!(compress_brotli_fixed, Compression::Brotli, 6);
compress!(compress_brotli_int96, Compression::Brotli, 7);

compress!(compress_gzip_binary, Compression::Gzip, 0);
compress!(compress_gzip_int32, Compression::Gzip, 1);
compress!(compress_gzip_int64, Compression::Gzip, 2);
compress!(compress_gzip_boolean, Compression::Gzip, 3);
compress!(compress_gzip_float, Compression::Gzip, 4);
compress!(compress_gzip_double, Compression::Gzip, 5);
compress!(compress_gzip_fixed, Compression::Gzip, 6);
compress!(compress_gzip_int96, Compression::Gzip, 7);

compress!(compress_snappy_binary, Compression::Snappy, 0);
compress!(compress_snappy_int32, Compression::Snappy, 1);
compress!(compress_snappy_int64, Compression::Snappy, 2);
compress!(compress_snappy_boolean, Compression::Snappy, 3);
compress!(compress_snappy_float, Compression::Snappy, 4);
compress!(compress_snappy_double, Compression::Snappy, 5);
compress!(compress_snappy_fixed, Compression::Snappy, 6);
compress!(compress_snappy_int96, Compression::Snappy, 7);

compress!(compress_lz4_binary, Compression::Lz4, 0);
compress!(compress_lz4_int32, Compression::Lz4, 1);
compress!(compress_lz4_int64, Compression::Lz4, 2);
compress!(compress_lz4_boolean, Compression::Lz4, 3);
compress!(compress_lz4_float, Compression::Lz4, 4);
compress!(compress_lz4_double, Compression::Lz4, 5);
compress!(compress_lz4_fixed, Compression::Lz4, 6);
compress!(compress_lz4_int96, Compression::Lz4, 7);

compress!(compress_zstd_binary, Compression::Zstd, 0);
compress!(compress_zstd_int32, Compression::Zstd, 1);
compress!(compress_zstd_int64, Compression::Zstd, 2);
compress!(compress_zstd_boolean, Compression::Zstd, 3);
compress!(compress_zstd_float, Compression::Zstd, 4);
compress!(compress_zstd_double, Compression::Zstd, 5);
compress!(compress_zstd_fixed, Compression::Zstd, 6);
compress!(compress_zstd_int96, Compression::Zstd, 7);

decompress!(decompress_brotli_binary, Compression::Brotli, 0);
decompress!(decompress_brotli_int32, Compression::Brotli, 1);
decompress!(decompress_brotli_int64, Compression::Brotli, 2);
decompress!(decompress_brotli_boolean, Compression::Brotli, 3);
decompress!(decompress_brotli_float, Compression::Brotli, 4);
decompress!(decompress_brotli_double, Compression::Brotli, 5);
decompress!(decompress_brotli_fixed, Compression::Brotli, 6);
decompress!(decompress_brotli_int96, Compression::Brotli, 7);

decompress!(decompress_gzip_binary, Compression::Gzip, 0);
decompress!(decompress_gzip_int32, Compression::Gzip, 1);
decompress!(decompress_gzip_int64, Compression::Gzip, 2);
decompress!(decompress_gzip_boolean, Compression::Gzip, 3);
decompress!(decompress_gzip_float, Compression::Gzip, 4);
decompress!(decompress_gzip_double, Compression::Gzip, 5);
decompress!(decompress_gzip_fixed, Compression::Gzip, 6);
decompress!(decompress_gzip_int96, Compression::Gzip, 7);

decompress!(decompress_snappy_binary, Compression::Snappy, 0);
decompress!(decompress_snappy_int32, Compression::Snappy, 1);
decompress!(decompress_snappy_int64, Compression::Snappy, 2);
decompress!(decompress_snappy_boolean, Compression::Snappy, 3);
decompress!(decompress_snappy_float, Compression::Snappy, 4);
decompress!(decompress_snappy_double, Compression::Snappy, 5);
decompress!(decompress_snappy_fixed, Compression::Snappy, 6);
decompress!(decompress_snappy_int96, Compression::Snappy, 7);

decompress!(decompress_lz4_binary, Compression::Lz4, 0);
decompress!(decompress_lz4_int32, Compression::Lz4, 1);
decompress!(decompress_lz4_int64, Compression::Lz4, 2);
decompress!(decompress_lz4_boolean, Compression::Lz4, 3);
decompress!(decompress_lz4_float, Compression::Lz4, 4);
decompress!(decompress_lz4_double, Compression::Lz4, 5);
decompress!(decompress_lz4_fixed, Compression::Lz4, 6);
decompress!(decompress_lz4_int96, Compression::Lz4, 7);

decompress!(decompress_zstd_binary, Compression::Zstd, 0);
decompress!(decompress_zstd_int32, Compression::Zstd, 1);
decompress!(decompress_zstd_int64, Compression::Zstd, 2);
decompress!(decompress_zstd_boolean, Compression::Zstd, 3);
decompress!(decompress_zstd_float, Compression::Zstd, 4);
decompress!(decompress_zstd_double, Compression::Zstd, 5);
decompress!(decompress_zstd_fixed, Compression::Zstd, 6);
decompress!(decompress_zstd_int96, Compression::Zstd, 7);
