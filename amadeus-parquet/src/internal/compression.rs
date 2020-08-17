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

//! Contains codec interface and supported codec implementations.
//!
//! See [`Compression`](crate::internal::basic::Compression) enum for all available compression
//! algorithms.
//!
//! # Example
//!
//! ```ignore
//! use amadeus_parquet::internal::{basic::Compression, compression::create_codec};
//!
//! let mut codec = match create_codec(Compression::Snappy) {
//!     Ok(Some(codec)) => codec,
//!     _ => panic!(),
//! };
//!
//! let data = vec![b'p', b'a', b'r', b'q', b'u', b'e', b't'];
//! let mut compressed = vec![];
//! codec.compress(&data[..], &mut compressed).unwrap();
//!
//! let mut output = vec![];
//! codec.decompress(&compressed[..], &mut output).unwrap();
//!
//! assert_eq!(output, data);
//! ```

use std::io::{self, Read, Write};

use flate2::{read, write, Compression};
use snap::raw::{decompress_len, max_compress_len, Decoder, Encoder};

use crate::internal::{
	basic::Compression as CodecType, errors::{ParquetError, Result}
};

/// Parquet compression codec interface.
pub trait Codec {
	/// Compresses data stored in slice `input_buf` and writes the compressed result
	/// to `output_buf`.
	/// Note that you'll need to call `clear()` before reusing the same `output_buf`
	/// across different `compress` calls.
	fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()>;

	/// Decompresses data stored in slice `input_buf` and writes output to `output_buf`.
	/// Returns the total number of bytes written.
	fn decompress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize>;
}

/// Given the compression type `codec`, returns a codec used to compress and decompress
/// bytes for the compression type.
/// This returns `None` if the codec type is `UNCOMPRESSED`.
pub fn create_codec(codec: CodecType) -> Result<Option<Box<dyn Codec>>> {
	match codec {
		CodecType::Brotli => Ok(Some(Box::new(BrotliCodec::new()))),
		CodecType::Gzip => Ok(Some(Box::new(GZipCodec::new()))),
		CodecType::Snappy => Ok(Some(Box::new(SnappyCodec::new()))),
		CodecType::Lz4 => Ok(Some(Box::new(LZ4Codec::new()))),
		CodecType::Zstd => Ok(Some(Box::new(ZSTDCodec::new()))),
		CodecType::Uncompressed => Ok(None),
		_ => Err(nyi_err!("The codec type {} is not supported yet", codec)),
	}
}

/// Codec for Snappy compression format.
pub struct SnappyCodec {
	decoder: Decoder,
	encoder: Encoder,
}

impl SnappyCodec {
	/// Creates new Snappy compression codec.
	fn new() -> Self {
		Self {
			decoder: Decoder::new(),
			encoder: Encoder::new(),
		}
	}
}

impl Codec for SnappyCodec {
	fn decompress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
		let len = decompress_len(input_buf)?;
		output_buf.resize(len, 0);
		self.decoder
			.decompress(input_buf, output_buf)
			.map_err(|e| e.into())
	}

	fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
		let required_len = max_compress_len(input_buf.len());
		if output_buf.len() < required_len {
			output_buf.resize(required_len, 0);
		}
		let n = self.encoder.compress(input_buf, &mut output_buf[..])?;
		output_buf.truncate(n);
		Ok(())
	}
}

/// Codec for GZIP compression algorithm.
pub struct GZipCodec {}

impl GZipCodec {
	/// Creates new GZIP compression codec.
	fn new() -> Self {
		Self {}
	}
}

impl Codec for GZipCodec {
	fn decompress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
		let mut decoder = read::GzDecoder::new(input_buf);
		decoder.read_to_end(output_buf).map_err(|e| e.into())
	}

	fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
		let mut encoder = write::GzEncoder::new(output_buf, Compression::default());
		encoder.write_all(input_buf)?;
		encoder.try_finish().map_err(|e| e.into())
	}
}

const BROTLI_DEFAULT_BUFFER_SIZE: usize = 4096;
const BROTLI_DEFAULT_COMPRESSION_QUALITY: u32 = 1; // supported levels 0-9
const BROTLI_DEFAULT_LG_WINDOW_SIZE: u32 = 22; // recommended between 20-22

/// Codec for Brotli compression algorithm.
pub struct BrotliCodec {}

impl BrotliCodec {
	/// Creates new Brotli compression codec.
	fn new() -> Self {
		Self {}
	}
}

impl Codec for BrotliCodec {
	fn decompress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
		brotli::Decompressor::new(input_buf, BROTLI_DEFAULT_BUFFER_SIZE)
			.read_to_end(output_buf)
			.map_err(|e| e.into())
	}

	fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
		let mut encoder = brotli::CompressorWriter::new(
			output_buf,
			BROTLI_DEFAULT_BUFFER_SIZE,
			BROTLI_DEFAULT_COMPRESSION_QUALITY,
			BROTLI_DEFAULT_LG_WINDOW_SIZE,
		);
		encoder.write_all(&input_buf[..])?;
		encoder.flush().map_err(|e| e.into())
	}
}

const LZ4_BUFFER_SIZE: usize = 4096;

/// Codec for LZ4 compression algorithm.
pub struct LZ4Codec {}

impl LZ4Codec {
	/// Creates new LZ4 compression codec.
	fn new() -> Self {
		Self {}
	}
}

impl Codec for LZ4Codec {
	fn decompress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
		Ok(lz_fear::framed::LZ4FrameReader::new(input_buf)
			.unwrap()
			.into_read()
			.read_to_end(output_buf)?)
	}

	fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
		use lz_fear::framed::{CompressionError, CompressionSettings};

		Ok(CompressionSettings::default()
			.independent_blocks(false)
			.block_size(64 * 1024)
			.compress(input_buf, output_buf)
			.map_err(|err| match err {
				CompressionError::ReadError(err) | CompressionError::WriteError(err) => err,
				CompressionError::InvalidBlockSize => unreachable!(),
			})?)
	}
}

/// Codec for Zstandard compression algorithm.
pub struct ZSTDCodec {}

impl ZSTDCodec {
	/// Creates new Zstandard compression codec.
	fn new() -> Self {
		Self {}
	}
}

/// Compression level (1-21) for ZSTD. Choose 1 here for better compression speed.
const ZSTD_COMPRESSION_LEVEL: i32 = 1;

impl Codec for ZSTDCodec {
	fn decompress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
		let mut decoder = zstd::Decoder::new(input_buf)?;
		match io::copy(&mut decoder, output_buf) {
			Ok(n) => Ok(n as usize),
			Err(e) => Err(e.into()),
		}
	}

	fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
		let mut encoder = zstd::Encoder::new(output_buf, ZSTD_COMPRESSION_LEVEL)?;
		encoder.write_all(&input_buf[..])?;
		match encoder.finish() {
			Ok(_) => Ok(()),
			Err(e) => Err(e.into()),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::fs::File;
	use test::Bencher;

	use crate::internal::{
		basic::Compression, file::reader::{
			FileReader, RowGroupReader, SerializedFileReader, SerializedRowGroupReader
		}, util::test_common::*
	};

	use super::*;

	fn test_roundtrip(c: CodecType, data: &Vec<u8>) {
		let mut c1 = create_codec(c).unwrap().unwrap();
		let mut c2 = create_codec(c).unwrap().unwrap();

		// Compress with c1
		let mut compressed = Vec::new();
		let mut decompressed = Vec::new();
		c1.compress(data.as_slice(), &mut compressed)
			.expect("Error when compressing");

		// Decompress with c2
		let mut decompressed_size = c2
			.decompress(compressed.as_slice(), &mut decompressed)
			.expect("Error when decompressing");
		assert_eq!(data.len(), decompressed_size);
		decompressed.truncate(decompressed_size);
		assert_eq!(*data, decompressed);

		compressed.clear();

		// Compress with c2
		c2.compress(data.as_slice(), &mut compressed)
			.expect("Error when compressing");

		// Decompress with c1
		decompressed_size = c1
			.decompress(compressed.as_slice(), &mut decompressed)
			.expect("Error when decompressing");
		assert_eq!(data.len(), decompressed_size);
		decompressed.truncate(decompressed_size);
		assert_eq!(*data, decompressed);
	}

	fn test_codec(c: CodecType) {
		let sizes = vec![100, 10000, 100000];
		for size in sizes {
			let mut data = random_bytes(size);
			test_roundtrip(c, &mut data);
		}
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn test_codec_snappy() {
		test_codec(CodecType::Snappy);
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn test_codec_gzip() {
		test_codec(CodecType::Gzip);
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn test_codec_brotli() {
		test_codec(CodecType::Brotli);
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn test_codec_lz4() {
		test_codec(CodecType::Lz4);
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn test_codec_zstd() {
		test_codec(CodecType::Zstd);
	}

	// Benches

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

	fn get_rg_reader() -> SerializedRowGroupReader<File> {
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
			#[cfg_attr(miri, ignore)]
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
			#[cfg_attr(miri, ignore)]
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
	// decompress!(decompress_lz4_int64, Compression::Lz4, 2);
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
}
