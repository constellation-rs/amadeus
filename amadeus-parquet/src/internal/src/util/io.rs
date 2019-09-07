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

use std::{
	cell::RefCell, cmp, fs::File, io::{self, BufWriter, Cursor, Initializer, Read, Seek, SeekFrom, Write}, rc::Rc
};

use crate::file::reader::ParquetReader;

// ----------------------------------------------------------------------
// Read/Write wrappers for `File`.

pub struct BufReader<R> {
	offset: u64,
	len: u64,
	inner: io::BufReader<R>,
}
impl<R: ParquetReader> BufReader<R> {
	pub fn new(inner: R) -> Self {
		Self::with_capacity(8 * 1024, inner)
	}
	pub fn with_capacity(capacity: usize, mut inner: R) -> Self {
		let offset = inner.seek(SeekFrom::Current(0)).unwrap();
		let len = inner.len();
		let inner = io::BufReader::with_capacity(capacity, inner);
		Self { offset, len, inner }
	}
	pub fn pos(&self) -> u64 {
		self.offset
	}
	pub fn len(&self) -> u64 {
		self.len
	}
}
impl<R: ParquetReader> BufReader<R> {
	pub fn seek_relative(&mut self, offset: i64) -> io::Result<()> {
		self.seek(SeekFrom::Current(offset)).map(drop)
	}
}
impl<R: ParquetReader> Read for BufReader<R> {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let read = self.inner.read(buf)?;
		self.offset += read as u64;
		Ok(read)
	}
	unsafe fn initializer(&self) -> Initializer {
		self.inner.initializer()
	}
}
impl<R: ParquetReader> Seek for BufReader<R> {
	fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
		let offset = match pos {
			SeekFrom::Start(n) => n as i64 - self.offset as i64,
			SeekFrom::Current(n) => n,
			SeekFrom::End(n) => self.len as i64 + n - self.offset as i64,
		};
		self.inner.seek_relative(offset)?;
		self.offset = (self.offset as i64 + offset) as u64;
		Ok(self.offset)
	}
}

/// Position trait returns the current position in the stream.
/// Should be viewed as a lighter version of `Seek` that does not allow seek operations,
/// and does not require mutable reference for the current position.
pub trait Position {
	/// Returns position in the stream.
	fn pos(&self) -> u64;
}

/// Struct that represents a slice of a file data with independent start position and
/// length. Internally clones provided file handle, wraps with BufReader and resets
/// position before any read.
///
/// This is workaround and alternative for `file.try_clone()` method. It clones `File`
/// while preserving independent position, which is not available with `try_clone()`.
///
/// Designed after `arrow::io::RandomAccessFile`.
pub struct FileSource<R: ParquetReader> {
	reader: Rc<RefCell<BufReader<R>>>,
	start: u64, // start position in a file
	end: u64,   // end position in a file
}

impl<R: ParquetReader> FileSource<R> {
	/// Creates new file reader with start and length from a file handle
	pub fn new(reader: Rc<RefCell<BufReader<R>>>, start: u64, length: u64) -> Self {
		Self {
			reader,
			start,
			end: start + length,
		}
	}
}

impl<R: ParquetReader> Read for FileSource<R> {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let bytes_to_read = cmp::min(buf.len(), (self.end - self.start) as usize);
		let buf = &mut buf[0..bytes_to_read];

		let mut reader = self.reader.borrow_mut();
		let pos = reader.pos();

		reader.seek_relative(self.start as i64 - pos as i64)?;

		let res = reader.read(buf);
		if let Ok(bytes_read) = res {
			self.start += bytes_read as u64;
		}

		res
	}
}

impl<R: ParquetReader> Position for FileSource<R> {
	fn pos(&self) -> u64 {
		self.start
	}
}

/// Struct that represents `File` output stream with position tracking.
/// Used as a sink in file writer.
pub struct FileSink {
	buf: BufWriter<File>,
	// This is not necessarily position in the underlying file,
	// but rather current position in the sink.
	pos: u64,
}

impl FileSink {
	/// Creates new file sink.
	/// Position is set to whatever position file has.
	pub fn new(file: &File) -> Self {
		let mut owned_file = file.try_clone().unwrap();
		let pos = owned_file.seek(SeekFrom::Current(0)).unwrap();
		Self {
			buf: BufWriter::new(owned_file),
			pos,
		}
	}
}

impl Write for FileSink {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		let num_bytes = self.buf.write(buf)?;
		self.pos += num_bytes as u64;
		Ok(num_bytes)
	}

	fn flush(&mut self) -> io::Result<()> {
		self.buf.flush()
	}
}

impl Position for FileSink {
	fn pos(&self) -> u64 {
		self.pos
	}
}

// Position implementation for Cursor to use in various tests.
impl<'a> Position for Cursor<&'a mut Vec<u8>> {
	fn pos(&self) -> u64 {
		self.position()
	}
}

#[cfg(test)]
mod tests {
	use std::{cell::RefCell, rc::Rc};

	use super::*;

	use crate::util::test_common::{get_temp_file, get_test_file};

	#[test]
	fn test_io_read_fully() {
		let mut buf = vec![0; 8];
		let file = Rc::new(RefCell::new(BufReader::new(get_test_file(
			"alltypes_plain.parquet",
		))));
		let mut src = FileSource::new(file, 0, 4);

		let bytes_read = src.read(&mut buf[..]).unwrap();
		assert_eq!(bytes_read, 4);
		assert_eq!(buf, vec![b'P', b'A', b'R', b'1', 0, 0, 0, 0]);
	}

	#[test]
	fn test_io_read_in_chunks() {
		let mut buf = vec![0; 4];
		let file = Rc::new(RefCell::new(BufReader::new(get_test_file(
			"alltypes_plain.parquet",
		))));
		let mut src = FileSource::new(file, 0, 4);

		let bytes_read = src.read(&mut buf[0..2]).unwrap();
		assert_eq!(bytes_read, 2);
		let bytes_read = src.read(&mut buf[2..]).unwrap();
		assert_eq!(bytes_read, 2);
		assert_eq!(buf, vec![b'P', b'A', b'R', b'1']);
	}

	#[test]
	fn test_io_read_pos() {
		let file = Rc::new(RefCell::new(BufReader::new(get_test_file(
			"alltypes_plain.parquet",
		))));
		let mut src = FileSource::new(file, 0, 4);

		let _ = src.read(&mut vec![0; 1]).unwrap();
		assert_eq!(src.pos(), 1);

		let _ = src.read(&mut vec![0; 4]).unwrap();
		assert_eq!(src.pos(), 4);
	}

	#[test]
	fn test_io_read_over_limit() {
		let file = Rc::new(RefCell::new(BufReader::new(get_test_file(
			"alltypes_plain.parquet",
		))));
		let mut src = FileSource::new(file, 0, 4);

		// Read all bytes from source
		let _ = src.read(&mut vec![0; 128]).unwrap();
		assert_eq!(src.pos(), 4);

		// Try reading again, should return 0 bytes.
		let bytes_read = src.read(&mut vec![0; 128]).unwrap();
		assert_eq!(bytes_read, 0);
		assert_eq!(src.pos(), 4);
	}

	#[test]
	fn test_io_seek_switch() {
		let mut buf = vec![0; 4];
		let file = Rc::new(RefCell::new(BufReader::new(get_test_file(
			"alltypes_plain.parquet",
		))));
		let mut src = FileSource::new(file.clone(), 0, 4);

		let _ = file
			.borrow_mut()
			.seek(SeekFrom::Start(5 as u64))
			.expect("File seek to a position");

		let bytes_read = src.read(&mut buf[..]).unwrap();
		assert_eq!(bytes_read, 4);
		assert_eq!(buf, vec![b'P', b'A', b'R', b'1']);
	}

	#[test]
	fn test_io_write_with_pos() {
		let mut file = get_temp_file("file_sink_test", &[b'a', b'b', b'c']);
		let _ = file.seek(SeekFrom::Current(3)).unwrap();

		// Write into sink
		let mut sink = FileSink::new(&file);
		assert_eq!(sink.pos(), 3);

		let _ = sink.write(&[b'd', b'e', b'f', b'g']).unwrap();
		assert_eq!(sink.pos(), 7);

		sink.flush().unwrap();
		assert_eq!(sink.pos(), file.seek(SeekFrom::Current(0)).unwrap());

		// Read data using file chunk
		let mut res = vec![0u8; 7];
		let file_len = file.metadata().unwrap().len();
		let mut chunk = FileSource::new(Rc::new(RefCell::new(BufReader::new(file))), 0, file_len);
		let _ = chunk.read(&mut res[..]).unwrap();

		assert_eq!(res, vec![b'a', b'b', b'c', b'd', b'e', b'f', b'g']);
	}
}
