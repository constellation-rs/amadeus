#!/usr/bin/env bash
set -o errexit; set -o pipefail; set -o nounset; set -o xtrace; shopt -s nullglob;

# sudo apt-get install libtool automake pkgconf bison flex

pushd thrift

./bootstrap.sh

./configure --without-as3 --without-cpp --without-qt5 --without-c_glib --without-csharp \
--without-java --without-erlang --without-nodejs --without-nodets --without-lua --without-python \
--without-py3 --without-perl --without-php --without-php_extension --without-dart --without-ruby \
--without-haskell --without-go --without-swift --without-rs --without-cl --without-haxe \
--without-netstd --without-d

make

popd

thrift/compiler/cpp/thrift --gen rs parquet-format/src/main/thrift/parquet.thrift && \
mv parquet.rs src/internal/format.rs