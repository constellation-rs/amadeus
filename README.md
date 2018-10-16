# streaming_algorithms

[![Crates.io](https://img.shields.io/crates/v/streaming_algorithms.svg?style=flat-square&maxAge=86400)](https://crates.io/crates/streaming_algorithms)
[![Apache-2.0 licensed](https://img.shields.io/crates/l/streaming_algorithms.svg?style=flat-square&maxAge=2592000)](LICENSE.txt)
[![Build Status](https://ci.appveyor.com/api/projects/status/github/alecmocatta/streaming_algorithms?branch=master&svg=true)](https://ci.appveyor.com/project/alecmocatta/streaming-algorithms)
[![Build Status](https://circleci.com/gh/alecmocatta/streaming_algorithms/tree/master.svg?style=shield)](https://circleci.com/gh/alecmocatta/streaming_algorithms)
[![Build Status](https://travis-ci.com/alecmocatta/streaming_algorithms.svg?branch=master)](https://travis-ci.com/alecmocatta/streaming_algorithms)

[Docs](https://docs.rs/streaming_algorithms/0.1.0)

SIMD-accelerated implementations of various [streaming algorithms](https://en.wikipedia.org/wiki/Streaming_algorithm).

This library is a work in progress. PRs are very welcome! Currently implemented algorithms include:

 * Count–min sketch
 * Top k (Count–min sketch plus a doubly linked hashmap to track heavy hitters / top k keys when ordered by aggregated value)
 * HyperLogLog
 * Reservoir sampling

A goal of this library is to enable composition of these algorithms; for example Top k + HyperLogLog to enable an approximate version of something akin to `SELECT key FROM table GROUP BY key ORDER BY COUNT(DISTINCT value) DESC LIMIT k`.

Run your application with `RUSTFLAGS="-C target-cpu=native"` to benefit from the SIMD-acceleration like so:

```bash
RUSTFLAGS="-C target-cpu=native" cargo run --release
```

See [this gist](https://gist.github.com/debasishg/8172796) for a good list of further algorithms to be implemented. Other resources are [Probabilistic data structures – Wikipedia](https://en.wikipedia.org/wiki/Category:Probabilistic_data_structures), [DataSketches – A similar Java library originating at Yahoo](https://datasketches.github.io/), and [Algebird  – A similar Java library originating at Twitter](https://github.com/twitter/algebird).

As these implementations are often in hot code paths, unsafe is used, albeit only when necessary to a) achieve the asymptotically optimal algorithm or b) mitigate an observed bottleneck.

## License
Licensed under Apache License, Version 2.0, ([LICENSE.txt](LICENSE.txt) or http://www.apache.org/licenses/LICENSE-2.0).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any additional terms or conditions.
