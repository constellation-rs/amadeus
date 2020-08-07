# amadeus-streaming

SIMD-accelerated implementations of various [streaming algorithms](https://en.wikipedia.org/wiki/Streaming_algorithm).

This is a subcrate of the [`amadeus`](https://github.com/constellation-rs/amadeus) project.

This library is a work in progress. PRs are very welcome! Currently implemented algorithms include:

 * Count–min sketch
 * Top k (Count–min sketch plus a doubly linked hashmap to track heavy hitters / top k keys when ordered by aggregated value)
 * HyperLogLog
 * Reservoir sampling

A goal of this library is to enable composition of these algorithms; for example Top k + HyperLogLog to enable an approximate version of something akin to `SELECT key FROM table GROUP BY key ORDER BY COUNT(DISTINCT value) DESC LIMIT k`.

Run your application with `RUSTFLAGS="-C target-cpu=native"` and the `nightly` feature to benefit from the SIMD-acceleration like so:

```bash
RUSTFLAGS="-C target-cpu=native" cargo run --features "streaming_algorithms/nightly" --release
```

See [this gist](https://gist.github.com/debasishg/8172796) for a good list of further algorithms to be implemented. Other resources are [Probabilistic data structures – Wikipedia](https://en.wikipedia.org/wiki/Category:Probabilistic_data_structures), [DataSketches – A similar Java library originating at Yahoo](https://datasketches.github.io/), and [Algebird  – A similar Java library originating at Twitter](https://github.com/twitter/algebird).

As these implementations are often in hot code paths, unsafe is used, albeit only when necessary to a) achieve the asymptotically optimal algorithm or b) mitigate an observed bottleneck.
