# streaming_algorithms

[![Crates.io](https://img.shields.io/crates/v/streaming_algorithms.svg?style=flat-square&maxAge=86400)](https://crates.io/crates/streaming_algorithms)
[![Apache-2.0 licensed](https://img.shields.io/crates/l/streaming_algorithms.svg?style=flat-square&maxAge=2592000)](LICENSE.txt)
[![Build Status](https://ci.appveyor.com/api/projects/status/github/alecmocatta/streaming_algorithms?branch=master&svg=true)](https://ci.appveyor.com/project/alecmocatta/streaming-algorithms)
[![Build Status](https://circleci.com/gh/alecmocatta/streaming_algorithms/tree/master.svg?style=shield)](https://circleci.com/gh/alecmocatta/streaming_algorithms)
[![Build Status](https://travis-ci.com/alecmocatta/streaming_algorithms.svg?branch=master)](https://travis-ci.com/alecmocatta/streaming_algorithms)

[Docs](https://docs.rs/streaming_algorithms/0.1.0)

Performant implementations of various [streaming algorithms](https://en.wikipedia.org/wiki/Streaming_algorithm).

This library is a work in progress. See the docs for what algorithms are currently implemented.

See [here](https://gist.github.com/debasishg/8172796) for a good list of algorithms to be implemented.

As these implementations are often in hot code paths, unsafe is used, albeit only when justified.

This library leverages the following prioritisation when deciding whether `unsafe` is justified for a particular implementation:
 1. Asymptotically optimal algorithm
 2. Trivial safety (i.e. no `unsafe` at all or extremely limited `unsafe` trivially contained to one or two lines)
 3. Constant-factor optimisations

## License
Licensed under Apache License, Version 2.0, ([LICENSE.txt](LICENSE.txt) or http://www.apache.org/licenses/LICENSE-2.0).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any additional terms or conditions.
