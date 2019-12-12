<p align="center">
    <img alt="Amadeus" src="https://raw.githubusercontent.com/constellation-rs/amadeus/master/logo.svg?sanitize=true" width="450" />
</p>

<p align="center">
    Harmonious distributed data processing & analysis in Rust
</p>

<p align="center">
    <a href="https://crates.io/crates/amadeus"><img src="https://img.shields.io/crates/v/amadeus.svg?maxAge=86400" alt="Crates.io" /></a>
    <a href="LICENSE.txt"><img src="https://img.shields.io/crates/l/amadeus.svg?maxAge=2592000" alt="Apache-2.0 licensed" /></a>
    <a href="https://dev.azure.com/alecmocatta/amadeus/_build/latest?branchName=master"><img src="https://dev.azure.com/alecmocatta/amadeus/_apis/build/status/tests?branchName=master" alt="Build Status" /></a>
</p>

<p align="center">
    <a href="https://docs.rs/amadeus/0.1.5">📖 Docs</a> | <a href="https://constellation.rs/amadeus">🌐 Home</a> | <a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬 Chat</a>
</p>

## Amadeus provides:

- **Distributed iterators:** like [Rayon](https://github.com/rayon-rs/rayon)'s parallel iterators, but distributed across a cluster.
- **Data connectors:** to work with CSV, JSON, Parquet, Postgres, S3 and more.
- **ETL and Data Science tooling:** focused on streaming processing & analysis.

Amadeus is a batteries-included, low-level reusable building block for the [Rust](https://www.rust-lang.org/) Distributed Computing and Big Data ecosystems.

## Principles

- **Fearless:** no data races, no unsafe, and lossless data canonicalization.
- **Make distributed computing trivial:** running distributed should be as easy and performant as running locally.
- **Data is gradually typed:** for maximum performance when the schema is known, and flexibility when it's not.
- **Simplicity:** keep interfaces and implementations as simple and reliable as possible.
- **Reliability:** minimize unhandled errors (including OOM), and only surface errors that couldn't be handled internally.

## Why Amadeus?

### Clean & Scalable applications

By design, Amadeus encourages you to write clean and reusable code that works, regardless of data scale, locally or distributed across a cluster. Write once, run at any data scale.

### Community

We aim to create a community that is welcoming and helpful to anyone that is interested! Come join us on [our Zulip chat](https://constellation.zulipchat.com/#narrow/stream/213231-amadeus) to:

 * get Amadeus working for your use case;
 * discuss direction for the project;
 * find good issues to get started with.

### Compatibility out of the box

Amadeus has deep, pluggable, integration with various file formats, databases and interfaces:

| Data format | [`Source`](https://docs.rs/amadeus/0.1.5/amadeus/trait.Source.html) | [`Sink`](https://docs.rs/amadeus/0.1.5/amadeus/trait.Sink.html) |
|---|---|---|
| CSV | ✔ | ✔ |
| JSON | ✔ | ✔ |
| XML | [👐](https://github.com/constellation-rs/amadeus/issues/15) |  |
| Parquet | ✔ | [🔨](https://github.com/constellation-rs/amadeus) |
| Avro | [🔨](https://github.com/constellation-rs/amadeus) |  |
| PostgreSQL | ✔ | [🔨](https://github.com/constellation-rs/amadeus) |
| HDF5 | [👐](https://github.com/constellation-rs/amadeus) |  |
| Redshift | [👐](https://github.com/constellation-rs/amadeus) |  |
| [CloudFront Logs](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html) | ✔ | – |
| [Common Crawl](http://commoncrawl.org/the-data/get-started/) | ✔ | – |
| S3 | ✔ | [🔨](https://github.com/constellation-rs/amadeus) |
| HDFS | [👐](https://github.com/constellation-rs/amadeus) | [👐](https://github.com/constellation-rs/amadeus) |

✔ = Working<br/>
🔨 = Work in Progress<br/>
👐 = Requested: check out the issue for how to help!

### Performance

Amadeus is routinely benchmarked and provisional results are very promising:

 * A 1.5x to 17x speedup reading Parquet data compared to the official Apache Arrow [`parquet`](https://crates.io/crates/parquet) crate with [these benchmarks](https://github.com/constellation-rs/amadeus/blob/3e96dbdfb77e8f874b6479c36ab4f344ff4781e4/amadeus-parquet/src/internal/file/reader.rs#L1100-L1184).

### Runs Everywhere

Amadeus is a library that can be used on its own as parallel threadpool, or with [**Constellation**](https://github.com/constellation-rs/constellation) as a distributed cluster.

[**Constellation**](https://github.com/constellation-rs/constellation) is a framework for process distribution and communication, and has backends for a bare cluster (Linux or macOS), a managed Kubernetes cluster, and more in the pipeline.

## Examples

This will read the Parquet partitions from the S3 bucket, and print the 100 most frequently occuring URLs.

```rust
use amadeus::prelude::*;

#[derive(Data)]
struct LogLine {
    url: Url,
    ip: IpAddr
}

fn main() {
    let pool = ThreadPool::new()?;

    let rows = Parquet::new(ParquetDirectory::new(S3Directory::new(
        AwsRegion::UsEast1,
        "us-east-1.data-analytics",
        "cflogworkshop/optimized/cf-accesslogs/",
    )))?;

    let top_pages = rows
        .dist_iter()
        .map(FnMut!(|row: Result<LogLine, _>| {
            (row.url, row.ip)
        }))
        .most_distinct(&pool, 100, 0.99, 0.002);

    println!("{:#?}", top_pages);
}
```

This is typed, so faster, and it goes an analytics step further also, prints top 100 URLs by distinct IPs logged.

<details>
<summary>See the same example but with data dynamically typed.</summary>

```rust
use amadeus::prelude::*;

fn main() {
    let pool = ThreadPool::new()?;

    let rows = Parquet::new(ParquetDirectory::new(S3Directory::new(
        AwsRegion::UsEast1,
        "us-east-1.data-analytics",
        "cflogworkshop/optimized/cf-accesslogs/",
    )))?;

    let top_pages = rows
        .dist_iter()
        .filter_map(FnMut!(|row: Result<Value, _>| {
            let row = row.ok()?.into_group().ok()?;
            row.get("url")?.into_url().ok()
        }))
        .most_frequent(&pool, 100, 0.99, 0.002);

    println!("{:#?}", top_pages);
}
```

</details>

What about loading this data into Postgres? This will create and populate a table called "accesslogs".

```rust
use amadeus::prelude::*;

fn main() {
    let pool = ThreadPool::new()?;

    let rows = Parquet::new(ParquetDirectory::new(S3Directory::new(
        AwsRegion::UsEast1,
        "us-east-1.data-analytics",
        "cflogworkshop/optimized/cf-accesslogs/",
    )))?;

    rows
        .dist_iter()
        .pipe(Postgres::new("127.0.0.1", PostgresTable::new("accesslogs")));

    println!("{:#?}", top_pages);
}
```

## Running Distributed

Operations can run on a parallel threadpool or on a distributed process pool.

Amadeus uses the [**Constellation**](https://github.com/constellation-rs/constellation) framework for process distribution and communication. Constellation has backends for a bare cluster (Linux or macOS), and a managed Kubernetes cluster.

```rust
fn main() {
    contellation::init(Resources::default());
    let process_pool = ProcessPool::new(processes, 1, Resources::default()).unwrap();
}
```

## Getting started

todo

### Examples

Take a look at the various [examples](examples).

## Contribution

Amadeus is an open source project! If you'd like to contribute, check out the list of [“good first issues”](https://github.com/constellation-rs/amadeus/contribute). These are all (or should be) issues that are suitable for getting started, and they generally include a detailed set of instructions for what to do. Please ask questions and ping us on [our Zulip chat](https://constellation.zulipchat.com/#narrow/stream/213231-amadeus) if anything is unclear!

## License
Licensed under Apache License, Version 2.0, ([LICENSE.txt](LICENSE.txt) or
http://www.apache.org/licenses/LICENSE-2.0).

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
licensed as above, without any additional terms or conditions.
