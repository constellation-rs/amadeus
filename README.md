<p align="center">
    <img alt="Amadeus" src="https://raw.githubusercontent.com/constellation-rs/amadeus/master/logo.svg?sanitize=true" width="450" />
</p>

<p align="center">
    Harmonious distributed data processing & analysis in Rust
</p>

<p align="center">
    <a href="https://crates.io/crates/amadeus"><img src="https://img.shields.io/crates/v/amadeus.svg?maxAge=86400" alt="Crates.io" /></a>
    <a href="LICENSE.txt"><img src="https://img.shields.io/crates/l/amadeus.svg?maxAge=2592000" alt="Apache-2.0 licensed" /></a>
    <a href="https://dev.azure.com/alecmocatta/amadeus/_build?definitionId=26"><img src="https://dev.azure.com/alecmocatta/amadeus/_apis/build/status/tests?branchName=master" alt="Build Status" /></a>
</p>

<p align="center">
    <a href="https://docs.rs/amadeus">📖 Docs</a> | <a href="https://constellation.rs/amadeus">🌐 Home</a> | <a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬 Chat</a>
</p>

## Amadeus provides:

- **Distributed streams:** like [Rayon](https://github.com/rayon-rs/rayon)'s parallel iterators, but distributed across a cluster.
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

| Data format | [`Source`](https://docs.rs/amadeus/0.3/amadeus/trait.Source.html) | [`Destination`](https://docs.rs/amadeus/0.3/amadeus/trait.Destination.html) |
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
use amadeus::data::{IpAddr, Url};
use std::error::Error;

#[derive(Data, Clone, PartialEq, Debug)]
struct LogLine {
    uri: Option<String>,
    requestip: Option<IpAddr>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = ThreadPool::new(None, None)?;

    let rows = Parquet::new(ParquetDirectory::new(S3Directory::new_with(
        AwsRegion::UsEast1,
        "us-east-1.data-analytics",
        "cflogworkshop/optimized/cf-accesslogs/",
        AwsCredentials::Anonymous,
    )), None)
    .await?;

    let top_pages = rows
        .par_stream()
        .map(|row: Result<LogLine, _>| {
            let row = row.unwrap();
            (row.uri, row.requestip)
        })
        .most_distinct(&pool, 100, 0.99, 0.002, 0.0808)
        .await;

    println!("{:#?}", top_pages);
    Ok(())
}
```

This is typed, so faster, and it goes an analytics step further also, prints top 100 URLs by distinct IPs logged.

<details>
<summary>See the same example but with data dynamically typed.</summary>

```rust
use amadeus::prelude::*;
use std::error::Error;
use amadeus::helpers::{FilterNullsAndDoubleUnwrap, GetFieldFromValue};
use amadeus::amadeus_parquet::get_row_predicate;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = ThreadPool::new(None, None)?;

    let column_name = "uri".to_string();
    
    let rows = Parquet::new(ParquetDirectory::new(S3Directory::new_with(
        AwsRegion::UsEast1,
        "us-east-1.data-analytics",
        "cflogworkshop/optimized/cf-accesslogs/",
        AwsCredentials::Anonymous,
    )), get_row_predicate(vec![column_name.clone()]))
    .await?;

    let top_pages = rows
        .par_stream()
        .get_field_from_value::<Option<String>>(column_name.clone())
        .filter_nulls_and_double_unwrap()
        .most_frequent(&pool, 100, 0.99, 0.002)
        .await;

    println!("{:#?}", top_pages);
    Ok(())
}
```

</details>

What about loading this data into Postgres? This will create and populate a table called "accesslogs".

```rust,ignore
use amadeus::prelude::*;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = ThreadPool::new(None, None)?;

    let rows = Parquet::new(ParquetDirectory::new(S3Directory::new_with(
        AwsRegion::UsEast1,
        "us-east-1.data-analytics",
        "cflogworkshop/optimized/cf-accesslogs/",
        AwsCredentials::Anonymous,
    )))
    .await?;

    // Note: this isn't yet implemented!
    rows.par_stream()
        .pipe(Postgres::new("127.0.0.1", PostgresTable::new("accesslogs")));

    Ok(())
}
```

## Running Distributed

Operations can run on a parallel threadpool or on a distributed process pool.

Amadeus uses the [**Constellation**](https://github.com/constellation-rs/constellation) framework for process distribution and communication. Constellation has backends for a bare cluster (Linux or macOS), and a managed Kubernetes cluster.

```rust
use amadeus::dist::prelude::*;
use amadeus::data::{IpAddr, Url};
use constellation::*;
use std::error::Error;

#[derive(Data, Clone, PartialEq, Debug)]
struct LogLine {
    uri: Option<String>,
    requestip: Option<IpAddr>,
}

fn main() -> Result<(), Box<dyn Error>> {
    init(Resources::default());

    // #[tokio::main] isn't supported yet so unfortunately setting up the Runtime must be done explicitly
    tokio::runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let pool = ProcessPool::new(None, None, None, Resources::default())?;

            let rows = Parquet::new(ParquetDirectory::new(S3Directory::new_with(
                AwsRegion::UsEast1,
                "us-east-1.data-analytics",
                "cflogworkshop/optimized/cf-accesslogs/",
                AwsCredentials::Anonymous,
            )))
            .await?;

            let top_pages = rows
                .dist_stream()
                .map(FnMut!(|row: Result<LogLine, _>| {
                    let row = row.unwrap();
                    (row.uri, row.requestip)
                }))
                .most_distinct(&pool, 100, 0.99, 0.002, 0.0808)
                .await;

            println!("{:#?}", top_pages);
            Ok(())
        })
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
