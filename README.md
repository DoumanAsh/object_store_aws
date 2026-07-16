# object_store_aws

[![Rust](https://github.com/DoumanAsh/object_store_aws/actions/workflows/rust.yml/badge.svg)](https://github.com/DoumanAsh/object_store_aws/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/object_store_aws.svg)](https://crates.io/crates/object_store_aws)
[![Documentation](https://docs.rs/object_store_aws/badge.svg)](https://docs.rs/crate/object_store_aws/)

AWS extensions for [object_store](https://crates.io/crates/object_store) crate based on AWS SDK

It provides credentials source and optional HTTP client based on SDK's http client

## Features

- `sso` - Enables SSO usage in [aws-config](https://crates.io/crates/aws-config)
- `ring` - Enables ring crypto backend for AWS HTTP client
- `aws-lc` - Enables aws-lc crypto backend for AWS HTTP client
- `aws-lc-fips` - Enables aws-lc FIPS friendly crypto backend for AWS HTTP client
- `http-builtin` - Enables builtin HTTP client of [object_store](https://crates.io/crates/object_store) for use with AWS storage
- `fs` - Enables builtin file system store of [object_store](https://crates.io/crates/object_store)

## Usage

```rust
use core::fmt;
use std::sync::Arc;
use object_store_aws::{object_store, init, AmazonS3Builder};
use object_store_aws::object_store::ObjectStore;
use object_store_aws::object_store::multipart::MultipartStore;

async fn create_s3_store() -> impl ObjectStore + MultipartStore + fmt::Debug + Send + Sync + Clone + 'static {
    let credentials = Arc::new(init(None).await.expect("to initialize S3 SDK"));
    AmazonS3Builder::from_env().with_bucket_name("my-super-bucket")
                               .with_region(credentials.region_str())
                               .with_credentials(credentials).build()
                               .expect("to create S3 store")
}
```
