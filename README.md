# s3-speed-comparison

This script runs a benchmark against a local MinIO server to compare the speeds
of uploads with and without multipart upload; additionally, it will compare the
speeds of multipart uploads with different chunk sizes.

## Usage

First, generate access keys for your MinIO instance. Then, run:

```sh
ACCESS_KEY=ABCDEF SECRET_KEY=123456 yarn benchmark
```
