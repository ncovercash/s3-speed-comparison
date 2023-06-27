# s3-speed-comparison

This script runs a benchmark against a local MinIO server to compare the speeds
of uploads with and without multipart upload; additionally, it will compare the
speeds of multipart uploads with different chunk sizes.

## Requirements

- S3-like storage and credentials
- At least 15 GB of allocated storage in the S3 instance
- Approximately eight minutes to let it cook (depending on your bandwidth/server)

## Usage

First, generate access keys for your MinIO instance. Then, run:

```sh
ACCESS_KEY=ABCDEF SECRET_KEY=123456 ENDPOINT=http://localhost:9000 yarn benchmark
```

If these are not specified, the script will default to `http://localhost:9000` with credentials `minioadmin`:`minioadmin`

Once you've run this, `results.json` will have been created. To visualize the data, run:

```sh
yarn chart results.json
```

This will print lots of data as well as generate a folder `results/` with a bunch of charts and tables.
