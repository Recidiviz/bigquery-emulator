# BigQuery Emulator (Recidiviz fork)

The BigQuery emulator provides a way to launch a BigQuery server on your local machine for testing and development.

The Recidiviz fork has many features, performance improvements, and bugfixes that are missing from the upstream repository.

# Features

- If you use the Go BigQuery client, you can launch the emulator within the testing process by [httptest](https://pkg.go.dev/net/http/httptest) .
- BigQuery emulator can be launched as a standalone process. So, you can use the BigQuery emulator from programs written in non-Go languages or the [bq](https://cloud.google.com/bigquery/docs/bq-command-line-tool) command, by specifying the address of the launched BigQuery emulator.
- BigQuery emulator utilizes SQLite for storage. You can select either memory or file as the data storage destination at startup, and if you set it to file, data can be persisted.
- You can load seed data from a JSON or YAML file on startup

## BigQuery API

We've implemented all the [BigQuery APIs](https://cloud.google.com/bigquery/docs/reference/rest) except the API to manipulate IAM resources. It is possible that some options are not supported, in which case please report them in an Issue.

## Google Cloud Storage

BigQuery emulator supports loading data from Google Cloud Storage and exporting table data. Currently, only CSV and JSON data types can be used for export.
If you use Google Cloud Storage emulator, please set `STORAGE_EMULATOR_HOST` environment variable.

## BigQuery Storage API

Supports gRPC-based read/write using [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage).
Supports both Apache `Avro` and `Arrow` formats.

## Google Standard SQL

BigQuery emulator supports many of the specifications present in [Google Standard SQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/introduction).
For example, it has the following features.

- 200+ standard functions
- Wildcard table
- Templated Argument Function
- JavaScript UDF

If you want to know which specific features are supported, please see [here](https://github.com/vantaboard/go-googlesqlite#status)

# Sponsor 

If this project is of useful to you or your team, consider sponsoring the original creator [@goccy](https://github.com/goccy)

# Installation
If Go is installed, you can install the latest version with the following command

```console
$ go install github.com/Recidiviz/bigquery-emulator/cmd/bigquery-emulator@latest
```

The BigQuery emulator depends on [go-googlesql](https://github.com/vantaboard/go-googlesql).
This library takes a very long time to install because it automatically builds the GoogleSQL library during install.
It may look like it hangs because it does not log anything during the build process, but if the `clang` process is running in the background, it is working fine, so just wait it out.
Also, for this reason, the following environment variables must be enabled for installation.

```console
CGO_ENABLED=1
CXX=clang++
```

You can also download the docker image with the following command

```console
$ docker pull ghcr.io/Recidiviz/bigquery-emulator:latest
```

You can also download the darwin(amd64) and linux(amd64) binaries directly from [releases](https://github.com/goccy/bigquery-emulator/releases)

## Development build modes

**Default: unified prebuilt stack (`googlesql` + `googlesql_unified_prebuilt`):** Native archives, release tarball **`go-googlesql-prebuilts-default-linux_amd64-<tag>.tar.gz`**, and downstream checklist live in [`go-googlesql` `docs/prebuilt-cgo.md`](https://github.com/vantaboard/go-googlesql/blob/main/docs/prebuilt-cgo.md). When you bump `github.com/vantaboard/go-googlesql`, use the **same** Git tag for the Go module, any prebuilt tarball you unpack, and [`docs/stack-release-policy.md`](https://github.com/vantaboard/go-googlesql/blob/main/docs/stack-release-policy.md).

**Host linker env:** [direnv](https://direnv.net/) with this repo’s [`.envrc`](.envrc), or [`go-googlesql/scripts/go-googlesql-stack-bootstrap.sh`](https://github.com/vantaboard/go-googlesql/blob/main/scripts/go-googlesql-stack-bootstrap.sh), so **`CGO_LDFLAGS_ALLOW`** / **`CGO_LDFLAGS`** match [`go-googlesql` `Taskfile.yml`](https://github.com/vantaboard/go-googlesql/blob/main/Taskfile.yml).

For normal `bigquery-emulator` work with sibling `replace` checkouts:

```console
$ task emulator:build
$ task docker:build
```

`emulator:build-linked` uses `go.work.linked` (same bootstrap and tags). `docker:build-linked` is an alias for `docker:build` ([`Dockerfile.linked`](Dockerfile.linked) + sibling build contexts).

For **repeat** host builds, use **`CC="ccache clang"`** and **`CXX="ccache clang++"`** (and on **Linux**, **`mold`** on **`PATH`**), or **`task test:linux`** for CI-parity tests inside **`go-googlesql:dev`**.

**CI:** [`.github/workflows/test.yml`](.github/workflows/test.yml) checks out **`vantaboard/go-googlesql`** and **`vantaboard/go-googlesqlite`** at the pinned **`go.mod`** versions beside this repo, runs **`ci-download-or-build-default-prebuilts.sh`** on **`go-googlesql`**, then **`task emulator:build`** and **`go test`** with [`go-googlesql-stack-bootstrap.sh`](https://github.com/vantaboard/go-googlesql/blob/main/scripts/go-googlesql-stack-bootstrap.sh) so the default **`googlesql,googlesql_unified_prebuilt`** link path matches local sibling development.

### Local `go-googlesql` base image (upgrade / CGO cache)

Docker builds use a **pinned Go+clang** base (`GO_GOOGLESQL_BASE`, default `ghcr.io/vantaboard/go-googlesql:0.5.5-recidiviz.3`). To validate against a **local** toolchain image you built from the `go-googlesql` repo (for example tag `go-googlesql:dev`), pass env when invoking Task:

```console
$ GO_GOOGLESQL_BASE=go-googlesql:dev task docker:build
$ GO_GOOGLESQL_BASE=go-googlesql:dev task docker:build-linked
```

Build the `go-googlesql:dev` image first (`task docker:build-dev` in `go-googlesql`). The runtime stage must stay compatible with the linked binary (same glibc/toolchain expectations as the chosen base).

### Sequential test runs and shared caches

When testing the full stack locally, run heavy **`go test` / Docker builds sequentially** across `go-googlesql`, `go-googlesqlite`, and `bigquery-emulator` so parallel CGO compiles do not exhaust memory. Reuse a shared **`GOCACHE`** and **`GOMODCACHE`** (see [go-googlesql README](https://github.com/vantaboard/go-googlesql#development)) for faster host-native runs.

**`GO_CACHE_ROOT`:** The [Taskfile](Taskfile.yml) **`task test:linux`** target bind-mounts **`GO_CACHE_ROOT`** (default **`$HOME/.cache/go-googlesql`**) into **`gocache`**, **`gomodcache`**, and **`ccache`** in the container—the same convention as **`go-googlesql`** and **`go-googlesqlite`**. Set **`GO_CACHE_ROOT`** consistently across sibling checkouts so one warm cache serves all three repos.

**Optional warm-up:** Run **`task -d ../go-googlesql docker:warm-cache`** once after a cold cache or toolchain change so the next **`task test:linux`** here pays less compile cost (pre-builds the **`-race`** graph without running tests).

# How to start the standalone server

If you can install the `bigquery-emulator` CLI, you can start the server using the following options.

```console
$ ./bigquery-emulator -h
Usage:
  bigquery-emulator [OPTIONS]

Application Options:
      --project=        specify the project name
      --dataset=        specify the dataset name
      --port=           specify the http port number. this port used by bigquery api (default: 9050)
      --grpc-port=      specify the grpc port number. this port used by bigquery storage api (default: 9060)
      --log-level=      specify the log level (debug/info/warn/error) (default: error)
      --log-format=     specify the log format (console/json) (default: console)
      --database=       specify the database file if required. if not specified, it will be on memory
      --data-from-yaml= specify the path to the YAML file that contains the initial data
  -v, --version         print version

Help Options:
  -h, --help            Show this help message
```

Start the server by specifying the project name

```console
$ ./bigquery-emulator --project=test
[bigquery-emulator] REST server listening at 0.0.0.0:9050
[bigquery-emulator] gRPC server listening at 0.0.0.0:9060
```

If you want to use docker image to start emulator, specify like the following.

```console
$ docker run -it ghcr.io/Recidiviz/bigquery-emulator:latest --project=test
```

## How to use from bq client

### 1. Start the standalone server

```console
$ ./bigquery-emulator --project=test --data-from-yaml=./server/testdata/data.yaml
[bigquery-emulator] REST server listening at 0.0.0.0:9050
[bigquery-emulator] gRPC server listening at 0.0.0.0:9060
```

* `server/testdata/data.yaml` is [here](https://github.com/Recidiviz/bigquery-emulator/blob/main/server/testdata/data.yaml)

### 2. Call endpoint from bq client

```console
$ bq --api http://0.0.0.0:9050 query --project_id=test "SELECT * FROM dataset1.table_a WHERE id = 1"

+----+-------+---------------------------------------------+------------+----------+---------------------+
| id | name  |                  structarr                  |  birthday  | skillNum |     created_at      |
+----+-------+---------------------------------------------+------------+----------+---------------------+
|  1 | alice | [{"key":"profile","value":"{\"age\": 10}"}] | 2012-01-01 |        3 | 2022-01-01 12:00:00 |
+----+-------+---------------------------------------------+------------+----------+---------------------+
```

## How to use from python client

> **For Python unit testing**: See the comprehensive [Python Testing Guide](test/python/README.md) for using the emulator with testcontainers, pytest fixtures, and `unittest.TestCase`.

### 1. Start the standalone server

```console
$ ./bigquery-emulator --project=test --dataset=dataset1
[bigquery-emulator] REST server listening at 0.0.0.0:9050
[bigquery-emulator] gRPC server listening at 0.0.0.0:9060
```

### 2. Call endpoint from python client

Create ClientOptions with api_endpoint option and use AnonymousCredentials to disable authentication.

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig

client_options = ClientOptions(api_endpoint="http://0.0.0.0:9050")
client = bigquery.Client(
  "test",
  client_options=client_options,
  credentials=AnonymousCredentials(),
)
client.query(query="...", job_config=QueryJobConfig())
```

If you use a DataFrame as the download destination for the query results,
You must either disable the BigQueryStorage client with `create_bqstorage_client=False` or
create a BigQueryStorage client that references the local grpc port (default 9060).

https://cloud.google.com/bigquery/docs/samples/bigquery-query-results-dataframe?hl=en

```python
result = client.query(sql).to_dataframe(create_bqstorage_client=False)
```

or

```python
from google.cloud import bigquery_storage

client_options = ClientOptions(api_endpoint="0.0.0.0:9060")
read_client = bigquery_storage.BigQueryReadClient(client_options=client_options)
result = client.query(sql).to_dataframe(bqstorage_client=read_client)
``` 

# Synopsis

If you use the Go language as a BigQuery client, you can launch the BigQuery emulator on the same process as the testing process.  
Import `github.com/Recidiviz/bigquery-emulator/server` ( and `github.com/Recidiviz/bigquery-emulator/types` ) and you can use `server.New` API to create the emulator server instance.

See the API reference for more information: https://pkg.go.dev/github.com/goccy/bigquery-emulator

```go
package main

import (
  "context"
  "fmt"

  "cloud.google.com/go/bigquery"
  "github.com/Recidiviz/bigquery-emulator/server"
  "github.com/Recidiviz/bigquery-emulator/types"
  "google.golang.org/api/iterator"
  "google.golang.org/api/option"
)

func main() {
  ctx := context.Background()
  const (
    projectID = "test"
    datasetID = "dataset1"
    routineID = "routine1"
  )
  bqServer, err := server.New(server.TempStorage)
  if err != nil {
    panic(err)
  }
  if err := bqServer.Load(
    server.StructSource(
      types.NewProject(
        projectID,
        types.NewDataset(
          datasetID,
        ),
      ),
    ),
  ); err != nil {
    panic(err)
  }
  if err := bqServer.SetProject(projectID); err != nil {
    panic(err)
  }
  testServer := bqServer.TestServer()
  defer testServer.Close()

  client, err := bigquery.NewClient(
    ctx,
    projectID,
    option.WithEndpoint(testServer.URL),
    option.WithoutAuthentication(),
  )
  if err != nil {
    panic(err)
  }
  defer client.Close()
}
```

# Debugging

If you have specified a database file when starting `bigquery-emulator`, you can check the status of the database by using the `googlesqlite-cli` tool. See [here](https://github.com/vantaboard/go-googlesqlite/tree/main/cmd/googlesqlite-cli#readme) for details.

# How it works

## BigQuery Emulator Architecture Overview

After receiving a query, `go-googlesqlite` parses and analyzes the input query using `google/googlesql`. 
Query metadata objects are extracted from the AST, then transformed into a SQLite-compatible query.
The [modernc.org/sqlite](https://modernc.org/sqlite) driver is then used to access the SQLite Database.

<img width="600px" src="https://user-images.githubusercontent.com/209884/196145011-e35c2df4-5f5d-43ce-b7df-08cd130b5d31.png"></img>


## Type Conversion Flow

BigQuery has a number of types that do not exist in SQLite (e.g. ARRAY and STRUCT).
In order to handle them in SQLite, `go-googlesqlite` encodes all types except `INT64` / `FLOAT64` / `BOOL` with the type information and data combination.
When using the encoded data, the data is decoded via a custom function registered with driver before use.

<img width="600px" src="https://user-images.githubusercontent.com/209884/196145033-aa032878-7e01-4ec7-9a23-b174b87e1a24.png"></img>


# Reference

Regarding the story of bigquery-emulator, there are the following articles.
- [How to create a BigQuery Emulator](https://docs.google.com/presentation/d/1j5TPCpXiE9CvBjq78W8BWz-cGxU8djW1qy9Y6eBHso8/edit?usp=sharing) ( Japanese )


# License

MIT
