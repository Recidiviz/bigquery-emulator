# Docker image (local build)

The emulator links **go-googlesql** (CGO + C++) and **go-googlesqlite** using the default **`googlesql,googlesql_unified_prebuilt`** tags (see [go-googlesql `docs/prebuilt-cgo.md`](https://github.com/vantaboard/go-googlesql/blob/main/docs/prebuilt-cgo.md)).

## Recommended: build from this repo ([`Taskfile.yml`](Taskfile.yml))

With sibling checkouts `../go-googlesql` and `../go-googlesqlite`, from **this** directory:

```bash
task docker:build
```

This uses [`Dockerfile.linked`](Dockerfile.linked), **`GO_GOOGLESQL_BASE`** (default pinned in [`Taskfile.yml`](Taskfile.yml)), and **`docker build --build-context`** so the image matches your local trees. Override paths if needed: `GO_GOOGLESQL_ROOT=... GO_GOOGLESQLITE_ROOT=... task docker:build`.

## Alternative: parent-directory context (`Dockerfile`)

Your `go.mod` uses `replace` to sibling modules; the classic image is built from a **parent directory** that contains all three repositories:

```text
your-workspace/
  go-googlesql/
  go-googlesqlite/
  bigquery-emulator/    ← this repo
```

From the **parent** directory (e.g. `~/Code` if your clones live there):

1. **One-time:** install a workspace-root `.dockerignore` so Docker does not try to send Bazel/cache trees under `go-googlesql` (often root-owned → `permission denied`):

   ```bash
   ./bigquery-emulator/docker/prep-context.sh
   ```

   This writes `../.dockerignore` relative to `bigquery-emulator` (the directory that contains `go-googlesql`, `go-googlesqlite`, and `bigquery-emulator`). It refuses to overwrite an existing `.dockerignore` that differs; merge by hand or remove the file first.

2. **Build the image:**

   ```bash
   docker build -f bigquery-emulator/Dockerfile -t bigquery-emulator:local .
   ```

If your Docker CLI supports `docker build --ignorefile …` (BuildKit 0.11+), you can skip `prep-context.sh` and pass `--ignorefile bigquery-emulator/docker/parent.dockerignore` instead of installing `.dockerignore`.

## Run

Default flags listen on all interfaces (HTTP **9050**, gRPC **9060**) with project `dev` and dataset `local`:

```bash
docker run --rm -p 9050:9050 -p 9060:9060 bigquery-emulator:local
```

Override project, dataset, or ports:

```bash
docker run --rm -p 8080:8080 -p 9090:9090 bigquery-emulator:local \
  --project=my-gcp-project \
  --dataset=analytics \
  --host=0.0.0.0 \
  --port=8080 \
  --grpc-port=9090 \
  --log-level=debug
```

Point BigQuery client libraries at the emulator using your usual mechanism (e.g. `BIGQUERY_EMULATOR_HOST` for some stacks, or API endpoint URL for REST). The REST base URL is `http://localhost:<port>` when the port is published as above.

Persist the SQLite backing store (default is a temp file) by mounting a file and passing `--database`:

```bash
docker run --rm -p 9050:9050 -v bqemu-data:/data bigquery-emulator:local \
  --project=dev --dataset=local --database=/data/emulator.db
```

Use in-memory storage:

```bash
docker run --rm -p 9050:9050 bigquery-emulator:local \
  --project=dev --dataset=local --database=:memory:
```

Environment variables (see `--help`) mirror the long flags, e.g. `BIGQUERY_EMULATOR_PROJECT`, `BIGQUERY_EMULATOR_DATASET`.

## Why not plain `docker build .` without `Dockerfile.linked`?

Published versions on the module proxy do not ship prebuilt `.a` archives; the supported stack uses sibling **`go-googlesql`** sources (with prebuilts) plus **`go-googlesql-stack-bootstrap.sh`**-compatible env at link time. [`Taskfile.yml`](Taskfile.yml) `docker:build` encodes that layout.

## Why not `docker build` inside only `bigquery-emulator/` without build contexts?

Published versions on the module proxy may not match your `replace`-based workspace; building from the three-repo layout matches `go.mod` and avoids API skew.
