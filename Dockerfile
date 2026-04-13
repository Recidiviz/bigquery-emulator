# syntax=docker/dockerfile:1.6
#
# BigQuery emulator needs CGO (go-googlesql) and must be built against the same
# go-googlesql / go-googlesqlite sources as this repo's go.mod replace lines.
#
# Build from the parent directory that contains all three repos side by side:
#   go-googlesql/
#   go-googlesqlite/
#   bigquery-emulator/
#
#   cd /path/to/parent
#   docker build -f bigquery-emulator/Dockerfile \
#     --ignorefile bigquery-emulator/docker/parent.dockerignore \
#     -t bigquery-emulator:local .

FROM golang:1.24-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
		clang \
		mold \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY go-googlesql /src/go-googlesql
COPY go-googlesqlite /src/go-googlesqlite
COPY bigquery-emulator /src/bigquery-emulator

WORKDIR /src/bigquery-emulator

# Align with go-googlesql Taskfile / scripts/go-googlesql-env.sh (default unified prebuilt path).
ENV CGO_ENABLED=1
ENV CC=clang
ENV CXX=clang++
ENV CGO_CXXFLAGS=-stdlib=libc++
ENV CGO_LDFLAGS_ALLOW="-Wl,--no-gc-sections|-Wl,--allow-multiple-definition|-fuse-ld=mold|-Wl,--whole-archive|-Wl,--no-whole-archive|-Wl,--start-group|-Wl,--end-group|-stdlib=libc\+\+"
ENV CGO_LDFLAGS="-Wl,--no-gc-sections -Wl,--allow-multiple-definition -fuse-ld=mold -stdlib=libc++"

RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	go build -tags googlesql,googlesql_unified_prebuilt -trimpath -ldflags="-s -w" \
		-o /out/bigquery-emulator \
		./cmd/bigquery-emulator

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
		ca-certificates \
		libstdc++6 \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/bigquery-emulator /usr/local/bin/bigquery-emulator

EXPOSE 9050 9060

ENTRYPOINT ["/usr/local/bin/bigquery-emulator"]

# --project is required; override at `docker run` time.
CMD ["--project=dev", "--dataset=local", "--host=0.0.0.0", "--port=9050", "--grpc-port=9060", "--log-level=info"]
