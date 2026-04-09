VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LINKED_GOWORK := $(CURDIR)/go.work.linked
# Pin matches the default in Dockerfile. Override for local upgrade testing, e.g. GO_GOOGLESQL_BASE=go-googlesql:dev
GO_GOOGLESQL_BASE ?= ghcr.io/vantaboard/go-googlesql:0.5.5-recidiviz.3

emulator/build:
	CGO_ENABLED=1 CXX=clang++ go build -o bigquery-emulator \
		./cmd/bigquery-emulator

emulator/build-linked:
	CGO_ENABLED=1 CXX=clang++ GOWORK=$(LINKED_GOWORK) go build -o bigquery-emulator \
		./cmd/bigquery-emulator

docker/build:
	docker build -t bigquery-emulator . \
		--build-arg VERSION=${VERSION} \
		--build-arg GO_GOOGLESQL_BASE=${GO_GOOGLESQL_BASE}

docker/build-linked:
	docker build -t bigquery-emulator:linked -f Dockerfile.linked . \
		--build-arg VERSION=${VERSION} \
		--build-arg GO_GOOGLESQL_BASE=${GO_GOOGLESQL_BASE} \
		--build-context go_googlesql=../go-googlesql \
		--build-context go_googlesqlite=../go-googlesqlite

# Same dev image + GO_CACHE_ROOT as ../go-googlesql. Requires sibling checkouts and go.work.linked (build go-googlesql:dev in go-googlesql first).
GO_GOOGLESQL_ROOT ?= $(abspath $(CURDIR)/../go-googlesql)
GO_GOOGLESQLITE_ROOT ?= $(abspath $(CURDIR)/../go-googlesqlite)
DOCKER_DEV_IMAGE ?= go-googlesql:dev
GO_CACHE_ROOT ?= $(HOME)/.cache/go-googlesql

.PHONY: test/linux
test/linux:
	docker run --rm \
		-e CGO_ENABLED=1 -e CC=clang -e CXX=clang++ \
		-e GOPROXY=https://proxy.golang.org,direct \
		-e GOSUMDB=sum.golang.org \
		-e CCACHE_DIR=/root/.ccache -e CCACHE_COMPRESS=1 \
		-e GOWORK=/work/bigquery-emulator/go.work.linked \
		-v "$(CURDIR)":/work/bigquery-emulator \
		-v "$(GO_GOOGLESQL_ROOT)":/work/go-googlesql \
		-v "$(GO_GOOGLESQLITE_ROOT)":/work/go-googlesqlite \
		-v "$(GO_CACHE_ROOT)/gocache":/root/.cache/go-build \
		-v "$(GO_CACHE_ROOT)/gomodcache":/go/pkg/mod \
		-v "$(GO_CACHE_ROOT)/ccache":/root/.ccache \
		-w /work/bigquery-emulator \
		$(DOCKER_DEV_IMAGE) \
		bash -c "go test -race -v ./... -count=1"
