#!/usr/bin/env bash
# Install a .dockerignore in the workspace root (parent of bigquery-emulator) so
# `docker build -f bigquery-emulator/Dockerfile ... .` skips Bazel/cache paths
# that are not readable by the Docker daemon.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BQ_ROOT="$(cd "$HERE/.." && pwd)"
WORKSPACE="$(cd "$BQ_ROOT/.." && pwd)"
SRC="$HERE/parent.dockerignore"
DST="$WORKSPACE/.dockerignore"
if [[ -f "$DST" ]] && ! cmp -s "$SRC" "$DST"; then
	echo "Refusing to overwrite existing $DST (differs from $SRC)." >&2
	echo "Merge manually or remove the file, then re-run." >&2
	exit 1
fi
cp "$SRC" "$DST"
echo "Wrote $DST"
echo "From $WORKSPACE run:"
echo "  docker build -f bigquery-emulator/Dockerfile -t bigquery-emulator:local ."
