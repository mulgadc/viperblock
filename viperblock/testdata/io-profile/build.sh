#!/usr/bin/env bash
set -euo pipefail

fixture=$(cd "$(dirname "$0")" && pwd)
tree=$(realpath "$1")
out=$(realpath -m "$2")
mkdir -p "$out"
overlay=$(mktemp)
trap 'rm -f -- "$overlay"' EXIT
python3 - "$tree" "$fixture" > "$overlay" <<'PY'
import json
from pathlib import Path
import sys

tree, fixture = (Path(arg) for arg in sys.argv[1:])
print(json.dumps({"Replace": {str(tree / "viperblock/io_profile_fixture_test.go"): str(fixture / "server_test.go")}}))
PY
export GOWORK=off
export GOFIPS140=v1.0.0
(cd "$tree"; go test -overlay "$overlay" -c ./viperblock -o "$out/server.test")
(cd "$tree/nbd"; go build -buildmode=c-shared -o "$out/plugin.so" viperblock.go)

# A test-only plugin build prevents the periodic syncer/uploader satisfying
# a barrier before the crash probe checks it.
scratch=$(mktemp -d)
trap 'rm -f -- "$overlay"; rm -rf -- "$scratch"' EXIT
sed '/LeaseEpoch:  lease_epoch,/a\        WALSyncInterval: -1, ChunkUploadInterval: -1,' \
    "$tree/nbd/viperblock.go" > "$scratch/profile_plugin.go"
grep -q 'WALSyncInterval: -1, ChunkUploadInterval: -1' "$scratch/profile_plugin.go"
(cd "$tree/nbd"; go build -buildmode=c-shared -o "$out/plugin-no-background.so" "$scratch/profile_plugin.go")
