#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")" && pwd)
worktree=$(realpath "$1")
output=$(realpath -m "$2")
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
cp "$root/main.go" "$scratch/"
cat > "$scratch/go.mod" <<MOD
module shimprobe

go 1.27

require libguestfs.org/nbdkit v0.0.0
replace libguestfs.org/nbdkit => $worktree/nbd/libguestfs.org/nbdkit
MOD
cd "$scratch"
GOWORK=off go build -buildmode=c-shared -o "$output" .
