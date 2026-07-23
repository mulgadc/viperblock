# Viperblock benchmarks

## Flush / image-import benchmarks

`viperblock/import_flush_bench_test.go` reproduces the spinifex image-import-and-flush-to-disk path (`v_utils.ImportDiskImage`) so flush performance can be compared across commits:

- `BenchmarkImportImage_SpinifexStyle` — file backend. Isolates viperblock's WAL->chunk flush to local disk, no network in the path.
- `BenchmarkImportImage_Predastore` — S3 backend against the predastore test server started in `TestMain`. The real production path: each flush uploads 4 MiB chunk objects that predastore erasure-codes and replicates over raft, so it captures upload cost (the likely end-to-end bottleneck).
- `BenchmarkFlush_LegacyWAL` / `BenchmarkFlush_ShardedParallel` — flush micro-benchmarks in `sharded_wal_bench_test.go`.

`VB_IMPORT_MB` sets the synthetic image size in MiB (default 256).

Point `TMPDIR` at real disk (not tmpfs) so flushes hit an actual device; otherwise the file backend measures RAM throughput.

```bash
# Single run
VB_IMPORT_MB=128 TMPDIR=/var/tmp \
  go test -run='^$' -bench='BenchmarkImportImage|BenchmarkFlush' \
  -benchtime=1x -count=10 ./viperblock
```

## Comparing two commits

Use throwaway worktrees so each commit builds against its own source, then `benchstat`. `GOWORK=off` isolates each worktree from the repo's `go.work`.

```bash
git worktree add --detach /tmp/vb-old <old-commit>
git worktree add --detach /tmp/vb-new <new-commit>

# Copy the benchmark file into the old worktree if it predates this commit:
cp viperblock/import_flush_bench_test.go /tmp/vb-old/viperblock/

for wt in /tmp/vb-old /tmp/vb-new; do
  ( cd "$wt/viperblock" && GOWORK=off VB_IMPORT_MB=128 TMPDIR=/var/tmp \
    go test -run='^$' -bench='BenchmarkImportImage_SpinifexStyle' \
    -benchtime=1x -count=10 . ) \
    | grep -E '^(goos|goarch|pkg|cpu|Benchmark)' > "$(basename "$wt").txt"
done

benchstat vb-old.txt vb-new.txt
git worktree remove /tmp/vb-old && git worktree remove /tmp/vb-new
```

Notes:

- `TestMain` starts a predastore server that installs a JSON slog logger on the process default; benchmarks call `silenceVBLogs()` at start to stop it corrupting result lines. Keep that call when adding new benchmarks here.
- The predastore variant leaves objects in the shared server's raft state under a unique per-iteration volume prefix; `TestMain` reclaims the data dir on teardown.
