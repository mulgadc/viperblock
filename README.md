<p align="center">
  <img src=".github/assets/banner.svg" alt="Viperblock by Mulga — WAL-backed block storage for QEMU and KVM, combining fast local writes with durable S3-backed volumes.” width="900">
</p>

<p align="center">
  <a href="https://go.dev"><img src="https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-AGPL--3.0-3fb950?style=flat-square" alt="License"></a>
  <a href="https://mulgadc.com"><img src="https://img.shields.io/badge/home-mulga-orange?style=flat-square&logo=data:image/svg%2bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGZpbGw9IiNmZmYiIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZD0iTTE2LjcxOCA4Ljg5MWMtMS4yODUgMS4zNy0zLjE3OCAyLjMxNy00LjY5NSAzLjQ0LS44NTQuNjMtMy4wODkgMi4yNy0xLjYxIDMuMzQ0IDEuNzg4IDEuMjk4IDYuMjQzLjE2NCA3Ljc4NS0xLjI2Ljg0LS43NzUuODE0LTEuODIyLS4zMjgtMi4yNTktMS4yMzUtLjQ3Mi0yLjY2LS4xMTEtMy45MTMuMDg3LS4wNDIuMDA3LS4xMzEuMDU1LS4xMjMtLjAyLjUyMy0uMzM2Ljk5My0uNzUgMS41MDMtMS4xMDIuNDkyLS4zNCAxLjA5Ny0uODI3IDEuNy0uODM4IDEuODcxLS4wMzQgMy43OTkuODkgNC4yODcgMi44MTUuODExIDMuMjAzLTMuMDA2IDUuNzE1LTUuNzg0IDUuOTQybDEuNjE0LS43NDdjLjYwNS0uMzYgMS4yMTctLjczNCAxLjc1Mi0xLjE5Ni4xMzMtLjExNS4yMy0uMjYzLjM0Mi0uMzczLjAyNy0uMDI2LjMwMi0uMTQ0LjE0NC0uMTUtMS40NDEuOTQ1LTMuMTI3IDEuNTcyLTQuODEzIDEuOTMyLS41OC4xMjMtMS4xOTUuMjQyLTEuNzg1LjI1Ny4wMTUuMDguMDg3LjA2NC4xNC4wOC40NzYuMTU0IDEuMDIuMjQ1IDEuNTE2LjMyLjA2OC4wNTItLjAwNi4wNzQtLjA2LjA4MS0uNDQ4LjA1OC0uOTIzLjE0My0xLjM3LjE2Ni0xLjI3LjA2NC0yLjU1LS4wNjgtMy43NzctLjM4bC0uNjktMS45NTdjLS4wNi4wMDYtLjA2My4wNi0uMDc5LjEwMy0uMDYuMTctLjMwNSAxLjQxNy0uMzg3IDEuNDM1LS42Ni0uMi0xLjI1MS0uNjQ3LTEuNzM2LTEuMTMybDEuMTUtMi4wNi0xLjgzNSAxLjAxMWMtLjI1OC0uNTgzLS4zMDUtMS4yNDMtLjIxOS0xLjg3bDIuMzc1LTEuMThjLS43MzktLjA0OS0xLjQ4Ni4wNjgtMi4yMi4xNDUuMTI1LS4zNi4yNzctLjcxOC40NjItMS4wNTEuMDY3LS4xMjIuNDI2LS43MjEuNTItLjcyNC44Ni4xNjQgMS43ODYuMjQxIDIuNjQ4LjA3NGwtMS44Ny0uNzcxLS4wNjktLjA5M2MuMzgyLS4zNi43Ny0uNzE3IDEuMTk5LTEuMDI0LjgwNC4yMzQgMS42My40MjIgMi40NzMuNDMzTDkuMzUgOS4zNDJhNyA3IDAgMCAxIC41NjctLjQwMWMuMTYtLjEwMy42MDktLjQwMi43NzItLjM4LjY1LjIyMSAxLjMyNC4zNzggMi4wMS40MzMtLjMxMy0uMzI1LS45MDktLjU2NC0xLjIxLS44NjgtLjAzMy0uMDM0LS4wNTgtLjAzNi0uMDQzLS4wOTguNzg3LS40NTIgMS42NjItLjkwMSAyLjM0LTEuNTE3LjY5LS42MjkgMS40MjEtMS42NTYuOTI0LTIuNjA1LjU3My4xODIgMS4wNjcuOTA2IDEuMDU0IDEuNTEyLS4wMzQgMS42NzYtMS44MjIgMy4xNy0yLjk0MyA0LjIyMiAxLjczMi0uNzI4IDMuNzE0LTIuMjMgMy43MS00LjMwNS0uMDAzLTEuNjg4LTEuNTQtMi4zNjUtMi45OTMtMi41MThhMy4yIDMuMiAwIDAgMS0uMzg1IDEuMDg3Yy0uNDI4LjcxOS0xLjMwMiAxLjE2OC0xLjc1OCAxLjkxNS0uMzExLjUxLS4zNyAxLjE5NS0xLjAzMSAxLjM5LS4yNy4wOC0uNjE3LjA5My0uODk3LjA5NGwuNjE1LS4zMjVjLjY4OS0uNTA2LjY3Ny0xLjQyIDEuMDk5LTIuMS0uMDUtLjA3LS41NDUtLjItLjY2LS4yMjktLjUxLS4xMjUtMS4zNzUtLjI4NS0xLjg4Ni0uMjUtLjE1Ny4wMS0uODY5LjEyMS0uOTMuMjQyLS4wODguMTc3LjM0MS45My40NjggMS4xMDEuMDI1LjAzNS4wNzcuMDI2LjA4LjAzLjAyNC4wMzEuMDA3LjEwNS0uMDYuMDhhNSA1IDAgMCAxLS40MDQtLjI4M2MtLjUwMy0uMzg4LTEuMDc4LS45NzQtMS40OTktMS40NDctLjA2OC0uMTY2LS4xNi0uMzUuMDAyLS40ODcuODg4LS41NSAxLjc2OC0xLjEyNiAyLjY3LTEuNjUxIDIuNjcxLTEuNTU4IDUuNzExLTMuMzE4IDguMjY3LS40NjUgMi4xMTkgMi4zNjYgMS41MTQgNS4yMTItLjUxMiA3LjM3MnptLTguMjI0LTUuMzhjLjY5OS0uMTIyIDIuMDE4LjU3MiAyLjIzNS0uNDU2LjAyMS0uMS0uMDM2LS4xNTcuMDItLjI1NS4wNDMtLjA3My4yODYtLjI1LjM3LS4zMTguMjctLjIxNy41NzctLjM4Ny44NDItLjYxMi0xLjAyOS4xNjYtMi4wNjUuNzU2LTIuOTY0IDEuMjc3LS4xNDkuMDg2LS4zMS4xNzYtLjQ1MS4yNzMtLjAzNy4wMjUtLjA3NS0uMDA2LS4wNTMuMDltLTQuODMgMTAuODQ0Yy0xLjcxNSAxLjg4MS0xLjMyNSA0LjU3LjQ5NCA2LjIxNyAxLjgxMSAxLjY0MSA0LjY2IDIuMjIzIDcuMDQ4IDIuMTA4IDIuMTUyLS4xMDMgNC4zMzctLjgxMiA2LjQ2LS4wOTEuODI1LjI4IDEuNTQ2LjgwNSAyLjE2IDEuNDExLS4wODMtLjQtLjMyNC0uODE5LS41NTgtMS4xNTktMi4xMy0zLjA5NS02LjI3LTEuOTM1LTkuNDI2LTIuNTUzLTEuNzExLS4zMzUtMy40OTEtMS4xMTgtNC41MzMtMi41NjctLjkwMS0xLjI1My0xLjA0Mi0yLjczLS41OTUtNC4xOTQtLjA5MS0uMDktLjk1Mi43MjEtMS4wNS44MjhtNi4zNjYtOC42NjdjLS4zODIuMzM5LS43ODcuNjYtMS4yMTIuOTQ4bC0xLjIwNy41OWMxLjAyMi4wMzkgMi4wOC0uNTQ2IDIuNDItMS41MzciLz48L3N2Zz4=" alt="mulgadc.com"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> ·
  <a href="#run-an-nbd-volume">NBD usage</a> ·
  <a href="#capabilities">Capabilities</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#storage-backends">Storage Backends</a> ·
  <a href="#spinifex-integration">Spinifex Integration</a> ·
  <a href="#development">Development</a> ·
  <a href="https://docs.mulgadc.com">Docs</a>
</p>

---

# Viperblock: WAL-backed block storage for QEMU and KVM

Viperblock is a block-storage engine for virtual machines running on QEMU and KVM. It provides durable writes, snapshots, caching and pluggable storage backends through an NBD interface.

Viperblock is the storage engine used by Spinifex for EBS-compatible volumes.

## Quick Start

### Dependencies

```bash
sudo apt install nbdkit nbdkit-plugin-dev
```

### Build

```bash
git clone https://github.com/mulgadc/viperblock.git
cd viperblock
make build
```

The build produces:

- `bin/sfs` — Simple File System demonstration
- `bin/vblock` — volume-management CLI
- `lib/nbdkit-viperblock-plugin.so` — nbdkit plugin

## Run an NBD volume

Viperblock volumes are served to QEMU/KVM through nbdkit. Spinifex manages this automatically; for standalone use:

```bash
nbdkit --filter=blocksize ./lib/nbdkit-viperblock-plugin.so \
  volume=my-volume \
  size=$((10*1024*1024*1024)) \
  base_dir=/data/viperblock \
  cache_size=20
```

Plugin parameters:

| Parameter | Description |
|-----------|-------------|
| `size` | Volume size in bytes |
| `volume` | Volume name |
| `base_dir` | Local storage directory (file backend) |
| `bucket` | S3 bucket (S3 backend) |
| `host` | S3 endpoint URL (S3 backend) |
| `region` | AWS region (S3 backend) |
| `access_key` / `secret_key` | S3 credentials (S3 backend) |
| `cache_size` | LRU cache as percentage of system memory |
| `shardwal` | Enable sharded WAL (`true`/`false`) |
| `gc_enabled` | Enable chunk garbage collection (`true`/`false`, default `false`) |

## Capabilities

- Durable write-ahead logging
- Sixteen sharded WAL writers
- Extent-based allocation and constant-time lookup
- Copy-on-write snapshots and clones
- File, memory and S3-compatible storage backends
- NBD access through nbdkit
- LRU read caching
- Arena-based memory allocation
- CRC32 integrity checks

## Architecture

<p align="center">
  <img src=".github/assets/platform.svg" alt="Viperblock: QEMU and KVM workloads on top, fast block I/O through NBD and an NVMe-backed write-ahead log, with durable volumes stored locally or in S3-compatible object storage." width="900">
</p>

See [DESIGN.md](DESIGN.md) for detailed write path, read path, WAL format, chunk format, and block mapping internals.

## Storage Backends

| Backend | Intended use |
| --- | --- |
| Memory | Tests and temporary volumes |
| Filesystem | Local development and standalone storage |
| S3-compatible | Distributed or remote persistent storage |

### SFS Demonstration

```bash
# File backend
./bin/sfs -btype file -dir /path/to/data -vol my-volume -voldata /tmp/vb

# S3-compatible backend
AWS_HOST="https://localhost:8443/" \
AWS_BUCKET="viperblock" \
AWS_ACCESS_KEY="EXAMPLEKEY" \
AWS_SECRET_KEY="EXAMPLEKEY" \
./bin/sfs -btype s3 -dir /path/to/data -vol my-volume -voldata /tmp/vb
```

### SFS Options

| Flag | Description | Default |
|------|-------------|---------|
| `-btype` | Backend type: `file`, `memory`, `s3` | `file` |
| `-vol` | Volume name | |
| `-size` | Volume size in bytes | 524288 |
| `-dir` | Directory to read into volume | |
| `-voldata` | Local directory for volume data | |
| `-createvol` | Initialize a new volume | |
| `-vbstate` | Viperblock state file path | |
| `-sfsstate` | SFS state file path | |

## Design Decisions

A summary of the key design choices. See [DESIGN.md](DESIGN.md) for the full treatment.

**WAL on fast local storage, chunks on S3.** Writes are acknowledged from memory and durably flushed to a local WAL (NVMe recommended). WAL entries are then consolidated into 4 MB chunks and uploaded to the backend. This separates write latency (local NVMe speed) from storage durability (S3 replication).

**Extent-based block mapping.** Rather than one index entry per 4 KB block, consecutive blocks are merged into extents (inspired by ext4). A 10-block sequential write produces one extent entry instead of ten, reducing memory usage and speeding up lookups.

**16-way sharded locking.** Both the `UnifiedBlockStore` and the sharded WAL use 16 shards keyed by `blockNum & 0xF`. Concurrent writes to different blocks never contend on the same lock.

**Copy-on-write snapshots.** `CreateSnapshot` freezes the block-to-object mapping without copying any data. Clones created from a snapshot read unmodified blocks from the source volume's chunks and only allocate new storage for blocks that are overwritten. This makes snapshot creation instant and clone creation near-instant regardless of volume size.

**CRC32 checksums everywhere.** Every WAL record includes a CRC32 checksum validated during replay and consolidation. Corrupt records are detected before they reach chunk storage.

## Spinifex Integration

Within Spinifex, Viperblock handles EBS-compatible volume storage and snapshots. It subscribes to volume-lifecycle NATS subjects, starts nbdkit for an attached volume, and returns an NBD URI for QEMU.

| Component | Role |
|-----------|------|
| **[Spinifex](https://github.com/mulgadc/spinifex)** | VM orchestration (EC2-compatible) |
| **Viperblock** | Block storage (EBS-compatible) |
| **[Predastore](https://github.com/mulgadc/predastore)** | Object storage (S3-compatible) |
| **[Northstar](https://github.com/mulgadc/northstar)** | Authoritative DNS (Route53-compatible) |

Viperblock can also be used standalone for any application that needs durable block storage with S3 as a backend tier.

## Development

```bash
make test
make preflight
```

## Research

The following papers informed the design of Viperblock:

- Hajkazemi, M. H. et al. "Beating the I/O bottleneck: a case for log-structured virtual disks." *EuroSys 2022.* https://doi.org/10.1145/3492321.3524271
- Zhou, D. et al. "Enabling high-performance and secure userspace NVM file systems with the trio architecture." *SOSP 2023.* https://doi.org/10.1145/3600006.3613171
- Li, H. et al. "Ursa: Hybrid block storage for cloud-scale virtual disks." *EuroSys 2019.* https://doi.org/10.1145/3302424.3303967

## Trademarks

Amazon Web Services, AWS and Amazon EBS are trademarks of Amazon.com, Inc. or its affiliates. Viperblock is not affiliated with or endorsed by Amazon Web Services.

## License

Viperblock is licensed under the [GNU Affero General Public License v3.0 (AGPLv3)](LICENSE) license.
