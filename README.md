# Viperblock - Block Storage for Edge, On-Premise, and Cloud Deployments

Viperblock is a block storage service which is compatible with object-storage (S3) for highly durable, consistent, and high-performance volumes. It is suitable for edge deployments, on-premise networks, and private cloud environments.

The system provides a network block device (NBD) with intelligent caching for read/write access, a log-structured database for writes, and batched operations. Viperblock enables the setup of highly available storage that can power virtual machines and containers with durable storage and persistence for critical edge and on-premise deployments.

# Design

Viperblock is a sophisticated block storage platform designed for durability, consistency, and high performance. Below is a detailed breakdown of the architecture.

## Core Architecture

Viperblock implements a layered approach to block storage:

1. **In-memory Writes Buffer**: New write operations are first stored in memory
2. **Write-Ahead Log (WAL)**: Data is periodically flushed to WAL files for durability, recommended to use high-speed NVMe devices as the WAL storage.
3. **Chunk Files**: WAL entries are consolidated into optimized chunk files which are stored on a specified backend (s3, or direct filesystem)
4. **Block Mapping**: A mapping system tracks where each logical block is stored in relation to the chunk file and offset position of the block.
5. **Storage Backends**: Multiple backend options (file, memory, S3) for flexible deployment

## Block Management

- **Default Block Size**: 4KB (standard size for physical disks and filesystems)
- **Default Chunk Size**: 4MB (`ObjBlockSize`)
- **Block Identification**: Each block has a unique ID (uint64) that represents its logical position

## Write Path

1. **In-memory Buffer**:

   - When `Write(block, data)` is called, a unique sequence number is assigned
   - The block is added to `Writes.Blocks` in memory
   - This provides fast acknowledgment to clients

2. **WAL (Write-Ahead Log)**:

   - The `Flush()` method moves blocks from memory to WAL files
   - WAL entries include: sequence number, block number, length, checksum, and data
   - This occurs every 5 seconds or when 64MB is written (`FlushInterval` and `FlushSize`)
   - WAL files provide durability in case of system crashes, which can be replayed to rebuild the filesystem or as a checkpoint to a specific time.

3. **Chunk File Creation**:
   - `WriteWALToChunk()` converts WAL entries to optimized chunk files
   - Blocks are deduplicated (newer versions with higher sequence numbers win)
   - Blocks are sorted by block number for speeding up sequential access
   - Multiple blocks are packed into a single chunk file with a 10-byte header to describe the chunk options.

## Block Mapping

The `BlocksToObject` structure is critical to Viperblock's operation:

```go
type BlocksToObject struct {
    mu sync.RWMutex
    BlockLookup map[uint64]BlockLookup
}

type BlockLookup struct {
    StartBlock   uint64
    NumBlocks    uint16
    ObjectID     uint64
    ObjectOffset uint32
}
```

- When a chunk file is created, entries are added to this mapping
- For each block or sequence of consecutive blocks, the system records:
  - The starting block number (`StartBlock`)
  - How many consecutive blocks are stored together (`NumBlocks`), inspired by `ext4` extents
  - Which chunk file contains the data (`ObjectID`)
  - Where in the chunk file the block data begins (`ObjectOffset`)

The `LookupBlockToObject()` function uses this mapping to quickly locate any block:

```go
func (vb *VB) LookupBlockToObject(block uint64) (objectID uint64, objectOffset uint32, err error)
```

## Storage Backends

Viperblock supports multiple storage backends:

1. **File Backend**: Stores chunks as files on the local filesystem
2. **Memory Backend**: Keeps data in memory (for testing or caching)
3. **S3 Backend**: Stores data in Amazon S3 or S3-compatible storage

This backend flexibility allows Viperblock to operate in various environments, from edge deployments to on-premise installations.

## Durability and Consistency

Several features ensure data durability and consistency:

- **Checksums**: All blocks have CRC32 checksums to detect corruption
- **Sequence Numbers**: Ensure the latest version of a block is used
- **WAL**: Provides durability before data is committed to chunk files
- **State Persistence**: System state can be saved and loaded (`SaveState`/`LoadState`)

## Performance Considerations

- **In-memory Cache**: Recently used blocks are cached for performance
- **Sequential Block Storage**: Blocks are sorted and stored sequentially when possible
- **Batched Operations**: Multiple blocks are combined into chunk files
- **Concurrent Access**: Thread-safe operations with fine-grained locking

This architecture makes Viperblock suitable for high-performance, durable block storage platform designed for edge, private cloud and secure on-premise deployments.

## Getting Started

### Dependencies

Install required `nbdkit` 

```
sudo apt install nbdkit nbdkit-plugin-dev
```

### Installation

Clone the repository and navigate to the project directory:

```bash
git clone https://github.com/yourusername/viperblock.git
cd viperblock
```

Build from source:

```bash
make
```

### Usage (NBD)

Work in progress for using Viperblock via NBD with KVM/QEMU to provide the block-storage for VM's.

### Usage (SFS)

To run a simulated filesystem, Simple File System (SFS) is included with Viperblock to simulate a real-world filesystem that interacts with the service.

To run Viperblock with different backend types, use the following commands:

#### Using S3 Backend

Viperblock is designed to be used in conjunction with [Predastore](https://github.com/mulgadc/predastore/) (designed by Mulga) as part of the Hive platform. Ensure Predastore is running on your local network as the S3 service, or alternatively Viperblock is compatible with Amazon S3 or other S3 implementations.

```bash
AWS_BUCKET="predastore" AWS_PROFILE=predastore AWS_REGION=ap-southeast-2 AWS_HOST="https://localhost:8443/" AWS_ACCESS_KEY="EXAMPLEKEY" AWS_SECRET_KEY="EXAMPLEKEY" ./bin/sfs -btype s3 -dir /dir/to/upload -sfsstate /tmp/state_s3.json -vbstate /tmp/vb_s3.json -voldata tmp -vol test1_s3
```

#### Using File Backend (for Development)

```bash
./bin/sfs -btype file -dir /dir/to/upload -sfsstate /tmp/state_file.json -vbstate /tmp/vb_file.json -voldata tmp -vol test1_file
```

### Command-Line Options

- `-btype string`: Backend type (file, memory, s3) (default "file")
- `-createvol`: Initialize a new volume with the specified `-vol` argument
- `-dir string`: Sample directory to read
- `-sfsstate string`: SFS (filesystem) state file to load
- `-size uint`: Volume size in bytes (default 524288)
- `-vbstate string`: Viperblock state file to load
- `-vol string`: Volume to read
- `-voldata string`: Volume data directory to store blocks

### Help

For more information on command-line options, run:

```bash
./bin/sfs -h
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the Apache 2.0 License.
