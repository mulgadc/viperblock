# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Viperblock codebase.

## Project Overview

Viperblock is a high-performance, WAL-backed block storage service that provides EBS-compatible functionality for the Hive platform. It can be used standalone or as part of the integrated Hive AWS-compatible stack.

## Core Architecture

Viperblock implements a sophisticated block storage system with:

- **In-memory Write Buffer**: Fast write acknowledgment with memory caching
- **Write-Ahead Log (WAL)**: Durability via WAL files on high-speed NVMe storage
- **Chunk-based Storage**: WAL entries consolidated into optimized chunk files
- **Block Mapping**: Efficient lookup system for logical block to storage location mapping
- **Multiple Backends**: Flexible storage backends (file, memory, S3-compatible)

## Build Commands

```bash
make build          # Build sfs and vblock binaries + NBD plugin
make test           # Run Go tests with LOG_IGNORE=1
make bench          # Run benchmarks
make clean          # Clean build artifacts (removes ./bin/sfs)
```

### **MANDATORY: Unit Testing Requirements**

**⚠️ CRITICAL: Always run unit tests before any commit or push operation.**

```bash
# REQUIRED before any git commit or push
make test           # Must pass with zero failures (uses LOG_IGNORE=1)

# Example workflow:
make test                    # Verify all tests pass (file and S3 backends)
git add .                    # Stage changes
git commit -m "..."          # Only after tests pass
git push origin main         # Only after tests pass
```

**Testing Policy:**
- **All unit tests MUST pass** before committing changes
- **No exceptions** - failing tests block commits
- Tests include file backend, S3 backend, and memory backend scenarios
- Tests must complete without errors, panics, or memory leaks
- Use `make test` which runs with `LOG_IGNORE=1` to suppress test logs
- If tests fail, fix issues before proceeding with git operations

**Test Coverage:**
- Block read/write operations across all backend types
- WAL (Write-Ahead Log) functionality and replay
- Volume creation, formatting, and state management
- Integration tests with Predastore S3 backend

### Key Binaries
- `./bin/sfs` - Simple File System demo with Viperblock integration
- `./bin/vblock` - Viperblock management utility
- `lib/nbdkit-viperblock-plugin.so` - NBD plugin for integration with nbdkit

## Usage Examples

### Standalone Usage (File Backend)
```bash
./bin/sfs -btype file -dir /dir/to/upload -sfsstate /tmp/state_file.json -vbstate /tmp/vb_file.json -voldata tmp -vol test1_file
```

### S3 Backend (with Predastore)
```bash
AWS_BUCKET="predastore" AWS_PROFILE=predastore AWS_REGION=ap-southeast-2 AWS_HOST="https://localhost:8443/" AWS_ACCESS_KEY="EXAMPLEKEY" AWS_SECRET_KEY="EXAMPLEKEY" ./bin/sfs -btype s3 -dir /dir/to/upload -sfsstate /tmp/state_s3.json -vbstate /tmp/vb_s3.json -voldata tmp -vol test1_s3
```

## Key Design Patterns

### Block Management
- **Block Size**: 4KB (standard filesystem alignment)
- **Chunk Size**: 4MB (`ObjBlockSize`) for optimized storage
- **Block Mapping**: Extent-based mapping inspired by ext4 filesystem design
- **Sequence Numbers**: Ensure latest version of blocks during conflicts

### Storage Backends
```go
// Backend interface for pluggable storage
type Backend interface {
    Init() error
    Read(objectID uint64) ([]byte, error)
    Write(objectID uint64, data []byte) error
    Delete(objectID uint64) error
}
```

**Available Backends**:
- **File Backend**: Local filesystem storage for development/testing
- **Memory Backend**: In-memory storage for testing and caching
- **S3 Backend**: S3-compatible storage (works with Predastore or AWS S3)

### Performance Optimizations
- **In-memory Caching**: Recently accessed blocks cached for performance
- **Sequential Storage**: Blocks sorted and stored sequentially when possible
- **Batched Operations**: Multiple blocks combined into chunk files
- **WAL Coalescing**: Multiple WAL entries merged before chunk creation

## Integration with Hive Platform

### NATS Integration
When used with Hive, Viperblock integrates via NATS messaging:

```bash
# EBS Service Topics:
ebs.createvolume             # Create new EBS volume
ebs.attachvolume             # Attach volume to instance
ebs.detachvolume             # Detach volume
ebs.describevolumes          # List volumes
ebs.mount                    # Mount volume via NBD
ebs.unmount                  # Unmount volume
```

### NBD Plugin Integration
- **Plugin Location**: `lib/nbdkit-viperblock-plugin.so`
- **NBD Protocol**: Exposes Viperblock volumes as network block devices
- **QEMU Integration**: Volumes can be attached to VMs via NBD protocol

## File Structure and Key Components

### Core Packages
```bash
viperblock/                  # Main package
├── types/                   # Type definitions and constants
├── backends/               # Storage backend implementations
│   ├── file/              # Local filesystem backend
│   ├── memory/            # In-memory backend
│   └── s3/                # S3-compatible backend
├── cmd/
│   ├── sfs/               # Simple filesystem demo
│   └── vblock/            # Volume management utility
├── nbd/                   # NBD plugin implementation
└── tests/                 # Test suites
```

### Key Data Structures
```go
// Core volume configuration
type VB struct {
    VolumeName   string
    VolumeSize   uint64
    BlockSize    uint64
    BaseDir      string
    Cache        Cache
    VolumeConfig VolumeConfig
    Backend      Backend
}

// Block to object mapping (inspired by ext4 extents)
type BlockLookup struct {
    StartBlock   uint64    // Starting block number
    NumBlocks    uint16    // Number of consecutive blocks
    ObjectID     uint64    // Which chunk file contains the data
    ObjectOffset uint32    // Offset within chunk file
}
```

## Development Guidelines

### Testing
- Use `LOG_IGNORE=1` environment variable to suppress logs during testing
- Test with different backend types (file, memory, s3)
- Verify WAL replay functionality after crashes
- Test block mapping and extent management

### Performance Considerations
- **WAL Configuration**: Place WAL on fastest storage (NVMe) for optimal performance
- **Cache Sizing**: Tune in-memory cache size based on available RAM
- **Chunk Size**: 4MB chunks balance storage efficiency and I/O performance
- **Flush Intervals**: Balance durability vs performance with flush timing

### Backend Development
When implementing new backends:
1. Implement the `Backend` interface
2. Add initialization in `viperblock.New()`
3. Ensure proper error handling and cleanup
4. Test with various volume sizes and access patterns

## Error Handling

### Common Error Types
- `ZeroBlock`: Indicates a block contains only zeros (optimization)
- Backend-specific errors (network timeouts, disk full, etc.)
- WAL corruption or replay failures
- Block mapping inconsistencies

### Recovery Procedures
- **WAL Replay**: Automatic replay of WAL entries on startup
- **State Recovery**: Volume state can be recovered from backend storage
- **Consistency Checks**: Block mapping validation during startup

## Command Line Options

### SFS (Simple File System)
```bash
-btype string      # Backend type: file, memory, s3 (default "file")
-createvol         # Initialize new volume
-dir string        # Directory to read/upload
-sfsstate string   # SFS state file path
-vbstate string    # Viperblock state file path
-size uint         # Volume size in bytes (default 524288)
-vol string        # Volume name
-voldata string    # Volume data directory
```

## Integration Notes

### With Predastore (S3 Backend)
- Set AWS environment variables for Predastore endpoint
- Use bucket name "predastore" for system integration
- Supports multipart uploads for large chunk files

### With Hive Platform
- Volumes exposed via NBD protocol for VM attachment
- Integrated with EC2 instance lifecycle management
- Supports EBS-style operations (create, attach, detach, snapshot)

### Standalone Usage
- Can be used independently of Hive platform
- Supports direct file I/O operations
- Useful for testing and development scenarios