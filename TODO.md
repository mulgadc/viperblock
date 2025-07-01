* Update viperblock_test.go to setup teting and benchmark framework to use generics and simplify the duplicated functions
* Add basic optimistations for read/write block performance
* Add WAL log for block2object lookup support, with checkpointing support and crc checksums for rebuilding
* Store block2object, WAL and data files on S3
* Confirm range query for S3 correct, parts are out of range
* Improve debugging via slog for background daemon
* Add support for boot disk, EFI vars, and root disk for qemu
* Add support for .raw and qcow conversion to viperblock native format
* dd benchmark for instance running on viperblock
* Add improved cache settings for read/write, write cache disabled at the moment.
* Simplify example qemu installation using viperblock and predastore.
* Add ctx support for S3 upload/get, handle retry errors, backoff, etc.
* Support https://wiki.debian.org/SecureBoot for EFI