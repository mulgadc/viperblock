
# Dev flow

Make changes to predastore-rewrite, and viperblock.

# Environment setup

Confirm unmounted first

sudo nbd-client -d /dev/nbd0 
sudo nbd-client -d /dev/nbd1

## Compile Viperblock with latest

nbdkit uses `lib/nbdkit-viperblock-plugin.so` confirm latest built from predastore changes.

```
cd viperblock ; make
```

## Specify AMI to mount

For testing purposes, define the AWS AMI to mount

```
export AWS_AMI=ami-10bc1adb8bfa29b57
```

## Clean previous WAL

Confirm a fresh state

```
rm -r-f /home/ben/hive/viperblock/$AWS_AMI
```

## Mount the viperblock

Note, add -r for read-only

nbdkit -r -f -p 42909 --pidfile /run/user/1000/nbdkit-vol-vol-b83fc4b28d6e6f693.pid /home/ben/Development/mulga/viperblock/lib/nbdkit-viperblock-plugin.so -v size=3758096384 volume=$AWS_AMI bucket=predastore region=ap-southeast-2 access_key=AKIA8DD3DGGZTO4EOWR0 secret_key=EGHkBFO9JH6NapedR6kJIKcFkrbCQLshk9VbGEEf base_dir=/home/ben/hive/viperblock/ host=0.0.0.0:8443 cache_size=0
export AWS_AMI=ami-10bc1adb8bfa29b57

Once nbdkit running, mount the specified disk.

```
sudo nbd-client 127.0.0.1 42909 /dev/nbd0
```

## Mount the ISO

Use our source of truth to compare

```
sudo qemu-nbd --connect=/dev/nbd1 ~/isos/noble-server-cloudimg-amd64.img 
```

# Viperblock debugging

Note, mount volume for read only use.

Read the first 0 - 512 blocks, note the corruption. Seems `nbd0` (viperblock) has this extra, possible headers from the distributed node/QUIC/padding?

Possible 11 bytes padding before `80` and the first line, does not exist in the original `nbd1` as the source.

```
00000000  eb 63 90 00 00 00 00 00  00 00 00 00 00 00 00 00  |.c..............|
00000010  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000050  00 00 00 00 00 00 00 00  00 00 00 80 00 
```

# 0 - 512 range which fails on qemu boot, disk not recognised.

hexdump -C -s 0 -n 512 /dev/nbd0 > efi_mini-nbd0.hex
hexdump -C -s 0 -n 512 /dev/nbd1 > efi_mini-nbd1.hex

# Larger blocks
hexdump -C -s 0 -n 4096 /dev/nbd0 > efi_mini-nbd0.hex
hexdump -C -s 0 -n 4096 /dev/nbd1 > efi_mini-nbd1.hex

# Difference range
hexdump -C -s 8192 -n 8192 /dev/nbd0 > efi_mini-nbd0.hex
hexdump -C -s 8192 -n 8192 /dev/nbd1 > efi_mini-nbd1.hex


## Differences

Note, nbd1 is correct

```sh
root@rog:/tmp# cat efi_mini-nbd0.hex 
00000000  eb 63 90 00 00 00 00 00  00 00 00 00 00 00 00 00  |.c..............|
00000010  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000050  00 00 00 00 00 00 00 00  00 00 00 80 00 08 00 00  |................|
00000060  00 00 00 00 ff fa 90 90  f6 c2 80 74 05 f6 c2 70  |...........t...p|
00000070  74 02 b2 80 ea 79 7c 00  00 31 c0 8e d8 8e d0 bc  |t....y|..1......|
00000080  00 20 fb a0 64 7c 3c ff  74 02 88 c2 52 bb 17 04  |. ..d|<.t...R...|
00000090  f6 07 03 74 06 be 88 7d  e8 17 01 be 05 7c b4 41  |...t...}.....|.A|
000000a0  bb aa 55 cd 13 5a 52 72  3d 81 fb 55 aa 75 37 83  |..U..ZRr=..U.u7.|
000000b0  e1 01 74 32 31 c0 89 44  04 40 88 44 ff 89 44 02  |..t21..D.@.D..D.|
000000c0  c7 04 10 00 66 8b 1e 5c  7c 66 89 5c 08 66 8b 1e  |....f..\|f.\.f..|
000000d0  60 7c 66 89 5c 0c c7 44  06 00 70 b4 42 cd 13 72  |`|f.\..D..p.B..r|
000000e0  05 bb 00 70 eb 76 b4 08  cd 13 73 0d 5a 84 d2 0f  |...p.v....s.Z...|
000000f0  83 d0 00 be 93 7d e9 82  00 66 0f b6 c6 88 64 ff  |.....}...f....d.|
00000100  40 66 89 44 04 0f b6 d1  c1 e2 02 88 e8 88 f4 40  |@f.D...........@|
00000110  89 44 08 0f b6 c2 c0 e8  02 66 89 04 66 a1 60 7c  |.D.......f..f.`||
00000120  66 09 c0 75 4e 66 a1 5c  7c 66 31 d2 66 f7 34 88  |f..uNf.\|f1.f.4.|
00000130  d1 31 d2 66 f7 74 04 3b  44 08 7d 37 fe c1 88 c5  |.1.f.t.;D.}7....|
00000140  30 c0 c1 e8 02 08 c1 88  d0 5a 88 c6 bb 00 70 8e  |0........Z....p.|
00000150  c3 31 db b8 01 02 cd 13  72 1e 8c c3 60 1e b9 00  |.1......r...`...|
00000160  01 8e db 31 f6 bf 00 80  8e c6 fc f3 a5 1f 61 ff  |...1..........a.|
00000170  26 5a 7c be 8e 7d eb 03  be 9d 7d e8 34 00 be a2  |&Z|..}....}.4...|
00000180  7d e8 2e 00 cd 18 eb fe  47 52 55 42 20 00 47 65  |}.......GRUB .Ge|
00000190  6f 6d 00 48 61 72 64 20  44 69 73 6b 00 52 65 61  |om.Hard Disk.Rea|
000001a0  64 00 20 45 72 72 6f 72  0d 0a 00 bb 01 00 b4 0e  |d. Error........|
000001b0  cd 10 ac 3c 00 75 f4 c3  00 00 00 00 00 00 00 00  |...<.u..........|
000001c0  02 00 ee e4 5c c8 01 00  00 00 ff ff 6f 00 00 00  |....\.......o...|
000001d0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
000001f0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 55 aa  |..............U.|
00000200  45 46 49 20 50 41 52 54  00 00 01 00 5c 00 00 00  |EFI PART....\...|
00000210  9e a4 77 df 00 00 00 00  01 00 00 00 00 00 00 00  |..w.............|
00000220  ff ff 6f 00 00 00 00 00  22 00 00 00 00 00 00 00  |..o.....".......|
00000230  de ff 6f 00 00 00 00 00  f0 b9 27 29 3c 3c b2 42  |..o.......')<<.B|
00000240  a1 be 10 63 06 23 7e c4  02 00 00 00 00 00 00 00  |...c.#~.........|
00000250  80 00 00 00 80 00 00 00  1d 14 8d 24 00 00 00 00  |...........$....|
00000260  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000400  af 3d c6 0f 83 84 72 47  8e 79 3d 69 d8 47 7d e4  |.=....rG.y=i.G}.|
00000410  e9 2d ac f1 31 74 d3 47  90 91 6c cc 9e e0 50 fd  |.-..1t.G..l...P.|
00000420  00 08 20 00 00 00 00 00  de ff 6f 00 00 00 00 00  |.. .......o.....|
00000430  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000a80  48 61 68 21 49 64 6f 6e  74 4e 65 65 64 45 46 49  |Hah!IdontNeedEFI|
00000a90  8c 02 68 bd 1a a9 17 4a  96 6e c4 4b c7 f3 db 88  |..h....J.n.K....|
00000aa0  00 08 00 00 00 00 00 00  ff 27 00 00 00 00 00 00  |.........'......|
00000ab0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000b00  28 73 2a c1 1f f8 d2 11  ba 4b 00 a0 c9 3e c9 3b  |(s*......K...>.;|
00000b10  a3 5a 18 36 49 7e aa 42  8d 54 d1 d4 76 e7 df ba  |.Z.6I~.B.T..v...|
00000b20  00 28 00 00 00 00 00 00  ff 77 03 00 00 00 00 00  |.(.......w......|
00000b30  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000b80  ff c2 13 bc e6 59 62 42  a3 52 b2 75 fd 6f 71 72  |.....YbB.R.u.oqr|
00000b90  5b 7a 22 61 79 f7 a2 4d  98 be 09 2e ee 68 92 d9  |[z"ay..M.....h..|
00000ba0  00 78 03 00 00 00 00 00  00 00 20 00 00 00 00 00  |.x........ .....|
00000bb0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00001000
```

Output from ISO mount (correct)

```sh
root@rog:/tmp# cat efi_mini-nbd1.hex 
00000000  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000050  00 80 00 08 00 00 00 00  00 00 ff fa 90 90 f6 c2  |................|
00000060  80 74 05 f6 c2 70 74 02  b2 80 ea 79 7c 00 00 31  |.t...pt....y|..1|
00000070  c0 8e d8 8e d0 bc 00 20  fb a0 64 7c 3c ff 74 02  |....... ..d|<.t.|
00000080  88 c2 52 bb 17 04 f6 07  03 74 06 be 88 7d e8 17  |..R......t...}..|
00000090  01 be 05 7c b4 41 bb aa  55 cd 13 5a 52 72 3d 81  |...|.A..U..ZRr=.|
000000a0  fb 55 aa 75 37 83 e1 01  74 32 31 c0 89 44 04 40  |.U.u7...t21..D.@|
000000b0  88 44 ff 89 44 02 c7 04  10 00 66 8b 1e 5c 7c 66  |.D..D.....f..\|f|
000000c0  89 5c 08 66 8b 1e 60 7c  66 89 5c 0c c7 44 06 00  |.\.f..`|f.\..D..|
000000d0  70 b4 42 cd 13 72 05 bb  00 70 eb 76 b4 08 cd 13  |p.B..r...p.v....|
000000e0  73 0d 5a 84 d2 0f 83 d0  00 be 93 7d e9 82 00 66  |s.Z........}...f|
000000f0  0f b6 c6 88 64 ff 40 66  89 44 04 0f b6 d1 c1 e2  |....d.@f.D......|
00000100  02 88 e8 88 f4 40 89 44  08 0f b6 c2 c0 e8 02 66  |.....@.D.......f|
00000110  89 04 66 a1 60 7c 66 09  c0 75 4e 66 a1 5c 7c 66  |..f.`|f..uNf.\|f|
00000120  31 d2 66 f7 34 88 d1 31  d2 66 f7 74 04 3b 44 08  |1.f.4..1.f.t.;D.|
00000130  7d 37 fe c1 88 c5 30 c0  c1 e8 02 08 c1 88 d0 5a  |}7....0........Z|
00000140  88 c6 bb 00 70 8e c3 31  db b8 01 02 cd 13 72 1e  |....p..1......r.|
00000150  8c c3 60 1e b9 00 01 8e  db 31 f6 bf 00 80 8e c6  |..`......1......|
00000160  fc f3 a5 1f 61 ff 26 5a  7c be 8e 7d eb 03 be 9d  |....a.&Z|..}....|
00000170  7d e8 34 00 be a2 7d e8  2e 00 cd 18 eb fe 47 52  |}.4...}.......GR|
00000180  55 42 20 00 47 65 6f 6d  00 48 61 72 64 20 44 69  |UB .Geom.Hard Di|
00000190  73 6b 00 52 65 61 64 00  20 45 72 72 6f 72 0d 0a  |sk.Read. Error..|
000001a0  00 bb 01 00 b4 0e cd 10  ac 3c 00 75 f4 c3 00 00  |.........<.u....|
000001b0  00 00 00 00 00 00 02 00  ee e4 5c c8 01 00 00 00  |..........\.....|
000001c0  ff ff 6f 00 00 00 00 00  00 00 00 00 00 00 00 00  |..o.............|
000001d0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
000001f0  00 00 00 00 55 aa 45 46  49 20 50 41 52 54 00 00  |....U.EFI PART..|
00000200  01 00 5c 00 00 00 9e a4  77 df 00 00 00 00 01 00  |..\.....w.......|
00000210  00 00 00 00 00 00 ff ff  6f 00 00 00 00 00 22 00  |........o.....".|
00000220  00 00 00 00 00 00 de ff  6f 00 00 00 00 00 f0 b9  |........o.......|
00000230  27 29 3c 3c b2 42 a1 be  10 63 06 23 7e c4 02 00  |')<<.B...c.#~...|
00000240  00 00 00 00 00 00 80 00  00 00 80 00 00 00 1d 14  |................|
00000250  8d 24 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |.$..............|
00000260  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
000003f0  00 00 00 00 00 00 af 3d  c6 0f 83 84 72 47 8e 79  |.......=....rG.y|
00000400  3d 69 d8 47 7d e4 e9 2d  ac f1 31 74 d3 47 90 91  |=i.G}..-..1t.G..|
00000410  6c cc 9e e0 50 fd 00 08  20 00 00 00 00 00 de ff  |l...P... .......|
00000420  6f 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |o...............|
00000430  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000a70  00 00 00 00 00 00 48 61  68 21 49 64 6f 6e 74 4e  |......Hah!IdontN|
00000a80  65 65 64 45 46 49 8c 02  68 bd 1a a9 17 4a 96 6e  |eedEFI..h....J.n|
00000a90  c4 4b c7 f3 db 88 00 08  00 00 00 00 00 00 ff 27  |.K.............'|
00000aa0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000af0  00 00 00 00 00 00 28 73  2a c1 1f f8 d2 11 ba 4b  |......(s*......K|
00000b00  00 a0 c9 3e c9 3b a3 5a  18 36 49 7e aa 42 8d 54  |...>.;.Z.6I~.B.T|
00000b10  d1 d4 76 e7 df ba 00 28  00 00 00 00 00 00 ff 77  |..v....(.......w|
00000b20  03 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
00000b30  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000b70  00 00 00 00 00 00 ff c2  13 bc e6 59 62 42 a3 52  |...........YbB.R|
00000b80  b2 75 fd 6f 71 72 5b 7a  22 61 79 f7 a2 4d 98 be  |.u.oqr[z"ay..M..|
00000b90  09 2e ee 68 92 d9 00 78  03 00 00 00 00 00 00 00  |...h...x........|
00000ba0  20 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  | ...............|
00000bb0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
*
00000ff0  00 00 00 00 00 00 52 e8  28 01 74 08 56 be 33 81  |......R.(.t.V.3.|
00001000
```

# fdisk compare

Note partitions not detected on /dev/nbd0

```sh
ben@rog:~/isos$ sudo fdisk /dev/nbd1

Welcome to fdisk (util-linux 2.39.3).
Changes will remain in memory only, until you decide to write them.
Be careful before using the write command.


Command (m for help): p
Disk /dev/nbd1: 3.5 GiB, 3758096384 bytes, 7340032 sectors
Units: sectors of 1 * 512 = 512 bytes
Sector size (logical/physical): 512 bytes / 512 bytes
I/O size (minimum/optimal): 512 bytes / 131072 bytes
Disklabel type: gpt
Disk identifier: 2927B9F0-3C3C-42B2-A1BE-106306237EC4

Device         Start     End Sectors  Size Type
/dev/nbd1p1  2099200 7339998 5240799  2.5G Linux filesystem
/dev/nbd1p14    2048   10239    8192    4M BIOS boot
/dev/nbd1p15   10240  227327  217088  106M EFI System
/dev/nbd1p16  227328 2097152 1869825  913M Linux extended boot

Partition table entries are not in disk order.

Command (m for help):
[1]+  Stopped                 sudo fdisk /dev/nbd1
ben@rog:~/isos$ sudo fdisk /dev/nbd0

Welcome to fdisk (util-linux 2.39.3).
Changes will remain in memory only, until you decide to write them.
Be careful before using the write command.

Device does not contain a recognized partition table.
Created a new DOS (MBR) disklabel with disk identifier 0xe5bd7c25.

Command (m for help): p
Disk /dev/nbd0: 3.5 GiB, 3758096384 bytes, 7340032 sectors
Units: sectors of 1 * 512 = 512 bytes
Sector size (logical/physical): 512 bytes / 512 bytes
I/O size (minimum/optimal): 512 bytes / 131072 bytes
Disklabel type: dos
Disk identifier: 0xe5bd7c25
```

### Testing with QEMU

```
qemu-system-x86_64 \
  -pidfile /run/user/1000/i-dde703b7693007a27.pid \
  -qmp unix:/run/user/1000/qmp-i-dde703b7693007a27.sock,server,nowait \
  -enable-kvm -cpu host \
  -display gtk \
  -serial stdio \
  -smp 2 -m 1024 \
  -drive file=nbd://127.0.0.1:42909,format=raw,if=none,media=disk,id=os \
  -device virtio-blk-pci,drive=os,bootindex=1 \
  -device virtio-gpu-pci \
  -device virtio-rng-pci \
  -device virtio-net-pci,netdev=net0 \
  -netdev user,id=net0,hostfwd=tcp:127.0.0.1:42499-:22 \
  -M q35
```

---

## Session: 2026-01-25

### Issue: 10-byte Data Corruption with S3 Backend (RESOLVED)

**Symptom:** Data read from viperblock via S3/predastore backend was shifted by 10 bytes. First bytes showed `eb 63 90` instead of expected zeros, fdisk could not detect partitions.

**Root Cause:** The S3 backend's `Write()` function ignored the `headers` parameter, while the file backend wrote both headers and data:

**File backend (WORKS):**
```go
// viperblock/backends/file/file.go:147-155
_, err = file.Write(*headers)  // Writes 10-byte chunk header
_, err = file.Write(*data)      // Then writes data
```

**S3 backend (BROKEN):**
```go
// viperblock/backends/s3/s3.go - BEFORE fix
Body: bytes.NewReader(*data)  // ONLY writes data, ignores headers!
```

The `createChunkFile()` function calculates block offsets including the header size:
```go
ObjectOffset: utils.SafeIntToUint32(headerLen + (i * int(vb.BlockSize)))
```

With file backend: header is written, so `offset=10` correctly points to data at position 10.
With S3 backend: header NOT written, so `offset=10` reads from position 10 when data is at position 0.

This caused a 10-byte offset mismatch (ChunkHeaderSize = 4 magic + 2 version + 4 blockSize = 10 bytes).

**Solution:** Fixed S3 backend to write headers + data together, matching file backend behavior:

```go
// viperblock/backends/s3/s3.go - AFTER fix
func (backend *Backend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
    // Combine headers and data to match file backend behavior
    var body []byte
    if headers != nil && len(*headers) > 0 {
        body = make([]byte, len(*headers)+len(*data))
        copy(body[:len(*headers)], *headers)
        copy(body[len(*headers):], *data)
    } else {
        body = *data
    }

    object := &s3.PutObjectInput{
        Bucket: aws.String(backend.config.Bucket),
        Key:    aws.String(filename),
        Body:   bytes.NewReader(body),  // Now includes headers
    }
    // ...
}
```

**Files Modified:**
- `viperblock/backends/s3/s3.go`: Write() function now concatenates headers + data

**Re-import Required:** Existing AMIs imported with the broken code need to be re-imported after rebuilding viperblock.

**Verification:**
```bash
# 1. Rebuild viperblock
cd ~/Development/mulga/viperblock && make

# 2. Delete corrupted AMI data from predastore
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 rm --recursive s3://predastore/ami-10bc1adb8bfa29b57/

# 3. Clean local state
rm -rf ~/hive/viperblock/ami-10bc1adb8bfa29b57

# 4. Re-import AMI via vblock
./bin/vblock -metadata path/to/ami.json -file ~/isos/noble-server-cloudimg-amd64.raw ...

# 5. Mount and verify
sudo nbd-client 127.0.0.1 42909 /dev/nbd0
hexdump -C -s 0 -n 512 /dev/nbd0  # Should show zeros at offset 0
sudo fdisk /dev/nbd0              # Should detect GPT partitions
```

---

## Key Insights

1. **Backend Parity is Critical:** All backends (file, S3, memory) must implement the same behavior for Write/Read operations, especially regarding headers.

2. **Offset Calculations Must Match Storage:** If BlockLookup includes header offsets, the storage must include headers too.

3. **Test with Multiple Backends:** Bugs may only manifest with specific backends - test file and S3 backends separately.


