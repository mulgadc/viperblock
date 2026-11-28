#!/usr/bin/env python3
"""Check two connects to an idle, disposable nbdkit launch using Predastore.

Supply the process ID, its Unix socket, and the volume directory under its
configured local WAL root. Keep the original nbdkit process alive for both
connects. Use a format-2 volume with a fresh forward lease.
"""
import argparse
import ctypes
import os
from pathlib import Path


def connect(socket):
    lib = ctypes.CDLL("libnbd.so.0")
    lib.nbd_create.restype = ctypes.c_void_p
    lib.nbd_connect_unix.argtypes = (ctypes.c_void_p, ctypes.c_char_p)
    lib.nbd_get_error.restype = ctypes.c_char_p
    lib.nbd_close.argtypes = (ctypes.c_void_p,)
    handle = lib.nbd_create()
    if not handle:
        raise RuntimeError("could not allocate a libnbd handle")
    try:
        if lib.nbd_connect_unix(handle, os.fsencode(socket)) < 0:
            return lib.nbd_get_error().decode()
        return None
    finally:
        lib.nbd_close(handle)


def wal_descriptors(pid, wal_dir):
    opened = []
    for fd in (Path("/proc") / str(pid) / "fd").iterdir():
        try:
            target = Path(os.readlink(fd))
        except FileNotFoundError:
            continue
        if target.is_relative_to(wal_dir) and ".bin" in target.name:
            opened.append(target)
    return opened


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--volume-dir", type=Path, required=True)
    args = parser.parse_args()
    wal_dir = args.volume_dir.resolve() / "wal"
    blocks = wal_dir / "blocks"
    saved = blocks.with_name("blocks-saved-by-test")
    if saved.exists() or (blocks.exists() and not blocks.is_dir()):
        raise RuntimeError("the fixture already has a block WAL obstruction")

    wal_dir.mkdir(parents=True, exist_ok=True)
    descriptors_before = set(wal_descriptors(args.pid, wal_dir))
    if blocks.exists():
        blocks.rename(saved)
    try:
        blocks.write_text("intentional fixture failure: expected a directory")
        error = connect(args.socket)
        if not error or "Could not open Block WAL" not in error:
            raise AssertionError(f"did not reach the intended block WAL failure: {error}")
        leaked = set(wal_descriptors(args.pid, wal_dir)) - descriptors_before
    finally:
        blocks.unlink(missing_ok=True)
        if saved.exists():
            saved.rename(blocks)

    error = connect(args.socket)
    if error:
        raise AssertionError(f"same-process reconnect failed: {error}")
    if leaked:
        raise AssertionError(f"failed Open leaked WAL handles: {sorted(leaked)}")
    print("PASS block WAL fault closed startup handles and same-process reconnect succeeded")


if __name__ == "__main__":
    main()
