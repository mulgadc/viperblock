import ctypes as c
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

lib = c.CDLL("libnbd.so.0")
lib.nbd_create.restype = c.c_void_p
lib.nbd_connect_unix.argtypes = [c.c_void_p, c.c_char_p]
lib.nbd_pread.argtypes = [c.c_void_p, c.c_void_p, c.c_size_t, c.c_uint64, c.c_uint32]
lib.nbd_get_errno.restype = c.c_int
lib.nbd_close.argtypes = [c.c_void_p]
mode = sys.argv[2]
expected_rc = -1 if mode == "error" else 0
offset = 0 if mode == "error" else 4096
count = int(sys.argv[3])
limit_kib = int(sys.argv[4]) if len(sys.argv) > 4 else None
if mode not in {"error", "debug"} or count <= 0:
    raise ValueError("expected error|debug and a positive burst length")

def private_kib(pid):
    rows = Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines()
    return next(int(row.split()[1]) for row in rows if row.startswith("Private_Dirty:"))

with tempfile.TemporaryDirectory(dir="/dev/shm") as temp:
    sock = Path(temp) / "nbd.sock"
    env = dict(os.environ, GOMAXPROCS="4")
    with subprocess.Popen([os.getenv("NBDKIT", "/usr/bin/nbdkit"), "-f", "-t", "4", "-U", str(sock), sys.argv[1]],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env) as server:
        try:
            for _ in range(500):
                if sock.exists():
                    break
                if server.poll() is not None:
                    raise RuntimeError("nbdkit exited before socket ready")
                time.sleep(0.01)
            handle = lib.nbd_create()
            assert handle and lib.nbd_connect_unix(handle, os.fsencode(sock)) == 0
            buf = c.create_string_buffer(4096)
            for n in range(4):
                before = private_kib(server.pid)
                start = time.monotonic_ns()
                for _ in range(1000 if n == 0 else count):
                    rc = lib.nbd_pread(handle, buf, len(buf), offset, 0)
                    if rc != expected_rc:
                        raise RuntimeError(f"{mode} read returned {rc}, expected {expected_rc}")
                    if rc == -1:
                        assert lib.nbd_get_errno() == 5
                elapsed = time.monotonic_ns() - start
                print(f"{mode} burst={n} private_dirty_kib={private_kib(server.pid)} previous_kib={before} ns_per_call={elapsed/(1000 if n == 0 else count):.1f}", flush=True)
                if n == 0:
                    warmed_kib = private_kib(server.pid)
            final_kib = private_kib(server.pid)
            lib.nbd_close(handle)
            if limit_kib is not None and final_kib - warmed_kib > limit_kib:
                raise RuntimeError(f"{mode} retained {final_kib - warmed_kib} KiB after warm-up")
        finally:
            server.terminate()
