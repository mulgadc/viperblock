"""Run the local encrypted 128 MiB NBD/Predastore fixture."""
import argparse
import ctypes as c
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import socket
import statistics
import subprocess
import tempfile
import time


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("server_test", type=Path)
    parser.add_argument("source", type=Path)
    parser.add_argument("plugin", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--nbdkit", default="/usr/bin/nbdkit")
    parser.add_argument("--mode", choices=["bench", "flush", "fua", "reclaim"], default="bench")
    parser.add_argument("--samples", type=int, default=6)
    parser.add_argument("--record", action="store_true")
    parser.add_argument("--wal-root", default="/dev/shm")
    return parser.parse_args()


def wait_ready(path, process):
    for _ in range(1000):
        if path.exists():
            return
        if process.poll() is not None:
            raise RuntimeError(f"{process.args[0]} exited while opening {path}")
        time.sleep(0.01)
    raise TimeoutError(path)


def stop(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def nbd_library():
    lib = c.CDLL("libnbd.so.0")
    lib.nbd_create.restype = c.c_void_p
    lib.nbd_connect_unix.argtypes = [c.c_void_p, c.c_char_p]
    lib.nbd_pread.argtypes = [c.c_void_p, c.c_void_p, c.c_size_t, c.c_uint64, c.c_uint32]
    lib.nbd_pwrite.argtypes = [c.c_void_p, c.c_void_p, c.c_size_t, c.c_uint64, c.c_uint32]
    lib.nbd_flush.argtypes = [c.c_void_p, c.c_uint32]
    lib.nbd_can_fua.argtypes = [c.c_void_p]
    lib.nbd_get_error.restype = c.c_char_p
    lib.nbd_close.argtypes = [c.c_void_p]
    lib.nbd_close.restype = None
    return lib


def success(lib, rc):
    if rc < 0:
        raise RuntimeError(lib.nbd_get_error().decode())


def connect(lib, path):
    handle = lib.nbd_create()
    if not handle:
        raise RuntimeError("nbd_create failed")
    success(lib, lib.nbd_connect_unix(handle, os.fsencode(path)))
    return handle


def percentile(samples, value):
    return sorted(samples)[int((len(samples)-1)*value)] / 1000


def drain(path):
    start = time.monotonic_ns()
    with socket.socket(socket.AF_UNIX) as change:
        change.settimeout(10)
        change.connect(str(path))
        with change.makefile("rb") as response:
            if response.readline() != b"OK\n":
                raise RuntimeError("forced drain failed")
    return (time.monotonic_ns() - start)/1000


def bench(lib, handle, image, count, executor=None, snapshot_path=None):
    buf = c.create_string_buffer(4096)
    written = c.create_string_buffer(bytes([0x77])*4096, 4096)
    stats = {"read": [], "write": [], "flush": []}
    drain_result = None
    for i in range(count):
        offset = (i % 16) * 4096
        start = time.monotonic_ns()
        if i % 10 == 0:
            success(lib, lib.nbd_pwrite(handle, written, 4096, (16 << 20)+offset, 0))
            stats["write"].append(time.monotonic_ns()-start)
            if i % 50 == 0:
                start = time.monotonic_ns()
                success(lib, lib.nbd_flush(handle, 0))
                stats["flush"].append(time.monotonic_ns()-start)
            if i == 200 and executor is not None:
                drain_result = executor.submit(drain, snapshot_path)
        else:
            success(lib, lib.nbd_pread(handle, buf, 4096, offset, 0))
            stats["read"].append(time.monotonic_ns()-start)
            if buf.raw != image[offset:offset+4096]:
                raise RuntimeError(f"incorrect bytes at {offset}")
    summary = {f"{op}_{kind}_us": val for op, rows in stats.items() for kind, val in
               (("p50", statistics.median(rows)/1000), ("p99", percentile(rows, 0.99)))}
    if drain_result is not None:
        summary["drain_us"] = drain_result.result()
    return summary


def main():
    args = arguments()
    args.out.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, AWS_ACCESS_KEY_ID="localfixture", AWS_SECRET_ACCESS_KEY="localfixturesecret",
               VB_ACCESS_KEY="localfixture", VB_SECRET_KEY="localfixturesecret",
               SSL_CERT_FILE=str((args.source / "config/server.pem").resolve()), GOMAXPROCS="4")
    lib = nbd_library()
    with tempfile.TemporaryDirectory(dir="/dev/shm") as tmp, tempfile.TemporaryDirectory(dir=args.wal_root) as waltmp:
        root = Path(tmp)
        env.update(VB_IO_PROFILE_ROOT=tmp, TMPDIR=tmp, VB_IO_PROFILE_RECORD="1" if args.record else "0")

        with (args.out / "predastore.log").open("w") as log:
            server = subprocess.Popen([str(args.server_test.resolve()), "-test.run", "^TestIOProfileServe$",
                                       "-test.v", "-test.timeout", "600s"], cwd=args.source / "viperblock",
                                      env=env, stdout=log, stderr=log)
        nbd = None
        handle = None
        try:
            wait_ready(root / "endpoint", server)
            endpoint = (root / "endpoint").read_text()
            image = (root / "image").read_bytes()
            sock = root / "nbd.sock"
            uri = f"nbd+unix:///?socket={sock}"

            def start_export(epoch, cache, wal=waltmp):
                sock.unlink(missing_ok=True)
                with (args.out / f"nbdkit-{epoch}.log").open("w") as trace:
                    process = subprocess.Popen([args.nbdkit, "-f", "-U", str(sock), str(args.plugin.resolve()),
                                                "size=134217728", "volume=io-profile-clone", "bucket=predastore",
                                                "region=ap-southeast-2", f"host=https://{endpoint}", f"base_dir={wal}",
                                                f"lease_epoch={epoch}", f"cache_size={cache}",
                                                f"encryption_key_file={root / 'key'}"], env=env, stdout=trace, stderr=trace)
                return process

            nbd = start_export(2, 0 if args.mode == "bench" else 128)
            wait_ready(sock, nbd)
            if args.mode == "bench":
                subprocess.run(["qemu-img", "dd", "-f", "raw", "-O", "raw", f"if={uri}",
                                f"of={root / 'readback'}", "bs=4096", "count=1024"], check=True, env=env)
                if (root / "readback").read_bytes() != image:
                    raise RuntimeError("QEMU inherited image differs")
                for trial in range(args.samples):
                    for depth in (1, 4, 8, 16):
                        subprocess.run(["/usr/bin/fio", "--readonly", "--name=read", "--ioengine=nbd",
                                        f"--uri={uri}", "--rw=randread", "--bs=4096", "--size=4M", "--io_size=512K",
                                        f"--iodepth={depth}", "--numjobs=1", "--randrepeat=1", "--output-format=json",
                                        f"--output={args.out / f'cold-{depth}-{trial}.json'}"], env=env, check=True)
                stop(nbd)
                nbd = start_export(3, 128)
                wait_ready(sock, nbd)
                handle = connect(lib, sock)
                bench(lib, handle, image, 100)
                rows = [bench(lib, handle, image, 600) for _ in range(args.samples)]
                (args.out / "mixed.json").write_text(json.dumps(rows, indent=2))
                with ThreadPoolExecutor(max_workers=1) as executor:
                    snapshot_path = Path(waltmp) / "io-profile-clone/snapshot.sock"
                    rows = [bench(lib, handle, image, 600, executor, snapshot_path) for _ in range(args.samples)]
                (args.out / "active-drain.json").write_text(json.dumps(rows, indent=2))
                return

            handle = connect(lib, sock)
            flags = 1 if args.mode == "fua" else 0
            if lib.nbd_can_fua(handle) != 1:
                raise RuntimeError("FUA missing on export")
            buf = c.create_string_buffer(bytes([0x7c])*4096, 4096)
            offset = 16 << 20
            success(lib, lib.nbd_pwrite(handle, buf, len(buf), offset, flags))
            if args.mode != "fua":
                success(lib, lib.nbd_flush(handle, 0))
            if args.mode == "reclaim":
                drain(Path(waltmp) / "io-profile-clone/snapshot.sock")
            else:
                generations = [path for path in Path(waltmp).rglob("wal.*.bin") if path.stat().st_size > 4096]
                if not generations:
                    raise RuntimeError("expected a flushed nonempty local generation")
            nbd.kill()
            nbd.wait(timeout=10)
            lib.nbd_close(handle)
            handle = None
            nbd = start_export(3, 0, root / "fresh" if args.mode == "reclaim" else waltmp)
            wait_ready(sock, nbd)
            subprocess.run(["qemu-io", "-f", "raw", "-c", f"read -P 0x7c {offset} 4096", uri], check=True, env=env)
            if args.mode != "reclaim" and '"recovered_blocks":1' not in (args.out / "nbdkit-3.log").read_text():
                raise RuntimeError("reopened through a path other than WAL recovery")
            print(f"{args.mode} byte verification passed")
        finally:
            if handle:
                lib.nbd_close(handle)
            stop(nbd)
            stop(server)


if __name__ == "__main__":
    main()
