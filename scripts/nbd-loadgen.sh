#!/usr/bin/env bash
#
# nbd-loadgen.sh — drive viperblock NBD I/O load on ONE instance while watching
# host memory, to observe nbdkit/qemu RSS growth (e.g. the read-cache slice-
# aliasing leak). Read-only load by default.
#
# SAFETY — this tool exists because an earlier, unguarded run drove the node
# into OOM (all four instances at once, direct-I/O streaming reads, no host
# memory guard; see docs/development/bugs/nbd-read-cache-slice-aliasing-leak.md
# "Field validation"). Guards here:
#   * single target only (no "all instances")
#   * aborts when host MemAvailable drops below FLOOR (default 2048 MiB)
#   * bounded duration (default 120 s) + in-guest hard-timer backstop
#   * stop-sentinel for prompt teardown; teardown on any exit
#   * write churn is opt-in (--write); reads target the RO second volume
#
# NOTE: the leaked cache memory is pinned in the LRU — it stops growing when
# I/O stops but does NOT shrink. Only a volume detach / process teardown frees
# it. Keep FLOOR well above the largest single nbdkit you expect.
#
# Usage:
#   ./nbd-loadgen.sh <instance-ip> [-d sec] [-f floor_mb] [-r read_dev]
#                    [--write] [-k keyfile] [-u user]
# Env: SPINIFEX_KEY overrides the default key path.
set -euo pipefail

usage() {
	echo "usage: $0 <instance-ip> [-d sec] [-f floor_mb] [-r read_dev] [--write] [-k keyfile] [-u user]" >&2
	exit 2
}

[ $# -ge 1 ] || usage
IP=$1
shift

DUR=120
FLOOR=2048
RDEV=/dev/vdb
WRITE=0
KEY="${SPINIFEX_KEY:-$HOME/.ssh/spinifex-key}"
SSH_USER=ec2-user

while [ $# -gt 0 ]; do
	case "$1" in
	-d) DUR=$2; shift 2 ;;
	-f) FLOOR=$2; shift 2 ;;
	-r) RDEV=$2; shift 2 ;;
	--write) WRITE=1; shift ;;
	-k) KEY=$2; shift 2 ;;
	-u) SSH_USER=$2; shift 2 ;;
	*) usage ;;
	esac
done

SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=6 "$SSH_USER@$IP")

mem_avail_mb() { awk '/^MemAvailable:/ {print int($2/1024)}' /proc/meminfo; }
rss_mb() { ps -eo rss,comm | awk -v p="$1" '$2 ~ p {s+=$1} END {printf "%d", s/1024}'; }

teardown() {
	echo ">> teardown: stopping in-guest load on $IP"
	"${SSH[@]}" 'touch /tmp/nbd-loadgen.stop 2>/dev/null;
		sudo pkill -9 -f nbd-guest-load 2>/dev/null;
		sudo pkill -9 dd 2>/dev/null;
		sudo pkill -9 openssl 2>/dev/null;
		sudo pkill -9 python3 2>/dev/null;
		sudo rm -f /root/nbd-loadgen.bin /tmp/nbd-loadgen.stop 2>/dev/null' 2>/dev/null || true
}
trap teardown EXIT INT TERM

echo ">> target=$IP dur=${DUR}s floor=${FLOOR}MiB read=$RDEV write=$WRITE"
echo ">> host MemAvailable=$(mem_avail_mb)MiB"
[ "$(mem_avail_mb)" -gt "$FLOOR" ] || { echo "!! host already below floor — refusing to start" >&2; exit 1; }

# Stage the in-guest load. It has its own hard timer and watches a stop-sentinel
# so the host can tear it down promptly.
"${SSH[@]}" "cat > /tmp/nbd-guest-load.sh" <<GUEST
#!/bin/bash
DUR=$DUR; RDEV=$RDEV; WRITE=$WRITE
rm -f /tmp/nbd-loadgen.stop
end=\$((SECONDS + DUR))
stop() { [ -f /tmp/nbd-loadgen.stop ] || [ \$SECONDS -ge \$end ]; }

# ~150 MiB guest RAM balloon (drives qemu host RSS), released on stop.
python3 - <<'PY' &
import time
b = bytearray(150 * 1024 * 1024)
for i in range(0, len(b), 4096):
    b[i] = 1
while True:
    time.sleep(1)
PY
MEM=\$!

# Sequential read churn over the RO volume — the read-cache leak trigger.
( while ! stop; do dd if=\$RDEV of=/dev/null bs=64k iflag=direct 2>/dev/null; done ) &
R=\$!

W=
if [ "\$WRITE" = "1" ]; then
	( while ! stop; do
		openssl enc -aes-256-ctr -nosalt -pass pass:loadx -in /dev/zero 2>/dev/null \
			| dd of=/root/nbd-loadgen.bin bs=1M count=256 iflag=fullblock oflag=direct 2>/dev/null
		sync; rm -f /root/nbd-loadgen.bin
	  done ) &
	W=\$!
fi

while ! stop; do sleep 1; done
kill \$MEM \$R \$W 2>/dev/null
rm -f /root/nbd-loadgen.bin
GUEST

"${SSH[@]}" 'chmod +x /tmp/nbd-guest-load.sh; sudo nohup /tmp/nbd-guest-load.sh >/tmp/nbd-guest-load.log 2>&1 & echo ">> in-guest load started"'

# Host watch loop: report RSS, abort on memory floor or timeout.
t0=$SECONDS
while [ $((SECONDS - t0)) -lt "$DUR" ]; do
	A=$(mem_avail_mb)
	printf ">> t=%3ds  host avail=%6dMiB  qemu=%5dMiB  nbdkit=%5dMiB\n" \
		$((SECONDS - t0)) "$A" "$(rss_mb qemu-system)" "$(rss_mb nbdkit)"
	if [ "$A" -lt "$FLOOR" ]; then
		echo "!! host avail ${A}MiB < floor ${FLOOR}MiB — ABORTING"
		break
	fi
	sleep 2
done
echo ">> run complete"
