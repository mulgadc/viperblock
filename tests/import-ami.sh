#!/bin/sh

USER=ben

# check HOST, ACCESS_KEY and SECRET_KEY defined
if [ -z "$HOST" ] || [ -z "$ACCESS_KEY" ] || [ -z "$SECRET_KEY" ]; then
    echo "HOST, ACCESS_KEY and SECRET_KEY must be defined"
    exit 1
fi

ISO=/home/ben/debian-12-genericcloud-arm64.qcow2

# Confirm nbd driver loaded
modprobe nbd

# umount if already exists
qemu-nbd -d /dev/nbd0
qemu-nbd -r --connect=/dev/nbd0 $ISO
echo "Mounted $ISO to /dev/nbd0"
sleep 1

SIZE=$(fdisk -l /dev/nbd0 | grep "Disk /dev/nbd0" | sed -E 's/.* ([0-9]+) bytes.*/\1/')

echo "Size: $SIZE"


go run cmd/vblock/main.go -metadata=tests/import-ami.json -file=/dev/nbd0 -size=$SIZE -volume="debian-12.10.0-arm64-genericcloud" -bucket=predastore -region=ap-southeast-2 -access_key=$ACCESS_KEY -secret_key=$SECRET_KEY -host=$HOST -base_dir=/tmp/vb/

# Unmount to finish
qemu-nbd -d /dev/nbd0

chown -R $USER:$USER /tmp/vb/
