#!/bin/sh

umount /mnt/nbd1
nbd-client -d /dev/nbd1
nbd-client -N default 127.0.0.1 10809 /dev/nbd1

# Check no error returned
if [ $? -ne 0 ]; then
    echo "Error: Failed to connect to NBD server"
    exit 1
fi

mkfs.ext4 /dev/nbd1
tune2fs -l /dev/nbd1

mount -o sync /dev/nbd1 /mnt/nbd1

# Check no error returned
if [ $? -ne 0 ]; then
    echo "Error: Failed to mount NBD device"
    exit 1
fi

cd /mnt/nbd1/

