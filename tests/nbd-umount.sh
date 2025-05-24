#!/bin/sh

rm /tmp/nbd1.sock 

umount /mnt/nbd1
nbd-client -d /dev/nbd1
