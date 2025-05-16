#!/bin/sh

umount /mnt/nbd1
nbd-client -d /dev/nbd1
