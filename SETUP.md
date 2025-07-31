# Setup instructions

## Import a qcow2

To import a qcow2 image into Viperblock, such as an OS cloud image that can be booted by a VM.

### Prerequisites

* Predastore (Hive object storage) is pre-installed and running.

### Step 1

Download the selected image

```
wget https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-generic-arm64.qcow2
```

Mount the qcow2 image

```
qemu-nbd -r --connect=/dev/nbd0 debian-12-genericcloud-arm64.qcow2
```

### Step 2

Copy block by block (zero blocks will be skipped), requires root to read the raw /dev/nbd0 device.

```
go run cmd/vblock/main.go -file=/dev/nbd0 -size=3221225472 -volume=vol-2 -bucket=predastore -region=ap-southeast-2 -access_key="X" -secret_key="Y" -base_dir="/tmp/vb/" -host="https://127.0.0.1:8443"
```

Confirm permissions on /tmp/vb/ are set by the same user mounting the disk using `nbdkit`.

### Step 3

Mount the new viperblock volume using `nbdkit`

```
./server/nbdkit -p 10810 --pidfile /tmp/vb-vol-2.pid ./plugins/golang/examples/ramdisk/nbdkit-goramdisk-plugin.so -v -f size=3221225472 volume=vol-2 bucket=predastore region=ap-southeast-2 access_key="X" secret_key="Y" base_dir="/tmp/vb/" host="https://127.0.0.1:8443" cache_size=10
```

### Step 4

Create a cloud-init ISO for the instance configuration, users, SSH access, etc.

Create the file

./user-data

```
#cloud-config
users:
  - name: ec2-user
    shell: /bin/bash
    groups:
      - sudo
    sudo: "ALL=(ALL) NOPASSWD:ALL"
    ssh_authorized_keys:
      - ssh-rsa YOURSSHPUBKEY

# Configure hostname and manage /etc/hosts
hostname: yourhost
manage_etc_hosts: true
```

./meta-data

```
# meta-data
instance-id: hive-vm-yourhost
local-hostname: yourhost
```

Create the ISO file to mount on boot:

```
# Linux
genisoimage -output cloud-init-yourhost.iso -volid cidata -joliet -rock user-data meta-data

# MacOS
#mkisofs -output cloud-init-yourhost.iso -volid cidata -joliet -rock meta-data user-data
```

### Step 5

Convert the cloud-init-yourhost.iso to Viperblock, note default 1MB size used, confirm if your cloud-init is larger before creating.

```
go run cmd/vblock/main.go -file=cloud-init-yourhost.iso -size=1048576 -volume=vol-2-cloudinit -bucket=predastore -region=ap-southeast-2 -access_key="X" -secret_key="Y" -base_dir="/tmp/vb/" -host="https://127.0.0.1:8443"
```

### Step 6

Mount the cloud-init.iso via NBD and disable cache for the cloud-init volume.

```
./server/nbdkit -p 10811 --pidfile /tmp/vb-vol-2-cloudinit.pid ./plugins/golang/examples/ramdisk/nbdkit-goramdisk-plugin.so -v -f size=1048576 volume=vol-2-cloudinit bucket=predastore region=ap-southeast-2 access_key="X" secret_key="Y" base_dir="/tmp/vb/" host="https://127.0.0.1:8443" cache_size=0
```

### Step 7

Create a blank volume for the EFI varstore, used by the UEFI firmware for a QEMU guest and is required to be unique and read/write for each instance. Use a 64MB image to begin with.

```
./server/nbdkit -p 10812 --pidfile /tmp/vb-vol-2-efivars.pid ./plugins/golang/examples/ramdisk/nbdkit-goramdisk-plugin.so -v -f size=67108864 volume=vol-2-efivars bucket=predastore region=ap-southeast-2 access_key="X" secret_key="Y" base_dir="/tmp/vb/" host="https://127.0.0.1:8443" cache_size=0
```

### Step 8

Launch the instance, which will read the cloud-image as the OS, configure the system with the cloud-init (e.g users, ssh keys, etc) and boot a fresh instance.

```
qemu-system-aarch64 \
   -nographic \
   -M virt,highmem=off \
   -accel hvf \
   -cpu host \
   -smp 4 \
   -m 3000 \
   -drive file=/path/to/QEMU_EFI.img,if=pflash,format=raw \
   -drive if=pflash,format=raw,file=nbd://192.168.64.5:10812/default \
   -drive file=nbd://192.168.64.5:10811/default,format=raw,if=virtio \
   -drive file=boot-efi.raw,format=raw,if=virtio \
   -device virtio-blk-pci,drive=debian,bootindex=1 \
   -drive if=none,media=disk,id=debian,format=raw,file=nbd://192.168.64.5:10810/default \
   -device qemu-xhci \
   -device usb-kbd \
   -device usb-tablet \
   -device intel-hda \
   -device hda-duplex \
   -netdev user,id=net0,hostfwd=tcp::2222-:22 -device virtio-net-device,netdev=net0 \
   -device virtio-rng-pci
```

### Final step to verify.

Congratulations, setup is now complete!

SSH into the host using the port forwarding from port 2222 to the local instance port 22.

```
ssh --p 2222 ec2-user@localhost    

ec2-user@hive4:~$ id
uid=1000(ec2-user) gid=1000(ec2-user) groups=1000(ec2-user),27(sudo)
```


# NBD Verification

Testing blocks from mounted qcow2 via NBD to Viperblock implementation.

qemu-nbd --connect=/dev/nbd0 debian-12-genericcloud-arm64.qcow2

root@ben-QEMU-Virtual-Machine:/home/ben# fdisk -l /dev/nbd0
Disk /dev/nbd0: 3 GiB, 3221225472 bytes, 6291456 sectors
Units: sectors of 1 * 512 = 512 bytes
Sector size (logical/physical): 512 bytes / 512 bytes
I/O size (minimum/optimal): 512 bytes / 131072 bytes
Disklabel type: gpt
Disk identifier: EE548149-EA6E-0B48-937F-EE61EA025B59

Device        Start     End Sectors  Size Type
/dev/nbd0p1  262144 6289407 6027264  2.9G Linux root (ARM-64)
/dev/nbd0p15   2048  262143  260096  127M EFI System

Partition table entries are not in disk order.

-
hexdump -C -s $((2048*512)) -n $((260096*512)) /dev/nbd0 > efi_partition-nbd0.hex
hexdump -C -s $((2048*512)) -n $((260096*512)) /dev/nbd8 > efi_partition-nbd8.hex
hexdump -C -s $((2048*512)) -n $((2049*512)) /dev/nbd0 > efi_mini-nbd0.hex
hexdump -C -s $((2048*512)) -n $((2049*512)) /dev/nbd1 > efi_mini-nbd1.hex

Compare byte differences

diff efi_partition-nbd0.hex efi_partition-nbd8.hex

# QEMU x84

Example

```
qemu-system-x86_64 \
   -enable-kvm \
   -nographic \
   -M ubuntu \
   -cpu host \
   -smp 4 \
   -m 3000 \
   -drive file=/usr/share/ovmf/OVMF.fd,if=pflash,format=raw \
   -drive file=nbd://127.0.0.1:34305/default,format=raw,if=virtio \
   -drive file=nbd://127.0.0.1:39449/default,format=raw,if=none,media=disk,id=debian \
   -device virtio-blk-pci,drive=debian,bootindex=1 \
   -netdev user,id=net0,hostfwd=tcp::2222-:22 \
   -device virtio-rng-pci
```