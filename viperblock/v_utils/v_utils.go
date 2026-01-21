package v_utils

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/utils"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

// Helper function to import disk image to S3 backend
func ImportDiskImage(s3Config *s3.S3Config, vbConfig *viperblock.VB, filename string) error {

	// Confirm filename can be opened
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open disk file: %v", err)
	}
	defer f.Close()

	// Get file stats
	fileInfo, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat disk file: %v", err)
	}

	var volumeName string

	// Set defaults for volume config
	// Pre-fill meta-data if any fields are missing
	if vbConfig.VolumeConfig.VolumeMetadata.VolumeID == "" {
		volumeName = fmt.Sprintf("vol-%d", time.Now().Unix())
		vbConfig.VolumeConfig.VolumeMetadata.VolumeID = viperblock.GenerateVolumeID("vol", volumeName, s3Config.Bucket, time.Now().Unix())
	}

	if vbConfig.VolumeConfig.VolumeMetadata.VolumeName == "" {
		vbConfig.VolumeConfig.VolumeMetadata.VolumeName = volumeName
	}

	if vbConfig.VolumeConfig.VolumeMetadata.SizeGiB == 0 {
		vbConfig.VolumeConfig.VolumeMetadata.SizeGiB = utils.SafeInt64ToUint64(fileInfo.Size()) / 1024 / 1024 / 1024
	}

	if vbConfig.VolumeConfig.VolumeMetadata.State == "" {
		vbConfig.VolumeConfig.VolumeMetadata.State = "available"
	}

	if vbConfig.VolumeConfig.VolumeMetadata.CreatedAt.IsZero() {
		vbConfig.VolumeConfig.VolumeMetadata.CreatedAt = time.Now()
	}

	if vbConfig.VolumeConfig.VolumeMetadata.AvailabilityZone == "" {
		vbConfig.VolumeConfig.VolumeMetadata.AvailabilityZone = s3Config.Region
	}

	// First, check if `AMIMetadata` is defined, otherwise skip
	if vbConfig.VolumeConfig.AMIMetadata.Name != "" {

		// Check AMI config and set defaults
		if vbConfig.VolumeConfig.AMIMetadata.ImageID == "" {
			vbConfig.VolumeConfig.AMIMetadata.ImageID = viperblock.GenerateVolumeID("ami", volumeName, s3Config.Bucket, time.Now().Unix())
		}

		if vbConfig.VolumeConfig.AMIMetadata.CreationDate.IsZero() {
			vbConfig.VolumeConfig.AMIMetadata.CreationDate = time.Now()
		}

		if vbConfig.VolumeConfig.AMIMetadata.Architecture == "" {
			vbConfig.VolumeConfig.AMIMetadata.Architecture = "x86_64"
		}

		if vbConfig.VolumeConfig.AMIMetadata.PlatformDetails == "" {
			vbConfig.VolumeConfig.AMIMetadata.PlatformDetails = "Linux/UNIX"
		}

		if vbConfig.VolumeConfig.AMIMetadata.RootDeviceType == "" {
			vbConfig.VolumeConfig.AMIMetadata.RootDeviceType = "ebs"
		}

		if vbConfig.VolumeConfig.AMIMetadata.Virtualization == "" {
			vbConfig.VolumeConfig.AMIMetadata.Virtualization = "hvm"
		}

		if vbConfig.VolumeConfig.AMIMetadata.ImageOwnerAlias == "" {
			vbConfig.VolumeConfig.AMIMetadata.ImageOwnerAlias = "hive"
		}

		if vbConfig.VolumeConfig.AMIMetadata.VolumeSizeGiB == 0 {
			// Convert from bytes to GiB
			vbConfig.VolumeConfig.AMIMetadata.VolumeSizeGiB = utils.SafeInt64ToUint64(fileInfo.Size()) / 1024 / 1024 / 1024
		}

	}

	vb, err := viperblock.New(vbConfig, "s3", *s3Config)
	if err != nil {
		return fmt.Errorf("failed to connect to Viperblock store: %v", err)
	}

	//vb.SetDebug(true)

	// Initialize the backend
	err = vb.Backend.Init()
	if err != nil {
		return fmt.Errorf("failed to initialize backend: %v", err)
	}

	//var walNum uint64

	// First, fetch the state from the remote backend
	_ = vb.LoadState()
	// err = vb.LoadState()

	// if err != nil {
	// 	// Soft error, since volume may be new
	// 	//return fmt.Errorf("failed to load state: %v", err)
	// }

	err = vb.LoadBlockState()

	if err != nil {
		return fmt.Errorf("failed to load block state: %v", err)
	}

	// Open the chunk WAL
	err = vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		return fmt.Errorf("failed to load WAL: %v", err)
	}

	// Open the block to object WAL
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		return fmt.Errorf("failed to load block WAL: %v", err)
	}

	var block uint64 = 0

	buf := make([]byte, vb.BlockSize)

	nullBlock := make([]byte, vb.BlockSize)

	for {

		n, err := f.Read(buf)

		// Check if the input is a Zero block
		if bytes.Equal(buf[:n], nullBlock) {
			//fmt.Printf("Null block found at %d, skipping\n", block)
			block++
			continue
		}

		//fmt.Println("Read", "block", block, "n", n, "err", err)

		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read disk file: %v", err)
		}

		vb.WriteAt(block*uint64(vb.BlockSize), buf[:n])

		//fmt.Println("Write", "block", hex.EncodeToString(buf[:n]))

		block++

		// Flush every 4MB
		if block%uint64(vb.BlockSize) == 0 {
			fmt.Println("Flush", "block", block)
			vb.Flush()
			vb.WriteWALToChunk(true)
		}

	}

	err = vb.Close()
	if err != nil {
		return fmt.Errorf("failed to close Viperblock store: %v", err)
	}

	// Implementation would go here
	return nil
}
