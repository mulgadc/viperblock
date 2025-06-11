package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

func main() {

	// Read the RAW disk file in 4096 byte chunks, starting from offset 0, to the Viperblock store

	file := flag.String("file", "", "The path to the RAW disk file")
	size := flag.Int("size", 3221225472, "size of volume in bytes")
	volume := flag.String("volume", "vol-1", "volume name")
	bucket := flag.String("bucket", "vol-1", "bucket name")
	region := flag.String("region", "ap-southeast-2", "S3 region")
	access_key := flag.String("access_key", "", "S3 access key")
	secret_key := flag.String("secret_key", "", "S3 secret key")
	host := flag.String("host", "https://127.0.0.1:8443", "S3 host")
	base_dir := flag.String("base_dir", "/tmp/vb/", "base directory for viperblock")
	metadata := flag.String("metadata", "", "metadata (JSON) file to describe volume or AMI")

	flag.Parse()

	// Loop through required arguments

	if *file == "" {
		log.Fatalf("Required argument file is missing")
	}

	if *size == 0 {
		log.Fatalf("Required argument size is missing")
	}

	if *volume == "" {
		log.Fatalf("Required argument volume is missing")
	}

	if *bucket == "" {
		log.Fatalf("Required argument bucket is missing")
	}

	if *region == "" {
		log.Fatalf("Required argument region is missing")
	}

	if *access_key == "" {
		log.Fatalf("Required argument access_key is missing")
	}

	if *secret_key == "" {
		log.Fatalf("Required argument secret_key is missing")
	}

	if *host == "" {
		log.Fatalf("Required argument host is missing")
	}

	if *base_dir == "" {
		log.Fatalf("Required argument base_dir is missing")
	}

	var volumeConfig viperblock.VolumeConfig

	if *metadata != "" {

		fmt.Println("Reading metadata from file", *metadata)
		// Check file exists
		if _, err := os.Stat(*metadata); os.IsNotExist(err) {
			log.Fatalf("Metadata file %s does not exist", *metadata)
		}

		// Read the metadata file
		metadata, err := os.ReadFile(*metadata)

		if err != nil {
			log.Fatalf("Failed to read metadata file: %v", err)
		}

		// Parse the metadata
		err = json.Unmarshal(metadata, &volumeConfig)
		if err != nil {
			log.Fatalf("Failed to parse metadata: %v", err)
		}

	}

	// Pre-fill meta-data if any fields are missing
	if volumeConfig.VolumeMetadata.VolumeID == "" {
		volumeConfig.VolumeMetadata.VolumeID = viperblock.GenerateVolumeID("vol", *volume, *bucket, time.Now().Unix())
	}

	if volumeConfig.VolumeMetadata.VolumeName == "" {
		volumeConfig.VolumeMetadata.VolumeName = *volume
	}

	if volumeConfig.VolumeMetadata.SizeGiB == 0 {
		volumeConfig.VolumeMetadata.SizeGiB = uint64(*size)
	}

	if volumeConfig.VolumeMetadata.State == "" {
		volumeConfig.VolumeMetadata.State = "available"
	}

	if volumeConfig.VolumeMetadata.CreatedAt.IsZero() {
		volumeConfig.VolumeMetadata.CreatedAt = time.Now()
	}

	if volumeConfig.VolumeMetadata.AvailabilityZone == "" {
		volumeConfig.VolumeMetadata.AvailabilityZone = *region
	}

	// First, check if `AMIMetadata` is defined, otherwise skip
	if volumeConfig.AMIMetadata.Name != "" {

		// Check AMI config and set defaults
		if volumeConfig.AMIMetadata.ImageID == "" {
			volumeConfig.AMIMetadata.ImageID = viperblock.GenerateVolumeID("ami", *volume, *bucket, time.Now().Unix())
		}

		if volumeConfig.AMIMetadata.CreationDate.IsZero() {
			volumeConfig.AMIMetadata.CreationDate = time.Now()
		}

		if volumeConfig.AMIMetadata.Architecture == "" {
			volumeConfig.AMIMetadata.Architecture = "x86_64"
		}

		if volumeConfig.AMIMetadata.PlatformDetails == "" {
			volumeConfig.AMIMetadata.PlatformDetails = "Linux/UNIX"
		}

		if volumeConfig.AMIMetadata.RootDeviceType == "" {
			volumeConfig.AMIMetadata.RootDeviceType = "ebs"
		}

		if volumeConfig.AMIMetadata.Virtualization == "" {
			volumeConfig.AMIMetadata.Virtualization = "hvm"
		}

		if volumeConfig.AMIMetadata.ImageOwnerAlias == "" {
			volumeConfig.AMIMetadata.ImageOwnerAlias = "hive"
		}

		if volumeConfig.AMIMetadata.VolumeSizeGiB == 0 {
			// Convert from bytes to GiB
			volumeConfig.AMIMetadata.VolumeSizeGiB = uint64(*size) / 1024 / 1024 / 1024
		}

	}

	f, err := os.OpenFile(*file, os.O_RDONLY, 0)
	if err != nil {
		log.Fatalf("Failed to open disk file: %v", err)
	}
	defer f.Close()

	cfg := s3.S3Config{
		VolumeName: *volume,
		VolumeSize: uint64(*size),
		Bucket:     *bucket,
		Region:     *region,
		AccessKey:  *access_key,
		SecretKey:  *secret_key,
		Host:       *host,
	}

	vbconfig := viperblock.VB{
		VolumeName: *volume,
		VolumeSize: uint64(*size),
		BaseDir:    *base_dir,
		Cache: viperblock.Cache{
			Config: viperblock.CacheConfig{
				Size: 0,
			},
		},
		VolumeConfig: volumeConfig,
	}

	vb, err := viperblock.New(vbconfig, "s3", cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Viperblock store: %v", err)
	}

	vb.SetDebug(true)

	// Initialize the backend
	err = vb.Backend.Init()
	if err != nil {
		log.Fatalf("Failed to initialize backend: %v", err)
	}

	//var walNum uint64

	// First, fetch the state from the remote backend
	err = vb.LoadState()

	if err != nil {
		log.Fatalf("Failed to load state: %v", err)
	}

	err = vb.LoadBlockState()

	if err != nil {
		log.Fatalf("Failed to load block state: %v", err)
	}

	// Open the chunk WAL
	err = vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		log.Fatalf("Failed to load WAL: %v", err)
	}

	// Open the block to object WAL
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		log.Fatalf("Failed to load block WAL: %v", err)
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
			log.Fatalf("Failed to read disk file: %v", err)
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
		log.Fatalf("Failed to close Viperblock store: %v", err)
	}

	// Write state to disk
	//vb.SaveState()
	//vb.SaveHotState("/tmp/viperblock/hotstate.json")
	//vb.SaveBlockState("/tmp/viperblock/blockstate.json")

}
