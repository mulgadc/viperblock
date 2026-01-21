package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mulgadc/viperblock/utils"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/mulgadc/viperblock/viperblock/v_utils"
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

	var volID string

	if volumeConfig.AMIMetadata.ImageID != "" {
		volID = volumeConfig.AMIMetadata.ImageID
	} else {
		volID = volumeConfig.VolumeMetadata.VolumeID
	}

	s3Config := s3.S3Config{
		VolumeName: volID,
		VolumeSize: utils.SafeIntToUint64(*size),
		Bucket:     *bucket,
		Region:     *region,
		AccessKey:  *access_key,
		SecretKey:  *secret_key,
		Host:       *host,
	}

	vbConfig := viperblock.VB{
		VolumeName: volID,
		VolumeSize: utils.SafeIntToUint64(*size),
		BaseDir:    *base_dir,
		Cache: viperblock.Cache{
			Config: viperblock.CacheConfig{
				Size: 0,
			},
		},
		VolumeConfig: volumeConfig,
	}

	err := v_utils.ImportDiskImage(&s3Config, &vbConfig, *file)

	if err != nil {
		log.Fatalf("Failed to import disk image: %v", err)
	}

	fmt.Println("Successfully imported disk image to Viperblock store")
}
