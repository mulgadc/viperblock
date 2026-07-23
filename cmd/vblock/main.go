package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/mulgadc/viperblock/utils"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/mulgadc/viperblock/viperblock/v_utils"

	_ "github.com/mulgadc/viperblock/internal/fipsboot"
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
	encryptionKeyFile := flag.String("encryption-key-file", "", "path to AES-256 master key (32 raw bytes, 0640 or stricter); falls back to ENCRYPTION_KEY_FILE env if unset")

	flag.Parse()

	// Loop through required arguments

	if *file == "" {
		slog.Error("Required argument file is missing")
		os.Exit(1)
	}

	if *size == 0 {
		slog.Error("Required argument size is missing")
		os.Exit(1)
	}

	if *volume == "" {
		slog.Error("Required argument volume is missing")
		os.Exit(1)
	}

	if *bucket == "" {
		slog.Error("Required argument bucket is missing")
		os.Exit(1)
	}

	if *region == "" {
		slog.Error("Required argument region is missing")
		os.Exit(1)
	}

	if *access_key == "" {
		slog.Error("Required argument access_key is missing")
		os.Exit(1)
	}

	if *secret_key == "" {
		slog.Error("Required argument secret_key is missing")
		os.Exit(1)
	}

	if *host == "" {
		slog.Error("Required argument host is missing")
		os.Exit(1)
	}

	if *base_dir == "" {
		slog.Error("Required argument base_dir is missing")
		os.Exit(1)
	}

	var volumeConfig viperblock.VolumeConfig

	if *metadata != "" {
		fmt.Println("Reading metadata from file", *metadata)
		// Check file exists
		if _, err := os.Stat(*metadata); os.IsNotExist(err) {
			slog.Error("Metadata file does not exist", "path", *metadata)
			os.Exit(1)
		}

		// Read the metadata file
		metadata, err := os.ReadFile(*metadata)

		if err != nil {
			slog.Error("Failed to read metadata file", "error", err)
			os.Exit(1)
		}

		// Parse the metadata
		err = json.Unmarshal(metadata, &volumeConfig)
		if err != nil {
			slog.Error("Failed to parse metadata", "error", err)
			os.Exit(1)
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

	mkey, err := viperblock.LoadMasterKeyFromFlagOrEnv(*encryptionKeyFile)
	if err != nil {
		slog.Error("Could not load encryption key", "error", err)
		os.Exit(1)
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
		VolumeConfig:      volumeConfig,
		MasterKey:         mkey,
		EncryptionEnabled: mkey != nil,
	}

	// A plain stderr line keeps the viperblock library free of any terminal-UI
	// dependency; the percentage throttle inside ImportDiskImage bounds this to
	// ≤101 renders.
	progress := func(current, total uint64) {
		fmt.Fprintf(os.Stderr, "\rFlushing image to storage %s / %s", utils.HumanBytes(current), utils.HumanBytes(total))
	}

	err = v_utils.ImportDiskImage(&s3Config, &vbConfig, *file, progress)

	if err != nil {
		slog.Error("Failed to import disk image", "error", err)
		os.Exit(1)
	}

	// Terminate the carriage-return progress line before the success message.
	fmt.Fprintln(os.Stderr)
	fmt.Println("Successfully imported disk image to Viperblock store")
}
