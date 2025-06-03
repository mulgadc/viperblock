package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

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

	/*
		required := []string{"file", "size", "volume", "bucket", "region", "access_key", "secret_key", "host", "base_dir"}
		for _, arg := range required {
			if flag.Lookup(arg) == nil {
				log.Fatalf("Required argument %s is missing", arg)
			}
		}
	*/

	fmt.Println("Opening file", *file)

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
			fmt.Printf("Null block found at %d, skipping\n", block)
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
