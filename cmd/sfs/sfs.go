// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/mulgadc/viperblock/simplefs"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

func main() {

	fmt.Println("Welcome to SimpleFS simulation for the Viper Block Storage")

	btype := flag.String("btype", "file", "Backend type (file, memory, s3)")
	fname := flag.String("dir", "", "Sample directory to read")
	vol := flag.String("vol", "", "Volume to read")

	// Optionally specify the state file, or init to create a new volume with specified size
	sfsstate := flag.String("sfsstate", "", "SFS (filesystem) state file to load")

	vbstate := flag.String("vbstate", "", "Viperblock state file to load")

	createvol := flag.Bool("createvol", false, "Initialize a new volume with specified vol argument")

	voldata := flag.String("voldata", "", "Volume data directory to store blocks")

	volsize := flag.Uint64("size", 2<<18, "Volume size in bytes")

	// S3 configuration (via environment variables)
	// aws_profile := os.Getenv("AWS_PROFILE")
	aws_region := os.Getenv("AWS_REGION")
	aws_bucket := os.Getenv("AWS_BUCKET")
	aws_access_key := os.Getenv("AWS_ACCESS_KEY")
	aws_secret_key := os.Getenv("AWS_SECRET_KEY")
	aws_host := os.Getenv("AWS_HOST")

	flag.Parse()

	if !*createvol && *vol == "" {
		fmt.Println("Must specify vol to create a new volume when createvol is defined")
		os.Exit(1)
	}

	// Verify arguments
	//if *sfsstate != "" && *createvol != "" {
	//	fmt.Println("Cannot specify both sfsstate and createvol")
	//	os.Exit(1)
	//}

	// Check if the directory exists
	if _, err := os.Stat(*fname); os.IsNotExist(err) {
		fmt.Println("Directory does not exist")
		os.Exit(1)
	}

	// Create the volume data directory if it doesn't exist
	if *voldata != "" {

		// Check if the directory exists
		if _, err := os.Stat(*voldata); os.IsNotExist(err) {
			fmt.Println("Volume data directory does not exist, creating it")
			os.MkdirAll(*voldata, 0755)
		}
	}

	var err error

	sfs := simplefs.New()

	if *sfsstate != "" {
		// First load the existing state

		if _, err := os.Stat(*sfsstate); os.IsNotExist(err) {
			fmt.Println("State file does not exist, will create a new one on save")
		} else {
			err = sfs.LoadState(*sfsstate)
			if err != nil {
				slog.Error("Could not load SFS state", "error", err)
				os.Exit(-1)
			}
		}

	}

	if *createvol {
		// Create a volume
		err = sfs.CreateVolume(*vol, *volsize)
		if err != nil {
			fmt.Println(err)
		}
	}

	// Create the viperblock backend

	var cfg interface{}

	if *btype == "file" {
		cfg = file.FileConfig{BaseDir: *voldata, VolumeName: *vol}
	} else if *btype == "s3" {
		cfg = s3.S3Config{
			Bucket:    aws_bucket,
			Region:    aws_region,
			AccessKey: aws_access_key,
			SecretKey: aws_secret_key,
			Host:      aws_host,
		}
	}

	fmt.Println("Creating Viperblock backend with btype, config", *btype, cfg)
	vb := viperblock.New(*btype, cfg)

	// Initialize the backend
	err = vb.Backend.Init()
	if err != nil {
		slog.Error("Could not initialize backend", "error", err)
		os.Exit(1)
	}

	if *vbstate != "" {
		// First load the existing state

		if _, err := os.Stat(*vbstate); os.IsNotExist(err) {
			fmt.Println("State file does not exist, will create a new one on save")
		} else {
			err = vb.LoadState(*vbstate)
			if err != nil {
				fmt.Println(err)
			}
		}

	}

	vb.SetWALBaseDir(*voldata)

	walNum := vb.WAL.WallNum.Add(1)

	err = vb.OpenWAL(fmt.Sprintf("%s/%s/wal.%08d.bin", *voldata, vb.Backend.GetVolume(), walNum))

	if err != nil {
		slog.Error("Could not open WAL", "error", err)
		os.Exit(1)
	}

	err = vb.ReadWAL()

	if err != nil {
		slog.Error("Could not read WAL", "error", err)
		os.Exit(1)
	}

	type SHA256 struct {
		Filename string
		Checksum string
		Len      int
	}

	// SHA256 comparisons for the local files and block storage version
	var localSHA256 map[string]SHA256
	//var blockSHA256 map[string]string

	localSHA256 = make(map[string]SHA256)
	//blockSHA256 = make(map[string]string)

	// Next, iterate over the directory and traverse the directory tree
	filepath.Walk(*fname, func(path string, info os.FileInfo, err error) error {

		// Skip directories
		if info.IsDir() {
			return nil
		}

		fmt.Println(path)
		if err != nil {
			return err
		}

		// Create a file
		blocks, err := sfs.CreateFile(path, uint64(info.Size()))
		if err != nil {
			slog.Error("Could not create file", "error", err)
			return err
		}

		fmt.Println(blocks)

		finput, err := os.Open(path)

		if err != nil {
			fmt.Println(err)
			return err
		}

		currentBlock := 0

		hash := sha256.New()

		// Read the input file, and write to the block storage main memory
		totalLen := 0

		for {

			// Read 4kb chunks
			buffer := make([]byte, vb.BlockSize)

			// Read a 4KB chunk
			n, err := finput.Read(buffer)

			// Calculate the SHA256 of the buffer for each loop
			hash.Write(buffer[:n])

			totalLen += n

			if err != nil {
				if err == io.EOF {
					break
				}
				slog.Error("File input error", "error", err)
			}

			// Append the currentBlock and raw data to main memory
			vb.Write(blocks[currentBlock], buffer)

			currentBlock++

		}

		entry := SHA256{}
		entry.Checksum = hex.EncodeToString(hash.Sum(nil))
		entry.Len = totalLen
		entry.Filename = path

		localSHA256[path] = entry

		return nil
	})

	//spew.Dump(localSHA256)

	fmt.Println("Read input file to main memory: ", len(vb.Writes.Blocks))

	// Next, flush the main memory to the WAL
	fmt.Println("Flushing main memory to WAL")

	vb.Flush()

	fmt.Println("Main memory after flush: ", len(vb.Writes.Blocks))

	vb.WriteWALToChunk(false)

	// Serialize the BlocksToObject
	err = vb.SaveBlockState(fmt.Sprintf("%s/blocks_to_object.json", *voldata))

	if err != nil {
		slog.Error("Could not serialize BlocksToObject", "error", err)
		os.Exit(1)
	}

	// Next, scan all written chunks and compare the SHA256 of the local file to the block storage version
	for k, file := range localSHA256 {
		fmt.Println("*localSHA256")
		fmt.Println(k, file.Filename)

		fmt.Println(sfs.Blocks[file.Filename].Filename)

		blocks := sfs.Blocks[k].Blocks

		fileBuffer := make([]byte, sfs.Blocks[file.Filename].Size)

		fmt.Println("fileBuffer", len(fileBuffer))

		for i, block := range blocks {

			objectID, objectOffset, err := vb.LookupBlockToObject(block)

			if err != nil {
				slog.Error("Could not lookup block to object", "error", err)
				os.Exit(1)
			}

			data, err := vb.Backend.Read(objectID, objectOffset, uint32(sfs.Blocksize))

			// Position in the buffer where this block should go
			pos := i * int(sfs.Blocksize)
			if pos >= len(fileBuffer) {
				break // Don't write past the buffer
			}

			// How much we can copy from this block
			bytesToCopy := int(sfs.Blocksize)
			if (pos + int(sfs.Blocksize)) > len(fileBuffer) {
				bytesToCopy = len(fileBuffer) - pos
			}

			copy(fileBuffer[pos:pos+bytesToCopy], data[:bytesToCopy])

			if err != nil {
				slog.Error("Could not read block", "error", err)
				os.Exit(1)
			}

			if err != nil {
				slog.Error("Could not lookup block to object", "error", err)
				os.Exit(1)
			}

		}

		// Compare the SHA256 of the local file to the block storage version
		blockSHA256 := sha256.New()
		blockSHA256.Write(fileBuffer)

		blockSHA256String := hex.EncodeToString(blockSHA256.Sum(nil))

		if blockSHA256String != file.Checksum {
			fmt.Println("SHA256 mismatch", blockSHA256String)
			fmt.Println("File   mismatch", file.Checksum)

			fmt.Println("File length", file.Len)
			fmt.Println("Block length", len(fileBuffer))

			fmt.Println("----")
			fmt.Println(string(fileBuffer))
			fmt.Println("----")
		} else {
			fmt.Println("SHA256 match")
		}

	}

	if *sfsstate != "" {
		// Save the state
		err = sfs.SaveState(*sfsstate)
		if err != nil {
			fmt.Println(err)
		}
	}

	// Save the state
	vb.SaveState(*vbstate)

	// Export the WAL number and chunk number
	fmt.Println("WAL number", vb.WAL.WallNum.Load())
	fmt.Println("Chunk number", vb.ObjectNum.Load())

}
