// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/mulgadc/viperblock/viperblock"
	backend "github.com/mulgadc/viperblock/viperblock/backends"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
)

func main() {
	fmt.Println("Welcome to Viper Block Storage")

	btype := flag.String("btype", "file", "Backend type (file, memory, s3)")
	fname := flag.String("file", "tests/data1", "Sample file to read")

	input := flag.String("input", "tests/input1", "Sample file for input")

	flag.Parse()

	backend, err := backend.New(*btype, file.FileConfig{BaseDir: "/tmp", VolumeName: "test"})

	if err != nil {
		slog.Error("Could not load specified backend type", "backend", btype)
		os.Exit(1)
	}

	backend.Init()

	err = backend.Open(*fname)

	if err != nil {
		slog.Error("Could not load file", "file", *fname, "error", err)
		os.Exit(1)
	}

	//backend.Read()

	// Read input file, output to our block location
	finput, err := os.Open(*input)

	if err != nil {
		slog.Error("Could not load file", "input", *input, "error", err)
		os.Exit(1)
	}

	// Read in 4kb chunks, and output
	//blocks := viperblock.Blocks{}

	vb := viperblock.New(*btype, file.FileConfig{BaseDir: "/tmp", VolumeName: "test"})

	fmt.Println(vb)

	finfo, err := finput.Stat()

	if err != nil {
		slog.Error("Could not get file info", "input", *input, "error", err)
		os.Exit(1)
	}

	// Calculate the number of blocks required
	numblocks := finfo.Size() / int64(vb.ObjBlockSize)
	overflow := finfo.Size() % int64(vb.ObjBlockSize)

	// If there's an overflow, we need an extra block
	if overflow > 0 {
		numblocks++
	}

	fmt.Printf("Number of blocks required: %d (filesize: %d bytes, block size: %d bytes, overflow: %d bytes)\n",
		numblocks, finfo.Size(), vb.ObjBlockSize, overflow)

	var currentBlock uint64
	//var currentOffset uint64
	//var blockOffset uint64

	// First, read the WAL

	walNum := vb.WAL.WallNum.Add(1)

	vb.OpenWAL(fmt.Sprintf("tests/data/input1/wal.%08d.bin", walNum))

	err = vb.ReadWAL()

	if err != nil {
		slog.Error("Could not read WAL", "error", err)
		os.Exit(1)
	}

	//os.Exit(0)

	// Read the input file, and write to the block storage main memory
	for {

		// Read 4kb chunks
		buffer := make([]byte, vb.BlockSize)

		// Read a 4KB chunk
		_, err := finput.Read(buffer)

		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Error("File input error", "error", err)
		}

		// Append the currentBlock and raw data to main memory
		vb.Write(currentBlock, buffer)

		currentBlock++

	}

	fmt.Println("Read input file to main memory: ", len(vb.Writes.Blocks))

	// Next, flush the main memory to the WAL
	fmt.Println("Flushing main memory to WAL")

	// Write the first and last block with new data
	newblocks := []int{0, 329}

	for _, block := range newblocks {
		buffer := make([]byte, vb.BlockSize)

		newmsg := fmt.Sprintf("Hello, World! Block %d", block)

		// Append to the beginning of the buffer, but do not increase the size
		copy(buffer[0:len(newmsg)], newmsg)

		fmt.Println("Writing NEW block", block, "with message", newmsg)
		err = vb.Write(uint64(block), buffer)

		if err != nil {
			slog.Error("Could not write block", "error", err)
			os.Exit(1)
		}
	}

	vb.Flush()

	fmt.Println("Main memory after flush: ", len(vb.Writes.Blocks))

	vb.WriteWALToChunk()

	// Next, more updates!
	newblocks = []int{0, 1, 2, 328, 329}

	for _, block := range newblocks {
		buffer := make([]byte, vb.BlockSize)

		newmsg := fmt.Sprintf("Hello, NEW World! Block %d", block)

		// Append to the beginning of the buffer, but do not increase the size
		copy(buffer[0:len(newmsg)], newmsg)

		fmt.Println("Writing NEW block", block, "with message", newmsg)
		err = vb.Write(uint64(block), buffer)

		if err != nil {
			slog.Error("Could not write block", "error", err)
			os.Exit(1)
		}
	}

	vb.Flush()

	fmt.Println("Main memory after flush: ", len(vb.Writes.Blocks))

	vb.WriteWALToChunk()

	// Serialize the BlocksToObject
	err = vb.SerializeBlocksToObject("tests/data/input1/blocks_to_object.json")

	if err != nil {
		slog.Error("Could not serialize BlocksToObject", "error", err)
		os.Exit(1)
	}

	// Next, locate specific blocks
	var blockId uint64 = 329
	objId, offset, err := vb.LookupBlockToObject(blockId)

	if err != nil {
		slog.Error("Could not lookup block", "error", err)
		os.Exit(1)
	}

	fmt.Println("Lookup block", blockId, "Object ID: ", objId, "Offset: ", offset)

	// Use the specified backend to read the block
	block, err := vb.Backend.Read(objId, offset, vb.BlockSize)

	if err != nil {
		slog.Error("Could not read block", "error", err)
		os.Exit(1)
	}

	fmt.Println("Block:")
	fmt.Println(string(block))

}
