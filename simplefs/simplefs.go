// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package simplefs

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type SimpleVolume struct {
	Used map[uint64]bool
	Free []uint64
	Size uint64
	Name string
}

type SimpleFS struct {
	Volume SimpleVolume
	mu     sync.RWMutex

	Blocks    map[string]SimpleBlock
	Blocksize uint64
}

type SimpleBlock struct {
	Filename string
	Size     uint64
	Blocks   []uint64
}

func New() *SimpleFS {
	return &SimpleFS{
		Blocksize: 4096,
		Blocks:    make(map[string]SimpleBlock),
	}
}

func (sfs *SimpleFS) CreateVolume(name string, size uint64) error {
	//fmt.Println("Creating volume", name, size)
	// Confirm size is a multiple of the blocksize
	if size%sfs.Blocksize != 0 {
		return fmt.Errorf("size must be a multiple of the blocksize")
	}

	totalBlocks := size / sfs.Blocksize

	sfs.Volume.Size = totalBlocks

	sfs.Volume.Name = name
	sfs.Volume.Used = make(map[uint64]bool)
	sfs.Volume.Free = make([]uint64, totalBlocks)

	for i := uint64(0); i < totalBlocks; i++ {
		sfs.Volume.Free[i] = i
	}

	return nil
}

// Format a selected volume with all free blocks
func (sfs *SimpleFS) FormatVolume() error {
	sfs.mu.Lock()
	defer sfs.mu.Unlock()

	sfs.Volume.Used = make(map[uint64]bool)
	sfs.Volume.Free = make([]uint64, sfs.Volume.Size)

	// Create free blocks
	for i := uint64(0); i < sfs.Volume.Size; i++ {
		sfs.Volume.Free[i] = i
	}

	// Reset the blocks
	sfs.Blocks = make(map[string]SimpleBlock)

	return nil
}

func (sfs *SimpleFS) AllocateBlocks(size uint64) (blocks []uint64, err error) {

	sfs.mu.Lock()
	defer sfs.mu.Unlock()

	blocks = make([]uint64, size)

	// First, error if there are not enough free blocks
	//fmt.Println("Allocating blocks", len(sfs.Volume.Free), size)
	if len(sfs.Volume.Free) < int(size) {
		return nil, fmt.Errorf("not enough free blocks")
	}

	for i := 0; i < int(size); i++ {
		blocks[i] = sfs.Volume.Free[i]
		sfs.Volume.Used[sfs.Volume.Free[i]] = true
		sfs.Volume.Free[i] = 0
	}

	// Remove allocated blocks from free list
	sfs.Volume.Free = sfs.Volume.Free[len(blocks):]

	return blocks, nil
}

// Free blocks
func (sfs *SimpleFS) FreeBlocks(blocks []uint64) error {
	sfs.mu.Lock()
	defer sfs.mu.Unlock()

	for _, block := range blocks {
		// Delete the block from the used map
		delete(sfs.Volume.Used, block)

		// Add the block to the free list
		sfs.Volume.Free = append(sfs.Volume.Free, block)
	}

	return nil
}

func (sfs *SimpleFS) CreateFile(pathname string, dataLen uint64) (blocks []uint64, err error) {

	// Allocate the number of blocks that fit the block size
	numBlocks := (dataLen + sfs.Blocksize - 1) / sfs.Blocksize

	// Check if the filename already exists, if so delete the previous blocks allocated
	if _, ok := sfs.Blocks[pathname]; ok {
		sfs.FreeBlocks(sfs.Blocks[pathname].Blocks)
		delete(sfs.Blocks, pathname)
	}

	blocks, err = sfs.AllocateBlocks(numBlocks)
	if err != nil {
		return nil, err
	}

	// Allocate blocks for the file
	sfs.mu.Lock()
	defer sfs.mu.Unlock()

	sfs.Blocks[pathname] = SimpleBlock{
		Filename: pathname,
		Size:     dataLen,
		Blocks:   blocks,
	}

	return blocks, nil
}

// Delete a file
func (sfs *SimpleFS) DeleteFile(pathname string) error {

	if _, ok := sfs.Blocks[pathname]; !ok {
		return fmt.Errorf("file not found")
	}

	sfs.FreeBlocks(sfs.Blocks[pathname].Blocks)

	sfs.mu.Lock()
	defer sfs.mu.Unlock()
	delete(sfs.Blocks, pathname)

	return nil
}

// Save the block tracking state to disk
func (sfs *SimpleFS) SaveState(filename string) error {
	sfs.mu.Lock()
	defer sfs.mu.Unlock()

	// Save as JSON
	jsonData, err := json.Marshal(sfs)
	if err != nil {
		return err
	}

	// Write to file
	err = os.WriteFile(filename, jsonData, 0644)
	if err != nil {
		return err
	}

	return nil
}

// Load the block tracking state from disk
func (sfs *SimpleFS) LoadState(filename string) error {
	sfs.mu.Lock()
	defer sfs.mu.Unlock()

	// Read from file
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// Parse JSON
	err = json.Unmarshal(jsonData, sfs)
	if err != nil {
		return err
	}

	return nil
}
