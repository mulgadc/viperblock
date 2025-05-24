package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

func main() {

	// Read the RAW disk file in 4096 byte chunks, starting from offset 0, to the Viperblock store

	file := flag.String("file", "", "The path to the RAW disk file")

	flag.Parse()

	if *file == "" {
		log.Fatalf("File is required")
	}

	f, err := os.OpenFile(*file, os.O_RDONLY, 0)
	if err != nil {
		log.Fatalf("Failed to open disk file: %v", err)
	}
	defer f.Close()

	cfg := s3.S3Config{
		VolumeName: "import",
		VolumeSize: 3221225472,
		Bucket:     "predastore",
		Region:     "ap-southeast-2",
		AccessKey:  "AKIAIOSFODNN7EXAMPLE",
		SecretKey:  "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Host:       "https://127.0.0.1:8443",
	}

	// Connect to the specified Viperblock store
	vb := viperblock.New("s3", cfg)

	if err != nil {
		log.Fatalf("Failed to connect to Viperblock store: %v", err)
	}

	//tmpDir := os.TempDir()

	// Generate a temp directory that ends in viperblock
	voldata := "/tmp/viperblock"

	//os.RemoveAll(voldata)

	// For testing, purge, and create

	vb.SetWALBaseDir(voldata)

	walNum := vb.WAL.WallNum.Add(1)

	err = vb.OpenWAL(fmt.Sprintf("%s/%s/wal.%08d.bin", voldata, vb.Backend.GetVolume(), walNum))

	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}

	err = vb.Backend.Init()

	if err != nil {
		log.Fatalf("Failed to initialize Viperblock store: %v", err)
	}

	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}

	var block uint64 = 0

	buf := make([]byte, vb.BlockSize)

	for {

		n, err := f.Read(buf)

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

	vb.Flush()
	vb.WriteWALToChunk(true)

	// Write state to disk
	vb.SaveState("/tmp/viperblock/state.json")
	vb.SaveHotState("/tmp/viperblock/hotstate.json")
	vb.SaveBlockState("/tmp/viperblock/blockstate.json")

}
