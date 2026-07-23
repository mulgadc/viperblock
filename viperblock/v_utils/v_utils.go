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

// ProgressFunc reports cumulative bytes flushed against the total. It is
// invoked at a bounded rate (throttled internally), so implementations may
// render directly. A nil ProgressFunc disables progress reporting.
type ProgressFunc func(current, total uint64)

// progressReporter throttles progress callbacks to integer-percentage steps.
// The render-frequency risk lives at the flush loop, so the throttle lives
// there too — as pure integer arithmetic that bounds invocations to ≤101 for a
// whole import (one per percent plus a final 100%), regardless of block count.
// It holds no terminal-UI dependency; the rendering cost stays with the
// caller's ProgressFunc.
type progressReporter struct {
	progress ProgressFunc
	total    uint64
	lastPct  int
}

// newProgressReporter seeds lastPct at -1 so the first report fires at 0%.
func newProgressReporter(progress ProgressFunc, total uint64) progressReporter {
	return progressReporter{progress: progress, total: total, lastPct: -1}
}

// report fires the callback only when the integer percentage advances.
func (p *progressReporter) report(current uint64) {
	if p.progress == nil || p.total == 0 {
		return
	}
	pct := utils.SafeUint64ToInt(current * 100 / p.total)
	if pct > p.lastPct {
		p.lastPct = pct
		p.progress(current, p.total)
	}
}

// finish emits a terminal 100% so callers reach full even when the last block
// did not land on a percentage boundary. It is skipped when a prior report
// already hit 100% (current == total), so the callback is never fired twice at
// the end and the total invocation count stays ≤101.
func (p *progressReporter) finish() {
	if p.progress == nil || p.total == 0 || p.lastPct >= 100 {
		return
	}
	p.progress(p.total, p.total)
}

// Helper function to import disk image to S3 backend. progress, when non-nil,
// receives throttled byte-count updates so each caller can render its own way.
func ImportDiskImage(s3Config *s3.S3Config, vbConfig *viperblock.VB, filename string, progress ProgressFunc) error {
	// Confirm filename can be opened
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open disk file: %w", err)
	}
	defer f.Close()

	// Get file stats
	fileInfo, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat disk file: %w", err)
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
			vbConfig.VolumeConfig.AMIMetadata.ImageOwnerAlias = "spinifex"
		}

		if vbConfig.VolumeConfig.AMIMetadata.VolumeSizeGiB == 0 {
			// Convert from bytes to GiB
			vbConfig.VolumeConfig.AMIMetadata.VolumeSizeGiB = utils.SafeInt64ToUint64(fileInfo.Size()) / 1024 / 1024 / 1024
		}
	}

	vb, err := viperblock.New(vbConfig, "s3", *s3Config)
	if err != nil {
		return fmt.Errorf("failed to connect to Viperblock store: %w", err)
	}

	//vb.SetDebug(true)

	// Initialize the backend
	err = vb.Backend.Init()
	if err != nil {
		return fmt.Errorf("failed to initialize backend: %w", err)
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
		return fmt.Errorf("failed to load block state: %w", err)
	}

	// Open the chunk WAL
	err = vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		return fmt.Errorf("failed to load WAL: %w", err)
	}

	// Open the block to object WAL
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		return fmt.Errorf("failed to load block WAL: %w", err)
	}

	// Encrypted volumes derive the AES-GCM nonce from VolumeUUID, which
	// SaveState mints on first use. Mint it now, before the first chunk is
	// sealed in the write loop, so every chunk and the AMI snapshot share one
	// stable UUID. Deferring the mint to CreateSnapshot would seal the chunks
	// under the zero UUID while the snapshot records the minted one, breaking
	// clone reads with a tag-verify failure.
	if vb.EncryptionEnabled {
		if err := vb.SaveState(); err != nil {
			return fmt.Errorf("failed to persist initial state: %w", err)
		}
	}

	var block uint64 = 0

	totalBytes := fileInfo.Size()
	if totalBytes < 0 {
		return fmt.Errorf("invalid file size: %d", totalBytes)
	}

	// Progress is measured in bytes so callers render a unit that matches the
	// download bar; the reporter throttles rendering to percentage steps.
	reporter := newProgressReporter(progress, utils.SafeInt64ToUint64(totalBytes))
	var current uint64

	buf := make([]byte, vb.BlockSize)

	nullBlock := make([]byte, vb.BlockSize)

	for {
		n, err := f.Read(buf)

		// Check if the input is a Zero block
		if bytes.Equal(buf[:n], nullBlock) {
			//fmt.Printf("Null block found at %d, skipping\n", block)
			block++
			current += utils.SafeIntToUint64(n)
			reporter.report(current)
			continue
		}

		//fmt.Println("Read", "block", block, "n", n, "err", err)

		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read disk file: %w", err)
		}

		if err := vb.WriteAt(block*uint64(vb.BlockSize), buf[:n]); err != nil {
			return fmt.Errorf("failed to write block %d: %w", block, err)
		}

		//fmt.Println("Write", "block", hex.EncodeToString(buf[:n]))

		block++
		current += utils.SafeIntToUint64(n)
		reporter.report(current)

		// Flush every 4MB
		if block%uint64(vb.BlockSize) == 0 {
			if err := vb.Flush(); err != nil {
				return fmt.Errorf("failed to flush at block %d: %w", block, err)
			}
			if err := vb.WriteWALToChunk(true); err != nil {
				return fmt.Errorf("failed to write WAL to chunk at block %d: %w", block, err)
			}
		}
	}

	reporter.finish()

	// Create a snapshot for AMI imports so that instance launches can use
	// zero-copy cloning (OpenFromSnapshot) instead of block-by-block copy.
	if vbConfig.VolumeConfig.AMIMetadata.Name != "" {
		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		_, err := vb.CreateSnapshot(snapshotID)
		if err != nil {
			return fmt.Errorf("failed to create AMI snapshot: %w", err)
		}
		vb.VolumeConfig.AMIMetadata.SnapshotID = snapshotID
	}

	err = vb.Close()
	if err != nil {
		return fmt.Errorf("failed to close Viperblock store: %w", err)
	}

	return nil
}
