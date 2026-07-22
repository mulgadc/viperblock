//go:build integration

package viperblock

import (
	"fmt"
	"testing"

	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/assert"
)

// TestInvalidS3Host drives the backend against a host that refuses every
// connection, so its cost is dominated by connect retries and timeouts rather
// than by the code under test. That makes it an integration-tier test: it
// exercises real network failure behaviour and is far too slow for the unit
// gate, where it alone consumed a sixth of the whole budget.
func TestInvalidS3Host(t *testing.T) {
	runWithBackends(t, "invalid_s3_write_and_read", func(t *testing.T, vb *VB) {
		// Skip if file backend
		if vb.Backend.GetBackendType() == "file" {
			t.Skip("Skipping test for file backend")
		}

		t.Run("Use Invalid S3 Bucket", func(t *testing.T) {
			// Get a temp free port
			tempPort, err := FindFreePort()
			assert.NoError(t, err)

			vb.Backend.SetConfig(s3.S3Config{
				VolumeName: vb.GetVolume(),
				VolumeSize: volumeSize,
				Region:     "ap-southeast-2",
				Bucket:     "bad_bucket",
				AccessKey:  AccessKey,
				SecretKey:  SecretKey,
				Host:       fmt.Sprintf("https://%s", tempPort),
				HTTPClient: testHTTPClient,
			})

			err = vb.Backend.Init()
			assert.Error(t, err)

			// Write a block
			// Next, write a new block and flush
			blockID := uint64(5)
			data := make([]byte, DefaultBlockSize)
			msg := "bad bucket block"
			copy(data[:len(msg)], msg)

			err = vb.WriteAt(blockID*uint64(vb.BlockSize), data)
			assert.NoError(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, _, err := vb.LookupBlockToObject(5)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)

			err = vb.Flush()
			assert.NoError(t, err)

			err = vb.WriteWALToChunk(true)
			assert.Error(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, _, err = vb.LookupBlockToObject(4)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)
		})
	})
}
