package v_utils

import (
	"testing"

	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/assert"
)

func TestImportDiskImage(t *testing.T) {
	s3Config := &s3.S3Config{}
	vbConfig := &viperblock.VB{
		VolumeConfig: viperblock.VolumeConfig{},
	}

	filename := "test.img"

	err := ImportDiskImage(s3Config, vbConfig, filename, nil)

	assert.ErrorContains(t, err, "failed to open disk file")

	// Correct file, different error
	filename = "../../tests/unit-test-disk-image.raw"

	err = ImportDiskImage(s3Config, vbConfig, filename, nil)

	assert.ErrorContains(t, err, "failed to connect to Viperblock store")

	// Test defaults are set
	assert.NotEmpty(t, vbConfig.VolumeConfig.VolumeMetadata.VolumeID)
	assert.NotEmpty(t, vbConfig.VolumeConfig.VolumeMetadata.VolumeName)
	assert.NotEmpty(t, vbConfig.VolumeConfig.VolumeMetadata.State)
	assert.NotZero(t, vbConfig.VolumeConfig.VolumeMetadata.CreatedAt)

	// Next, create a simulated AMI and check defaults
	vbConfig = &viperblock.VB{
		VolumeConfig: viperblock.VolumeConfig{
			AMIMetadata: viperblock.AMIMetadata{
				Name: "Test AMI",
			},
		},
	}

	err = ImportDiskImage(s3Config, vbConfig, filename, nil)

	assert.ErrorContains(t, err, "failed to connect to Viperblock store")

	// Test AMI defaults are set
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.ImageID)
	assert.NotZero(t, vbConfig.VolumeConfig.AMIMetadata.CreationDate)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.Architecture)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.PlatformDetails)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.RootDeviceType)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.Virtualization)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.ImageOwnerAlias)
}

// TestProgressReporterThrottlesToPercent drives the reporter over a simulated
// multi-GiB stream in 4 KiB steps and asserts the callback fires at most 101
// times (one per percent plus the final 100%), not proportional to block count.
// It also checks the contract: current is monotonic and ends exactly at total.
func TestProgressReporterThrottlesToPercent(t *testing.T) {
	const blockSize = 4096
	const total uint64 = 4 * 1024 * 1024 * 1024 // 4 GiB

	var calls int
	var last uint64
	monotonic := true
	reporter := newProgressReporter(func(current, reportedTotal uint64) {
		calls++
		if current < last {
			monotonic = false
		}
		last = current
		assert.Equal(t, total, reportedTotal)
	}, total)

	for current := uint64(blockSize); current <= total; current += blockSize {
		reporter.report(current)
	}
	reporter.finish()

	assert.True(t, monotonic, "current must never decrease")
	assert.LessOrEqual(t, calls, 101, "callback bounded to one per percent plus final")
	assert.Equal(t, total, last, "final report ends exactly at total")
	assert.Less(t, uint64(calls), total/blockSize, "renders must not scale with block count")
}

// TestProgressReporterNilTolerated verifies a nil ProgressFunc is a no-op and
// that a zero total never divides by zero or emits a callback.
func TestProgressReporterNilTolerated(t *testing.T) {
	reporter := newProgressReporter(nil, 1024)
	assert.NotPanics(t, func() {
		reporter.report(512)
		reporter.finish()
	})

	var called bool
	zero := newProgressReporter(func(current, total uint64) { called = true }, 0)
	assert.NotPanics(t, func() {
		zero.report(0)
		zero.finish()
	})
	assert.False(t, called, "zero-total import reports nothing")
}
