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

	err := ImportDiskImage(s3Config, vbConfig, filename)

	assert.ErrorContains(t, err, "failed to open disk file")

	// Correct file, different error
	filename = "../../tests/unit-test-disk-image.raw"

	err = ImportDiskImage(s3Config, vbConfig, filename)

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

	err = ImportDiskImage(s3Config, vbConfig, filename)

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
