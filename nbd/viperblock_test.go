package main

import (
	"fmt"
	"syscall"
	"testing"

	"github.com/mulgadc/viperblock/viperblock"
	"github.com/stretchr/testify/assert"

	"libguestfs.org/nbdkit"
)

// TestBackendErrToPluginErrorMapsErrNoSpaceToENOSPC pins that a
// viperblock.ErrNoSpace write error reaches nbdkit as ENOSPC, not a generic
// I/O error.
func TestBackendErrToPluginErrorMapsErrNoSpaceToENOSPC(t *testing.T) {
	err := fmt.Errorf("drain chunk upload: %w", viperblock.ErrNoSpace)

	perr := backendErrToPluginError("Could not write data", err)

	assert.Equal(t, syscall.ENOSPC, perr.Errno,
		"an ErrNoSpace-wrapped error must set PluginError.Errno to syscall.ENOSPC")
	assert.Contains(t, perr.Errmsg, "Could not write data")
	assert.Contains(t, perr.Errmsg, err.Error())
}

// TestBackendErrToPluginErrorLeavesOtherErrorsAlone guards against
// over-matching: an unrelated write failure must not be given an ENOSPC
// errno it did not earn.
func TestBackendErrToPluginErrorLeavesOtherErrorsAlone(t *testing.T) {
	err := fmt.Errorf("connection reset by peer")

	perr := backendErrToPluginError("Could not write data", err)

	assert.Equal(t, syscall.Errno(0), perr.Errno,
		"an unrelated error must not set Errno (nbdkit defaults to EIO when Errno is 0)")
	assert.Contains(t, perr.Errmsg, "Could not write data")
}

// TestPluginErrorErrnoPropagatesAsError pins that the returned value
// satisfies error via nbdkit.PluginError, since implError type-asserts on it.
func TestPluginErrorErrnoPropagatesAsError(t *testing.T) {
	var err error = backendErrToPluginError("boom", fmt.Errorf("%w", viperblock.ErrNoSpace))

	perr, ok := err.(nbdkit.PluginError)
	if assert.True(t, ok, "backendErrToPluginError must return a value assignable to error via nbdkit.PluginError") {
		assert.Equal(t, syscall.ENOSPC, perr.Errno)
	}
}
