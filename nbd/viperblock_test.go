package main

import (
	"fmt"
	"syscall"
	"testing"

	"github.com/mulgadc/viperblock/viperblock"
	"github.com/stretchr/testify/assert"

	"libguestfs.org/nbdkit"
)

// TestBackendErrToPluginErrorMapsErrNoSpaceToENOSPC pins the guest-visible
// contract this plugin promises: a write-path error that classifies as
// viperblock.ErrNoSpace (a full predastore backend, or a full local disk)
// must reach nbdkit as a real ENOSPC, not a generic I/O error, so the guest
// kernel sees "no space left on device" instead of a bare EIO. PluginError's
// Errno field is what implError (the C bridge) passes to nbdkit's
// set_error, so this is the whole contract, verified without needing a live
// nbdkit process or guest.
func TestBackendErrToPluginErrorMapsErrNoSpaceToENOSPC(t *testing.T) {
	err := fmt.Errorf("drain chunk upload: %w", viperblock.ErrNoSpace)

	perr := backendErrToPluginError("Could not write data", err)

	assert.Equal(t, syscall.ENOSPC, perr.Errno,
		"an ErrNoSpace-wrapped error must set PluginError.Errno to syscall.ENOSPC")
	assert.Contains(t, perr.Errmsg, "Could not write data")
	assert.Contains(t, perr.Errmsg, err.Error())
}

// TestBackendErrToPluginErrorLeavesOtherErrorsAlone guards against
// over-matching: an unrelated write failure (e.g. a transient network error)
// must not be given an ENOSPC errno it did not earn — that would misinform
// the guest that the volume is out of space when it may simply need to
// retry.
func TestBackendErrToPluginErrorLeavesOtherErrorsAlone(t *testing.T) {
	err := fmt.Errorf("connection reset by peer")

	perr := backendErrToPluginError("Could not write data", err)

	assert.Equal(t, syscall.Errno(0), perr.Errno,
		"an unrelated error must not set Errno (nbdkit defaults to EIO when Errno is 0)")
	assert.Contains(t, perr.Errmsg, "Could not write data")
}

// TestPluginErrorErrnoPropagatesAsError is a narrow guard on the
// nbdkit.PluginError contract itself: it must satisfy the error interface
// (implError type-asserts err.(PluginError)) so PWrite/Zero/Flush returning
// it are actually routed through the Errno-aware path instead of falling
// back to Error(err.Error()).
func TestPluginErrorErrnoPropagatesAsError(t *testing.T) {
	var err error = backendErrToPluginError("boom", fmt.Errorf("%w", viperblock.ErrNoSpace))

	perr, ok := err.(nbdkit.PluginError)
	if assert.True(t, ok, "backendErrToPluginError must return a value assignable to error via nbdkit.PluginError") {
		assert.Equal(t, syscall.ENOSPC, perr.Errno)
	}
}
