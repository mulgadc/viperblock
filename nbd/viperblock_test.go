package main

import (
	"fmt"
	"syscall"
	"testing"

	"github.com/mulgadc/viperblock/viperblock"
	"github.com/stretchr/testify/assert"

	"libguestfs.org/nbdkit"
)

// resetPluginConfigState clears package-level nbdkit config vars so each
// test starts from ConfigComplete's zero state instead of leaking values
// set by a previous test.
func resetPluginConfigState() {
	size = 0
	volume = ""
	bucket = ""
	region = ""
	access_key = ""
	secret_key = ""
	host = ""
	base_dir = ""
	encryption_key_file = ""
	loadedMasterKey = nil
}

// TestCredentialFromArgOrEnv_ArgvTakesPriority pins that a non-empty argv
// value wins over the environment, matching LoadMasterKeyFromFlagOrEnv's
// flag-then-env precedence.
func TestCredentialFromArgOrEnv_ArgvTakesPriority(t *testing.T) {
	t.Setenv("VB_TEST_CRED", "from-env")

	got := credentialFromArgOrEnv("from-argv", "VB_TEST_CRED")
	assert.Equal(t, "from-argv", got)
}

// TestCredentialFromArgOrEnv_FallsBackToEnv pins that an empty argv value
// falls back to the named environment variable.
func TestCredentialFromArgOrEnv_FallsBackToEnv(t *testing.T) {
	t.Setenv("VB_TEST_CRED", "from-env")

	got := credentialFromArgOrEnv("", "VB_TEST_CRED")
	assert.Equal(t, "from-env", got)
}

// TestCredentialFromArgOrEnv_EmptyWhenNeitherSet pins that an empty argv
// value with no environment variable set resolves to empty.
func TestCredentialFromArgOrEnv_EmptyWhenNeitherSet(t *testing.T) {
	got := credentialFromArgOrEnv("", "VB_TEST_CRED_UNSET")
	assert.Equal(t, "", got)
}

// TestConfigComplete_AcceptsEnvSuppliedCredentials pins that ConfigComplete
// succeeds when credentials arrive via VB_ACCESS_KEY/VB_SECRET_KEY instead
// of argv, and that the resolved package vars carry the env values.
func TestConfigComplete_AcceptsEnvSuppliedCredentials(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()

	t.Setenv("VB_ACCESS_KEY", "env-access-key")
	t.Setenv("VB_SECRET_KEY", "env-secret-key")

	size = 1
	volume = "vol-1"
	bucket = "bucket-1"
	region = "region-1"
	base_dir = "/data"
	host = "localhost:9000"

	p := &ViperBlockPlugin{}
	err := p.ConfigComplete()
	assert.NoError(t, err)
	assert.Equal(t, "env-access-key", access_key)
	assert.Equal(t, "env-secret-key", secret_key)
}

// TestConfigComplete_StillAcceptsArgvSuppliedCredentials pins the transition
// path: an unpinned spinifex still passing credentials via argv (Config
// callbacks) must keep working with no environment variables set.
func TestConfigComplete_StillAcceptsArgvSuppliedCredentials(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()

	size = 1
	volume = "vol-1"
	bucket = "bucket-1"
	region = "region-1"
	access_key = "argv-access-key"
	secret_key = "argv-secret-key"
	base_dir = "/data"
	host = "localhost:9000"

	p := &ViperBlockPlugin{}
	err := p.ConfigComplete()
	assert.NoError(t, err)
	assert.Equal(t, "argv-access-key", access_key)
	assert.Equal(t, "argv-secret-key", secret_key)
}

// TestConfigComplete_RejectsMissingCredentialsFromBothSources pins that
// ConfigComplete still errors when credentials are absent from both argv
// and the environment.
func TestConfigComplete_RejectsMissingCredentialsFromBothSources(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()

	size = 1
	volume = "vol-1"
	bucket = "bucket-1"
	region = "region-1"

	p := &ViperBlockPlugin{}
	err := p.ConfigComplete()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "access_key parameter is required")
}

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
