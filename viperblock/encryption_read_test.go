// Copyright 2026 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

// Stage 3 round-trip smoke tests for the decrypt-on-read paths. The
// comprehensive matrix (tamper detection, cross-volume swap, in-place
// replay, snapshot-clone source identity, magic-bump rejection, property
// tests) lands in Stage 5; these are the minimum to gate that the read
// paths see plaintext after Stage 2 sealed it on write.

package viperblock

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/require"
)

func openWALsForReadTest(t *testing.T, vb *VB) {
	t.Helper()
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
}

func TestEncryptedRoundTrip_LegacyReadPath(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-rt-legacy", key)
	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = 16 * DefaultBlockSize
	require.NoError(t, vb.SaveState())
	openWALsForReadTest(t, vb)

	blockSize := uint64(vb.BlockSize)
	payload := make([]byte, 2*blockSize)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	require.NoError(t, vb.WriteAt(0, payload))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	got, err := vb.ReadAt(0, 2*blockSize)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, got), "encrypted round-trip plaintext mismatch (legacy read)")
}

func TestEncryptedRoundTrip_BlockStorePath(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-rt-blockstore", key)
	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = 16 * DefaultBlockSize
	vb.UseBlockStore = true
	vb.BlockStore = NewUnifiedBlockStore(vb.BlockSize)
	require.NoError(t, vb.SaveState())
	openWALsForReadTest(t, vb)

	blockSize := uint64(vb.BlockSize)
	payload := make([]byte, 3*blockSize)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	require.NoError(t, vb.WriteAt(0, payload))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	got, err := vb.ReadAt(0, 3*blockSize)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, got), "encrypted round-trip plaintext mismatch (blockstore read)")
}

func TestEncryptedRoundTrip_MultiChunk(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-rt-multichunk", key)
	vb.BlockSize = DefaultBlockSize
	// Force two chunks: 2 blocks per chunk, write 5 blocks across them.
	vb.ObjBlockSize = 2 * DefaultBlockSize
	require.NoError(t, vb.SaveState())
	openWALsForReadTest(t, vb)

	blockSize := uint64(vb.BlockSize)
	payload := make([]byte, 5*blockSize)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	require.NoError(t, vb.WriteAt(0, payload))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	got, err := vb.ReadAt(0, 5*blockSize)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, got), "encrypted multi-chunk round-trip plaintext mismatch")
}
