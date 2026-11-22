package viperblock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

func TestAcknowledgedBatchedWALSurvivesProcessKill(t *testing.T) {
	const blockCount = 128
	const ack = "lab-wal-flush-ack"
	if childRoot := os.Getenv("VB_LAB_RESTART_CHILD_ROOT"); childRoot != "" {
		vb := openRestartTestVolume(t, childRoot)
		path := filepath.Join(vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))
		require.NoError(t, vb.OpenWAL(&vb.WAL, path))
		for i := range uint64(blockCount) {
			block := bytes.Repeat([]byte{byte(i + 1)}, int(vb.BlockSize))
			require.NoError(t, vb.WriteAt(i*uint64(vb.BlockSize), block))
		}
		require.NoError(t, vb.Flush())
		fmt.Println(ack)
		_, _ = io.Copy(io.Discard, os.Stdin)
		return
	}

	root := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAcknowledgedBatchedWALSurvivesProcessKill$")
	cmd.Env = append(os.Environ(), "VB_LAB_RESTART_CHILD_ROOT="+root)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	defer stdin.Close()
	require.NoError(t, cmd.Start())
	var gotAck bool
	scanner := bufio.NewScanner(stdout)
	var childOutput bytes.Buffer
	for scanner.Scan() {
		if scanner.Text() == ack {
			gotAck = true
			break
		}
		childOutput.WriteString(scanner.Text() + "\n")
	}
	if !gotAck {
		_ = cmd.Wait()
		t.Fatalf("no flush acknowledgement: %v\n%s", scanner.Err(), childOutput.String())
	}
	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
	require.Error(t, cmd.Wait())
	require.Equal(t, syscall.SIGKILL, cmd.ProcessState.Sys().(syscall.WaitStatus).Signal())

	reopened := openRestartTestVolume(t, root)
	require.NoError(t, reopened.RecoverLocalWALs())
	for i := range uint64(blockCount) {
		got, err := reopened.ReadAt(i*uint64(reopened.BlockSize), uint64(reopened.BlockSize))
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, int(reopened.BlockSize)), got)
	}
}

func openRestartTestVolume(t *testing.T, root string) *VB {
	t.Helper()
	const name = "lab-restart"
	local, backend := filepath.Join(root, "local"), filepath.Join(root, "backend")
	require.NoError(t, os.MkdirAll(local, 0700))
	require.NoError(t, os.MkdirAll(backend, 0700))
	config := VB{
		VolumeName: name, VolumeSize: 64 << 20, BaseDir: local,
		WALSyncInterval: -1, ChunkUploadInterval: -1,
	}
	vb, err := New(&config, "file", file.FileConfig{
		VolumeName: name, VolumeSize: config.VolumeSize, BaseDir: backend,
	})
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	return vb
}
