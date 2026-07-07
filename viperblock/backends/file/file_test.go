package file

import (
	"bytes"
	"context"
	"testing"

	"github.com/mulgadc/viperblock/types"
)

// newTestBackend returns an initialised file backend rooted at a temp dir.
func newTestBackend(t *testing.T) *Backend {
	t.Helper()
	b := New(FileConfig{VolumeName: "vol-test", VolumeSize: 1 << 20, BaseDir: t.TempDir()})
	if err := b.InitCtx(context.Background()); err != nil {
		t.Fatalf("InitCtx: %v", err)
	}
	return b
}

// TestCtxWrappersRoundTrip exercises the context-aware Backend methods, which
// delegate to their plain counterparts, through a write-then-read round trip.
func TestCtxWrappersRoundTrip(t *testing.T) {
	ctx := context.Background()
	headers := []byte("hdr!")
	payload := []byte("chunk-payload-0123456789")

	tests := []struct {
		name  string
		write func(b *Backend) error
		read  func(b *Backend) ([]byte, error)
	}{
		{
			name: "WriteCtx/ReadCtx",
			write: func(b *Backend) error {
				return b.WriteCtx(ctx, types.FileTypeChunk, 7, &headers, &payload)
			},
			read: func(b *Backend) ([]byte, error) {
				return b.ReadCtx(ctx, types.FileTypeChunk, 7, 0, 0)
			},
		},
		{
			name: "WriteToCtx/ReadFromCtx",
			write: func(b *Backend) error {
				return b.WriteToCtx(ctx, "vol-test", types.FileTypeChunk, 9, &headers, &payload)
			},
			read: func(b *Backend) ([]byte, error) {
				return b.ReadFromCtx(ctx, "vol-test", types.FileTypeChunk, 9, 0, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackend(t)
			if err := tc.write(b); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, err := tc.read(b)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			want := append(append([]byte{}, headers...), payload...)
			if !bytes.Equal(got, want) {
				t.Fatalf("round trip mismatch: got %q want %q", got, want)
			}
		})
	}
}
