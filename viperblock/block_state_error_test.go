package viperblock

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type checkpointReadErrorBackend struct {
	types.Backend

	err error
}

var _ types.Backend = (*checkpointReadErrorBackend)(nil)

func (b *checkpointReadErrorBackend) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeBlockCheckpoint {
		return nil, b.err
	}
	return b.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
}

func TestLoadBlockStateCtx_ClassifiesBackendReadErrors(t *testing.T) {
	tests := []struct {
		name    string
		readErr error
		wantErr error
	}{
		{
			name:    "missing checkpoint means empty volume",
			readErr: fmt.Errorf("checkpoint: %w", os.ErrNotExist),
		},
		{
			name:    "backend rejection fails closed",
			readErr: fmt.Errorf("checkpoint: %w", types.ErrBackendNonRetryable),
			wantErr: types.ErrBackendNonRetryable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vb := newFileBackedVB(t, "vol-checkpoint-error", nil)
			vb.Backend = &checkpointReadErrorBackend{Backend: vb.Backend, err: tt.readErr}

			err := vb.LoadBlockStateCtx(context.Background())
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}
