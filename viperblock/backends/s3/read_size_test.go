package s3

import (
	"testing"

	"github.com/mulgadc/viperblock/types"
)

// Every object type has its own read-size label, so no two kinds of read are
// counted together.
func TestFileTypeNamesAreDistinct(t *testing.T) {
	seen := map[string]types.FileType{}
	for ft := types.FileTypeConfig; ft <= types.FileTypeSnapshotMarker; ft++ {
		name := fileTypeName(ft)
		if name == "other" {
			t.Errorf("file type %d has no name", ft)
		}
		if prev, dup := seen[name]; dup {
			t.Errorf("file types %d and %d share the name %q", prev, ft, name)
		}
		seen[name] = ft
	}
	if fileTypeName(types.FileTypeSnapshotMarker+1) != "other" {
		t.Error("an unknown file type is not named other")
	}
}
