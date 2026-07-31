package viperblock

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// sealReceipt is the on-disk shape written by WriteSealReceipt. Field names
// are part of the contract with the consumer that parses this file.
type sealReceipt struct {
	Volume   string `json:"volume"`
	PID      int    `json:"pid"`
	SealedAt string `json:"sealed_at"`
}

// WriteSealReceipt records that this process sealed volume and removed its
// local state, so the caller that killed it can tell a clean seal from a
// volume that never held state on this node. The receipt is written to
// <baseDir>/<volume>.sealed, a sibling of the state directory removed by
// RemoveLocalFiles, so cleanup never takes it with them.
func WriteSealReceipt(baseDir, volume string) error {
	receipt := sealReceipt{
		Volume:   volume,
		PID:      os.Getpid(),
		SealedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"),
	}

	data, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("marshal seal receipt: %w", err)
	}

	path := filepath.Join(baseDir, volume+".sealed")
	if err := writeFileAtomic(path, data, 0640); err != nil {
		return fmt.Errorf("write seal receipt: %w", err)
	}

	return nil
}
