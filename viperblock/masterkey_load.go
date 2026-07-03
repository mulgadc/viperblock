package viperblock

import (
	"os"

	"github.com/mulgadc/predastore/pkg/masterkey"
)

// EncryptionKeyEnv names the environment variable that supplies the master
// key file path when no CLI flag is given.
const EncryptionKeyEnv = "ENCRYPTION_KEY_FILE"

// LoadMasterKeyFromFlagOrEnv resolves a master-key file path from flagValue,
// falling back to the ENCRYPTION_KEY_FILE environment variable when flagValue
// is empty. Returns (nil, nil) when neither is set so the caller can run the
// volume unencrypted; otherwise loads via masterkey.LoadShared.
func LoadMasterKeyFromFlagOrEnv(flagValue string) (*masterkey.Key, error) {
	keyPath := flagValue
	if keyPath == "" {
		keyPath = os.Getenv(EncryptionKeyEnv)
	}
	if keyPath == "" {
		return nil, nil
	}
	return masterkey.LoadShared(keyPath)
}
