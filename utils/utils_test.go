package utils

import "testing"

// TestHumanBytes locks the IEC formatting so it stays identical to spinifex's
// HumanBytes; the two modules format byte counts independently but must agree.
func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
		{859994624, "820.2 MiB"},
		{1073741824, "1.0 GiB"},
		{3758096384, "3.5 GiB"},
		{1099511627776, "1.0 TiB"},
	}
	for _, c := range cases {
		if got := HumanBytes(c.in); got != c.want {
			t.Errorf("HumanBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}
