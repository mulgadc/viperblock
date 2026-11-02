package nbdkit

import (
	"os"
	"strings"
	"syscall"
	"testing"
)

// replaceStdin points fd 0 at newFD for the duration of the test.
func replaceStdin(t *testing.T, newFD int) {
	t.Helper()
	saved, err := syscall.Dup(0)
	if err != nil {
		t.Fatalf("dup stdin: %v", err)
	}
	if err := syscall.Dup3(newFD, 0, 0); err != nil {
		t.Fatalf("dup3 onto stdin: %v", err)
	}
	t.Cleanup(func() {
		_ = syscall.Dup3(saved, 0, 0)
		_ = syscall.Close(saved)
	})
}

func TestStdioReport_SilentWhenStdioUnchanged(t *testing.T) {
	if report, foreign := stdioReport(); foreign {
		t.Fatalf("stdio is as it was at load, want no report, got %q", report)
	}
}

func TestStdioReport_SilentWhenStdioIsDevNull(t *testing.T) {
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer devnull.Close()
	replaceStdin(t, int(devnull.Fd()))

	if report, foreign := stdioReport(); foreign {
		t.Fatalf("stdin is /dev/null as nbdkit leaves it, want no report, got %q", report)
	}
}

// A descriptor another thread allocated into the slot nbdkit just closed is
// what makes nbdkit exit with "open: Success"; the report has to name it.
func TestStdioReport_NamesForeignDescriptorOnStdin(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	replaceStdin(t, int(r.Fd()))

	report, foreign := stdioReport()
	if !foreign {
		t.Fatal("stdin holds a pipe, want a report")
	}
	if !strings.Contains(report, " 0=pipe:[") {
		t.Fatalf("report must name what holds fd 0, got %q", report)
	}
}
