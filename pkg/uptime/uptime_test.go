package uptime

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "uptime")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestUptimeFromFile(t *testing.T) {
	got, err := FromFile(writeFile(t, "5.50 1.00\n"))()
	if err != nil {
		t.Fatalf("FromFile: %v", err)
	}
	if got != 5500*time.Millisecond {
		t.Errorf("uptime = %v, want 5.5s", got)
	}

	if _, err := FromFile(filepath.Join(t.TempDir(), "missing"))(); err == nil {
		t.Error("missing file: err = nil, want error")
	}

	for _, content := range []string{"abc", "", "-1.0 2.0", "NaN 1"} {
		if _, err := FromFile(writeFile(t, content))(); err == nil {
			t.Errorf("content %q: err = nil, want error", content)
		}
	}
}
