package mailer

import (
	"os"
	"testing"
)

// readSrc reads a source file from the package directory.
func readSrc(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}
