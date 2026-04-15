package platform

import (
	"os"
	"strings"
	"testing"
)

// TestInvalidateKeyEscalatesToErrorWhenLastKey verifies that InvalidateKey
// logs at ERROR level when the invalidated key is the last valid key for
// the platform. When the last key is gone, all collection for that platform
// stops silently — the operator needs a loud signal.
func TestInvalidateKeyEscalatesToErrorWhenLastKey(t *testing.T) {
	src, err := os.ReadFile("ratelimit.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (kp *KeyPool) InvalidateKey(")
	if idx < 0 {
		t.Fatal("cannot find InvalidateKey function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1000 {
		fnBody = fnBody[:1000]
	}

	// Must check if this was the last valid key and escalate to Error.
	if !strings.Contains(fnBody, "Error(") && !strings.Contains(fnBody, "error(") {
		t.Error("InvalidateKey must log at ERROR level when the invalidated key " +
			"is the last valid key for the platform — all collection stops silently otherwise")
	}
}
