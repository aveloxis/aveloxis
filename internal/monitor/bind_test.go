package monitor

import (
	"os"
	"strings"
	"testing"
)

// TestDefaultBindAddressIsLoopback verifies the monitor and API default
// listen addresses bind to localhost, not all interfaces. A bare ":port"
// makes Go listen on 0.0.0.0 which exposes unauthenticated endpoints
// to the network.
func TestDefaultBindAddressIsLoopback(t *testing.T) {
	src, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Monitor default should be 127.0.0.1:5555, not :5555.
	if strings.Contains(code, `"monitor", ":5555"`) {
		t.Error("monitor default address must be 127.0.0.1:5555, not :5555 — " +
			"bare :port listens on all interfaces, exposing the unauthenticated " +
			"dashboard to the network")
	}

	// API default should be 127.0.0.1:8383, not :8383.
	if strings.Contains(code, `"addr", ":8383"`) {
		t.Error("API default address must be 127.0.0.1:8383, not :8383 — " +
			"bare :port listens on all interfaces, exposing the unauthenticated " +
			"API to the network")
	}
}
