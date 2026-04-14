package api

import (
	"os"
	"strings"
	"testing"
)

// TestNoCORSWildcard verifies that the API server does not use
// Access-Control-Allow-Origin: * which allows any website the operator
// visits to read collected data via fetch() from any origin.
func TestNoCORSWildcard(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if strings.Contains(code, `Allow-Origin", "*"`) {
		t.Error("API server must not use Access-Control-Allow-Origin: * — " +
			"this allows any website the operator visits to read all collected data. " +
			"Use a configurable origin or restrict to localhost.")
	}
}

// TestCORSLocalhostOnlyFunction verifies the setCORSIfLocalhost helper exists
// to restrict cross-origin access to localhost origins.
func TestCORSLocalhostOnlyFunction(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "setCORSIfLocalhost") {
		t.Error("API server must use setCORSIfLocalhost to restrict CORS to localhost origins only")
	}
}
