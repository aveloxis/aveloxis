// Source-contract test for v0.19.2: platform.Client must declare
// SearchUserByEmail so the scheduler's search-resolve task can use
// the same Client abstraction it uses for everything else.

package platform

import (
	"os"
	"strings"
	"testing"
)

// TestPlatformClientHasSearchUserByEmail pins the interface method.
func TestPlatformClientHasSearchUserByEmail(t *testing.T) {
	data, err := os.ReadFile("platform.go")
	if err != nil {
		t.Fatalf("read platform.go: %v", err)
	}
	src := string(data)

	// Locate the Client interface definition.
	idx := strings.Index(src, "type Client interface")
	if idx < 0 {
		t.Fatal("could not locate type Client interface in platform.go")
	}
	end := strings.Index(src[idx:], "\n}")
	if end < 0 {
		t.Fatal("could not find end of Client interface")
	}
	iface := src[idx : idx+end]

	if !strings.Contains(iface, "SearchUserByEmail") {
		t.Error("platform.Client must declare SearchUserByEmail(ctx, email) (login string, userID int64, err error). " +
			"The scheduler's v0.19.2 search-resolve task uses this method to find a gh_user_id for " +
			"contributors with email but no platform identity.")
	}
}
