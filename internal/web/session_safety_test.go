package web

import (
	"os"
	"strings"
	"testing"
)

// TestSessionMapHasMutex verifies the sessions map is protected by a mutex.
// Without synchronization, concurrent HTTP requests cause a fatal
// "concurrent map writes" panic that crashes the web server.
func TestSessionMapHasMutex(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must have a sync.RWMutex or sync.Mutex guarding sessions.
	if !strings.Contains(code, "sessionMu") && !strings.Contains(code, "sync.Map") {
		t.Error("server.go must protect the sessions map with a mutex (sessionMu) " +
			"or use sync.Map — concurrent HTTP requests cause fatal 'concurrent map writes' panic")
	}
}

// TestCreateSessionLocks verifies createSession acquires a write lock.
func TestCreateSessionLocks(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *Server) createSession(")
	if idx < 0 {
		t.Fatal("cannot find createSession function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 500 {
		fnBody = fnBody[:500]
	}

	if !strings.Contains(fnBody, "sessionMu") {
		t.Error("createSession must acquire sessionMu lock before writing to sessions map")
	}
}

// TestGetSessionLocks verifies getSession acquires a read lock.
func TestGetSessionLocks(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *Server) getSession(")
	if idx < 0 {
		t.Fatal("cannot find getSession function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 500 {
		fnBody = fnBody[:500]
	}

	if !strings.Contains(fnBody, "sessionMu") {
		t.Error("getSession must acquire sessionMu read lock before reading sessions map")
	}
}

// TestHandleLogoutLocks verifies handleLogout acquires a write lock.
func TestHandleLogoutLocks(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *Server) handleLogout(")
	if idx < 0 {
		t.Fatal("cannot find handleLogout function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 500 {
		fnBody = fnBody[:500]
	}

	if !strings.Contains(fnBody, "sessionMu") {
		t.Error("handleLogout must acquire sessionMu lock before deleting from sessions map")
	}
}
