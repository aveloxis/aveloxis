// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSessionMapHasMutex verifies the sessions map is protected by a mutex.
// Without synchronization, concurrent HTTP requests cause a fatal
// "concurrent map writes" panic that crashes the web server.
func TestSessionMapHasMutex(t *testing.T) {
	// Server stays in server.go with New and Handler (P-2); the map and its
	// mutex are its fields.
	code := srctest.TypeBody(t, srctest.Read(t, "internal/web/server.go"), "Server")

	// Must have a sync.RWMutex or sync.Mutex guarding sessions.
	if !strings.Contains(code, "sessionMu") && !strings.Contains(code, "sync.Map") {
		t.Error("Server must protect the sessions map with a mutex (sessionMu) " +
			"or use sync.Map — concurrent HTTP requests cause fatal 'concurrent map writes' panic")
	}
}

// TestCreateSessionLocks verifies createSession acquires a write lock.
func TestCreateSessionLocks(t *testing.T) {
	fnBody := srctest.PackageFunc(t, "internal/web", "func (s *Server) createSession(")

	if !strings.Contains(fnBody, "sessionMu") {
		t.Error("createSession must acquire sessionMu lock before writing to sessions map")
	}
}

// TestGetSessionLocks verifies getSession acquires a read lock.
func TestGetSessionLocks(t *testing.T) {
	fnBody := srctest.PackageFunc(t, "internal/web", "func (s *Server) getSession(")

	if !strings.Contains(fnBody, "sessionMu") {
		t.Error("getSession must acquire sessionMu read lock before reading sessions map")
	}
}

// TestHandleLogoutLocks verifies handleLogout acquires a write lock.
func TestHandleLogoutLocks(t *testing.T) {
	fnBody := srctest.PackageFunc(t, "internal/web", "func (s *Server) handleLogout(")

	if !strings.Contains(fnBody, "sessionMu") {
		t.Error("handleLogout must acquire sessionMu lock before deleting from sessions map")
	}
}
