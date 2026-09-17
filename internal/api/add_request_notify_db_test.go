// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

// lockedBuffer is a log sink safe to read while the logging goroutine writes.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitForLog polls logs until it contains want. The notification runs in its
// own goroutine, so there is nothing to join.
func waitForLog(t *testing.T, logs *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("no %q in the log within 10s; log:\n%s", want, logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNotifyAddRequestSubmittedLogsAListFailure (AVELOXIS_TEST_DB): when the
// pending-request lookup fails, the operator notice is not sent and the log
// says why ("Everything that errors should be logged"; round-9 review: the
// API twin returned silently while the web twin logged). The store is closed
// before use, so every query fails and no mail is attempted.
func TestNotifyAddRequestSubmittedLogsAListFailure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	store, err := db.NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	logs := &lockedBuffer{}
	s := &Server{store: store, logger: slog.New(slog.NewTextHandler(logs, nil)),
		mailer: mailer.New(mailer.Config{OperatorEmail: "operator@example.com"}, nil)}
	s.notifyAddRequestSubmitted(1)
	waitForLog(t, logs, "add-request email: list failed")
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "closed pool") {
		t.Errorf("the list failure must be a WARN carrying the error; log:\n%s", logs.String())
	}
}
