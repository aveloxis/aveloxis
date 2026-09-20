// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57 fix-review round 1: a repo row stored before the store refused
// credentials in repo_git reached `git clone` (argv, visible in ps) and the
// "cloning repository" log line on every cycle. The facade refuses such a
// URL at its entry, before any store call or subprocess — a nil store and
// an empty clone dir prove both — and the refusal's own log line redacts it.
func TestFacadeRefusesARepoURLWithUserinfo(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	dir := t.TempDir()
	fc := NewFacadeCollector(nil, logger, dir)
	_, err := fc.CollectRepo(context.Background(), 4242, "https://user:s3cret@github.com/owner/name")
	if !errors.Is(err, platform.ErrURLUserinfo) {
		t.Fatalf("CollectRepo = %v, want platform.ErrURLUserinfo", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("a refused URL created %d entries under the clone dir", len(entries))
	}
	if bytes.Contains(logs.Bytes(), []byte("s3cret")) {
		t.Errorf("the refusal logged the credential: %s", logs.String())
	}
	if !bytes.Contains(logs.Bytes(), []byte("level=ERROR")) || !bytes.Contains(logs.Bytes(), []byte("repo_id=4242")) {
		t.Errorf("the refusal must be logged at ERROR naming the repo: %s", logs.String())
	}
}
