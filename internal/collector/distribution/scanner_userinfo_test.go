// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57 fix-review round 3: the distribution worker is its own pool, not
// behind the scheduler's job gate, and it handed the stored URL to a THIRD
// PARTY (ecosyste.ms, as ?repository_url=). The scanner refuses at its entry
// with an error, so the worker records a failure and the row is not
// reclaimed every tick; the ERROR names the repo with the URL redacted.
func TestCompositeScannerRefusesUserinfo(t *testing.T) {
	var logs bytes.Buffer
	s := &CompositeScanner{Logger: slog.New(slog.NewTextHandler(&logs, nil))}
	dists, manifests, complete, err := s.Scan(context.Background(), 5, "owner", "name", "https://user:s3cret@github.com/owner/name")
	if !errors.Is(err, platform.ErrURLUserinfo) {
		t.Fatalf("Scan = %v, want platform.ErrURLUserinfo", err)
	}
	if complete || dists != nil || manifests != nil {
		t.Errorf("a refused scan reported complete=%v with %d distributions and %d manifests", complete, len(dists), len(manifests))
	}
	if bytes.Contains(logs.Bytes(), []byte("s3cret")) || !bytes.Contains(logs.Bytes(), []byte("level=ERROR")) {
		t.Errorf("the refusal must be an ERROR without the credential: %s", logs.String())
	}
}
