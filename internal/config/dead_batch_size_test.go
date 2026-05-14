// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v0.20.18 removed the dead `batch_size` config field. The field
// was declared in CollectionConfig and defaulted to 1000 but had
// been unused by production code since at least v0.18.x — no path
// ever read cfg.Collection.BatchSize. Operators who tuned it saw
// no behavior change and reasonably assumed something was wrong.
//
// This negative tripwire prevents the field from being silently
// re-added. If someone reintroduces a `BatchSize int
// \`json:"batch_size"\`` field without also wiring it through to
// actual consumption, this test fails.
//
// If a real need ever arises for a batch-size knob in
// CollectionConfig, choose an unambiguous name (e.g.
// `staging_flush_size` for stagingFlushSize, or
// `process_batch_size` for processBatchSize) so operators don't
// have to guess what it controls.

func TestDeadBatchSizeFieldNotReintroduced(t *testing.T) {
	src, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)

	// Two pin checks. The first is the JSON tag (what operators
	// see); the second is the Go field name. Either reappearing
	// fails the test.
	if strings.Contains(body, `json:"batch_size"`) {
		t.Error(`config.go declares a json:"batch_size" tag — this field was removed in v0.20.18 because nothing reads it. If a tunable batch knob is genuinely needed, use a name that says what it controls (e.g. "staging_flush_size" or "process_batch_size") and wire it through to the actual consumer.`)
	}

	// Locate CollectionConfig to bound the field-name search.
	idx := strings.Index(body, "type CollectionConfig struct")
	if idx < 0 {
		t.Fatal("cannot find CollectionConfig struct definition")
	}
	end := strings.Index(body[idx:], "\n}")
	if end < 0 {
		t.Fatal("cannot find end of CollectionConfig")
	}
	structBody := body[idx : idx+end]

	// Match the field name as a token (whitespace before, type
	// after) so we don't false-match BreadthBatchSize / similar.
	if strings.Contains(structBody, "\tBatchSize ") ||
		strings.Contains(structBody, "\tBatchSize\t") {
		t.Error("CollectionConfig still has a BatchSize field — removed in v0.20.18 as dead config. If reintroducing, give it a name that says what it controls and verify a production code path actually reads it.")
	}
}

// TestNoExampleConfigsCarryDeadBatchSize keeps the various
// committed example JSON files clean of the removed key. The
// v0.20.12 TestExampleConfigIncludesEveryJSONField tripwire
// already enforces the inverse (every json tag must be in the
// example); this one enforces it for the removal direction so
// re-adding `"batch_size": N` to an example file fails CI even
// before config.go gets a matching field.
func TestNoExampleConfigsCarryDeadBatchSize(t *testing.T) {
	repoRoot, err := findCfgRepoRoot()
	if err != nil {
		t.Fatalf("finding repo root: %v", err)
	}
	candidates := []string{
		"aveloxis.example.json",
		"aveloxis.docker.example.json",
		"aveloxis.sharded.example.json",
		"aveloxis-shadow-graphql.json",
	}
	for _, name := range candidates {
		p := filepath.Join(repoRoot, name)
		body, err := os.ReadFile(p)
		if err != nil {
			// Not all variants are present in every checkout;
			// skip the ones that don't exist.
			continue
		}
		if strings.Contains(string(body), `"batch_size"`) {
			t.Errorf(`%s still contains the removed "batch_size" key — v0.20.18 dropped the field because no code reads it. Remove the key from this example.`, name)
		}
	}
}

// findCfgRepoRoot walks up looking for go.mod. Independent of
// the helper in scripts/spdx_coverage_test.go (different package)
// so this test stays self-contained.
func findCfgRepoRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
