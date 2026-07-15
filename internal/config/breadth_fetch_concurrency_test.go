// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.8: breadth_fetch_concurrency — the contributor breadth worker's
// fetcher-pool size. Pre-v0.27.8 the Run loop was strictly sequential
// (one contributor, one HTTP request in flight at a time), leaving the
// 73-key / 365K-req-hr API pool essentially idle while a
// 2.3M-contributor fleet crawled at HTTP-RTT speed. The knob controls
// how many fetcher goroutines consume the per-cycle contributor list
// concurrently.

package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestCollectionConfigHasBreadthFetchConcurrency(t *testing.T) {
	data, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `json:"breadth_fetch_concurrency"`) {
		t.Error("CollectionConfig must declare BreadthFetchConcurrency with " +
			`json tag "breadth_fetch_concurrency" — the v0.27.8 fetcher-pool size ` +
			"for the contributor breadth worker")
	}
}

func TestBreadthFetchConcurrencyDefault(t *testing.T) {
	c := &CollectionConfig{}
	if got := c.BreadthFetchConcurrencyOrDefault(); got != 8 {
		t.Errorf("zero-value BreadthFetchConcurrencyOrDefault() = %d, want 8 "+
			"(the v0.27.8 default fetcher-pool size)", got)
	}
}

func TestBreadthFetchConcurrencyExplicitValue(t *testing.T) {
	c := &CollectionConfig{BreadthFetchConcurrency: 16}
	if got := c.BreadthFetchConcurrencyOrDefault(); got != 16 {
		t.Errorf("explicit BreadthFetchConcurrencyOrDefault() = %d, want 16", got)
	}
}

func TestBreadthFetchConcurrencyNegativeClampsToDefault(t *testing.T) {
	c := &CollectionConfig{BreadthFetchConcurrency: -3}
	if got := c.BreadthFetchConcurrencyOrDefault(); got != 8 {
		t.Errorf("negative BreadthFetchConcurrencyOrDefault() = %d, want 8 "+
			"(negative/zero inputs collapse to the default per the house accessor pattern)", got)
	}
}

func TestBreadthFetchConcurrencyJSONRoundTrip(t *testing.T) {
	var c CollectionConfig
	if err := json.Unmarshal([]byte(`{"breadth_fetch_concurrency": 12}`), &c); err != nil {
		t.Fatal(err)
	}
	if c.BreadthFetchConcurrency != 12 {
		t.Errorf("JSON breadth_fetch_concurrency = %d, want 12", c.BreadthFetchConcurrency)
	}
	if got := c.BreadthFetchConcurrencyOrDefault(); got != 12 {
		t.Errorf("effective value = %d, want 12", got)
	}
}
