// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"testing"
)

// v0.28.19: the API listen address is configurable.
//
// `aveloxis start api` spawns the process with only --config, so before
// this the address could be changed only by running `aveloxis api
// --addr …` by hand — and two instances on one host collided on 8383.

func TestAPIAddrDefaultsToLoopback(t *testing.T) {
	var a APIConfig
	if got, want := a.AddrOrDefault(), "127.0.0.1:8383"; got != want {
		t.Errorf("AddrOrDefault() on an unset config = %q, want %q. The default must stay LOOPBACK: the API serves the whole catalog, require_auth is off by default, and exempt_cidrs waives rate limiting for RFC1918 — a routable default would expose all three.", got, want)
	}
	if got, want := DefaultConfig().API.AddrOrDefault(), "127.0.0.1:8383"; got != want {
		t.Errorf("DefaultConfig().API.AddrOrDefault() = %q, want %q", got, want)
	}
}

func TestAPIAddrHonoursTheConfiguredValue(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:9383", ":9383", "10.0.0.5:8383"} {
		a := APIConfig{Addr: addr}
		if got := a.AddrOrDefault(); got != addr {
			t.Errorf("AddrOrDefault() = %q, want the configured %q", got, addr)
		}
	}
	// Whitespace-only is unset, not a bind to "".
	if got, want := (APIConfig{Addr: "   "}).AddrOrDefault(), "127.0.0.1:8383"; got != want {
		t.Errorf("a blank addr resolved to %q, want the default %q — binding to an empty address listens on every interface, which is the opposite of what a blank value should mean", got, want)
	}
}

func TestAPIAddrRoundTripsThroughJSON(t *testing.T) {
	var cfg Config
	if err := json.Unmarshal([]byte(`{"api":{"addr":"0.0.0.0:9383"}}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got, want := cfg.API.AddrOrDefault(), "0.0.0.0:9383"; got != want {
		t.Errorf("api.addr round-tripped to %q, want %q — the JSON tag is what operators actually edit", got, want)
	}
	// Absent means default, not empty.
	var bare Config
	if err := json.Unmarshal([]byte(`{"api":{}}`), &bare); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got, want := bare.API.AddrOrDefault(), "127.0.0.1:8383"; got != want {
		t.Errorf("an api block with no addr resolved to %q, want %q", got, want)
	}
}
