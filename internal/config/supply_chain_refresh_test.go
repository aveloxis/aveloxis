// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// collection.supply_chain_refresh_hours (v0.29.61, worklist 48): the
// cadence of the Aveloxis-owned supply-chain views, apart from the weekly
// 8Knot rebuild. One default layer (SR-10): absent → daily; an explicit 0
// → no scheduled refresh (`aveloxis refresh-views` still works); a
// negative value is a configuration error, never a silent coercion.
func TestSupplyChainRefreshIntervalDefaultsAndOff(t *testing.T) {
	var c CollectionConfig
	if got, on := c.SupplyChainRefreshInterval(); !on || got != 24*time.Hour {
		t.Errorf("absent: interval=%v on=%v, want 24h on (the profile's day-resolution figures cannot change faster)", got, on)
	}
	zero := 0
	c.SupplyChainRefreshHours = &zero
	if got, on := c.SupplyChainRefreshInterval(); on || got != 0 {
		t.Errorf("explicit 0: interval=%v on=%v, want off", got, on)
	}
	six := 6
	c.SupplyChainRefreshHours = &six
	if got, on := c.SupplyChainRefreshInterval(); !on || got != 6*time.Hour {
		t.Errorf("6: interval=%v on=%v, want 6h on", got, on)
	}
}

// End to end (SR-10): the JSON value reaches the accessor the scheduler
// reads, through Load and DefaultConfig, and a negative value is refused
// at load time with a message that names the key.
func TestSupplyChainRefreshHoursLoadsFromJSON(t *testing.T) {
	write := func(body string) string {
		p := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cfg, err := Load(write(`{"collection": {"supply_chain_refresh_hours": 6}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, on := cfg.Collection.SupplyChainRefreshInterval(); !on || got != 6*time.Hour {
		t.Errorf("loaded 6: interval=%v on=%v", got, on)
	}
	cfg, err = Load(write(`{"collection": {}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, on := cfg.Collection.SupplyChainRefreshInterval(); !on || got != 24*time.Hour {
		t.Errorf("absent after Load: interval=%v on=%v, want the daily default", got, on)
	}
	for _, bad := range []string{"-1", fmt.Sprint(MaxSupplyChainRefreshHours + 1)} {
		if _, err := Load(write(`{"collection": {"supply_chain_refresh_hours": ` + bad + `}}`)); err == nil {
			t.Errorf("supply_chain_refresh_hours %s must be refused at load (negative, or an hours value whose Duration overflows)", bad)
		} else if !strings.Contains(err.Error(), "supply_chain_refresh_hours") {
			t.Errorf("the refusal must name the key: %v", err)
		}
	}
	if d := time.Duration(MaxSupplyChainRefreshHours) * time.Hour; d <= 0 {
		t.Errorf("the bound itself must be representable, got %v", d)
	}
}
