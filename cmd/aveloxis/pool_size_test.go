// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// TestServePoolSizeDerivation pins the decision table (v0.29.58): the
// override wins and is honored as written, the server budget caps demand,
// no budget leaves demand alone, and a derived size never goes below the
// non-serve default.
func TestServePoolSizeDerivation(t *testing.T) {
	cases := []struct {
		name                                  string
		override, demand, serverMax, reserved int
		want                                  int32
		source                                string
	}{
		{"demand within budget", 0, 200, 300, 3, 200, "demand"},
		{"demand capped by budget", 0, 520, 300, 3, 300 - 3 - siblingProcessPools*db.DefaultPoolMaxConns, "server budget"},
		{"override wins over budget", 150, 520, 300, 3, 150, "database.pool_max_conns"},
		{"no server answer: demand alone", 0, 520, 0, 0, 520, "demand"},
		{"tiny demand rises to the floor", 0, 5, 300, 3, db.DefaultPoolMaxConns, "default floor"},
		{"override below the floor is honored as written", 4, 500, 300, 3, 4, "database.pool_max_conns"},
		// review round 4: a server that cannot fund the demand still caps,
		// even below the floor — an uncapped pool would open connections
		// the server refuses at runtime.
		{"budget below the floor still caps", 0, 500, 50, 3, 50 - 3 - siblingProcessPools*db.DefaultPoolMaxConns, "server budget"},
		{"budget exactly zero caps at one", 0, 500, 43, 3, 1, "server budget"},
		{"reserve larger than max caps at one", 0, 500, 40, 45, 1, "server budget"},
	}
	for _, c := range cases {
		ps := servePoolSize(c.override, c.demand, c.serverMax, c.reserved)
		if ps.Size != c.want {
			t.Errorf("%s: size = %d, want %d", c.name, ps.Size, c.want)
		}
		if !strings.Contains(ps.Source, c.source) {
			t.Errorf("%s: source = %q, want it to name %q", c.name, ps.Source, c.source)
		}
		if ps.Demand != c.demand || ps.Attributes[0] != "pool_size" {
			t.Errorf("%s: the decision must carry its denominator (demand %d) and lead the attributes with pool_size", c.name, c.demand)
		}
	}
}

// TestPoolMaxConnsKnobReachesThePool is the end-to-end test the config
// rule requires (SR-10): the JSON value database.pool_max_conns becomes
// the pool's ceiling, through decideServePool and NewPostgresStore, on a
// real server — not just the accessor.
func TestPoolMaxConnsKnobReachesThePool(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	pw, _ := u.User.Password()
	raw := `{"database":{"host":"` + u.Hostname() + `","port":` + strconv.Itoa(port) + `,"user":"` + u.User.Username() +
		`","password":"` + pw + `","dbname":"` + strings.TrimPrefix(u.Path, "/") + `","sslmode":"disable","pool_max_conns":7}}`
	var cfg config.Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ps := decideServePool(ctx, &cfg, 120, logger)
	if ps.Size != 7 || ps.Override != 7 {
		t.Fatalf("decideServePool with pool_max_conns=7 chose %d (source %q)", ps.Size, ps.Source)
	}
	if ps.Budget <= 0 {
		t.Fatalf("the server budget must have been read on a live server, got %d", ps.Budget)
	}
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger, ps.Size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if got := store.PoolState().MaxConns; got != 7 {
		t.Fatalf("pool MaxConns = %d, want the configured 7", got)
	}
}
