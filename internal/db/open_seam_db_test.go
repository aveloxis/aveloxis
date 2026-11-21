// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db/stmttrace"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A pool's BeforeConnect hook is handed each new connection's configuration
// and may replace its tracer. A pool opened through the store's opener
// keeps the statement seam on whatever the hook leaves (review round 8: a
// hook that set the tracer to nil took the write-policy judge off every
// connection of that pool, and nothing noticed). The caller's hook still
// runs first, and its error is still the pool's.
func TestAPoolsBeforeConnectHookCannotTakeTheStatementSeamOff(t *testing.T) {
	openTestStore(t) // the package's database; the harness has set the seam
	if stmttrace.Wrap == nil {
		t.Fatal("the DB-tier harness did not set the statement seam: this test has nothing to show")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(os.Getenv("AVELOXIS_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	cfg.BeforeConnect = func(_ context.Context, cc *pgx.ConnConfig) error {
		ran++
		cc.Tracer = nil
		return nil
	}
	pool, err := OpenPoolConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tracer := conn.Conn().Config().Tracer
	conn.Release()
	if ran == 0 {
		t.Error("the caller's BeforeConnect hook did not run")
	}
	if _, seamed := tracer.(stmttrace.Seamed); !seamed {
		t.Errorf("the connection's tracer is %T after a hook that set it to nil; want the statement seam put back", tracer)
	}

	// The hook's refusal is the pool's.
	refused := errors.New("the hook refuses")
	cfg2, _ := pgxpool.ParseConfig(os.Getenv("AVELOXIS_TEST_DB"))
	cfg2.BeforeConnect = func(context.Context, *pgx.ConnConfig) error { return refused }
	pool2, err := OpenPoolConfig(ctx, cfg2)
	if err == nil {
		t.Cleanup(pool2.Close)
		var c *pgxpool.Conn
		if c, err = pool2.Acquire(ctx); err == nil {
			c.Release() // a hook that did not refuse: the pool must still close
		}
	}
	if !errors.Is(err, refused) {
		t.Errorf("a pool whose hook refuses: %v; want the hook's error", err)
	}

	// A configuration that already carries the seam is not wrapped twice.
	cfg3, _ := pgxpool.ParseConfig(os.Getenv("AVELOXIS_TEST_DB"))
	withStatementSeam(cfg3.ConnConfig)
	once := cfg3.ConnConfig.Tracer
	withStatementSeam(cfg3.ConnConfig)
	if cfg3.ConnConfig.Tracer != once {
		t.Error("a configuration that carries the seam was wrapped again")
	}
}
