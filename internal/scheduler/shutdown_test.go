package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestSchedulerRunClosesStoreOnCtxCancel pins the v0.20.0 graceful
// pgx-pool shutdown. Pre-v0.20.0, the ctx-cancel branch released
// queue locks and returned, but `s.store.Close()` happened only via
// the `defer store.Close()` in `runServe`. If the runtime SIGKILLed
// before defers ran (or if a worker goroutine borrowed a connection
// from the pool that wasn't returned cleanly), the postgres backend
// stayed alive until TCP keepalive fired — minutes to tens of
// minutes. The 26-minute orphan from the 2026-05-08 incident is
// the canonical example.
//
// The fix: explicitly call s.store.Close() in the ctx-cancel branch
// AFTER releaseOurLocks, and log "pgx pool closed" so the operator
// can confirm clean shutdown.
func TestSchedulerRunClosesStoreOnCtxCancel(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func (s *Scheduler) Run(")
	if fnIdx < 0 {
		t.Fatal("cannot find Scheduler.Run")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	body := rest[:end+1]

	// The Scheduler.Run body must contain s.store.Close() AFTER the
	// ctx.Done case-arm AND `releaseOurLocks` (the canonical shutdown
	// sequence). Pinning by relative position is more robust than
	// trying to extract the case-arm precisely — the new bounded-wait
	// inner select introduces additional `case <-...` clauses.
	doneIdx := strings.Index(body, "case <-ctx.Done():")
	if doneIdx < 0 {
		t.Fatal("cannot find ctx.Done case in Scheduler.Run")
	}
	closeIdx := strings.Index(body, "s.store.Close()")
	if closeIdx < 0 {
		t.Error("Scheduler.Run must call s.store.Close() somewhere — " +
			"the ctx.Done branch needs explicit pool close so backends " +
			"disconnect cleanly. Without this, the pgxpool stays open " +
			"until runServe's defer fires, which can miss SIGKILL paths " +
			"and leaves backends grinding for the full TCP-keepalive " +
			"window.")
	} else if closeIdx < doneIdx {
		t.Error("s.store.Close() appears BEFORE the ctx.Done case-arm. " +
			"It must be inside the ctx.Done branch, after releaseOurLocks.")
	}
	if !strings.Contains(body, "pgx pool closed") {
		t.Error("Scheduler.Run should log " +
			`"pgx pool closed" (or similar) after s.store.Close() so ` +
			"the operator can confirm a graceful shutdown completed.")
	}
	// Pin the relative ordering: releaseOurLocks → s.store.Close() →
	// the log line.
	releaseIdx := strings.Index(body, "releaseOurLocks(context.Background())")
	if releaseIdx < 0 {
		t.Fatal("cannot find releaseOurLocks call site")
	}
	if !(releaseIdx < closeIdx) {
		t.Error("Scheduler.Run must release queue locks BEFORE closing " +
			"the pgx pool — otherwise releaseOurLocks's UPDATE might fire " +
			"on a closed pool and leave queue rows in 'collecting' state.")
	}
}

// TestSchedulerConfigHasShutdownGrace pins the configurable
// ShutdownGrace knob. Pre-v0.20.0, ctx-cancel waited UNBOUNDED for
// workers to finish via `for range workers { sem <- struct{}{} }`. A
// worker mid-26-minute UPDATE blocks shutdown for the full duration.
// ShutdownGrace bounds the wait at a sensible default (10 seconds);
// after that, the scheduler closes the pool even with workers in
// flight, accepting that those backends become orphans we can clean
// up in their own thread.
func TestSchedulerConfigHasShutdownGrace(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "ShutdownGrace") {
		t.Error("scheduler.go must declare a ShutdownGrace field on Config " +
			"so the ctx-cancel branch can bound how long it waits for " +
			"in-flight workers. Without a bound, a single 26-minute " +
			"UPDATE blocks shutdown for that duration.")
	}
}

// TestPostgresStoreSetsApplicationName pins that pgx connections
// register an application_name so post-stop verification can filter
// `pg_stat_activity` to "is THIS aveloxis-serve still active". Pre-
// v0.20.0 connections had no application_name — operators couldn't
// distinguish aveloxis backends from any other postgres client.
func TestPostgresStoreSetsApplicationName(t *testing.T) {
	data, err := os.ReadFile("../db/postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "application_name") {
		t.Error("postgres.go must set application_name on pgx connections " +
			"(via cfg.ConnConfig.RuntimeParams or AfterConnect hook). " +
			"v0.20.0 stop-verification needs this to filter " +
			"pg_stat_activity to aveloxis-managed backends only.")
	}
}
