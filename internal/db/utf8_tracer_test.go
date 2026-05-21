// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// v0.23.5 — boundary-layer UTF-8 scrub via pgx QueryTracer +
// BatchTracer.
//
// The 2026-05-21 post-v0.23.4 diagnostic on the production log
// showed 72 occurrences of SQLSTATE 22021 (`invalid byte sequence
// for encoding "UTF8"`) on `failed to upsert commit` warns, all
// from Linux-kernel-style repos with Latin-1 author names or file
// paths containing high-bit bytes (0xb3, 0xe1, 0xf6 — typical
// of pre-UTF-8 European author names and source files that
// embedded ISO-8859-1 codepoints in identifiers).
//
// v0.19.2 added `safeUTF8` as the primitive and applied it
// surgically to PopulateAffiliations (after seeing 0x89 PNG-
// signature bytes in contributor company fields). v0.23.5
// generalizes that fix: a pgx tracer registered on the conn
// config scrubs Args (for pool/tx Exec/Query/QueryRow) and
// QueuedQueries[*].Arguments (for SendBatch) in place. The
// tracer fires before wire encoding and mutates the caller's
// args via the shared underlying array. Net effect: every TEXT/
// VARCHAR/JSONB parameter that flows through pgx is guaranteed
// valid UTF-8 at write time. Zero call-site changes.

// --- Source-contract pins -------------------------------------

func TestUTF8TracerFileExists(t *testing.T) {
	_, err := os.Stat("utf8_tracer.go")
	if err != nil {
		t.Fatal("expected internal/db/utf8_tracer.go to exist for v0.23.5; " +
			"see CLAUDE.md `Changes in v0.23.5`. The file holds the " +
			"utf8ScrubTracer type that scrubs all pgx TEXT params before " +
			"they hit the wire.")
	}
}

func TestUTF8TracerImplementsQueryAndBatchTracer(t *testing.T) {
	// Compile-time interface satisfaction.
	var _ pgx.QueryTracer = utf8ScrubTracer{}
	var _ pgx.BatchTracer = utf8ScrubTracer{}
}

func TestNewPostgresStoreRegistersUTF8Tracer(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "ConnConfig.Tracer") {
		t.Error("postgres.go must assign cfg.ConnConfig.Tracer so the " +
			"utf8ScrubTracer fires on every conn opened by pgxpool. " +
			"Without this, the tracer code exists but no queries are " +
			"actually scrubbed.")
	}
	if !strings.Contains(code, "utf8ScrubTracer") {
		t.Error("postgres.go must reference utf8ScrubTracer{} as the " +
			"tracer value assigned to ConnConfig.Tracer.")
	}
}

// --- Behavioral unit tests (no DB) ----------------------------

func TestScrubArgsInPlaceLeavesValidStringsUntouched(t *testing.T) {
	args := []any{"hello", "world", 42}
	scrubArgsInPlace(args)
	if args[0] != "hello" || args[1] != "world" || args[2] != 42 {
		t.Errorf("valid args must pass through unchanged, got %v", args)
	}
}

func TestScrubArgsInPlaceFixesInvalidString(t *testing.T) {
	// 0xb3 alone is an invalid UTF-8 sequence. This is the exact
	// byte pattern from the 2026-05-21 kernel-repo log line:
	// `error="ERROR: invalid byte sequence for encoding \"UTF8\": 0xb3"`.
	bad := "name\xb3surname"
	args := []any{bad}
	scrubArgsInPlace(args)
	got, ok := args[0].(string)
	if !ok {
		t.Fatalf("expected string after scrub, got %T", args[0])
	}
	if !utf8.ValidString(got) {
		t.Errorf("scrubbed string must be valid UTF-8, got %q (bytes %v)",
			got, []byte(got))
	}
}

func TestScrubArgsInPlaceHandlesPointerToString(t *testing.T) {
	bad := "head\xe1\x7a\x71tail"
	args := []any{&bad}
	scrubArgsInPlace(args)
	p, ok := args[0].(*string)
	if !ok || p == nil {
		t.Fatalf("expected *string after scrub, got %T", args[0])
	}
	if !utf8.ValidString(*p) {
		t.Errorf("scrubbed *string content must be valid UTF-8, got %q", *p)
	}
}

func TestScrubArgsInPlaceLeavesNonStringTypesAlone(t *testing.T) {
	// Important: BYTEA columns take []byte and may legitimately
	// contain non-UTF-8 bytes. We must NOT scrub those.
	rawBytes := []byte{0x89, 0x50, 0x4E, 0x47} // PNG signature start
	now := time.Now()
	var nilPtr *string
	args := []any{rawBytes, 42, 3.14, true, now, nil, nilPtr}
	scrubArgsInPlace(args)
	gotBytes, ok := args[0].([]byte)
	if !ok {
		t.Fatalf("expected []byte to pass through unchanged, got %T", args[0])
	}
	if string(gotBytes) != string(rawBytes) {
		t.Errorf("[]byte must NOT be scrubbed (BYTEA columns accept raw " +
			"bytes); the tracer's job is TEXT/VARCHAR/JSONB safety, not " +
			"to corrupt legitimate binary parameters")
	}
	if args[1] != 42 || args[2] != 3.14 || args[3] != true {
		t.Errorf("scalar args must pass through, got %v", args[:4])
	}
	if !args[4].(time.Time).Equal(now) {
		t.Errorf("time.Time must pass through unchanged")
	}
	if args[5] != nil {
		t.Errorf("nil must pass through")
	}
	// A nil *string must not crash, must remain nil-valued.
	if args[6] == nil {
		// args[6] holds a typed nil pointer; the interface itself is
		// non-nil but the pointer it wraps is nil. Both shapes are
		// acceptable as long as we didn't panic above.
	}
}

func TestScrubArgsInPlaceMutatesCallerSlice(t *testing.T) {
	// The whole point of this design is that the args slice header
	// is passed by value but the underlying array is shared, so
	// mutating args[i] in the tracer affects what pgx encodes.
	// Verify the semantic: a caller-owned slice sees the mutation.
	bad := "x\xb3y"
	callerArgs := []any{bad}
	tracerView := callerArgs // shares underlying array
	scrubArgsInPlace(tracerView)
	got, ok := callerArgs[0].(string)
	if !ok || !utf8.ValidString(got) {
		t.Errorf("caller's slice must see the scrubbed value "+
			"(shared underlying array); got %v", callerArgs[0])
	}
}

func TestTraceQueryStartScrubs(t *testing.T) {
	bad := "kernel\xb3file"
	args := []any{1, bad, 2}
	tr := utf8ScrubTracer{}
	data := pgx.TraceQueryStartData{SQL: "INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", Args: args}
	tr.TraceQueryStart(context.Background(), nil, data)
	got, ok := args[1].(string)
	if !ok || !utf8.ValidString(got) {
		t.Errorf("TraceQueryStart must mutate the Args slice in place; "+
			"got %v (%T)", args[1], args[1])
	}
	if args[0] != 1 || args[2] != 2 {
		t.Errorf("non-string args must pass through unchanged")
	}
}

func TestTraceBatchStartScrubsAllQueuedQueries(t *testing.T) {
	bad1 := "file\xb3a"
	bad2 := "file\xe1\x7a\x71b"
	good := "file_c"
	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO commits (filename) VALUES ($1)", bad1)
	batch.Queue("INSERT INTO commits (filename) VALUES ($1)", good)
	batch.Queue("INSERT INTO commits (filename) VALUES ($1)", bad2)
	tr := utf8ScrubTracer{}
	tr.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{Batch: batch})

	for i, qq := range batch.QueuedQueries {
		if len(qq.Arguments) != 1 {
			t.Fatalf("queued query %d: expected 1 arg, got %d", i, len(qq.Arguments))
		}
		s, ok := qq.Arguments[0].(string)
		if !ok {
			t.Errorf("queued query %d: expected string arg, got %T", i, qq.Arguments[0])
			continue
		}
		if !utf8.ValidString(s) {
			t.Errorf("queued query %d: scrub must have produced valid UTF-8, got %q (bytes %v)",
				i, s, []byte(s))
		}
	}
	// Confirm the good string survived without alteration.
	if s, _ := batch.QueuedQueries[1].Arguments[0].(string); s != good {
		t.Errorf("valid string must pass through batch tracer unchanged, got %q", s)
	}
}

func TestUTF8TracerEndMethodsAreNoOps(t *testing.T) {
	// These shouldn't panic and shouldn't do anything observable;
	// the contract is "implements the interface, doesn't crash".
	tr := utf8ScrubTracer{}
	tr.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{})
	tr.TraceBatchQuery(context.Background(), nil, pgx.TraceBatchQueryData{})
	tr.TraceBatchEnd(context.Background(), nil, pgx.TraceBatchEndData{})
}
