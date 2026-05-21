// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// utf8ScrubTracer is a pgx tracer that scrubs invalid UTF-8 bytes
// from string and *string parameters before they reach the wire.
// Registered on cfg.ConnConfig.Tracer in NewPostgresStore.
//
// # Why a tracer (v0.23.5)
//
// PostgreSQL rejects every TEXT, VARCHAR, CHAR, and JSONB parameter
// that contains invalid UTF-8 with SQLSTATE 22021 (`invalid byte
// sequence for encoding "UTF8"`). The rejection is per-statement: a
// 500-row batch dies on a single poisoned row, and aveloxis's
// retry-on-failure loop on UpsertCommit / UpsertContributorBatch
// just hits the same error again on the next attempt.
//
// Sources of invalid UTF-8 in production:
//
//   - git log output from old kernel-style repos with Latin-1
//     author names ("Lászlo Kövács" → 0xe1, 0xf6, 0xe1) or file
//     paths with high-bit codepoints. The 2026-05-21 log showed 72
//     `failed to upsert commit` warnings of this shape.
//   - GitHub profile fields (cntrb_company, cntrb_full_name) where
//     users occasionally paste binary content into their bio. The
//     2026-05-02 log showed 0x89 (PNG signature start) inside
//     contributor_affiliations INSERTs. v0.19.2 added a surgical
//     safeUTF8 to PopulateAffiliations; v0.23.5 generalizes.
//   - GitLab / GitHub API responses where free-text fields land,
//     particularly issue / PR / commit message bodies.
//
// # How the tracer works
//
// pgx fires TraceQueryStart immediately before encoding query
// arguments for the wire. The Args slice it receives shares its
// underlying array with the caller's `...args` variadic. Mutating
// `Args[i]` is therefore visible to pgx's encoder, which then sends
// the scrubbed value. Symmetric for batches: TraceBatchStart fires
// before SendBatch encodes the batch, and `Batch.QueuedQueries[*].
// Arguments` is exposed as a mutable []any.
//
// Net effect: a single registration on cfg.ConnConfig.Tracer
// protects every Exec, Query, QueryRow, SendBatch — across the
// pool, transactions, and prepared-statement modes — with zero
// call-site changes. ~437 call sites in internal/db/ are
// automatically covered.
//
// # What is NOT scrubbed
//
//   - []byte parameters. Those route to BYTEA columns which
//     accept any byte sequence. Scrubbing them would silently
//     corrupt legitimate binary data (CycloneDX SBOM blobs,
//     scancode JSON marshaled to bytes, etc.).
//   - Other primitive types (int, float, bool, time.Time). These
//     never carry invalid UTF-8 by construction.
//   - nil values. Pass-through.
//   - Outputs from queries. Postgres returns valid UTF-8 by
//     definition; scrubbing on the output path would be redundant.
//
// # Defensive layering
//
// v0.19.2's explicit safeUTF8 calls in PopulateAffiliations are NOT
// removed — they serve as belt-and-suspenders for the JSON
// scrubber (sanitizeJSONForJSONB) which works at the marshaling
// layer rather than the parameter layer. The two layers are
// independent and both correct.
type utf8ScrubTracer struct{}

func (utf8ScrubTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	scrubArgsInPlace(data.Args)
	return ctx
}

func (utf8ScrubTracer) TraceQueryEnd(_ context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
}

func (utf8ScrubTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if data.Batch == nil {
		return ctx
	}
	for _, qq := range data.Batch.QueuedQueries {
		if qq == nil {
			continue
		}
		scrubArgsInPlace(qq.Arguments)
	}
	return ctx
}

func (utf8ScrubTracer) TraceBatchQuery(_ context.Context, _ *pgx.Conn, _ pgx.TraceBatchQueryData) {
}

func (utf8ScrubTracer) TraceBatchEnd(_ context.Context, _ *pgx.Conn, _ pgx.TraceBatchEndData) {
}

// scrubArgsInPlace walks the args slice and replaces any string /
// *string element whose UTF-8 is invalid with a scrubbed version.
// Non-string types (including []byte) pass through unchanged.
// The slice header itself is reused; only the element slots are
// rewritten. Safe to call on a nil or empty slice.
func scrubArgsInPlace(args []any) {
	for i, a := range args {
		switch v := a.(type) {
		case string:
			if !utf8.ValidString(v) {
				args[i] = safeUTF8(v)
			}
		case *string:
			if v != nil && !utf8.ValidString(*v) {
				cleaned := safeUTF8(*v)
				args[i] = &cleaned
			}
		}
	}
}
