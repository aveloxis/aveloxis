// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// catalog_diff.go — the schema half of `aveloxis data-test`'s snapshot
// comparison (0.29.142): indexes (with their validity), constraints and the
// types of the columns both sides have, in the compared schemas. ContentDiff
// sees rows only; a migrate that skipped a gated unique index, dropped a
// foreign key or widened a column to a type that renders the same JSON passes
// a row comparison. Column defaults are left out: every migrate restamps the
// tool_version defaults with its own version (setToolVersionDefaults), so a
// default comparison would differ on every table by construction.

import (
	"context"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CatalogDiffEntry is one schema object that differs between the sides.
type CatalogDiffEntry struct {
	Kind   string `json:"kind"`   // "index", "constraint", "column-type"
	Table  string `json:"table"`  // schema.table the object belongs to
	Object string `json:"object"` // index or constraint name, or column name
	Change string `json:"change"` // "added" (new side only), "removed", "changed"
	// Released and New are the object's definitions on each side.
	Released string `json:"released,omitempty"`
	New      string `json:"new,omitempty"`
}

// RuleKind is the expectation kind that covers the entry: "index-added",
// "constraint-changed", "column-type-changed", ….
func (e CatalogDiffEntry) RuleKind() string { return e.Kind + "-" + e.Change }

type catalogObject struct{ kind, table, object, def string }

func catalogObjects(ctx context.Context, pool *pgxpool.Pool, schemas []string) (map[string]catalogObject, error) {
	out := map[string]catalogObject{}
	collect := func(kind, sql string) error {
		rows, err := pool.Query(ctx, sql, schemas)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var o catalogObject
			if err := rows.Scan(&o.table, &o.object, &o.def); err != nil {
				return err
			}
			o.kind = kind
			out[kind+"|"+o.table+"|"+o.object] = o
		}
		return rows.Err()
	}
	if err := collect("index", `
		SELECT n.nspname || '.' || t.relname, i.relname,
		       pg_get_indexdef(i.oid) || CASE WHEN x.indisvalid THEN '' ELSE ' [INVALID]' END
		FROM pg_index x
		JOIN pg_class i ON i.oid = x.indexrelid
		JOIN pg_class t ON t.oid = x.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = ANY($1)`); err != nil {
		return nil, err
	}
	if err := collect("constraint", `
		SELECT n.nspname || '.' || t.relname, c.conname, pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = ANY($1)`); err != nil {
		return nil, err
	}
	if err := collect("column-type", `
		SELECT n.nspname || '.' || t.relname, a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		JOIN pg_class t ON t.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = ANY($1) AND t.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped`); err != nil {
		return nil, err
	}
	return out, nil
}

// CatalogDiff compares the two sides' indexes, constraints and column types.
// A column one side lacks is ContentDiff's (OnlyNewColumns /
// OnlyReleasedColumns), so only columns on both sides are compared by type.
func CatalogDiff(ctx context.Context, released, newPool *pgxpool.Pool, schemas []string) ([]CatalogDiffEntry, error) {
	rel, err := catalogObjects(ctx, released, schemas)
	if err != nil {
		return nil, err
	}
	nw, err := catalogObjects(ctx, newPool, schemas)
	if err != nil {
		return nil, err
	}
	var out []CatalogDiffEntry
	for k, r := range rel {
		n, ok := nw[k]
		switch {
		case !ok && r.kind != "column-type":
			out = append(out, CatalogDiffEntry{Kind: r.kind, Table: r.table, Object: r.object, Change: "removed", Released: r.def})
		case ok && n.def != r.def:
			out = append(out, CatalogDiffEntry{Kind: r.kind, Table: r.table, Object: r.object, Change: "changed", Released: r.def, New: n.def})
		}
	}
	for k, n := range nw {
		if _, ok := rel[k]; !ok && n.kind != "column-type" {
			out = append(out, CatalogDiffEntry{Kind: n.kind, Table: n.table, Object: n.object, Change: "added", New: n.def})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Object < b.Object
	})
	return out, nil
}
