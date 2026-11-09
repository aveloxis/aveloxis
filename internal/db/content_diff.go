// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// content_diff.go — the row-content comparison of `aveloxis data-test`'s
// snapshot mode (0.29.142, the main-vs-branch evaluation): two databases
// cloned from ONE template, each migrated by a different binary, compared row
// by row. RowCountDiff and ColumnFillDiff (beside this file) see counts and
// fill rates; a value a migration rewrote in place is invisible to both.
//
// Each row is compared as `to_jsonb(t)` minus the columns one side lacks and
// the caller's ignored columns: the JSON form does not depend on column order
// (a table that gained a column would mismatch every row as row::text), and
// every session pins TimeZone, DateStyle and extra_float_digits so both sides
// render timestamps and floats alike. Hashes are computed inside each
// database — one aggregate per table, then one per primary-key bucket for a
// table that differs — so only the differing buckets' rows cross the wire.
// A table without a primary key is compared by its row count and the sum of
// its row hashes (a multiset comparison: it says the table differs, not which
// rows). The comparison is only meaningful between clones of one template:
// two collections assign different surrogate keys (shadow-diff compares on
// natural keys for that case).

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ContentDiffOptions bounds a content diff.
type ContentDiffOptions struct {
	// Schemas are compared in full (every base table).
	Schemas []string
	// Ignore names columns left out of the comparison: "schema.table" →
	// column names, or "*" → columns ignored in every table. One-side
	// columns are always left out (and reported).
	Ignore map[string][]string
	// Buckets is the number of primary-key buckets a differing table is
	// split into before its rows are read; 0 → 1024.
	Buckets int
	// MaxRows caps the differing rows read in detail per table; a table
	// with more is reported as truncated. 0 → 100,000.
	MaxRows int
}

// ContentDiffRow is one differing row of a keyed table.
type ContentDiffRow struct {
	Key  string `json:"key"`  // the primary key as a JSON object
	Kind string `json:"kind"` // "added" (new side only), "removed" (released only), "changed"
	// Columns are the compared columns whose values differ (changed rows).
	Columns []string `json:"columns,omitempty"`
	// Released and New are the compared row as JSON on each side (absent
	// on the side the row is missing from).
	Released json.RawMessage `json:"released,omitempty"`
	New      json.RawMessage `json:"new,omitempty"`
}

// ContentDiffTable is one table's result.
type ContentDiffTable struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	// Status: "same", "differs", "only-released", "only-new".
	Status string `json:"status"`
	Keyed  bool   `json:"keyed"`
	// Key is the primary key's columns, in index order (keyed tables).
	Key          []string `json:"key,omitempty"`
	ReleasedRows int64    `json:"released_rows"`
	NewRows      int64    `json:"new_rows"`
	// OnlyReleasedColumns / OnlyNewColumns are left out of the comparison.
	OnlyReleasedColumns []string         `json:"only_released_columns,omitempty"`
	OnlyNewColumns      []string         `json:"only_new_columns,omitempty"`
	Rows                []ContentDiffRow `json:"rows,omitempty"`
	// Truncated: more differing rows than MaxRows; Rows holds none.
	Truncated bool `json:"truncated,omitempty"`
	// DifferingRows counts the differing rows found (keyed tables).
	DifferingRows int64 `json:"differing_rows"`
}

// Name is the table's schema-qualified name.
func (t ContentDiffTable) Name() string { return t.Schema + "." + t.Table }

// ContentDiffReport is the whole comparison.
type ContentDiffReport struct {
	Schemas []string           `json:"schemas"`
	Tables  []ContentDiffTable `json:"tables"`
}

// contentSessionSQL pins what changes a value's text form between sessions.
const contentSessionSQL = `SET LOCAL TimeZone = 'UTC'; SET LOCAL DateStyle = 'ISO, YMD'; SET LOCAL extra_float_digits = 3; SET LOCAL IntervalStyle = 'iso_8601'`

// ContentDiff compares every base table of opts.Schemas row by row.
func ContentDiff(ctx context.Context, released, newPool *pgxpool.Pool, opts ContentDiffOptions) (*ContentDiffReport, error) {
	if opts.Buckets <= 0 {
		opts.Buckets = 1024
	}
	if opts.MaxRows <= 0 {
		opts.MaxRows = 100000
	}
	relTables, err := contentTables(ctx, released, opts.Schemas)
	if err != nil {
		return nil, fmt.Errorf("released tables: %w", err)
	}
	newTables, err := contentTables(ctx, newPool, opts.Schemas)
	if err != nil {
		return nil, fmt.Errorf("new tables: %w", err)
	}
	names := map[string]bool{}
	for n := range relTables {
		names[n] = true
	}
	for n := range newTables {
		names[n] = true
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)

	report := &ContentDiffReport{Schemas: opts.Schemas}
	for _, name := range sorted {
		schema, table, _ := strings.Cut(name, ".")
		res := ContentDiffTable{Schema: schema, Table: table}
		rel, inRel := relTables[name]
		nw, inNew := newTables[name]
		switch {
		case !inNew:
			res.Status = "only-released"
			if res.ReleasedRows, err = contentCount(ctx, released, schema, table); err != nil {
				return nil, err
			}
		case !inRel:
			res.Status = "only-new"
			if res.NewRows, err = contentCount(ctx, newPool, schema, table); err != nil {
				return nil, err
			}
		default:
			if err := diffOneTable(ctx, released, newPool, rel, nw, opts, &res); err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
		}
		report.Tables = append(report.Tables, res)
	}
	return report, nil
}

// contentTable is one side's view of a table: its columns and primary key.
type contentTable struct {
	schema, table string
	columns       map[string]bool
	pk            []string
}

func contentTables(ctx context.Context, pool *pgxpool.Pool, schemas []string) (map[string]*contentTable, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.table_schema, c.table_name, c.column_name
		FROM information_schema.columns c
		JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name
		WHERE t.table_type = 'BASE TABLE' AND c.table_schema = ANY($1)`, schemas)
	if err != nil {
		return nil, err
	}
	out := map[string]*contentTable{}
	for rows.Next() {
		var s, tb, col string
		if err := rows.Scan(&s, &tb, &col); err != nil {
			rows.Close()
			return nil, err
		}
		name := s + "." + tb
		if out[name] == nil {
			out[name] = &contentTable{schema: s, table: tb, columns: map[string]bool{}}
		}
		out[name].columns[col] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, t := range out {
		pkRows, err := pool.Query(ctx, `
			SELECT a.attname
			FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = to_regclass($1) AND i.indisprimary
			ORDER BY array_position(i.indkey, a.attnum)`, pgx.Identifier{t.schema, t.table}.Sanitize())
		if err != nil {
			return nil, err
		}
		for pkRows.Next() {
			var col string
			if err := pkRows.Scan(&col); err != nil {
				pkRows.Close()
				return nil, err
			}
			t.pk = append(t.pk, col)
		}
		pkRows.Close()
		if err := pkRows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func contentCount(ctx context.Context, pool *pgxpool.Pool, schema, table string) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM `+pgx.Identifier{schema, table}.Sanitize()).Scan(&n)
	return n, err
}

// contentQuery runs fn in a read-only transaction with the pinned session.
func contentQuery(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, contentSessionSQL); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// sqlLiteral quotes a catalog name as a string literal.
func sqlLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func diffOneTable(ctx context.Context, released, newPool *pgxpool.Pool, rel, nw *contentTable, opts ContentDiffOptions, res *ContentDiffTable) error {
	ignored := map[string]bool{}
	for _, c := range opts.Ignore["*"] {
		ignored[c] = true
	}
	for _, c := range opts.Ignore[rel.schema+"."+rel.table] {
		ignored[c] = true
	}
	// Columns left out on each side: the other side's missing ones and the
	// ignored ones. The key must survive on both sides. Never nil: a nil
	// slice is sent as NULL, `to_jsonb(t) - NULL` is NULL, and every row
	// would hash alike — two different tables reported the same.
	dropRel, dropNew := []string{}, []string{}
	for c := range rel.columns {
		if !nw.columns[c] {
			res.OnlyReleasedColumns = append(res.OnlyReleasedColumns, c)
			dropRel = append(dropRel, c)
		} else if ignored[c] {
			dropRel = append(dropRel, c)
		}
	}
	for c := range nw.columns {
		if !rel.columns[c] {
			res.OnlyNewColumns = append(res.OnlyNewColumns, c)
			dropNew = append(dropNew, c)
		} else if ignored[c] {
			dropNew = append(dropNew, c)
		}
	}
	sort.Strings(res.OnlyReleasedColumns)
	sort.Strings(res.OnlyNewColumns)

	tbl := pgx.Identifier{rel.schema, rel.table}.Sanitize()
	rowExpr := `(to_jsonb(t) - $1::text[])`
	totals := func(pool *pgxpool.Pool, drop []string) (int64, string, error) {
		var n int64
		var sum string
		err := contentQuery(ctx, pool, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT COUNT(*), COALESCE(SUM(hashtextextended(`+rowExpr+`::text, 0)), 0)::text FROM `+tbl+` t`, drop).Scan(&n, &sum)
		})
		return n, sum, err
	}
	relN, relSum, err := totals(released, dropRel)
	if err != nil {
		return err
	}
	newN, newSum, err := totals(newPool, dropNew)
	if err != nil {
		return err
	}
	res.ReleasedRows, res.NewRows = relN, newN
	if relN == newN && relSum == newSum {
		res.Status = "same"
		return nil
	}
	res.Status = "differs"
	keyed := len(rel.pk) > 0 && strings.Join(rel.pk, ",") == strings.Join(nw.pk, ",")
	for _, k := range rel.pk {
		if ignored[k] {
			keyed = false
		}
	}
	if !keyed {
		return nil // a multiset comparison says that it differs, not where
	}
	res.Keyed = true
	res.Key = append([]string(nil), rel.pk...)

	keyExpr := contentKeyExpr(rel.pk)
	bucketExpr := `((hashtextextended(` + keyExpr + `::text, 7) % $2::bigint + $2::bigint) % $2::bigint)::int`

	buckets := func(pool *pgxpool.Pool, drop []string) (map[int]string, error) {
		out := map[int]string{}
		err := contentQuery(ctx, pool, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT `+bucketExpr+`, COUNT(*)::text || ':' || SUM(hashtextextended(`+rowExpr+`::text, 0))::text FROM `+tbl+` t GROUP BY 1`, drop, opts.Buckets)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var b int
				var v string
				if err := rows.Scan(&b, &v); err != nil {
					return err
				}
				out[b] = v
			}
			return rows.Err()
		})
		return out, err
	}
	relB, err := buckets(released, dropRel)
	if err != nil {
		return err
	}
	newB, err := buckets(newPool, dropNew)
	if err != nil {
		return err
	}
	var differing []int
	for b := 0; b < opts.Buckets; b++ {
		if relB[b] != newB[b] {
			differing = append(differing, b)
		}
	}

	rowHashes := func(pool *pgxpool.Pool, drop []string) (map[string]int64, error) {
		out := map[string]int64{}
		err := contentQuery(ctx, pool, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT `+keyExpr+`::text, hashtextextended(`+rowExpr+`::text, 0) FROM `+tbl+` t WHERE `+bucketExpr+` = ANY($3::int[])`, drop, opts.Buckets, differing)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var k string
				var h int64
				if err := rows.Scan(&k, &h); err != nil {
					return err
				}
				out[k] = h
			}
			return rows.Err()
		})
		return out, err
	}
	relH, err := rowHashes(released, dropRel)
	if err != nil {
		return err
	}
	newH, err := rowHashes(newPool, dropNew)
	if err != nil {
		return err
	}
	kinds := map[string]string{}
	for k, h := range relH {
		if nh, ok := newH[k]; !ok {
			kinds[k] = "removed"
		} else if nh != h {
			kinds[k] = "changed"
		}
	}
	for k := range newH {
		if _, ok := relH[k]; !ok {
			kinds[k] = "added"
		}
	}
	res.DifferingRows = int64(len(kinds))
	if len(kinds) > opts.MaxRows {
		res.Truncated = true
		return nil
	}
	keys := make([]string, 0, len(kinds))
	for k := range kinds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	full := func(pool *pgxpool.Pool, drop []string) (map[string]json.RawMessage, error) {
		out := map[string]json.RawMessage{}
		err := contentQuery(ctx, pool, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT `+keyExpr+`::text, `+rowExpr+`::text FROM `+tbl+` t WHERE `+keyExpr+` = ANY($2::jsonb[])`, drop, keys)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var k, v string
				if err := rows.Scan(&k, &v); err != nil {
					return err
				}
				out[k] = json.RawMessage(v)
			}
			return rows.Err()
		})
		return out, err
	}
	relRows, err := full(released, dropRel)
	if err != nil {
		return err
	}
	newRows, err := full(newPool, dropNew)
	if err != nil {
		return err
	}
	for _, k := range keys {
		row := ContentDiffRow{Key: k, Kind: kinds[k], Released: relRows[k], New: newRows[k]}
		if row.Kind == "changed" {
			cols, err := changedColumns(relRows[k], newRows[k])
			if err != nil {
				return fmt.Errorf("row %s: %w", k, err)
			}
			row.Columns = cols
		}
		res.Rows = append(res.Rows, row)
	}
	return nil
}

// contentKeyExpr is the primary key of alias t as a JSON object, the form a
// ContentDiffRow's Key carries.
func contentKeyExpr(pk []string) string {
	pairs := make([]string, 0, len(pk))
	for _, k := range pk {
		pairs = append(pairs, sqlLiteral(k)+", t."+pgx.Identifier{k}.Sanitize())
	}
	return `jsonb_build_object(` + strings.Join(pairs, ", ") + `)`
}

// changedColumns lists the keys whose JSON values differ between two rows
// (jsonb's text form is canonical, so equal values have equal text).
func changedColumns(a, b json.RawMessage) ([]string, error) {
	var ma, mb map[string]json.RawMessage
	if err := json.Unmarshal(a, &ma); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &mb); err != nil {
		return nil, err
	}
	var out []string
	for k, va := range ma {
		if vb, ok := mb[k]; !ok || string(va) != string(vb) {
			out = append(out, k)
		}
	}
	for k := range mb {
		if _, ok := ma[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}
