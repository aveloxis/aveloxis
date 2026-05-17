// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RowCountDiffRow is one row in a RowCountDiff report — the result
// of comparing one table's row count across two databases.
type RowCountDiffRow struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	ReleasedRows int64  `json:"released_rows"`
	NewRows      int64  `json:"new_rows"`
	// Status is one of:
	//   "PASS" — counts are equal
	//   "FLAG" — new DB has more rows than released (likely new
	//           coverage from the change being tested)
	//   "FAIL" — released DB has more rows than new (data loss /
	//           regression introduced by the change)
	Status string `json:"status"`
}

// RowCountDiffReport is the structured result of comparing two
// databases' row counts across a set of schemas.
type RowCountDiffReport struct {
	Schemas []string          `json:"schemas"`
	Rows    []RowCountDiffRow `json:"rows"`
}

// HasFailures returns true if any row has FAIL status. Used by the
// `aveloxis data-test` harness to decide its exit code.
func (r *RowCountDiffReport) HasFailures() bool {
	for _, row := range r.Rows {
		if row.Status == "FAIL" {
			return true
		}
	}
	return false
}

// classifyRowCountDiff is the pure-logic classifier extracted for
// testability. PASS: counts match. FLAG: new has more rows (new
// coverage). FAIL: released has more rows (data loss). The
// asymmetry matches the operator's stated v0.22.8 goal — flagging
// regressions specifically, not just any difference.
func classifyRowCountDiff(released, newCount int64) string {
	switch {
	case released == newCount:
		return "PASS"
	case newCount > released:
		return "FLAG"
	default:
		return "FAIL"
	}
}

// RowCountDiff queries information_schema.tables on both databases
// to enumerate base tables in the given schemas, then runs
// SELECT COUNT(*) on each table in each DB and classifies the
// result. Used by `aveloxis data-test` (v0.22.8) to detect
// row-loss regressions after schema-changing releases.
//
// Tables in only one DB (e.g., a new table added by the release
// being tested) appear with zero count for the missing side, which
// resolves to FLAG (or FAIL if the released side has the table and
// new doesn't — schema regression).
//
// Both DBs are queried sequentially; for the typical
// `aveloxis data-test` use case the diff is fast (a few seconds)
// because base-table counts are an indexed PG primitive. The
// COUNT(*) over multi-million-row tables (commits, messages,
// pull_request_commits) can be slower; if this becomes a bottleneck
// a future flag could approximate via pg_class.reltuples.
func RowCountDiff(ctx context.Context, released, newPool *pgxpool.Pool, schemas []string) (*RowCountDiffReport, error) {
	if len(schemas) == 0 {
		// aveloxis_data is the primary data schema; aveloxis_ops
		// holds queue / staging / API key state. Both are touched
		// by a single-repo collection.
		schemas = []string{"aveloxis_data", "aveloxis_ops"}
	}

	releasedCounts, err := tableRowCounts(ctx, released, schemas)
	if err != nil {
		return nil, fmt.Errorf("released DB row counts: %w", err)
	}
	newCounts, err := tableRowCounts(ctx, newPool, schemas)
	if err != nil {
		return nil, fmt.Errorf("new DB row counts: %w", err)
	}

	// Union of tables across both DBs.
	tables := map[string]struct{}{}
	for k := range releasedCounts {
		tables[k] = struct{}{}
	}
	for k := range newCounts {
		tables[k] = struct{}{}
	}

	report := &RowCountDiffReport{Schemas: schemas}
	for fq := range tables {
		// fq is "schema.table"
		var schema, table string
		for i := range len(fq) {
			if fq[i] == '.' {
				schema = fq[:i]
				table = fq[i+1:]
				break
			}
		}
		row := RowCountDiffRow{
			Schema:       schema,
			Table:        table,
			ReleasedRows: releasedCounts[fq],
			NewRows:      newCounts[fq],
		}
		row.Status = classifyRowCountDiff(row.ReleasedRows, row.NewRows)
		report.Rows = append(report.Rows, row)
	}
	// Deterministic ordering for human-readable reports and
	// reproducible test fixtures: FAIL first, then FLAG, then PASS;
	// then by schema, then by table.
	sort.Slice(report.Rows, func(i, j int) bool {
		statusOrder := func(s string) int {
			switch s {
			case "FAIL":
				return 0
			case "FLAG":
				return 1
			default:
				return 2
			}
		}
		si, sj := statusOrder(report.Rows[i].Status), statusOrder(report.Rows[j].Status)
		if si != sj {
			return si < sj
		}
		if report.Rows[i].Schema != report.Rows[j].Schema {
			return report.Rows[i].Schema < report.Rows[j].Schema
		}
		return report.Rows[i].Table < report.Rows[j].Table
	})
	return report, nil
}

// tableRowCounts enumerates base tables in the given schemas and
// returns a map of "schema.table" → row count. Excludes views and
// materialized views — derived data isn't a meaningful regression
// signal (a missing matview row would also be missing in the
// released DB if the same input data was collected).
func tableRowCounts(ctx context.Context, pool *pgxpool.Pool, schemas []string) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		  AND table_schema = ANY($1)
		ORDER BY table_schema, table_name
	`, schemas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type tableRef struct{ schema, name string }
	var tables []tableRef
	for rows.Next() {
		var t tableRef
		if err := rows.Scan(&t.schema, &t.name); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	counts := map[string]int64{}
	for _, t := range tables {
		var n int64
		// schema/table names come from information_schema and are
		// safe to interpolate (they're not user-controlled). Using
		// %q would double-quote them, which is the safe identifier
		// quoting for Postgres.
		q := fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q`, t.schema, t.name)
		if err := pool.QueryRow(ctx, q).Scan(&n); err != nil {
			return nil, fmt.Errorf("count %s.%s: %w", t.schema, t.name, err)
		}
		counts[t.schema+"."+t.name] = n
	}
	return counts, nil
}
