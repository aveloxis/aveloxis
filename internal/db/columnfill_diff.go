// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// columnfill_diff.go — per-column fill-rate comparison for
// `aveloxis data-test` (v0.26.1).
//
// The row-count diff (rowcount_diff.go) catches missing ROWS; it is
// blind to a column the new binary stopped populating. The canonical
// example: issue_labels.platform_label_id is 0 on every row under the
// GraphQL path — row counts identical, data silently gone. This diff
// compares, per column, how many rows carry a MEANINGFUL value in each
// database.
//
// "Populated" is type-aware because this schema's idioms for "not
// filled in" are TEXT DEFAULT '' and INTEGER DEFAULT 0, not NULL:
//   - text-ish columns:    IS NOT NULL AND <> ''
//   - numeric columns:     IS NOT NULL AND <> 0
//   - everything else:     IS NOT NULL (booleans exempt from the
//     zero-check — false is a legitimate value; timestamps, uuids,
//     jsonb likewise)
//
// Classification is deliberately asymmetric, mirroring the row diff's
// philosophy of flagging REGRESSIONS specifically:
//   - FAIL: populated in released, COMPLETELY unpopulated in new — a
//     column went dark (dropped mapping, renamed JSON tag, a code path
//     that no longer fills the field).
//   - FLAG: any other difference. The two collections run ~30 minutes
//     apart against a live repo, so small drift (an edited profile, a
//     late comment) is expected — a hard FAIL on any inequality would
//     make the harness flaky. The report shows both numbers for human
//     review.
//   - PASS: equal populated counts (including both-zero).

package db

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ColumnFillDiffRow is one column's fill comparison across the two
// databases.
type ColumnFillDiffRow struct {
	Schema            string `json:"schema"`
	Table             string `json:"table"`
	Column            string `json:"column"`
	ReleasedPopulated int64  `json:"released_populated"`
	NewPopulated      int64  `json:"new_populated"`
	Status            string `json:"status"` // PASS | FLAG | FAIL (see file header)
}

// ColumnFillDiffReport is the structured result of the per-column
// comparison. Only non-PASS rows are retained — with ~2,500 columns
// across the schemas, an all-rows report would bury the signal.
type ColumnFillDiffReport struct {
	Schemas        []string            `json:"schemas"`
	ColumnsChecked int                 `json:"columns_checked"`
	Rows           []ColumnFillDiffRow `json:"rows"` // FAIL + FLAG only
}

// HasFailures reports whether any column went dark.
func (r *ColumnFillDiffReport) HasFailures() bool {
	for _, row := range r.Rows {
		if row.Status == "FAIL" {
			return true
		}
	}
	return false
}

// classifyColumnFill — see the file header for the FAIL/FLAG/PASS
// contract and its rationale.
func classifyColumnFill(released, newPopulated int64) string {
	switch {
	case released == newPopulated:
		return "PASS"
	case released > 0 && newPopulated == 0:
		return "FAIL"
	default:
		return "FLAG"
	}
}

// populatedPredicate builds the type-aware "carries a meaningful
// value" SQL predicate for one (already-quoted) column identifier.
func populatedPredicate(quotedCol, dataType string) string {
	switch dataType {
	case "text", "character varying", "character":
		return fmt.Sprintf("%s IS NOT NULL AND %s <> ''", quotedCol, quotedCol)
	case "smallint", "integer", "bigint", "numeric", "real", "double precision":
		return fmt.Sprintf("%s IS NOT NULL AND %s <> 0", quotedCol, quotedCol)
	default:
		// boolean, timestamps, uuid, jsonb, arrays, ...
		return fmt.Sprintf("%s IS NOT NULL", quotedCol)
	}
}

type columnRef struct {
	schema, table, column, dataType string
}

// ColumnFillDiff enumerates every column of every base table in the
// given schemas (default: the same set as RowCountDiff) and compares
// per-column populated counts between the two databases. Column
// enumeration comes from the RELEASED database — a column that exists
// only in the new schema has no baseline to regress from (it surfaces
// as FLAG via the count comparison when present in both).
func ColumnFillDiff(ctx context.Context, released, newPool *pgxpool.Pool, schemas []string) (*ColumnFillDiffReport, error) {
	if len(schemas) == 0 {
		schemas = []string{"aveloxis_data", "aveloxis_ops", "aveloxis_scan"}
	}

	cols, err := enumerateColumns(ctx, released, schemas)
	if err != nil {
		return nil, fmt.Errorf("enumerate released columns: %w", err)
	}

	releasedFill, err := populatedCounts(ctx, released, cols)
	if err != nil {
		return nil, fmt.Errorf("released DB fill counts: %w", err)
	}
	newFill, err := populatedCounts(ctx, newPool, cols)
	if err != nil {
		return nil, fmt.Errorf("new DB fill counts: %w", err)
	}

	report := &ColumnFillDiffReport{Schemas: schemas, ColumnsChecked: len(cols)}
	for _, c := range cols {
		key := c.schema + "." + c.table + "." + c.column
		row := ColumnFillDiffRow{
			Schema:            c.schema,
			Table:             c.table,
			Column:            c.column,
			ReleasedPopulated: releasedFill[key],
			NewPopulated:      newFill[key],
		}
		row.Status = classifyColumnFill(row.ReleasedPopulated, row.NewPopulated)
		if row.Status != "PASS" {
			report.Rows = append(report.Rows, row)
		}
	}
	sort.Slice(report.Rows, func(i, j int) bool {
		order := func(s string) int {
			if s == "FAIL" {
				return 0
			}
			return 1
		}
		oi, oj := order(report.Rows[i].Status), order(report.Rows[j].Status)
		if oi != oj {
			return oi < oj
		}
		ki := report.Rows[i].Schema + "." + report.Rows[i].Table + "." + report.Rows[i].Column
		kj := report.Rows[j].Schema + "." + report.Rows[j].Table + "." + report.Rows[j].Column
		return ki < kj
	})
	return report, nil
}

func enumerateColumns(ctx context.Context, pool *pgxpool.Pool, schemas []string) ([]columnRef, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.table_schema, c.table_name, c.column_name, c.data_type
		FROM information_schema.columns c
		JOIN information_schema.tables t
		  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
		WHERE t.table_type = 'BASE TABLE'
		  AND c.table_schema = ANY($1)
		ORDER BY c.table_schema, c.table_name, c.ordinal_position
	`, schemas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []columnRef
	for rows.Next() {
		var c columnRef
		if err := rows.Scan(&c.schema, &c.table, &c.column, &c.dataType); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// populatedCounts computes the populated count for every column, ONE
// single-scan query per table (all of a table's columns aggregated in
// one pass).
func populatedCounts(ctx context.Context, pool *pgxpool.Pool, cols []columnRef) (map[string]int64, error) {
	byTable := map[string][]columnRef{}
	for _, c := range cols {
		key := c.schema + "." + c.table
		byTable[key] = append(byTable[key], c)
	}

	out := map[string]int64{}
	for _, tableCols := range byTable {
		schema, table := tableCols[0].schema, tableCols[0].table
		exprs := make([]string, len(tableCols))
		for i, c := range tableCols {
			quoted := fmt.Sprintf("%q", c.column)
			exprs[i] = fmt.Sprintf("COUNT(*) FILTER (WHERE %s)", populatedPredicate(quoted, c.dataType))
		}
		// Identifiers come from information_schema, quoted with %q —
		// same safety posture as tableRowCounts.
		q := fmt.Sprintf(`SELECT %s FROM %q.%q`, strings.Join(exprs, ", "), schema, table)
		row := pool.QueryRow(ctx, q)
		dests := make([]any, len(tableCols))
		vals := make([]int64, len(tableCols))
		for i := range vals {
			dests[i] = &vals[i]
		}
		if err := row.Scan(dests...); err != nil {
			return nil, fmt.Errorf("fill counts for %s.%s: %w", schema, table, err)
		}
		for i, c := range tableCols {
			out[c.schema+"."+c.table+"."+c.column] = vals[i]
		}
	}
	return out, nil
}
