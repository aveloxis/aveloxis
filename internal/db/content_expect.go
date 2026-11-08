// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// content_expect.go — what a release is ALLOWED and REQUIRED to change, held
// against a ContentDiffReport (0.29.142, the main-vs-branch evaluation). A
// diff no rule covers is a failure, and so is a must-differ rule nothing
// matched: an expectation that never happens means the release did not do
// what it claims (a migrate step that silently skipped its work passes a
// "may differ" list). One rule file per release under
// cmd/aveloxis/testdata/expect-<version>.json.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ContentExpectations is a release's rule file.
type ContentExpectations struct {
	// Ignore is passed to ContentDiffOptions.Ignore ("*" or "schema.table").
	Ignore map[string][]string `json:"ignore"`
	Rules  []ContentRule       `json:"rules"`
	// VerifyOK names data-verify checks that must be OK on the local side
	// whatever the released side says: a release that repairs a state the
	// released side FAILs by design (trimmed earlier-era buckets) would
	// otherwise hide an incomplete repair behind the baseline.
	VerifyOK []string `json:"verify_ok,omitempty"`
}

// ContentRule allows (may-differ) or requires (must-differ) one kind of
// difference in one table.
type ContentRule struct {
	Name  string `json:"name"`
	Table string `json:"table"` // schema.table
	// Mode is "may-differ" or "must-differ".
	Mode string `json:"mode"`
	// Kinds the rule covers: "added", "removed", "changed" (rows of a keyed
	// table or a whole one-side table), "column-added", "column-removed"
	// (a column one side lacks), "unkeyed" (a table without a primary key
	// that differs; only its count and hash sum are known), and the schema
	// objects CatalogDiff reports: "index-added", "index-removed",
	// "index-changed", "constraint-added", "constraint-removed",
	// "constraint-changed", "column-type-changed". Empty = added, removed and
	// changed.
	Kinds []string `json:"kinds,omitempty"`
	// Objects limits a schema-object rule to these index, constraint or
	// column names (empty = any object of the table). A must-differ rule
	// that names objects is met only when EVERY one of them appears.
	Objects []string `json:"objects,omitempty"`
	// Columns limits a "changed" rule to these columns (empty = any), and
	// names the columns of a column-added/removed rule.
	Columns []string `json:"columns,omitempty"`
	// Where is a SQL predicate over alias t, evaluated on the side the row
	// exists on (new for added and changed rows, released for removed).
	Where string `json:"where,omitempty"`
	// RaiseOnly columns must not decrease (changed rows; numeric values).
	RaiseOnly []string `json:"raise_only,omitempty"`
	// NullOnly: a column-added rule's columns must be NULL in every row.
	NullOnly bool   `json:"null_only,omitempty"`
	Reason   string `json:"reason"`
}

// ContentViolation is one difference no rule allows, or a rule that failed.
type ContentViolation struct {
	Table  string `json:"table"`
	Key    string `json:"key,omitempty"`
	Detail string `json:"detail"`
}

// LoadContentExpectations parses a rule file and checks its rules.
func LoadContentExpectations(raw []byte) (*ContentExpectations, error) {
	var e ContentExpectations
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, fmt.Errorf("expectations: %w", err)
	}
	known := []string{"added", "removed", "changed", "column-added", "column-removed", "unkeyed",
		"index-added", "index-removed", "index-changed", "constraint-added", "constraint-removed", "constraint-changed",
		"column-type-changed"}
	for i, r := range e.Rules {
		if r.Table == "" || !strings.Contains(r.Table, ".") {
			return nil, fmt.Errorf("expectations rule %d (%s): table must be schema.table", i, r.Name)
		}
		if r.Mode != "may-differ" && r.Mode != "must-differ" {
			return nil, fmt.Errorf("expectations rule %d (%s): mode must be may-differ or must-differ, got %q", i, r.Name, r.Mode)
		}
		for _, k := range r.Kinds {
			if !slices.Contains(known, k) {
				return nil, fmt.Errorf("expectations rule %d (%s): unknown kind %q", i, r.Name, k)
			}
		}
		// Columns limits a "changed" rule but NAMES a column-added/removed
		// rule's columns (each required by must-differ): one rule cannot be
		// both.
		if (r.covers("column-added") || r.covers("column-removed")) && len(r.Kinds) > 0 &&
			slices.ContainsFunc(r.Kinds, func(k string) bool { return k != "column-added" && k != "column-removed" }) {
			return nil, fmt.Errorf("expectations rule %d (%s): a column-added/removed rule covers no other kind (its columns are names, not a limit); split it", i, r.Name)
		}
		if r.Reason == "" {
			return nil, fmt.Errorf("expectations rule %d (%s): every rule states its reason", i, r.Name)
		}
	}
	return &e, nil
}

func (r ContentRule) covers(kind string) bool {
	if len(r.Kinds) == 0 {
		return kind == "added" || kind == "removed" || kind == "changed"
	}
	return slices.Contains(r.Kinds, kind)
}

// CheckContent holds the row report and the schema-object differences
// against the rules. released and newPool evaluate the rules' Where
// predicates and NullOnly checks. A data problem a rule cannot judge (a
// raise-only column that is not numeric) is a violation, never an error: an
// error here would end a run that took an hour of migrates without its
// report.
func (e *ContentExpectations) CheckContent(ctx context.Context, released, newPool *pgxpool.Pool, report *ContentDiffReport, catalog []CatalogDiffEntry) ([]ContentViolation, error) {
	var out []ContentViolation
	used := make([]bool, len(e.Rules))
	seenObjects := make([]map[string]bool, len(e.Rules))
	for i := range seenObjects {
		seenObjects[i] = map[string]bool{}
	}
	for _, c := range catalog {
		hit := false
		// An index built INVALID (an interrupted or failed concurrent build)
		// serves nothing: no rule allows it.
		invalid := c.Kind == "index" && strings.HasSuffix(c.New, " [INVALID]")
		for i, r := range e.Rules {
			if !invalid && r.Table == c.Table && r.covers(c.RuleKind()) && (len(r.Objects) == 0 || slices.Contains(r.Objects, c.Object)) {
				used[i], hit = true, true
				seenObjects[i][c.Object] = true
			}
		}
		if invalid {
			out = append(out, ContentViolation{Table: c.Table, Detail: fmt.Sprintf("index %s is INVALID on the new side (%s)", c.Object, c.Change)})
			continue
		}
		if !hit {
			detail := fmt.Sprintf("%s %s %s and no rule allows it", c.Kind, c.Object, c.Change)
			if c.Change == "changed" {
				detail += fmt.Sprintf(" (released: %s; new: %s)", c.Released, c.New)
			}
			out = append(out, ContentViolation{Table: c.Table, Detail: detail})
		}
	}
	rulesFor := func(table string) []int {
		var idx []int
		for i, r := range e.Rules {
			if r.Table == table {
				idx = append(idx, i)
			}
		}
		return idx
	}
	for _, t := range report.Tables {
		name := t.Name()
		idx := rulesFor(name)
		switch t.Status {
		case "only-new", "only-released":
			kind := map[string]string{"only-new": "added", "only-released": "removed"}[t.Status]
			rows := map[string]int64{"only-new": t.NewRows, "only-released": t.ReleasedRows}[t.Status]
			hit := false
			for _, i := range idx {
				if e.Rules[i].covers(kind) && e.Rules[i].Where == "" {
					hit = true
					// The table's existence is allowed either way; a
					// must-differ rule is met only by rows (a created table
					// a backfill never filled did not do the work).
					if rows > 0 {
						used[i] = true
					}
				}
			}
			if !hit {
				out = append(out, ContentViolation{Table: name, Detail: fmt.Sprintf("the table exists on one side only (%s) and no rule allows it", t.Status)})
			}
			continue
		}
		for _, cols := range []struct {
			kind string
			list []string
			pool *pgxpool.Pool
		}{{"column-added", t.OnlyNewColumns, newPool}, {"column-removed", t.OnlyReleasedColumns, released}} {
			for _, col := range cols.list {
				hit := false
				for _, i := range idx {
					r := e.Rules[i]
					if !r.covers(cols.kind) || !slices.Contains(r.Columns, col) {
						continue
					}
					seenObjects[i][col] = true
					if r.NullOnly {
						var n int64
						if err := cols.pool.QueryRow(ctx, `SELECT COUNT(*) FROM `+pgx.Identifier{t.Schema, t.Table}.Sanitize()+` WHERE `+pgx.Identifier{col}.Sanitize()+` IS NOT NULL`).Scan(&n); err != nil {
							return nil, fmt.Errorf("%s.%s null check: %w", name, col, err)
						}
						if n > 0 {
							out = append(out, ContentViolation{Table: name, Detail: fmt.Sprintf("rule %q: column %s must be NULL everywhere; %d rows are not", r.Name, col, n)})
						}
					}
					used[i], hit = true, true
				}
				if !hit {
					out = append(out, ContentViolation{Table: name, Detail: fmt.Sprintf("column %s exists on one side only (%s) and no rule allows it", col, cols.kind)})
				}
			}
		}
		if t.Status != "differs" {
			continue
		}
		if !t.Keyed {
			hit := false
			for _, i := range idx {
				if e.Rules[i].covers("unkeyed") {
					used[i], hit = true, true
				}
			}
			if !hit {
				out = append(out, ContentViolation{Table: name, Detail: fmt.Sprintf("an unkeyed table differs (released %d rows, new %d) and no rule allows it", t.ReleasedRows, t.NewRows)})
			}
			continue
		}
		if t.Truncated {
			out = append(out, ContentViolation{Table: name, Detail: fmt.Sprintf("%d differing rows, more than the detail cap: not verified", t.DifferingRows)})
			continue
		}
		passing, err := e.wherePassing(ctx, released, newPool, t, idx)
		if err != nil {
			return nil, err
		}
		for _, row := range t.Rows {
			if row.Kind != "changed" {
				hit := false
				for _, i := range idx {
					if e.Rules[i].covers(row.Kind) && passing[i][row.Key] {
						used[i], hit = true, true
					}
				}
				if !hit {
					out = append(out, ContentViolation{Table: name, Key: row.Key, Detail: fmt.Sprintf("row %s and no rule allows it", row.Kind)})
				}
				continue
			}
			for _, col := range row.Columns {
				hit := false
				var why []string
				for _, i := range idx {
					r := e.Rules[i]
					if !r.covers("changed") || (len(r.Columns) > 0 && !slices.Contains(r.Columns, col)) || !passing[i][row.Key] {
						continue
					}
					if slices.Contains(r.RaiseOnly, col) {
						lowered, problem := jsonValueDecreased(row.Released, row.New, col)
						if problem != "" {
							why = append(why, fmt.Sprintf("rule %q cannot judge raise-only %s: %s", r.Name, col, problem))
							continue
						}
						if lowered {
							why = append(why, fmt.Sprintf("rule %q is raise-only on %s and the value went down", r.Name, col))
							continue
						}
					}
					used[i], hit = true, true
				}
				if !hit {
					detail := fmt.Sprintf("column %s changed and no rule allows it", col)
					if len(why) > 0 {
						detail += " (" + strings.Join(why, "; ") + ")"
					}
					out = append(out, ContentViolation{Table: name, Key: row.Key, Detail: detail})
				}
			}
		}
	}
	for i, r := range e.Rules {
		if r.Mode != "must-differ" {
			continue
		}
		if !used[i] {
			out = append(out, ContentViolation{Table: r.Table, Detail: fmt.Sprintf("rule %q must see a difference and saw none: %s", r.Name, r.Reason)})
			continue
		}
		// A must-differ rule naming schema objects, or the columns a
		// column-added rule names, needs every one: the pkey alone must not
		// meet a rule that also names a partial unique the migrate skipped.
		var names []string
		names = append(names, r.Objects...)
		if r.covers("column-added") || r.covers("column-removed") {
			names = append(names, r.Columns...)
		}
		for _, n := range names {
			if !seenObjects[i][n] {
				out = append(out, ContentViolation{Table: r.Table, Detail: fmt.Sprintf("rule %q names %s and it did not appear: %s", r.Name, n, r.Reason)})
			}
		}
	}
	return out, nil
}

// wherePassing evaluates each rule's Where predicate for the table's
// differing rows, on the side each row exists on: rule index → key → passes.
// A rule without a predicate passes every row.
func (e *ContentExpectations) wherePassing(ctx context.Context, released, newPool *pgxpool.Pool, t ContentDiffTable, idx []int) (map[int]map[string]bool, error) {
	out := map[int]map[string]bool{}
	var newKeys, relKeys []string
	for _, row := range t.Rows {
		if row.Kind == "removed" {
			relKeys = append(relKeys, row.Key)
		} else {
			newKeys = append(newKeys, row.Key)
		}
	}
	tbl := pgx.Identifier{t.Schema, t.Table}.Sanitize()
	keyExpr := contentKeyExpr(t.Key)
	for _, i := range idx {
		r := e.Rules[i]
		out[i] = map[string]bool{}
		if r.Where == "" {
			for _, row := range t.Rows {
				out[i][row.Key] = true
			}
			continue
		}
		for _, side := range []struct {
			pool *pgxpool.Pool
			keys []string
		}{{newPool, newKeys}, {released, relKeys}} {
			if len(side.keys) == 0 {
				continue
			}
			// The pinned, read-only session ContentDiff computed the keys in:
			// a timestamptz key renders in UTC only there, and an operator's
			// predicate must not write.
			err := contentQuery(ctx, side.pool, func(tx pgx.Tx) error {
				rows, err := tx.Query(ctx, `SELECT `+keyExpr+`::text FROM `+tbl+` t WHERE `+keyExpr+` = ANY($1::jsonb[]) AND (`+r.Where+`)`, side.keys)
				if err != nil {
					return err
				}
				defer rows.Close()
				for rows.Next() {
					var k string
					if err := rows.Scan(&k); err != nil {
						return err
					}
					out[i][k] = true
				}
				return rows.Err()
			})
			if err != nil {
				return nil, fmt.Errorf("rule %q where on %s: %w", r.Name, t.Name(), err)
			}
		}
	}
	return out, nil
}

// jsonValueDecreased reports whether column col's numeric value is lower in
// the new row than in the released one. NULL is below every value: NULL to a
// value rises, a value to NULL falls. A problem (a missing column, a value
// that is not a number) is returned as text, never as "not lowered" (SR-5):
// the caller reports it as a violation.
func jsonValueDecreased(released, newRow json.RawMessage, col string) (lowered bool, problem string) {
	num := func(raw json.RawMessage) (*big.Float, bool, string) {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, false, err.Error()
		}
		v, ok := m[col]
		if !ok {
			return nil, false, "the column is missing"
		}
		if string(v) == "null" {
			return nil, true, ""
		}
		f, _, err := big.ParseFloat(string(v), 10, 256, big.ToNearestEven)
		if err != nil {
			return nil, false, fmt.Sprintf("%s is not a number", v)
		}
		return f, false, ""
	}
	a, aNull, pa := num(released)
	if pa != "" {
		return false, "released: " + pa
	}
	b, bNull, pb := num(newRow)
	if pb != "" {
		return false, "new: " + pb
	}
	switch {
	case aNull:
		return false, ""
	case bNull:
		return true, ""
	}
	return b.Cmp(a) < 0, ""
}
