// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// trafficRepoIDFK is the repo_id declaration every traffic table shares
// with repo_clones: deleting or renumbering a repository goes through the
// same cascade and dedup rules for its traffic as for its daily clones.
const trafficRepoIDFK = "repo_id BIGINT NOT NULL REFERENCES aveloxis_data.repos(repo_id) " +
	"ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED"

// trafficKindCheck limits traffic_kind to the two model.TrafficKind values,
// the spelling the collector stages and the REST answer returns.
var trafficKindCheck = "traffic_kind TEXT NOT NULL CHECK (traffic_kind IN ('" +
	string(model.TrafficKindViews) + "', '" + string(model.TrafficKindClones) + "'))"

// trafficProvenance are the columns every aveloxis_data row carries
// (repo_clones' shape): who wrote it, with which version, from where, when.
var trafficProvenance = []string{
	"tool_source TEXT DEFAULT 'aveloxis'",
	"tool_version TEXT DEFAULT ''",
	"data_source TEXT DEFAULT ''",
	"data_collection_date TIMESTAMPTZ DEFAULT NOW()",
}

type trafficTableSpec struct {
	table   string
	columns []string // normalized declarations that must appear in the body
	unique  string   // the table's one natural key, the writers' ON CONFLICT arbiter
	noTotal bool     // windows and weeks keep distinct counts only
}

// trafficTableSpecs: daily views keyed like repo_clones (one row per
// repository and UTC day); the top paths and referrers keyed by the UTC
// collection date, so a later read of the same day replaces that day's
// list; windows keyed by both ends, so no window length is assumed; weeks
// keyed by their Monday.
var trafficTableSpecs = []trafficTableSpec{
	{
		table: "repo_views",
		columns: []string{
			"repo_view_id BIGSERIAL PRIMARY KEY",
			"view_timestamp TIMESTAMPTZ NOT NULL",
			"total_views INT DEFAULT 0",
			"unique_views INT DEFAULT 0",
		},
		unique: "UNIQUE (repo_id, view_timestamp)",
	},
	{
		table: "repo_traffic_paths",
		columns: []string{
			"repo_traffic_path_id BIGSERIAL PRIMARY KEY",
			"snapshot_date DATE NOT NULL",
			"path TEXT NOT NULL",
			"title TEXT DEFAULT ''",
			"total_views INT DEFAULT 0",
			"unique_views INT DEFAULT 0",
		},
		unique: "UNIQUE (repo_id, snapshot_date, path)",
	},
	{
		table: "repo_traffic_referrers",
		columns: []string{
			"repo_traffic_referrer_id BIGSERIAL PRIMARY KEY",
			"snapshot_date DATE NOT NULL",
			"referrer TEXT NOT NULL",
			"total_views INT DEFAULT 0",
			"unique_views INT DEFAULT 0",
		},
		unique: "UNIQUE (repo_id, snapshot_date, referrer)",
	},
	{
		table: "repo_traffic_windows",
		columns: []string{
			"repo_traffic_window_id BIGSERIAL PRIMARY KEY",
			trafficKindCheck,
			"window_start DATE NOT NULL",
			"window_end DATE NOT NULL",
			"uniques INT DEFAULT 0",
		},
		unique:  "UNIQUE (repo_id, traffic_kind, window_start, window_end)",
		noTotal: true,
	},
	{
		table: "repo_traffic_weeks",
		columns: []string{
			"repo_traffic_week_id BIGSERIAL PRIMARY KEY",
			trafficKindCheck,
			"week_start DATE NOT NULL",
			"uniques INT DEFAULT 0",
		},
		unique:  "UNIQUE (repo_id, traffic_kind, week_start)",
		noTotal: true,
	},
}

// trafficTableBodies returns the comment-stripped, whitespace-normalized
// body of every CREATE TABLE in schema.sql, by unqualified aveloxis_data
// table name.
func trafficTableBodies(t *testing.T) map[string]string {
	t.Helper()
	schema := srctest.StripSQLComments(srctest.Read(t, "internal/db/schema.sql"))
	re := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS aveloxis_data\.(\w+)\s*\((.*?)\n\);`)
	bodies := map[string]string{}
	for _, m := range re.FindAllStringSubmatch(schema, -1) {
		bodies[m[1]] = srctest.NormalizeWS(m[2])
	}
	srctest.MinCount(t, "aveloxis_data CREATE TABLE blocks in schema.sql", len(bodies), 100)
	return bodies
}

// TestSchemaDeclaresTrafficTables pins the five GitHub traffic tables
// (aveloxis/aveloxis#234) in the base schema, which every `aveloxis
// migrate` runs, so existing deployments get them without a migrate.go
// step: their columns, the repo_id foreign key shared with repo_clones,
// the unique key each writer's ON CONFLICT relies on, and the traffic_kind
// CHECK.
func TestSchemaDeclaresTrafficTables(t *testing.T) {
	bodies := trafficTableBodies(t)
	checked := 0
	for _, tbl := range trafficTableSpecs {
		t.Run(tbl.table, func(t *testing.T) {
			body, ok := bodies[tbl.table]
			if !ok {
				t.Fatalf("schema.sql has no CREATE TABLE IF NOT EXISTS aveloxis_data.%s", tbl.table)
			}
			checked++
			want := append([]string{trafficRepoIDFK}, tbl.columns...)
			want = append(want, trafficProvenance...)
			want = append(want, tbl.unique)
			for _, decl := range want {
				if !strings.Contains(body, decl) {
					t.Errorf("aveloxis_data.%s must declare %q\nbody: %s", tbl.table, decl, body)
				}
			}
			if n := strings.Count(body, "UNIQUE"); n != 1 {
				t.Errorf("aveloxis_data.%s declares %d UNIQUE keys, want exactly %q: a second key "+
					"would be a second ON CONFLICT arbiter the writers do not name", tbl.table, n, tbl.unique)
			}
			if tbl.noTotal {
				// Totals over a window or week are the sum of the daily rows;
				// only GitHub's distinct counts, which cannot be summed, are kept.
				for _, banned := range []string{"total", "count"} {
					if strings.Contains(body, banned) {
						t.Errorf("aveloxis_data.%s must not store a %s column: a period's views or "+
							"clones are the sum of the daily rows\nbody: %s", tbl.table, banned, body)
					}
				}
			}
		})
	}
	if checked != len(trafficTableSpecs) {
		t.Fatalf("checked %d of %d traffic tables", checked, len(trafficTableSpecs))
	}
}
