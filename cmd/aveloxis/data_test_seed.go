// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// data_test_seed.go — `aveloxis data-test --from-db <source> --seed-shape
// <file>`: build the snapshot mode's template from a source database, shaped
// like the production fleet the shape file describes (data_test_seed.sql).
// The source is cloned first and never written. The shape file holds
// aggregates only (counts), so it can come from production (the Track 0a
// script) without any row leaving it.

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed data_test_seed.sql
var dataTestSeedSQL string

// seedTemplateDB is the template the seed writes and the snapshot mode then
// clones. One spelling.
const seedTemplateDB = "aveloxis_dt_template"

// seedShape is the shape file: the fleet's counts the seed tops the clone up
// to. A count below what the clone already holds adds nothing.
type seedShape struct {
	FleetRepos            int64 `json:"fleet_repos"`
	GoneStamped           int64 `json:"gone_stamped"`
	Adopted               int64 `json:"adopted"`
	AdoptedDepthFactor    int64 `json:"adopted_depth_factor"` // 1 (or 0) = the source's depth
	Contributors          int64 `json:"contributors"`
	ContributorsAliases   int64 `json:"contributors_aliases"`
	ContributorIdentities int64 `json:"contributor_identities"`
	// Source says where the numbers come from (production's Track 0a
	// output, or a provisional estimate); it is printed in the report.
	Source string `json:"source"`
}

// loadSeedShape reads and checks a shape file.
func loadSeedShape(path string) (seedShape, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return seedShape{}, fmt.Errorf("--seed-shape: %w", err)
	}
	var s seedShape
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return seedShape{}, fmt.Errorf("--seed-shape %s: %w", path, err)
	}
	if s.AdoptedDepthFactor == 0 {
		s.AdoptedDepthFactor = 1
	}
	for name, v := range s.settings() {
		n, _ := strconv.ParseInt(v, 10, 64)
		if n < 0 {
			return seedShape{}, fmt.Errorf("--seed-shape %s: %s must not be negative", path, name)
		}
	}
	if s.Source == "" {
		return seedShape{}, fmt.Errorf("--seed-shape %s: say where the numbers come from (source)", path)
	}
	return s, nil
}

// settings are the dtseed.* values the seed SQL reads.
func (s seedShape) settings() map[string]string {
	return map[string]string{
		"dtseed.fleet_repos":            strconv.FormatInt(s.FleetRepos, 10),
		"dtseed.gone_stamped":           strconv.FormatInt(s.GoneStamped, 10),
		"dtseed.adopted":                strconv.FormatInt(s.Adopted, 10),
		"dtseed.adopted_depth_factor":   strconv.FormatInt(s.AdoptedDepthFactor, 10),
		"dtseed.contributors":           strconv.FormatInt(s.Contributors, 10),
		"dtseed.contributors_aliases":   strconv.FormatInt(s.ContributorsAliases, 10),
		"dtseed.contributor_identities": strconv.FormatInt(s.ContributorIdentities, 10),
	}
}

// seedScript is the seed with its settings, for a caller's transaction.
func seedScript(s seedShape) string {
	settings := s.settings()
	names := make([]string, 0, len(settings))
	for name := range settings {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "SET LOCAL %s = '%s';\n", name, settings[name])
	}
	b.WriteString(dataTestSeedSQL)
	return b.String()
}

// seedCounts is what the seed left, for the report.
type seedCounts struct {
	Repos, Synthetic, Gone, Adopted, Contributors, Aliases, Identities int64
}

func readSeedCounts(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (seedCounts, error) {
	var c seedCounts
	err := q.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM aveloxis_data.repos),
		(SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_owner = 'dtseed'),
		(SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_gone_at IS NOT NULL),
		(SELECT COUNT(*) FROM aveloxis_data.repo_forge_id_changes WHERE adopted_at IS NOT NULL),
		(SELECT COUNT(*) FROM aveloxis_data.contributors),
		(SELECT COUNT(*) FROM aveloxis_data.contributors_aliases),
		(SELECT COUNT(*) FROM aveloxis_data.contributor_identities)`).Scan(
		&c.Repos, &c.Synthetic, &c.Gone, &c.Adopted, &c.Contributors, &c.Aliases, &c.Identities)
	return c, err
}

// buildSeedTemplate clones source into the template, then in one
// transaction rewinds it (rewind: SQL rolling a newer database back to the
// released schema; "" = none) and seeds it (shape; nil = no seed), then
// VACUUM ANALYZEs it (a clone carries no statistics). The source is never
// written.
func (r *snapshotRun) buildSeedTemplate(source, rewind string, shape *seedShape) (seedCounts, error) {
	if err := r.clone(source, seedTemplateDB); err != nil {
		return seedCounts{}, err
	}
	pool, err := pgxpool.New(r.ctx, r.databaseDSN(seedTemplateDB))
	if err != nil {
		return seedCounts{}, err
	}
	defer pool.Close()
	tx, err := pool.Begin(r.ctx)
	if err != nil {
		return seedCounts{}, err
	}
	defer func() { _ = tx.Rollback(r.ctx) }()
	if rewind != "" {
		r.logger.Info("seed: rewinding the template to the released schema", "template", seedTemplateDB)
		if _, err := tx.Exec(r.ctx, rewind, pgx.QueryExecModeSimpleProtocol); err != nil {
			return seedCounts{}, fmt.Errorf("rewind: %w", err)
		}
	}
	if shape != nil {
		r.logger.Info("seed: shaping the template", "template", seedTemplateDB, "source", source, "shape_source", shape.Source)
		if _, err := tx.Exec(r.ctx, seedScript(*shape), pgx.QueryExecModeSimpleProtocol); err != nil {
			return seedCounts{}, fmt.Errorf("seed: %w", err)
		}
	}
	if err := tx.Commit(r.ctx); err != nil {
		return seedCounts{}, fmt.Errorf("seed commit: %w", err)
	}
	if _, err := pool.Exec(r.ctx, `VACUUM (ANALYZE)`, pgx.QueryExecModeSimpleProtocol); err != nil {
		return seedCounts{}, fmt.Errorf("seed: VACUUM ANALYZE: %w", err)
	}
	counts, err := readSeedCounts(r.ctx, pool)
	if err != nil {
		return seedCounts{}, err
	}
	r.logger.Info("seed: template ready", "repos", counts.Repos, "synthetic", counts.Synthetic, "gone", counts.Gone,
		"adopted", counts.Adopted, "contributors", counts.Contributors, "aliases", counts.Aliases, "identities", counts.Identities)
	return counts, nil
}
