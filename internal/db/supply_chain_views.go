// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// The supply-chain package views (v0.29.60) are AVELOXIS-OWNED: the GUI's
// dependencies page reads them through the API and nothing in 8Knot does,
// and they are two aggregates of a few seconds, not the hours-long 8Knot
// batch in matviews.sql. v0.29.61 (worklist 48) gave them their own
// lifecycle, apart from the 8Knot set in every way that costs something:
//
//   - built from the Go SQL the API runs live for a cohort
//     (PackageExposureMatviewSQL / PackageAdvisoryMatviewSQL — one body,
//     SR-17), never from matviews.sql;
//   - each view is one statement, so a missing member is built alone (the
//     whole-set rule of CreateMaterializedViewsIfNotExist exists because
//     matviews.sql is one batch — that reason does not apply here);
//   - governed by the store's supplyChainMode (serve asks for IfMissing,
//     migrate for Rebuild whether or not --skip-views was passed; the zero
//     value builds none) and NOT by collection.materialized_views, which is
//     the 8Knot switch;
//   - refreshed by the scheduler every collection.supply_chain_refresh_hours
//     (RefreshSupplyChainViews) and by `aveloxis refresh-views` (--set
//     supply-chain or all), never by the weekly 8Knot rebuild.

// SupplyChainViewNames are the two Aveloxis-owned materialized views, bare
// (schema aveloxis_data). The ONE list: the builder, the probe, the refresh,
// the deploy checklist's count and the tests that keep matviews.sql and
// matviewNames free of them all read it.
var SupplyChainViewNames = []string{"explorer_package_exposure", "explorer_package_advisory"}

type supplyChainView struct {
	name  string
	body  string
	index string // the unique key REFRESH CONCURRENTLY needs
}

func supplyChainViewDefs() []supplyChainView {
	return []supplyChainView{
		{SupplyChainViewNames[0], PackageExposureMatviewSQL(), "(ecosystem, package_name)"},
		{SupplyChainViewNames[1], PackageAdvisoryMatviewSQL(), "(ecosystem, package_name, vuln_id)"},
	}
}

// createSupplyChainView drops and re-creates one view with its unique
// index in ONE transaction, so a failing CREATE leaves the previous view
// and its data in place (the 8Knot batch has the same property for the
// opposite reason: one statement batch, all or nothing).
func createSupplyChainView(ctx context.Context, pg *PostgresStore, v supplyChainView) error {
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // a no-op after Commit
	for _, stmt := range []string{
		fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.%s CASCADE", v.name),
		fmt.Sprintf("CREATE MATERIALIZED VIEW aveloxis_data.%s AS %s", v.name, v.body),
		fmt.Sprintf("CREATE UNIQUE INDEX idx_%s_pkg ON aveloxis_data.%s %s", v.name, v.name, v.index),
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// CreateSupplyChainViews (re-)creates both views from their current
// definition — the path that applies a CHANGED definition, and what
// `aveloxis migrate` does on every run because it costs seconds. Every
// failure is logged and returned; the other view is still attempted.
func CreateSupplyChainViews(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	var failed []error
	for _, v := range supplyChainViewDefs() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := time.Now()
		if err := createSupplyChainView(ctx, pg, v); err != nil {
			logger.Error("supply-chain view creation failed — the API aggregates live for this view until a migrate builds it",
				"view", v.name, "error", err, "duration", time.Since(start).Truncate(time.Millisecond))
			failed = append(failed, fmt.Errorf("view %s: %w", v.name, err))
			continue
		}
		logger.Info("supply-chain view created", "view", v.name, "duration", time.Since(start).Truncate(time.Millisecond))
	}
	return errors.Join(failed...)
}

// CreateSupplyChainViewsIfMissing is serve's startup rule for the pair:
// build whichever member is absent, leave the present ones alone (their
// data refreshes on the scheduler's cadence). Reports whether anything was
// built so the caller can skip an immediate refresh of fresh views.
func CreateSupplyChainViewsIfMissing(ctx context.Context, pg *PostgresStore, logger *slog.Logger) (bool, error) {
	present, err := supplyChainViewsPresentSet(ctx, pg)
	if err != nil {
		return false, fmt.Errorf("probing for supply-chain views: %w", err)
	}
	built := false
	var failed []error
	for _, v := range supplyChainViewDefs() {
		if present[v.name] {
			continue
		}
		if ctx.Err() != nil {
			return built, ctx.Err()
		}
		start := time.Now()
		if err := createSupplyChainView(ctx, pg, v); err != nil {
			logger.Error("supply-chain view creation failed — the API aggregates live for this view until a migrate builds it",
				"view", v.name, "error", err, "duration", time.Since(start).Truncate(time.Millisecond))
			failed = append(failed, fmt.Errorf("view %s: %w", v.name, err))
			continue
		}
		built = true
		logger.Info("supply-chain view created (was missing)", "view", v.name, "duration", time.Since(start).Truncate(time.Millisecond))
	}
	if !built && len(failed) == 0 {
		logger.Info("supply-chain views already exist, left alone at startup (their data refreshes on collection.supply_chain_refresh_hours; a changed definition lands on the next `aveloxis migrate`)",
			"views", len(present))
	}
	return built, errors.Join(failed...)
}

// supplyChainViewsPresentSet names the members of the pair that exist.
func supplyChainViewsPresentSet(ctx context.Context, pg *PostgresStore) (map[string]bool, error) {
	rows, err := pg.pool.Query(ctx,
		`SELECT matviewname FROM pg_matviews WHERE schemaname = 'aveloxis_data' AND matviewname = ANY($1)`, SupplyChainViewNames)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	present := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		present[name] = true
	}
	return present, rows.Err()
}

// supplyChainViewsPresent counts the members of the pair that exist.
func supplyChainViewsPresent(ctx context.Context, pg *PostgresStore) (int, error) {
	present, err := supplyChainViewsPresentSet(ctx, pg)
	if err != nil {
		return 0, err
	}
	return len(present), nil
}

// RefreshSupplyChainViews refreshes the data of whichever members of the
// pair exist (CONCURRENTLY: the unique index is part of the build). It
// asks the database, not the config, for the same reason
// RefreshMaterializedViews does. A pair that is not built is a logged
// no-op — serve or migrate builds it — not an error.
func RefreshSupplyChainViews(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	start := time.Now()
	present, err := supplyChainViewsPresentSet(ctx, pg)
	if err != nil {
		return fmt.Errorf("checking for supply-chain views: %w", err)
	}
	if len(present) == 0 {
		logger.Info("no supply-chain views in this database — nothing to refresh (`aveloxis migrate` or the next `aveloxis serve` start builds them; the API aggregates live meanwhile)")
		return nil
	}
	var failed []error
	for _, name := range SupplyChainViewNames {
		if !present[name] {
			continue
		}
		if err := refreshMatview(ctx, pg, logger, "aveloxis_data."+name); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			failed = append(failed, err)
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	logger.Info("supply-chain view refresh complete",
		"views", len(present), "total_duration", time.Since(start).Truncate(time.Millisecond), "failed", len(failed))
	if len(failed) > 0 {
		return boundedJoin(fmt.Sprintf("supply-chain view refresh left %d of %d views stale", len(failed), len(present)), failed, partialFailureSample)
	}
	return nil
}
