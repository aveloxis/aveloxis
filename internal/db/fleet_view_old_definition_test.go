// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// A fleet view that EXISTS under an earlier definition (review round 1 of
// the vulnerability program's first part, Medium). serve builds the pair
// only when a member is missing, and a failed re-create keeps the previous
// view: either way a view can be older than the build reading it, and then
// lacks a column the reader selects (closed_with_reason, 0.29.1xx).
// PostgreSQL answers 42703. That used to be an error on every fleet
// package read; it is the live aggregate now, as for a view not built.
func TestFleetReadersAggregateLiveOverAnOlderViewDefinition(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	drop := []string{
		`DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_advisory`,
		`DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_exposure`,
	}
	for _, stmt := range drop {
		mustExecRetry(ctx, t, store, stmt)
	}
	t.Cleanup(func() {
		for _, stmt := range drop {
			cleanupExecRetry(context.Background(), store, stmt)
		}
	})
	// The view as an earlier release built it: every column the reader
	// selects but the newest.
	cols := strings.Split(strings.Join(strings.Fields(exposureSelectColumns()), ""), ",")
	if cols[len(cols)-1] != "closed_with_reason" {
		t.Fatalf("the reader's last column is %q; this test drops the newest column and must be told which that is", cols[len(cols)-1])
	}
	mustExecRetry(ctx, t, store, fmt.Sprintf(`CREATE MATERIALIZED VIEW aveloxis_data.explorer_package_exposure AS SELECT %s FROM (%s) x`,
		strings.Join(cols[:len(cols)-1], ", "), packageExposureSQL("")))
	// The premise: the view is there and cannot answer the reader.
	_, err := store.pool.Exec(ctx, `SELECT `+exposureSelectColumns()+` FROM aveloxis_data.explorer_package_exposure LIMIT 0`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42703" {
		t.Fatalf("the older view answers the reader's columns with %v; the premise is 42703", err)
	}

	page, err := store.ListPackageExposure(ctx, PackageExposureQuery{Search: "aveloxis-test-no-such-package"})
	if err != nil {
		t.Fatalf("the leaderboard over an older view definition: %v", err)
	}
	if !page.Live {
		t.Error("the leaderboard must say it aggregated live")
	}
	_, live, err := store.GetPackageExposure(ctx, "npm", "aveloxis-test-no-such-package", nil)
	if !errors.Is(err, ErrPackageNotFound) || !live {
		t.Errorf("a profile over an older view definition: live %v, err %v; want the typed not-found from the live aggregate", live, err)
	}
}

// The same for the ADVISORY view and its reader (review round 2: that
// reader's 42703 arm was not driven; the test above dropped the advisory
// view, which is the 42P01 arm). An older definition here is one without
// the reader's last column.
func TestTheAdvisoryReaderAggregatesLiveOverAnOlderViewDefinition(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const drop = `DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_advisory`
	mustExecRetry(ctx, t, store, drop)
	t.Cleanup(func() { cleanupExecRetry(context.Background(), store, drop) })
	mustExecRetry(ctx, t, store, `CREATE MATERIALIZED VIEW aveloxis_data.explorer_package_advisory AS
		SELECT ecosystem, package_name, vuln_id, cve_id, severity, max_cvss, fixed_version, repos, repos_unresolved FROM (`+packageAdvisorySQL("")+`) x`)
	_, err := store.pool.Exec(ctx, `SELECT first_seen FROM aveloxis_data.explorer_package_advisory LIMIT 0`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42703" {
		t.Fatalf("the older advisory view answers first_seen with %v; the premise is 42703", err)
	}
	olderFleetViewLogged = sync.Map{}
	t.Cleanup(func() { olderFleetViewLogged = sync.Map{} })
	if _, err := store.GetPackageAdvisories(ctx, "npm", "aveloxis-test-no-such-package", nil); err != nil {
		t.Errorf("a package's advisories over an older view definition: %v; want the live aggregate's answer", err)
	}
	// It is logged under ITS view's name (round 3: under the exposure
	// view's, the exposure view's line would silence this one).
	if _, ok := olderFleetViewLogged.Load(fleetAdvisoryView); !ok {
		t.Error("the advisory reader did not note the older advisory view")
	}
	if _, ok := olderFleetViewLogged.Load(fleetExposureView); ok {
		t.Error("the advisory reader noted the EXPOSURE view")
	}
}

// One line per VIEW that is an earlier definition, however many reads meet
// it (review round 2: one line per process covered both views, so the
// second view's was never written).
func TestAnOlderFleetViewIsLoggedOncePerView(t *testing.T) {
	var buf bytes.Buffer
	s := &PostgresStore{logger: slog.New(slog.NewTextHandler(&buf, nil))}
	olderFleetViewLogged = sync.Map{}
	t.Cleanup(func() { olderFleetViewLogged = sync.Map{} })
	older := fmt.Errorf("q: %w", &pgconn.PgError{Code: "42703", Message: "column \"x\" does not exist"})
	for i := 0; i < 3; i++ {
		s.noteFleetViewFallback(fleetExposureView, older)
	}
	s.noteFleetViewFallback(fleetAdvisoryView, older)
	s.noteFleetViewFallback(fleetAdvisoryView, older)
	// A view that is simply not built is a configuration, not logged.
	s.noteFleetViewFallback(fleetExposureView, &pgconn.PgError{Code: "42P01"})
	out := buf.String()
	if n := strings.Count(out, "level=WARN"); n != 2 {
		t.Errorf("%d lines, want one per view (2):\n%s", n, out)
	}
	for _, view := range []string{fleetExposureView, fleetAdvisoryView} {
		if strings.Count(out, "view="+view) != 1 {
			t.Errorf("the line for %s is missing or repeated:\n%s", view, out)
		}
	}
}

// The predicate, and what it must not swallow (SR-5).
func TestFleetViewCannotAnswer(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"undefined_table: the view is not built", &pgconn.PgError{Code: "42P01"}, true},
		{"undefined_column: the view is an earlier definition", fmt.Errorf("q: %w", &pgconn.PgError{Code: "42703"}), true},
		{"insufficient_privilege", &pgconn.PgError{Code: "42501"}, false},
		{"query_canceled", &pgconn.PgError{Code: "57014"}, false},
		{"transport", errors.New("failed to connect"), false},
		{"deadline", context.DeadlineExceeded, false},
		{"nil", nil, false},
	} {
		if got := fleetViewCannotAnswer(c.err); got != c.want {
			t.Errorf("%s: fleetViewCannotAnswer = %v, want %v", c.name, got, c.want)
		}
	}
}
