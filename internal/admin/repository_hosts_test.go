// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// The repository host operations are an administrator's, decided here: a
// caller that is not one reaches no store method at all (the fake fails the
// test on any call it was not given).
func TestRepositoryHostOperationsAreAdministratorOnly(t *testing.T) {
	ctx := context.Background()
	notAdmin := Actor{UserID: 2, Kind: Session}
	svc := New(adminStore(t), Options{}, nil, discardLogger())
	if _, err := svc.ListRepositoryHosts(ctx, notAdmin, nil); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("list: %v", err)
	}
	if _, _, err := svc.PreviewRepositoryHostDistrust(ctx, notAdmin, "repo.example"); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("preview: %v", err)
	}
	if _, _, err := svc.DistrustRepositoryHost(ctx, notAdmin, "repo.example", ""); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("distrust: %v", err)
	}
	if _, err := svc.RestoreRepositoryHost(ctx, notAdmin, "repo.example"); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("restore: %v", err)
	}
}

// A distrust reaches the store with the host in its one spelling and with
// WHO decided it, and is logged with what it did.
func TestDistrustRepositoryHostRecordsWhoAndWhat(t *testing.T) {
	st := adminStore(t)
	var gotHost, gotBy, gotNote string
	want := db.RepositoryHostImpact{StoredResults: 12, Repositories: 3, Findings: 7, Trees: 2}
	st.distrustRepositoryHost = func(_ context.Context, host, by, note string) (db.RepositoryHostImpact, bool, error) {
		gotHost, gotBy, gotNote = host, by, note
		return want, true, nil
	}
	svc, _, logs := testService(t, st, Options{})
	im, changed, err := svc.DistrustRepositoryHost(context.Background(), adminActor(), "  Repo.Example:8443. ", "  the domain changed hands  ")
	if err != nil || !changed || im != want {
		t.Fatalf("impact %+v changed %v err %v", im, changed, err)
	}
	if gotHost != "repo.example:8443" || gotNote != "the domain changed hands" {
		t.Errorf("the store was given host %q note %q", gotHost, gotNote)
	}
	if gotBy == "" || !strings.Contains(gotBy, "1") {
		t.Errorf("the store was told the distrust was by %q; want the actor", gotBy)
	}
	line := auditLine(logs, "admin: distrust repository host")
	if line == "" || !strings.Contains(line, "repo.example:8443") || !strings.Contains(line, "stored_results_deleted=12") || !strings.Contains(line, "findings_to_close_as_version_lost=7") {
		t.Errorf("the audit line = %q; want the host and what the distrust did", line)
	}
}

// A distrust of a host that is already distrusted does nothing, and its log
// line does not say it deleted anything (review round 1: it gave the count
// of what is stored as "deleted").
func TestDistrustingADistrustedHostLogsNothingDeleted(t *testing.T) {
	st := adminStore(t)
	st.distrustRepositoryHost = func(context.Context, string, string, string) (db.RepositoryHostImpact, bool, error) {
		return db.RepositoryHostImpact{StoredResults: 12, Findings: 7}, false, nil
	}
	svc, _, logs := testService(t, st, Options{})
	if _, changed, err := svc.DistrustRepositoryHost(context.Background(), adminActor(), "repo.example", ""); err != nil || changed {
		t.Fatalf("changed %v err %v", changed, err)
	}
	line := auditLine(logs, "admin: distrust repository host")
	if !strings.Contains(line, "changed=false") || !strings.Contains(line, "stored_results_deleted=0") || strings.Contains(line, "findings_to_close_as_version_lost=7") {
		t.Errorf("the audit line = %q; want changed=false and nothing reported as done", line)
	}
}

// A preview names the host as it will be acted on, and changes nothing: the
// fake has no distrust function, so reaching one fails the test.
func TestPreviewRepositoryHostDistrustChangesNothing(t *testing.T) {
	st := adminStore(t)
	st.previewRepositoryHost = func(_ context.Context, host string) (db.RepositoryHostImpact, error) {
		if host != "repo.example" {
			t.Errorf("previewed %q", host)
		}
		return db.RepositoryHostImpact{StoredResults: 4}, nil
	}
	host, im, err := New(st, Options{}, nil, discardLogger()).PreviewRepositoryHostDistrust(context.Background(), adminActor(), "REPO.example")
	if err != nil || host != "repo.example" || im.StoredResults != 4 {
		t.Errorf("host %q impact %+v err %v", host, im, err)
	}
}

// What is not a host is refused before the store is reached.
func TestRepositoryHostInputIsChecked(t *testing.T) {
	svc := New(adminStore(t), Options{}, nil, discardLogger())
	ctx := context.Background()
	for _, bad := range []string{"", "   ", "https://repo.example", "repo.example/maven2", "user@repo.example", "repo example", strings.Repeat("a", MaxRepositoryHostBytes+1)} {
		if _, _, err := svc.DistrustRepositoryHost(ctx, adminActor(), bad, ""); err == nil {
			t.Errorf("%q was accepted as a host", bad)
		} else if ref, ok := Classify(err); !ok || ref.Kind != InvalidInput {
			t.Errorf("%q: %v; want an invalid-input refusal", bad, err)
		}
		if _, err := svc.RestoreRepositoryHost(ctx, adminActor(), bad); err == nil {
			t.Errorf("restore accepted %q", bad)
		}
	}
	if _, _, err := svc.DistrustRepositoryHost(ctx, adminActor(), "repo.example", strings.Repeat("n", MaxRepositoryHostNoteBytes+1)); err == nil {
		t.Error("a note over the bound was accepted")
	}
}

// A restore is logged with who.
func TestRestoreRepositoryHost(t *testing.T) {
	st := adminStore(t)
	st.restoreRepositoryHost = func(_ context.Context, host string) (bool, error) { return host == "repo.example", nil }
	svc, _, logs := testService(t, st, Options{})
	changed, err := svc.RestoreRepositoryHost(context.Background(), adminActor(), "Repo.Example")
	if err != nil || !changed {
		t.Fatalf("changed %v err %v", changed, err)
	}
	if line := auditLine(logs, "admin: restore repository host"); !strings.Contains(line, "repo.example") {
		t.Errorf("the audit line = %q", line)
	}
}

// auditLine is the first logged line holding needle, or "".
func auditLine(logs *logSink, needle string) string {
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, needle) {
			return l
		}
	}
	return ""
}

// A dismissal reaches the store with who, is logged, and is all it does:
// the fake has no distrust function.
func TestDismissRepositoryHostSignals(t *testing.T) {
	st := adminStore(t)
	var by string
	st.dismissRepositoryHost = func(_ context.Context, host, who string) (int, error) {
		by = who
		if host != "repo.example" {
			t.Errorf("dismissed %q", host)
		}
		return 2, nil
	}
	svc, _, logs := testService(t, st, Options{})
	n, err := svc.DismissRepositoryHostSignals(context.Background(), adminActor(), "Repo.Example")
	if err != nil || n != 2 || by == "" {
		t.Fatalf("n %d by %q err %v", n, by, err)
	}
	if line := auditLine(logs, "admin: dismiss repository host signals"); !strings.Contains(line, "signals_dismissed=2") {
		t.Errorf("the audit line = %q", line)
	}
}
