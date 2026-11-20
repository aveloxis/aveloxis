// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"slices"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// What depends on a host, and what distrusting it does (summary/70, 4.5,
// D14). The operator is shown the first before confirming the second.
func TestRepositoryHostImpactAndPurge(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const host, other = "test-impact-host.invalid", "test-impact-other.invalid"
	const url, otherURL = "https://test-impact-host.invalid/m2", "https://test-impact-other.invalid/m2"
	repoA := vulnRepo(t, store, ctx, "host-impact-a")
	repoB := vulnRepo(t, store, ctx, "host-impact-b")
	clean := func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'https://test-impact-%'`)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.distrusted_repository_hosts WHERE host LIKE 'test-impact-%'`)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.package_repository_hosts WHERE host LIKE 'test-impact-%'`)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = ANY($1)`, []int64{repoA, repoB})
	}
	t.Cleanup(clean)
	clean()

	for _, k := range [][2]string{{url, "p1"}, {url, "p2"}, {otherURL, "p1"}} {
		if err := store.PutMavenPomResult(ctx, k[0], "g", k[1], "1", true, []byte(`{"group":"g","artifact":"a"}`), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.NotePackageRepositoryHostAnswered(ctx, host); err != nil {
		t.Fatal(err)
	}
	// Repository A: one row through the host with a current finding and a
	// closed one, one row through the other host, and a tree using the host.
	// Repository B: no row through the host, but a tree that depends on it.
	if err := store.ReplaceRepoLibyear(ctx, repoA, []*model.LibyearRow{
		{Name: "g:through", PackageManager: "maven", CurrentVersion: "1.0", Purl: "pkg:maven/g/through@1.0", VersionHosts: []string{host}},
		{Name: "g:elsewhere", PackageManager: "maven", CurrentVersion: "1.0", Purl: "pkg:maven/g/elsewhere@1.0", VersionHosts: []string{other}},
		{Name: "g:own", PackageManager: "maven", CurrentVersion: "1.0", Purl: "pkg:maven/g/own@1.0"},
	}, nil, model.InventoryCoverage{MavenTreeHosts: []model.MavenTreeHosts{{Root: "pom.xml", Hosts: []string{host, other}}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceRepoLibyear(ctx, repoB, nil, nil, model.InventoryCoverage{
		MavenTreeHosts: []model.MavenTreeHosts{{Root: "a/pom.xml", Hosts: []string{host}}, {Root: "b/pom.xml", Hosts: []string{other}}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertVulnerabilityBatch(ctx, repoA, []*model.VulnerabilityRow{
		{VulnID: "open-through", PackageName: "g:through", PackagePurl: "pkg:maven/g/through@1.0", Source: "osv.dev", DependencyKind: "direct"},
		// The same purl as the row, and closed below: a closed finding is
		// not one the distrust will close.
		{VulnID: "closed-through", PackageName: "g:through", PackagePurl: "pkg:maven/g/through@1.0", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "open-elsewhere", PackageName: "g:elsewhere", PackagePurl: "pkg:maven/g/elsewhere@1.0", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}

	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_deps_vulnerabilities SET resolved_at = NOW() WHERE repo_id = $1 AND vuln_id = 'closed-through'`, repoA)

	want := RepositoryHostImpact{StoredResults: 2, Repositories: 2, Findings: 1, Trees: 2}
	im, err := store.PreviewRepositoryHostDistrust(ctx, "Test-Impact-Host.invalid")
	if err != nil {
		t.Fatal(err)
	}
	if im != want {
		t.Errorf("preview = %+v, want %+v (2 stored results; repositories A by its row and B by its tree alone; 1 current finding at a version through the host; 2 trees)", im, want)
	}
	// The preview changes nothing.
	if hosts, _ := store.GetDistrustedRepositoryHosts(ctx); slices.Contains(hosts, host) {
		t.Fatal("a preview distrusted the host")
	}

	got, changed, err := store.DistrustAndPurgeRepositoryHost(ctx, host, "user 1", "abandoned")
	if err != nil || !changed || got != want {
		t.Fatalf("distrust: impact %+v changed %v err %v", got, changed, err)
	}
	var left, otherLeft int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FILTER (WHERE repository_url = $1), COUNT(*) FILTER (WHERE repository_url = $2)
		FROM aveloxis_data.maven_pom_results`, url, otherURL).Scan(&left, &otherLeft); err != nil {
		t.Fatal(err)
	}
	if left != 0 || otherLeft != 1 {
		t.Errorf("after the purge: %d results left for the host, %d for the other; want 0 and 1", left, otherLeft)
	}
	// A second distrust does nothing and says so.
	if _, changed, _ := store.DistrustAndPurgeRepositoryHost(ctx, host, "user 2", ""); changed {
		t.Error("distrusting a host twice reported a change")
	}

	list, err := store.ListRepositoryHosts(ctx, []string{"test-impact-configured.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	byHost := map[string]RepositoryHost{}
	for _, h := range list {
		byHost[h.Host] = h
	}
	if h := byHost[host]; h.DistrustedAt == nil || h.DistrustedBy != "user 1" || h.Note != "abandoned" || h.FirstFetchedAt == nil || h.Impact.StoredResults != 0 || h.Impact.Findings != 1 {
		t.Errorf("the distrusted host in the list = %+v", h)
	}
	// A configured host that never answered is listed too, so it can be
	// distrusted before it is ever asked.
	if h, ok := byHost["test-impact-configured.invalid"]; !ok || h.FirstFetchedAt != nil || h.DistrustedAt != nil {
		t.Errorf("a configured host that never answered = %+v (listed %v)", h, ok)
	}
}

// What the periodic check saw of a host (summary/70, 4.5, D14): a
// definitive answer stands until another replaces it; "could not tell"
// never replaces one; a dismissal hides a signal until a new one begins.
func TestRepositoryHostSignalsStandDismissAndReturn(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const host = "test-signal-host.invalid"
	clean := func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.repository_host_signals WHERE host LIKE 'test-signal-%'`)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.package_repository_hosts WHERE host LIKE 'test-signal-%'`)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.distrusted_repository_hosts WHERE host LIKE 'test-signal-%'`)
	}
	t.Cleanup(clean)
	clean()
	signal := func() RepositoryHostSignal {
		t.Helper()
		list, err := store.ListRepositoryHosts(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range list {
			if h.Host == host {
				for _, g := range h.Signals {
					if g.Kind == "certificate" {
						return g
					}
				}
			}
		}
		t.Fatal("the host has no certificate row")
		return RepositoryHostSignal{}
	}
	record := func(state, detail string) {
		t.Helper()
		if err := store.RecordRepositoryHostObservations(ctx, "Test-Signal-Host.invalid.", []HostObservation{{Kind: "certificate", State: state, Detail: detail}}); err != nil {
			t.Fatal(err)
		}
	}

	// Never able to tell: unknown, and not something to attend to.
	record("unknown", "timeout")
	if g := signal(); g.State != "unknown" || g.UnknownSince == nil || g.NeedsAttention() {
		t.Fatalf("a first check that could not tell = %+v", g)
	}
	record("signal", "expired")
	first := signal()
	if first.State != "signal" || first.Detail != "expired" || first.UnknownSince != nil || !first.NeedsAttention() {
		t.Fatalf("a signal = %+v", first)
	}
	// The same answer again: it began when it began.
	record("signal", "expired")
	if g := signal(); !g.Since.Equal(first.Since) {
		t.Errorf("a repeated signal moved its start from %s to %s", first.Since, g.Since)
	}
	// A look that could not tell does not clear a signal.
	record("unknown", "connection refused")
	if g := signal(); g.State != "signal" || g.UnknownSince == nil || g.UnknownDetail != "connection refused" || !g.Since.Equal(first.Since) || !g.NeedsAttention() {
		t.Errorf("an unknown after a signal = %+v; the signal must stand, with the unknown noted", g)
	}
	// Dismissed: off the attention list while nothing new begins.
	if n, err := store.DismissRepositoryHostSignals(ctx, host, "user 1"); err != nil || n != 1 {
		t.Fatalf("dismiss: %d %v", n, err)
	}
	if g := signal(); g.NeedsAttention() || g.State != "signal" {
		t.Errorf("a dismissed signal = %+v", g)
	}
	record("signal", "expired")
	if g := signal(); g.NeedsAttention() {
		t.Error("the same signal, seen again, came back after its dismissal")
	}
	if n, _ := store.DismissRepositoryHostSignals(ctx, host, "user 1"); n != 0 {
		t.Errorf("a second dismissal dismissed %d", n)
	}
	// Clear, then a signal again: a NEW signal, which returns.
	record("clear", "")
	if g := signal(); g.State != "clear" || g.NeedsAttention() {
		t.Errorf("clear = %+v", g)
	}
	record("signal", "wrong name")
	if g := signal(); !g.NeedsAttention() || g.Detail != "wrong name" {
		t.Errorf("a new signal after a dismissal = %+v; it must return to the list", g)
	}
	if err := store.RecordRepositoryHostObservations(ctx, host, []HostObservation{{Kind: "certificate", State: "fine"}}); err == nil {
		t.Error("a state that is not one of the three was stored")
	}

	// The hosts the check looks at: answered, and not distrusted.
	if err := store.NotePackageRepositoryHostAnswered(ctx, host); err != nil {
		t.Fatal(err)
	}
	if hosts, _ := store.RepositoryHostFirstFetches(ctx); hosts[host].IsZero() {
		t.Error("an answered host is not checked")
	}
	if _, err := store.DistrustRepositoryHost(ctx, host, "user 1", ""); err != nil {
		t.Fatal(err)
	}
	if hosts, _ := store.RepositoryHostFirstFetches(ctx); !hosts[host].IsZero() {
		t.Error("a distrusted host is still checked: nothing contacts a distrusted host")
	}
}
