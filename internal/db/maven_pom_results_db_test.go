// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
	"time"
)

// The stored results of the Maven resolver (summary/70, 4.5): one per
// (serving repository, group, artifact, version). A hit from a configured
// repository does not expire (a released pom does not change); a miss does
// (the artifact may be published later).

func TestMavenPomResultsAreStoredPerRepositoryAndCoordinate(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const central, jenkins = "https://test-central.invalid/maven2", "https://test-jenkins.invalid/public"
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'https://test-%'`)
	})
	cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'https://test-%'`)

	if _, stored, err := store.GetMavenPomResult(ctx, central, "g", "a", "1"); err != nil || stored {
		t.Fatalf("nothing stored yet: stored %v err %v", stored, err)
	}
	// A hit, with no expiry.
	if err := store.PutMavenPomResult(ctx, central, "g", "a", "1", true, []byte(`{"group":"g","artifact":"a","version":"1"}`), 0); err != nil {
		t.Fatal(err)
	}
	res, stored, err := store.GetMavenPomResult(ctx, central, "g", "a", "1")
	if err != nil || !stored || !res.Found || res.Expired || string(res.Extract) == "" {
		t.Errorf("a stored hit: %+v stored %v err %v", res, stored, err)
	}
	// The key is all four parts and the repository: none of these is it.
	for _, k := range [][4]string{{jenkins, "g", "a", "1"}, {central, "g2", "a", "1"}, {central, "g", "a2", "1"}, {central, "g", "a", "2"}} {
		if _, stored, _ := store.GetMavenPomResult(ctx, k[0], k[1], k[2], k[3]); stored {
			t.Errorf("%v answered from another key's row", k)
		}
	}
	// A miss, with an expiry; then expired.
	if err := store.PutMavenPomResult(ctx, jenkins, "g", "a", "1", false, nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if res, stored, _ := store.GetMavenPomResult(ctx, jenkins, "g", "a", "1"); !stored || res.Found || res.Expired {
		t.Errorf("a fresh miss: %+v stored %v", res, stored)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.maven_pom_results SET expires_at = NOW() - INTERVAL '1 second' WHERE repository_url = $1`, jenkins)
	if res, stored, _ := store.GetMavenPomResult(ctx, jenkins, "g", "a", "1"); !stored || res.Found || !res.Expired {
		t.Errorf("an expired miss is still STORED (a failed refresh serves it), and says it is due: %+v stored %v", res, stored)
	}
	// A later answer replaces the stored one: the miss becomes a hit.
	if err := store.PutMavenPomResult(ctx, jenkins, "g", "a", "1", true, []byte(`{"group":"g"}`), 0); err != nil {
		t.Fatal(err)
	}
	if res, _, _ := store.GetMavenPomResult(ctx, jenkins, "g", "a", "1"); !res.Found || res.Expired {
		t.Errorf("a miss replaced by a hit: %+v", res)
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.maven_pom_results WHERE repository_url = $1`, jenkins).Scan(&n); err != nil || n != 1 {
		t.Errorf("%d rows for one key (err %v): the write must replace, not add", n, err)
	}
}

// The host registry: first seen once, last answered each time.
func TestThePackageRepositoryHostRegistry(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const host = "test-registry-host.invalid"
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.package_repository_hosts WHERE host = $1`, host)
	})
	cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.package_repository_hosts WHERE host = $1`, host)
	if err := store.NotePackageRepositoryHostAnswered(ctx, host); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.package_repository_hosts
		SET first_fetched_at = NOW() - INTERVAL '30 days', last_answered_at = NOW() - INTERVAL '30 days' WHERE host = $1`, host)
	if err := store.NotePackageRepositoryHostAnswered(ctx, host); err != nil {
		t.Fatal(err)
	}
	var first, last time.Time
	if err := store.pool.QueryRow(ctx, `SELECT first_fetched_at, last_answered_at FROM aveloxis_ops.package_repository_hosts WHERE host = $1`, host).Scan(&first, &last); err != nil {
		t.Fatal(err)
	}
	if time.Since(first) < 29*24*time.Hour {
		t.Errorf("first_fetched_at moved to %v; it is set once", first)
	}
	if time.Since(last) > time.Hour {
		t.Errorf("last_answered_at = %v; it moves with each answer", last)
	}
	if err := store.NotePackageRepositoryHostAnswered(ctx, ""); err != nil {
		t.Errorf("an empty host: %v", err)
	}
}

// The distrusted list, and the store's own refusal: nothing is stored for a
// distrusted host, decided in the statement, so a fetch in flight when the
// host was distrusted cannot put back what a purge removed.
func TestNothingIsStoredForADistrustedHost(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const bad, good = "https://test-bad-host.invalid:8443/m2", "https://test-good-host.invalid/m2"
	clean := func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'https://test-%'`)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.distrusted_repository_hosts WHERE host LIKE 'test-%'`)
	}
	t.Cleanup(clean)
	clean()

	changed, err := store.DistrustRepositoryHost(ctx, "Test-Bad-Host.invalid:8443", "an administrator", "taken over")
	if err != nil || !changed {
		t.Fatalf("distrust: changed %v err %v", changed, err)
	}
	if changed, _ := store.DistrustRepositoryHost(ctx, "test-bad-host.invalid:8443", "someone else", ""); changed {
		t.Error("distrusting a host twice changed something; the first entry stands")
	}
	hosts, err := store.GetDistrustedRepositoryHosts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, h := range hosts {
		found = found || h == "test-bad-host.invalid:8443"
	}
	if !found {
		t.Errorf("the distrusted list = %v; want the host in its one spelling", hosts)
	}
	for _, repo := range []string{bad, good} {
		if err := store.PutMavenPomResult(ctx, repo, "g", "a", "1", true, []byte(`{"group":"g","artifact":"a"}`), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, stored, _ := store.GetMavenPomResult(ctx, bad, "g", "a", "1"); stored {
		t.Error("a result was stored for a distrusted host")
	}
	if _, stored, _ := store.GetMavenPomResult(ctx, good, "g", "a", "1"); !stored {
		t.Error("a result for another host was not stored")
	}
	// A result that got in while the host was distrusted (review round 1:
	// a write that began just before the decision committed does not see
	// it, and lands after the purge). While the host is distrusted nothing
	// serves it; a restore must not bring it back into use.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.maven_pom_results (repository_url, group_id, artifact_id, version, found, extract, fetched_at)
		VALUES ($1, 'g', 'stray', '1', TRUE, '{"group":"g","artifact":"stray"}'::jsonb, NOW())`, bad); err != nil {
		t.Fatal(err)
	}
	// Restored, it is stored again.
	if changed, err := store.RestoreRepositoryHost(ctx, "test-bad-host.invalid:8443"); err != nil || !changed {
		t.Fatalf("restore: changed %v err %v", changed, err)
	}
	if _, stored, _ := store.GetMavenPomResult(ctx, bad, "g", "stray", "1"); stored {
		t.Error("a result that arrived while the host was distrusted is in use after the restore")
	}
	if _, stored, _ := store.GetMavenPomResult(ctx, good, "g", "a", "1"); !stored {
		t.Error("the restore removed another host's result")
	}
	if changed, _ := store.RestoreRepositoryHost(ctx, "test-bad-host.invalid:8443"); changed {
		t.Error("restoring a host that is not distrusted changed something")
	}
	if err := store.PutMavenPomResult(ctx, bad, "g", "a", "1", true, []byte(`{"group":"g","artifact":"a"}`), 0); err != nil {
		t.Fatal(err)
	}
	if _, stored, _ := store.GetMavenPomResult(ctx, bad, "g", "a", "1"); !stored {
		t.Error("after the host was restored its result was still refused")
	}
	if _, err := store.DistrustRepositoryHost(ctx, "  ", "x", ""); err == nil {
		t.Error("an empty host was distrusted")
	}
}
