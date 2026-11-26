// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// The repository-hosts page of the administrator console (summary/70, 4.5,
// D14): the hosts poms were fetched from, what depends on each, and the two
// actions, each behind its own confirm step.
func TestRepositoryHostsPageListsDistrustsAndRestores(t *testing.T) {
	f := newConsoleFixture(t)
	ctx := context.Background()
	const host = "test-web-host.invalid"
	clean := func() {
		c := context.Background()
		_, _ = f.store.RestoreRepositoryHost(c, host)
		_, _ = f.store.Pool().Exec(c, `DELETE FROM aveloxis_ops.package_repository_hosts WHERE host LIKE 'test-web-%'`)
		_, _ = f.store.Pool().Exec(c, `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'https://test-web-%'`)
	}
	t.Cleanup(clean)
	clean()
	if err := f.store.NotePackageRepositoryHostAnswered(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := f.store.PutMavenPomResult(ctx, "https://"+host+"/m2", "g", "a", "1", true, []byte(`{"group":"g","artifact":"a"}`), 0); err != nil {
		t.Fatal(err)
	}
	f.srv.WithRepositoryHosts([]string{"test-web-configured.invalid"})

	// Administrator-only, every route.
	for _, path := range []string{"/admin/repository-hosts", "/admin/repository-hosts/distrust?host=" + host, "/admin/repository-hosts/restore?host=" + host} {
		if w := f.get(path, "user-tok"); w.Code != http.StatusForbidden {
			t.Errorf("a non-admin's GET %s = %d; want 403", path, w.Code)
		}
	}
	for _, path := range []string{"/admin/repository-hosts/distrust", "/admin/repository-hosts/restore"} {
		if w := f.post(path, "user-tok", url.Values{"host": {host}}); w.Code != http.StatusForbidden {
			t.Errorf("a non-admin's POST %s = %d; want 403", path, w.Code)
		}
	}
	if hosts, _ := f.store.GetDistrustedRepositoryHosts(ctx); len(hosts) != 0 && strings.Contains(strings.Join(hosts, " "), host) {
		t.Fatal("a non-admin's request distrusted the host")
	}

	// The list: the host that answered, and the configured one that has not.
	w := f.get("/admin/repository-hosts", "admin-tok")
	body := w.Body.String()
	if w.Code != http.StatusOK || !strings.Contains(body, host) || !strings.Contains(body, "test-web-configured.invalid") {
		t.Fatalf("GET /admin/repository-hosts = %d; want the page with both hosts\n%s", w.Code, body)
	}
	if !strings.Contains(body, "/admin/repository-hosts/distrust?host="+host) {
		t.Error("the page offers no distrust action for the host")
	}

	// The confirm step says what will happen and changes nothing. The
	// action itself is a separate control: a POST from that page.
	w = f.get("/admin/repository-hosts/distrust?host="+host, "admin-tok")
	body = w.Body.String()
	if w.Code != http.StatusOK || !strings.Contains(body, "1 stored result") || !strings.Contains(body, `action="/admin/repository-hosts/distrust"`) {
		t.Errorf("the distrust confirm page = %d; want what the distrust will do, and the form that does it\n%s", w.Code, body)
	}
	if hosts, _ := f.store.GetDistrustedRepositoryHosts(ctx); strings.Contains(strings.Join(hosts, " "), host) {
		t.Fatal("opening the confirm page distrusted the host")
	}

	if w := f.post("/admin/repository-hosts/distrust", "admin-tok", url.Values{"host": {host}, "note": {"abandoned"}}); w.Code != http.StatusFound {
		t.Fatalf("POST distrust = %d\n%s", w.Code, w.Body.String())
	}
	hosts, _ := f.store.GetDistrustedRepositoryHosts(ctx)
	if !strings.Contains(strings.Join(hosts, " "), host) {
		t.Fatal("the host was not distrusted")
	}
	if _, stored, _ := f.store.GetMavenPomResult(ctx, "https://"+host+"/m2", "g", "a", "1"); stored {
		t.Error("the host's stored result was not purged")
	}
	body = f.get("/admin/repository-hosts", "admin-tok").Body.String()
	if !strings.Contains(body, "/admin/repository-hosts/restore?host="+host) || strings.Contains(body, "/admin/repository-hosts/distrust?host="+host) {
		t.Error("a distrusted host must offer restore, and not distrust")
	}

	// Restore, the same two steps.
	if w := f.get("/admin/repository-hosts/restore?host="+host, "admin-tok"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `action="/admin/repository-hosts/restore"`) {
		t.Errorf("the restore confirm page = %d", w.Code)
	}
	if w := f.post("/admin/repository-hosts/restore", "admin-tok", url.Values{"host": {host}}); w.Code != http.StatusFound {
		t.Fatalf("POST restore = %d\n%s", w.Code, w.Body.String())
	}
	if hosts, _ := f.store.GetDistrustedRepositoryHosts(ctx); strings.Contains(strings.Join(hosts, " "), host) {
		t.Error("the host is still distrusted after the restore")
	}

	// What is not a host is refused, and never echoed into the page.
	if w := f.get("/admin/repository-hosts/distrust?host="+url.QueryEscape(`"><script>x</script>`), "admin-tok"); w.Code != http.StatusBadRequest || strings.Contains(w.Body.String(), "<script>x") {
		t.Errorf("a host that is not one = %d; want 400 without the value echoed", w.Code)
	}
}

// The attention list: a host with a signal is listed first with what was
// seen; marking it as seen takes it off that list and changes nothing else;
// a new signal brings it back; "could not tell" is shown as not known.
func TestRepositoryHostsPageAttentionList(t *testing.T) {
	f := newConsoleFixture(t)
	ctx := context.Background()
	const host = "test-web-signal.invalid"
	clean := func() {
		c := context.Background()
		_, _ = f.store.Pool().Exec(c, `DELETE FROM aveloxis_ops.repository_host_signals WHERE host LIKE 'test-web-%'`)
		_, _ = f.store.Pool().Exec(c, `DELETE FROM aveloxis_ops.package_repository_hosts WHERE host LIKE 'test-web-%'`)
		_, _ = f.store.RestoreRepositoryHost(c, host)
	}
	t.Cleanup(clean)
	clean()
	if err := f.store.NotePackageRepositoryHostAnswered(ctx, host); err != nil {
		t.Fatal(err)
	}
	record := func(kind, state, detail string) {
		t.Helper()
		if err := f.store.RecordRepositoryHostObservations(ctx, host, []db.HostObservation{{Kind: kind, State: state, Detail: detail}}); err != nil {
			t.Fatal(err)
		}
	}
	// section is the page's card under a heading, up to the next card.
	section := func(heading string) string {
		t.Helper()
		body := f.get("/admin/repository-hosts", "admin-tok").Body.String()
		i := strings.Index(body, "<h2>"+heading+"</h2>")
		if i < 0 {
			t.Fatalf("the page has no %q section", heading)
		}
		rest := body[i+1:]
		if j := strings.Index(rest, "<h2>"); j >= 0 {
			rest = rest[:j]
		}
		return rest
	}

	record("certificate", "unknown", "connection refused")
	if strings.Contains(section("Needs a decision"), host) {
		t.Error("a host whose check could not tell is on the attention list: not known is never a signal")
	}
	if s := section("Declared by collected repositories"); !strings.Contains(s, host) || !strings.Contains(s, "not known at the last look: whether the certificate is valid") {
		t.Errorf("a check that could not tell must be shown as not known\n%s", s)
	}

	record("unresolvable", "signal", "no such host")
	att := section("Needs a decision")
	if !strings.Contains(att, host) || !strings.Contains(att, "the name no longer resolves, since ") || !strings.Contains(att, "/admin/repository-hosts/distrust?host="+host) {
		t.Fatalf("a host with a signal is not on the attention list with what was seen and the distrust action\n%s", att)
	}

	if w := f.post("/admin/repository-hosts/dismiss", "user-tok", url.Values{"host": {host}}); w.Code != http.StatusForbidden {
		t.Errorf("a non-admin's dismiss = %d; want 403", w.Code)
	}
	if !strings.Contains(section("Needs a decision"), host) {
		t.Fatal("a non-admin's dismiss took the host off the list")
	}
	if w := f.post("/admin/repository-hosts/dismiss", "admin-tok", url.Values{"host": {host}}); w.Code != http.StatusFound {
		t.Fatalf("dismiss = %d\n%s", w.Code, w.Body.String())
	}
	if strings.Contains(section("Needs a decision"), host) {
		t.Error("a dismissed host is still on the attention list")
	}
	// Dismissing changed nothing else: the host is not distrusted, and its
	// signal is still shown where the host is now listed.
	if hosts, _ := f.store.GetDistrustedRepositoryHosts(ctx); strings.Contains(strings.Join(hosts, " "), host) {
		t.Fatal("a dismissal distrusted the host")
	}
	if s := section("Declared by collected repositories"); !strings.Contains(s, "the name no longer resolves") {
		t.Error("a dismissed signal is no longer shown at all; it is seen, not gone")
	}
	// A new signal returns it.
	record("certificate", "signal", "expired")
	if !strings.Contains(section("Needs a decision"), "the certificate is not valid for this host") {
		t.Error("a new signal did not bring the host back to the attention list")
	}
}

// Distrusting a configured repository says that it is one.
func TestDistrustConfirmNamesAConfiguredRepository(t *testing.T) {
	f := newConsoleFixture(t)
	f.srv.WithRepositoryHosts([]string{"Test-Web-Mirror.invalid"})
	if body := f.get("/admin/repository-hosts/distrust?host=test-web-mirror.invalid", "admin-tok").Body.String(); !strings.Contains(body, "one of the configured repositories") {
		t.Errorf("the confirm page for a configured host does not say so\n%s", body)
	}
	if body := f.get("/admin/repository-hosts/distrust?host=test-web-other.invalid", "admin-tok").Body.String(); strings.Contains(body, "one of the configured repositories") {
		t.Error("the confirm page calls a host that is not configured a configured repository")
	}
}
