// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostwatch"
)

// Registration data is fetched over https only: a plain-http source is
// refused before any request leaves, and the check then says "not known".
func TestRegistrationDataIsHTTPSOnly(t *testing.T) {
	var asked atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		asked.Add(1)
		_, _ = w.Write([]byte(`{"services":[[["com"],["https://rdap.example/"]]]}`))
	}))
	t.Cleanup(srv.Close)
	if _, err := fetchRegistrationData(context.Background(), srv.URL+"/dns.json"); !errors.Is(err, errRegistrationDataNotHTTPS) {
		t.Errorf("a plain-http fetch: %v; want it refused", err)
	}
	if _, err := fetchRegistrationData(context.Background(), "https://user:secret@rdap.example/x"); !errors.Is(err, errRegistrationDataNotHTTPS) {
		t.Errorf("a URL with credentials: %v; want it refused", err)
	}
	old := rdapBootstrapURL
	rdapBootstrapURL = srv.URL + "/dns.json"
	t.Cleanup(func() { rdapBootstrapURL = old })
	o := RepositoryHostRegistration()(context.Background(), "repo.example.com", time.Now())
	if o.Kind != hostwatch.KindRegisteredLater || o.State != hostwatch.Unknown {
		t.Errorf("with an unusable bootstrap source: %+v; want not known", o)
	}
	if n := asked.Load(); n != 0 {
		t.Errorf("the plain-http server was asked %d times", n)
	}
}

// A registry's 404 is "no record", which the check reports as not known.
func TestRegistrationNotFoundIsTheRegistryMiss(t *testing.T) {
	if !errors.Is(&registryStatusError{url: "https://rdap.example/domain/x.com", status: http.StatusNotFound}, errRegistryNotFound) {
		t.Fatal("a 404 registry answer no longer unwraps to errRegistryNotFound, which the registration check relies on")
	}
}
