// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.56 — the Go module proxy case-encodes module paths and versions
// (every uppercase letter becomes '!' + its lowercase form). The
// resolver sent paths verbatim, so proxy.golang.org answered 404 for
// every module with an uppercase letter (github.com/Masterminds/semver/v3,
// github.com/Azure/go-ansiterm, …): 157 WARN lines and 1,883 dependencies
// in two hours of the 2026-09-17 chaoss.tv log, and zero Go libyear rows
// with an uppercase module name in production. Verified live the same
// day: /github.com/Masterminds/semver/v3/@latest → 404,
// /github.com/!masterminds/semver/v3/@latest → 200.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestGoProxyEscape(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{in: "github.com/spf13/cobra", want: "github.com/spf13/cobra"},
		{in: "github.com/Masterminds/semver/v3", want: "github.com/!masterminds/semver/v3"},
		{in: "github.com/Azure/azure-sdk-for-go/sdk/azcore", want: "github.com/!azure/azure-sdk-for-go/sdk/azcore"},
		{in: "github.com/kubearmor/KubeArmor/protobuf", want: "github.com/kubearmor/!kube!armor/protobuf"},
		{in: "v1.0.0-RC1", want: "v1.0.0-!r!c1"},
		{in: "v0.0.0-20240101000000-abcdef123456", want: "v0.0.0-20240101000000-abcdef123456"},
		{in: "", want: ""},
		// '!' is the escape character itself and is not legal in a module
		// path or version; non-ASCII is not legal either. Either would make
		// the escaped form ambiguous, so refuse instead of guessing.
		{in: "github.com/foo!bar/baz", wantErr: true},
		{in: "github.com/foö/bar", wantErr: true},
	}
	for _, tc := range cases {
		got, err := goProxyEscape(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("goProxyEscape(%q) = %q, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("goProxyEscape(%q) error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("goProxyEscape(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestResolveGoLibyearEscapesUppercaseModulePaths drives the resolver
// against a proxy that behaves like proxy.golang.org: only the
// case-encoded path exists. Both lookups (@latest and the pinned
// version's .info) must use the encoded form; the stored name and purl
// keep the module's real spelling.
func TestResolveGoLibyearEscapesUppercaseModulePaths(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/github.com/!masterminds/semver/v3/@latest":
			_, _ = io.WriteString(w, `{"Version":"v3.3.1","Time":"2024-11-19T20:18:15Z"}`)
		case "/github.com/!masterminds/semver/v3/@v/v3.2.0.info":
			_, _ = io.WriteString(w, `{"Version":"v3.2.0","Time":"2022-11-15T20:59:21Z"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	old := goProxyBase
	goProxyBase = srv.URL
	defer func() { goProxyBase = old }()

	row, err := resolveGoLibyear(context.Background(),
		libyearDep{Name: "github.com/Masterminds/semver/v3", Version: "v3.2.0", Manager: "go"})
	if err != nil {
		t.Fatalf("resolveGoLibyear: %v (requested paths %v)", err, paths)
	}
	if row.LatestVersion != "3.3.1" {
		t.Errorf("LatestVersion = %q, want 3.3.1", row.LatestVersion)
	}
	if row.CurrentReleaseDate == "" {
		t.Errorf("CurrentReleaseDate empty: the pinned version's .info lookup was not case-encoded (paths %v)", paths)
	}
	if row.Name != "github.com/Masterminds/semver/v3" {
		t.Errorf("Name = %q: the stored name must keep the module's real spelling", row.Name)
	}
	// The purl is built from the real name exactly as before (buildPurl's
	// own golang normalization), never from the proxy encoding.
	if want := buildPurl("golang", "github.com/Masterminds/semver/v3", "v3.2.0"); row.Purl != want {
		t.Errorf("Purl = %q, want %q", row.Purl, want)
	}
	if strings.Contains(row.Purl, "!") {
		t.Errorf("Purl = %q carries the proxy's case encoding", row.Purl)
	}
	if row.Libyear <= 0 {
		t.Errorf("Libyear = %v, want > 0 (v3.2.0 was released two years before v3.3.1)", row.Libyear)
	}
}

// TestResolveGoLibyearPinnedVersionInfoPath pins the .info URL for the
// version in use. go.mod versions keep their "v" (parseGoModVersions,
// for purls and OSV), and the resolver prefixed another one, requesting
// @v/vv1.8.0.info. The proxy 404'd and the error was swallowed, so on
// 2026-09-17 0 of 825,136 Go libyear rows written in the previous 7 days
// had a current release date, and every Go libyear was 0.
func TestResolveGoLibyearPinnedVersionInfoPath(t *testing.T) {
	for _, version := range []string{"v1.8.0", "1.8.0"} {
		t.Run(version, func(t *testing.T) {
			var infoPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/@latest"):
					_, _ = io.WriteString(w, `{"Version":"v1.8.1","Time":"2024-06-22T17:02:30Z"}`)
				case r.URL.Path == "/github.com/spf13/cobra/@v/v1.8.0.info":
					infoPath = r.URL.Path
					_, _ = io.WriteString(w, `{"Version":"v1.8.0","Time":"2023-11-04T23:13:39Z"}`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			old := goProxyBase
			goProxyBase = srv.URL
			defer func() { goProxyBase = old }()

			row, err := resolveGoLibyear(context.Background(),
				libyearDep{Name: "github.com/spf13/cobra", Version: version, Manager: "go"})
			if err != nil {
				t.Fatalf("resolveGoLibyear: %v", err)
			}
			if infoPath == "" || row.CurrentReleaseDate == "" {
				t.Fatalf("the pinned version's .info was never found (CurrentReleaseDate %q)", row.CurrentReleaseDate)
			}
			if row.Libyear <= 0 {
				t.Errorf("Libyear = %v, want > 0", row.Libyear)
			}
		})
	}
}

// TestResolveGoLibyearFailsWhenTheVersionDateSaysNothing: the pinned
// version's .info failing WITHOUT an answer (timeout, 5xx, rate limit) must
// fail the dependency, not store libyear 0. Storing it reads as "perfectly
// fresh", and since v0.29.56 the answer is cached for a day — one blip
// would fabricate that 0 for every repo in the process. A definitive miss
// (the proxy says that version does not exist) costs only the date.
func TestResolveGoLibyearFailsWhenTheVersionDateSaysNothing(t *testing.T) {
	serve := func(infoStatus int) (*db.LibyearRow, error) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/@latest") {
				_, _ = io.WriteString(w, `{"Version":"v1.8.1","Time":"2024-06-22T17:02:30Z"}`)
				return
			}
			w.WriteHeader(infoStatus)
		}))
		defer srv.Close()
		old := goProxyBase
		goProxyBase = srv.URL
		defer func() { goProxyBase = old }()
		return resolveGoLibyear(context.Background(),
			libyearDep{Name: "github.com/spf13/cobra", Version: "v1.8.0", Manager: "go"})
	}

	// 500, not 503: a 503 enters doRegistryRequest's Retry-After ladder and
	// this test would spend ~15 s of real time sleeping through it (the
	// registrySleep seam exists so tests assert waits without paying them).
	// Both are failures that say nothing, which is what is under test here;
	// the retry ladder has its own tests.
	row, err := serve(http.StatusInternalServerError)
	if err == nil {
		t.Errorf("a 500 on the version's .info must fail the dependency, got row %+v", row)
	}
	if isDefinitiveRegistryMiss(err) {
		t.Errorf("err = %v must not be a definitive miss (it would be cached)", err)
	}

	// The proxy answering "no such version" is an answer: keep the row, lose
	// the date (and with it the libyear, which stays 0 honestly).
	row, err = serve(http.StatusNotFound)
	if err != nil {
		t.Fatalf("a 404 on the version's .info must not fail the dependency: %v", err)
	}
	if row.LatestVersion != "1.8.1" || row.CurrentReleaseDate != "" {
		t.Errorf("row = %+v, want the latest version with no current date", row)
	}
}
