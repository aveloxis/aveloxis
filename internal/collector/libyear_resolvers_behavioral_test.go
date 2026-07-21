// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.30 — behavioral coverage for ALL 12 libyear resolvers against
// REAL captured registry responses (testdata/registries/*, fetched
// live 2026-07-21 and array-trimmed without reshaping). Before this,
// 7 registries had ZERO tests of any kind and were structurally
// untestable (URLs hardcoded through curl) — the audit's G2 gap,
// carrying the exact preconditions of the npm/cargo whole-ecosystem
// outages (v0.27.19: silent zero rows for the product's entire life).
// The transport is now net/http with injectable per-registry bases.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func serveFixture(t *testing.T, fixture string) *httptest.Server {
	t.Helper()
	data, err := os.ReadFile("testdata/registries/" + fixture)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/upload-time"):
			// Hackage's second call returns a bare timestamp.
			_, _ = io.WriteString(w, `"2024-06-01T10:00:00Z"`)
		default:
			_, _ = w.Write(data)
		}
	}))
}

func TestAllLibyearResolversParseRealRegistryShapes(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		fixture  string
		base     *string
		resolver func(context.Context, libyearDep) (*db.LibyearRow, error)
		dep      libyearDep
		purlWant string // canonical prefix the resolved row must carry
	}{
		{"npm", "", &npmRegistryBase, resolveNPMLibyear,
			libyearDep{Name: "lodash", Version: "4.17.0", Manager: "npm"}, "pkg:npm/lodash@"},
		{"pypi", "pypi_flask.json", &pypiRegistryBase, resolvePyPILibyear,
			libyearDep{Name: "flask", Version: "2.0.0", Manager: "pypi"}, "pkg:pypi/flask@"},
		{"goproxy", "goproxy_cobra_latest.json", &goProxyBase, resolveGoLibyear,
			libyearDep{Name: "github.com/spf13/cobra", Version: "1.8.0", Manager: "go"}, "pkg:golang/github.com/spf13/cobra@"},
		{"cargo", "crates_serde.json", &cratesRegistryBase, resolveCargoLibyear,
			libyearDep{Name: "serde", Version: "1.0.0", Manager: "cargo"}, "pkg:cargo/serde@"},
		{"rubygems", "rubygems_rails.json", &rubygemsRegistryBase, resolveRubyGemsLibyear,
			libyearDep{Name: "rails", Version: "7.0.0", Manager: "gem"}, "pkg:gem/rails@"},
		{"maven", "maven_commonslang.json", &mavenSearchBase, resolveMavenLibyear,
			libyearDep{Name: "org.apache.commons:commons-lang3", Version: "3.12.0", Manager: "maven"}, "pkg:maven/org.apache.commons/commons-lang3@"},
		{"packagist", "packagist_monolog.json", &packagistRegistryBase, resolvePackagistLibyear,
			libyearDep{Name: "monolog/monolog", Version: "3.0.0", Manager: "composer"}, "pkg:composer/monolog/monolog@"},
		{"hex", "hex_phoenix.json", &hexRegistryBase, resolveHexLibyear,
			libyearDep{Name: "phoenix", Version: "1.7.0", Manager: "hex"}, "pkg:hex/phoenix@"},
		{"nuget", "nuget_newtonsoft.json", &nugetRegistryBase, resolveNuGetLibyear,
			libyearDep{Name: "Newtonsoft.Json", Version: "13.0.0", Manager: "nuget"}, "pkg:nuget/newtonsoft.json@"},
		{"pubdev", "pubdev_http.json", &pubDevRegistryBase, resolvePubDevLibyear,
			libyearDep{Name: "http", Version: "1.0.0", Manager: "pub"}, "pkg:pub/http@"},
		{"hackage", "hackage_aeson_preferred.json", &hackageRegistryBase, resolveHackageLibyear,
			libyearDep{Name: "aeson", Version: "2.0.0.0", Manager: "haskell"}, "pkg:hackage/aeson@"},
		{"swiftpm", "github_alamofire_latest.json", &githubAPIBase, resolveSwiftPMLibyear,
			// SwiftPM carries the git URL in Requirement (Package.swift's
			// .package(url:)) — Name holds the package label.
			libyearDep{Name: "Alamofire", Version: "5.8.0", Manager: "swift",
				Requirement: "https://github.com/Alamofire/Alamofire.git"}, "pkg:swift/"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var srv *httptest.Server
			if tc.fixture != "" {
				srv = serveFixture(t, tc.fixture)
			} else {
				// npm's fixture-driven suite exists since v0.27.19
				// (libyear_registry_test.go) — here it rides a minimal
				// real-shaped body purely so the table covers all 12.
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.WriteString(w, `{"dist-tags":{"latest":"4.17.21"},"time":{"4.17.21":"2021-02-20T15:42:16.891Z","4.17.0":"2016-12-31T22:33:53.421Z"},"license":"MIT"}`)
				}))
			}
			defer srv.Close()
			old := *tc.base
			*tc.base = srv.URL
			defer func() { *tc.base = old }()

			row, err := tc.resolver(ctx, tc.dep)
			if err != nil {
				t.Fatalf("%s resolver error against REAL captured shape: %v", tc.name, err)
			}
			if row == nil {
				t.Fatalf("%s: nil row", tc.name)
			}
			if row.LatestVersion == "" {
				t.Errorf("%s: LatestVersion empty — the parser read nothing from the real registry shape (the silent-zero-rows failure mode)", tc.name)
			}
			if !strings.HasPrefix(row.Purl, tc.purlWant) {
				t.Errorf("%s: purl %q, want canonical prefix %q", tc.name, row.Purl, tc.purlWant)
			}
		})
	}
}
