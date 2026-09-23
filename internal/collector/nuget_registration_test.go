// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// TestNuGetResolverReadsTheSemVer2HivePagesAndStableLatest — 2026-09-23
// log review, three defects in resolveNuGetLibyear, probed live:
//  1. It read registration5-semver1, which omits every package whose
//     versions are SemVer 2 only: microsoft.semantickernel.agents.openai
//     and azure.core.experimental 404 there and resolve on
//     registration5-gz-semver2.
//  2. For a large package NuGet inlines NO page (Microsoft.Extensions.*,
//     System.Text.Json, AWSSDK.Core): the resolver never fetched a page by
//     its @id, so the latest version and the current date were empty and
//     libyear was unknown for the most common .NET packages.
//  3. "Latest" was the last entry, a prerelease (Newtonsoft.Json's latest
//     read 14.0.1-beta2), so libyear was measured against a beta. Latest is
//     the newest LISTED STABLE version; a package with no stable version
//     falls back to its newest listed one.
func TestNuGetResolverReadsTheSemVer2HivePagesAndStableLatest(t *testing.T) {
	var hits []string
	var srv *httptest.Server
	entry := func(v, published string, listed bool) string {
		return fmt.Sprintf(`{"catalogEntry":{"version":%q,"published":%q,"listed":%t,"licenseExpression":"MIT"}}`, v, published, listed)
	}
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.Path)
		switch r.URL.Path {
		case "/v3/registration5-gz-semver2/example.big/index.json":
			// Two pages, neither inlined: the resolver must fetch both.
			_, _ = io.WriteString(w, `{"items":[`+
				`{"@id":"`+srv.URL+`/page/1.json","lower":"1.0.0","upper":"2.0.0"},`+
				`{"@id":"`+srv.URL+`/page/2.json","lower":"2.1.0","upper":"3.0.0-beta1"}]}`)
		case "/page/1.json":
			_, _ = io.WriteString(w, `{"items":[`+entry("1.0.0", "2020-01-01T00:00:00Z", true)+`,`+entry("2.0.0", "2021-01-01T00:00:00Z", true)+`]}`)
		case "/page/2.json":
			_, _ = io.WriteString(w, `{"items":[`+entry("2.1.0", "2022-01-01T00:00:00Z", true)+`,`+
				entry("2.2.0", "2022-06-01T00:00:00Z", false)+`,`+ // unlisted: never "latest"
				entry("3.0.0-beta1", "2023-01-01T00:00:00Z", true)+`]}`) // prerelease: never "latest"
		case "/v3/registration5-gz-semver2/example.unlisted/index.json":
			// NuGet publishes an unlisted version as 1900-01-01.
			_, _ = io.WriteString(w, `{"items":[{"@id":"x","items":[`+entry("1.0.0", "1900-01-01T00:00:00+00:00", false)+`,`+entry("2.0.0", "2022-01-01T00:00:00Z", true)+`]}]}`)
		case "/v3/registration5-gz-semver2/example.pre/index.json":
			_, _ = io.WriteString(w, `{"items":[{"@id":"x","items":[`+entry("0.1.0-preview.1", "2024-01-01T00:00:00Z", true)+`,`+entry("0.1.0-preview.2", "2024-03-01T00:00:00Z", true)+`]}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	old := nugetRegistryBase
	nugetRegistryBase = srv.URL
	defer func() { nugetRegistryBase = old }()

	row, err := resolveNuGetLibyear(context.Background(), libyearDep{Name: "Example.Big", Version: "1.0.0", Manager: "nuget"})
	if err != nil {
		t.Fatalf("resolve: %v (requests: %v)", err, hits)
	}
	if !strings.Contains(strings.Join(hits, " "), "/v3/registration5-gz-semver2/") {
		t.Errorf("must read the SemVer-2 hive, requested %v", hits)
	}
	if row.LatestVersion != "2.1.0" || row.LatestReleaseDate != "2022-01-01T00:00:00Z" {
		t.Errorf("latest = %q @ %q, want the newest listed STABLE 2.1.0 (not the unlisted 2.2.0 or the beta)", row.LatestVersion, row.LatestReleaseDate)
	}
	if row.CurrentReleaseDate != "2020-01-01T00:00:00Z" {
		t.Errorf("current date = %q, want 1.0.0's — found only by fetching the non-inlined page", row.CurrentReleaseDate)
	}

	row, err = resolveNuGetLibyear(context.Background(), libyearDep{Name: "Example.Unlisted", Version: "1.0.0", Manager: "nuget"})
	if err != nil {
		t.Fatal(err)
	}
	if row.CurrentReleaseDate != "" {
		t.Errorf("a pin on an unlisted version has an unknown date, not NuGet's 1900 placeholder: %q", row.CurrentReleaseDate)
	}

	row, err = resolveNuGetLibyear(context.Background(), libyearDep{Name: "Example.Pre", Version: "0.1.0-preview.1", Manager: "nuget"})
	if err != nil {
		t.Fatal(err)
	}
	if row.LatestVersion != "0.1.0-preview.2" {
		t.Errorf("a package with no stable version falls back to its newest listed one, got %q", row.LatestVersion)
	}
}

// TestNuGetResolverLiveCanary (AVELOXIS_TEST_NETWORK=1) pairs the mock
// with nuget.org: the SemVer-2-only packages from the 2026-09-23 log now
// resolve, a package with non-inlined pages yields a latest version and a
// current date, and Newtonsoft.Json's latest is stable.
func TestNuGetResolverLiveCanary(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("set AVELOXIS_TEST_NETWORK=1 to query nuget.org")
	}
	ctx := context.Background()
	for _, dep := range []libyearDep{
		{Name: "Microsoft.SemanticKernel.Agents.OpenAI", Version: "1.30.0-alpha", Manager: "nuget"},
		{Name: "Azure.Core.Experimental", Version: "0.1.0-preview.30", Manager: "nuget"},
		{Name: "System.Text.Json", Version: "6.0.0", Manager: "nuget"},
		{Name: "Newtonsoft.Json", Version: "13.0.1", Manager: "nuget"},
	} {
		row, err := resolveNuGetLibyear(ctx, dep)
		if err != nil {
			t.Errorf("%s: %v", dep.Name, err)
			continue
		}
		if row.LatestVersion == "" || row.LatestReleaseDate == "" {
			t.Errorf("%s: no latest version (%q @ %q)", dep.Name, row.LatestVersion, row.LatestReleaseDate)
		}
		if dep.Name == "System.Text.Json" || dep.Name == "Newtonsoft.Json" {
			if !nugetIsStable(row.LatestVersion) {
				t.Errorf("%s: latest %q is a prerelease", dep.Name, row.LatestVersion)
			}
			if row.CurrentReleaseDate == "" {
				t.Errorf("%s: current version %s has no release date — a page was not fetched", dep.Name, dep.Version)
			}
		}
		t.Logf("%s %s → latest %s (%s), current date %s", dep.Name, dep.Version, row.LatestVersion, row.LatestReleaseDate, row.CurrentReleaseDate)
	}
}

// TestNPMResolverToleratesAnUnpublishedTimeEntry — 2026-09-23 log review:
// 4 npm resolutions failed with "json: cannot unmarshal object into Go
// struct field .time.unpublished of type string". A package with an
// unpublish record carries time.unpublished as an OBJECT beside the
// per-version date strings; the decode must skip it, not fail the package.
func TestNPMResolverToleratesAnUnpublishedTimeEntry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"dist-tags":{"latest":"2.0.0"},"time":{"1.0.0":"2020-01-01T00:00:00Z","2.0.0":"2022-01-01T00:00:00Z","unpublished":{"time":"2023-01-01T00:00:00Z","versions":["3.0.0"]}},"license":"MIT"}`)
	}))
	defer srv.Close()
	old := npmRegistryBase
	npmRegistryBase = srv.URL
	defer func() { npmRegistryBase = old }()
	row, err := resolveNPMLibyear(context.Background(), libyearDep{Name: "left-pad", Version: "1.0.0", Manager: "npm"})
	if err != nil {
		t.Fatalf("an unpublish record must not fail the package: %v", err)
	}
	if row.LatestVersion != "2.0.0" || row.CurrentReleaseDate != "2020-01-01T00:00:00Z" || row.LatestReleaseDate != "2022-01-01T00:00:00Z" {
		t.Errorf("row: %+v", row)
	}
}
