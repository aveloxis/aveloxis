// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// One function says why a row has a libyear or lacks one (summary/70,
// 4.1.8), from the dependency, the resolver's row and the resolver's error.
func TestLibyearStatus(t *testing.T) {
	dated := &model.LibyearRow{CurrentReleaseDate: "2021-01-01T00:00:00Z", LatestReleaseDate: "2024-01-01T00:00:00Z"}
	noCurrent := &model.LibyearRow{LatestReleaseDate: "2024-01-01T00:00:00Z"}
	noLatest := &model.LibyearRow{CurrentReleaseDate: "2021-01-01T00:00:00Z"}
	unreadable := &model.LibyearRow{CurrentReleaseDate: "last Tuesday", LatestReleaseDate: "2024-01-01T00:00:00Z"}
	pinned := model.LibyearDep{Name: "x", Manager: "npm", Version: "1.0.0"}
	loose := model.LibyearDep{Name: "x", Manager: "npm"}
	cases := []struct {
		name string
		dep  model.LibyearDep
		row  *model.LibyearRow
		err  error
		want string
	}{
		{"both dates", pinned, dated, nil, model.LibyearResolved},
		{"pinned, no date for the version", pinned, noCurrent, nil, model.LibyearNoReleaseDate},
		{"pinned, no date for the latest", pinned, noLatest, nil, model.LibyearNoReleaseDate},
		{"pinned, a date the parser cannot read", pinned, unreadable, nil, model.LibyearNoReleaseDate},
		{"no version declared", loose, noCurrent, nil, model.LibyearNoVersion},
		// A row with both dates and no declared version cannot be built by a
		// resolver; if one ever is, the dates decide.
		{"no version declared, yet dated", loose, dated, nil, model.LibyearResolved},
		{"a GitHub Action has no timeline", model.LibyearDep{Name: "actions/checkout", Manager: "githubactions", Version: "v4"}, &model.LibyearRow{NoLibyear: true}, nil, model.LibyearNoTimeline},
		{"a GitHub Action, whatever else is said", model.LibyearDep{Manager: "githubactions"}, nil, errors.New("x"), model.LibyearNoTimeline},
		{"the registry says the package does not exist", pinned, nil, &registryStatusError{url: "u", status: http.StatusNotFound}, model.LibyearNotOnRegistry},
		{"gone is the same answer", pinned, nil, &registryStatusError{url: "u", status: http.StatusGone}, model.LibyearNotOnRegistry},
		{"a wrapped miss", pinned, nil, fmt.Errorf("maven metadata for x: %w", errRegistryNotFound), model.LibyearNotOnRegistry},
		{"a forge's definitive answer", pinned, nil, fmt.Errorf("release lookup: %w", platform.ErrNotFound), model.LibyearNotOnRegistry},
		{"a coordinate that cannot be asked", pinned, nil, fmt.Errorf("invalid maven coordinate: x: %w", errRegistryCannotBeAsked), model.LibyearCannotBeAsked},
		{"a server error is no answer", pinned, nil, &registryStatusError{url: "u", status: http.StatusBadGateway}, model.LibyearLookupFailed},
		{"a rate limit is no answer", pinned, nil, &registryStatusError{url: "u", status: http.StatusTooManyRequests}, model.LibyearLookupFailed},
		{"any other error is no answer", pinned, nil, errors.New("unexpected end of JSON input"), model.LibyearLookupFailed},
		// A resolver that returns no row and no error has said nothing.
		{"no row and no error", pinned, nil, nil, model.LibyearLookupFailed},
		// An error wins over a row: a resolver that fails returns no usable row.
		{"an error beside a row", pinned, dated, errors.New("boom"), model.LibyearLookupFailed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := libyearStatus(c.dep, c.row, c.err); got != c.want {
				t.Errorf("libyearStatus = %q, want %q", got, c.want)
			}
		})
	}
}

// settleLibyear is what the collection loop applies to a resolved row: the
// flag, the number and the status agree, by construction.
func TestSettleLibyear(t *testing.T) {
	pinned := model.LibyearDep{Name: "x", Manager: "npm", Version: "1.0.0"}

	ok := &model.LibyearRow{CurrentReleaseDate: "2021-01-01T00:00:00Z", LatestReleaseDate: "2024-01-01T00:00:00Z", Libyear: 3.0}
	settleLibyear(pinned, ok)
	if ok.NoLibyear || ok.Libyear != 3.0 || ok.Status != model.LibyearResolved {
		t.Errorf("a dated row = %+v; want its number kept and status resolved", ok)
	}

	// calcLibyear returns 0 for a missing date; stored as a number that
	// would read "perfectly fresh" (v0.29.57).
	undated := &model.LibyearRow{LatestReleaseDate: "2024-01-01T00:00:00Z", Libyear: 0}
	settleLibyear(pinned, undated)
	if !undated.NoLibyear || undated.Status != model.LibyearNoReleaseDate {
		t.Errorf("an undated row = %+v; want NoLibyear and no_release_date", undated)
	}

	// Never a number beside the NULL.
	stray := &model.LibyearRow{CurrentReleaseDate: "nonsense", LatestReleaseDate: "2024-01-01T00:00:00Z", Libyear: 7.5}
	settleLibyear(pinned, stray)
	if !stray.NoLibyear || stray.Libyear != 0 {
		t.Errorf("an unreadable date = %+v; want NoLibyear and the number cleared", stray)
	}

	action := &model.LibyearRow{NoLibyear: true}
	settleLibyear(model.LibyearDep{Manager: "githubactions", Version: "v4"}, action)
	if !action.NoLibyear || action.Status != model.LibyearNoTimeline {
		t.Errorf("an Action's row = %+v; want NoLibyear kept and no_timeline", action)
	}

	settleLibyear(pinned, nil) // must not panic
}

// The status and the stored flag never disagree: a row is NULL exactly when
// its status is not "resolved".
func TestLibyearStatusAgreesWithTheStoredFlag(t *testing.T) {
	n := 0
	for _, manager := range []string{"npm", "githubactions"} {
		for _, version := range []string{"", "1.0.0"} {
			for _, cur := range []string{"", "2021-01-01T00:00:00Z", "nonsense"} {
				for _, lat := range []string{"", "2024-01-01T00:00:00Z"} {
					dep := model.LibyearDep{Manager: manager, Version: version}
					row := &model.LibyearRow{CurrentReleaseDate: cur, LatestReleaseDate: lat, NoLibyear: manager == "githubactions"}
					settleLibyear(dep, row)
					n++
					if row.NoLibyear != (row.Status != model.LibyearResolved) {
						t.Errorf("%+v: NoLibyear %v with status %q", dep, row.NoLibyear, row.Status)
					}
				}
			}
		}
	}
	if n != 24 {
		t.Errorf("examined %d rows, want 24", n)
	}
}

// "Cannot be asked" is an answer, as "does not exist" is: both are cached, so
// the question is not put again for a day. A failure that says nothing is
// never cached (SR-5).
func TestCacheableRegistryAnswers(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errRegistryNotFound, true},
		{fmt.Errorf("x: %w", errRegistryCannotBeAsked), true},
		{&registryStatusError{url: "u", status: http.StatusNotFound}, true},
		{&registryStatusError{url: "u", status: http.StatusBadGateway}, false},
		{errors.New("timeout"), false},
		{context.DeadlineExceeded, false},
	} {
		if got := isCacheableRegistryAnswer(c.err); got != c.want {
			t.Errorf("isCacheableRegistryAnswer(%v) = %v, want %v", c.err, got, c.want)
		}
	}
	// And "cannot be asked" is not "does not exist".
	if isDefinitiveRegistryMiss(errRegistryCannotBeAsked) {
		t.Error("a coordinate that cannot be asked is not a miss: nothing was asked")
	}

	cache := newTTLCache[*model.LibyearRow](registryCacheTTL)
	calls := 0
	resolve := func(context.Context, model.LibyearDep) (*model.LibyearRow, error) {
		calls++
		return nil, fmt.Errorf("invalid maven coordinate: x: %w", errRegistryCannotBeAsked)
	}
	dep := model.LibyearDep{Name: "x", Manager: "maven", Version: "1"}
	for range 3 {
		if _, err := resolveLibyearCached(context.Background(), cache, dep, resolve); !errors.Is(err, errRegistryCannotBeAsked) {
			t.Fatalf("err = %v", err)
		}
	}
	if calls != 1 {
		t.Errorf("the resolver ran %d times for one uncacheable-looking coordinate; \"cannot be asked\" must be cached", calls)
	}
}

// The coordinates the resolvers refuse before any request are typed.
func TestResolversTypeWhatCannotBeAsked(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name string
		call func() error
	}{
		{"a Go module path that cannot be case-encoded", func() error {
			_, err := resolveGoLibyear(ctx, model.LibyearDep{Name: "github.com/x/y!z", Manager: "go", Version: "v1.0.0"})
			return err
		}},
		{"a Maven coordinate with no group", func() error {
			_, err := resolveMavenLibyear(ctx, model.LibyearDep{Name: "artifact-only", Manager: "maven", Version: "1"})
			return err
		}},
		{"a Maven coordinate with an unresolved property", func() error {
			_, err := resolveMavenLibyear(ctx, model.LibyearDep{Name: "${project.groupId}:core", Manager: "maven", Version: "1"})
			return err
		}},
		{"a SwiftPM package on a host the resolver does not support", func() error {
			_, err := resolveSwiftPMLibyear(ctx, nil, model.LibyearDep{Name: "x", Manager: "swiftpm", Version: "1.0.0", Requirement: "https://example.org/a/b.git"})
			return err
		}},
	} {
		if err := c.call(); !errors.Is(err, errRegistryCannotBeAsked) {
			t.Errorf("%s: err = %v; want it to wrap errRegistryCannotBeAsked", c.name, err)
		}
	}
}

// What each resolver establishes about the declared version, apart from its
// date. The four that cannot tell a version they lack from a version with no
// date never claim a miss.
func TestResolversReportWhetherTheVersionIsOnTheRegistry(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		manager  string
		fixture  string
		base     *string
		resolver func(context.Context, model.LibyearDep) (*model.LibyearRow, error)
		name     string
		present  string // a version the fixture holds; "" = the latest it names (the captured fixtures are trimmed, and some no longer list their own latest)
	}{
		{"npm", "", &npmRegistryBase, resolveNPMLibyear, "lodash", "4.17.0"},
		{"pypi", "pypi_flask.json", &pypiRegistryBase, resolvePyPILibyear, "flask", "2.0.1"},
		{"cargo", "crates_serde.json", &cratesRegistryBase, resolveCargoLibyear, "serde", "1.0.229"},
		{"packagist", "packagist_monolog.json", &packagistRegistryBase, resolvePackagistLibyear, "monolog/monolog", "3.10.0"},
		{"hex", "hex_phoenix.json", &hexRegistryBase, resolveHexLibyear, "phoenix", "1.8.9"},
		{"nuget", "nuget_newtonsoft.json", &nugetRegistryBase, resolveNuGetLibyear, "Newtonsoft.Json", ""},
		{"pub", "pubdev_http.json", &pubDevRegistryBase, resolvePubDevLibyear, "http", "0.2.8+2"},
		{"rubygems", "rubygems_rails.json", &rubygemsRegistryBase, resolveRubyGemsLibyear, "rails", ""},
		{"maven", "maven_commonslang3_metadata.xml", &mavenRepositoryBase, resolveMavenLibyear, "org.apache.commons:commons-lang3", ""},
		{"hackage", "hackage_aeson_preferred.json", &hackageRegistryBase, resolveHackageLibyear, "aeson", ""},
		{"cran", "crandb_commonmark.json", &cranRegistryBase, resolveCRANLibyear, "commonmark", "1.9.2"},
	}
	provers := 0
	for _, tc := range cases {
		t.Run(tc.manager, func(t *testing.T) {
			var srv *httptest.Server
			if tc.fixture != "" {
				srv = serveFixture(t, tc.fixture)
			} else {
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"dist-tags":{"latest":"4.17.21"},"time":{"4.17.21":"2021-02-20T15:42:16.891Z","4.17.0":"2016-12-31T22:33:53.421Z"},"license":"MIT"}`))
				}))
			}
			defer srv.Close()
			old := *tc.base
			*tc.base = srv.URL
			defer func() { *tc.base = old }()

			canProve, listed := resolverCanProveVersionMiss[tc.manager]
			if !listed {
				t.Fatalf("resolverCanProveVersionMiss has no entry for %q", tc.manager)
			}

			// A version the registry does not have.
			absent, err := tc.resolver(ctx, model.LibyearDep{Name: tc.name, Version: "0.0.0-not-a-release", Manager: tc.manager})
			if err != nil || absent == nil {
				t.Fatalf("absent version: row %v err %v", absent, err)
			}
			want := model.VersionNotEstablished
			if canProve {
				want = model.VersionNotOnRegistry
			}
			if absent.VersionFound != want {
				t.Errorf("a version the registry lacks: VersionFound = %d, want %d (can prove a miss: %v)", absent.VersionFound, want, canProve)
			}
			// (The fake Hackage server dates any version it is asked about,
			// so only the provers are held to this.)
			if canProve && absent.CurrentReleaseDate != "" {
				t.Errorf("a version the registry lacks came back dated %q", absent.CurrentReleaseDate)
			}

			// No version declared: nothing was looked for.
			none, err := tc.resolver(ctx, model.LibyearDep{Name: tc.name, Manager: tc.manager})
			if err != nil || none == nil {
				t.Fatalf("no version: row %v err %v", none, err)
			}
			if none.VersionFound != model.VersionNotEstablished {
				t.Errorf("no declared version: VersionFound = %d, want not established", none.VersionFound)
			}

			// A version the fixture holds.
			present := tc.present
			if present == "" {
				present = absent.LatestVersion
			}
			latest, err := tc.resolver(ctx, model.LibyearDep{Name: tc.name, Version: present, Manager: tc.manager})
			if err != nil || latest == nil {
				t.Fatalf("present version %q: row %v err %v", present, latest, err)
			}
			if canProve && latest.VersionFound != model.VersionOnRegistry {
				t.Errorf("a version the registry has, %q: VersionFound = %d, want on-registry", present, latest.VersionFound)
			}
			if !canProve && latest.VersionFound == model.VersionNotOnRegistry {
				t.Errorf("a resolver that cannot prove a miss claimed one for %q", present)
			}
			if canProve {
				provers++
			}
		})
	}
	if provers != 8 {
		t.Errorf("%d resolvers proved a version miss; want 8 (npm, pypi, cargo, packagist, hex, nuget, pub, cran)", provers)
	}
}

// Every resolver the collection loop dispatches to states what it can prove.
func TestEveryResolverStatesWhatItCanProve(t *testing.T) {
	for _, manager := range []string{"npm", "pypi", "go", "cargo", "rubygems", "maven", "packagist", "hex", "nuget", "pub", "hackage", "swiftpm", "cran"} {
		if _, ok := resolverCanProveVersionMiss[manager]; !ok {
			t.Errorf("resolverCanProveVersionMiss has no entry for %q", manager)
		}
	}
	if len(resolverCanProveVersionMiss) != 13 {
		t.Errorf("resolverCanProveVersionMiss has %d entries; the loop dispatches to 13 resolvers", len(resolverCanProveVersionMiss))
	}
	for _, manager := range []string{"go", "rubygems", "maven", "hackage", "swiftpm"} {
		if resolverCanProveVersionMiss[manager] {
			t.Errorf("%s cannot tell a version it lacks from one with no date; it must not claim to", manager)
		}
	}
}
