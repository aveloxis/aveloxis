// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// libyear_release_date_errors_test.go — v0.29.56. Four resolvers make a
// SECOND request for a version's release date (Go's .info, Maven's .pom
// Last-Modified, Hackage's upload-time, SwiftPM's release-by-tag). All four
// must treat that request the same way:
//
//   - a definitive miss (the registry says that version does not exist)
//     costs the DATE — the row is kept, with libyear 0 honestly;
//   - a failure that says nothing (5xx, timeout, rate limit) FAILS the
//     dependency, so nothing is stored and nothing is cached.
//
// Swallowing it stores libyear 0, which reads as "perfectly fresh" — and
// since v0.29.56 the answer is cached process-wide for a day, so one blip
// would fabricate that 0 for every repository for 24 hours. The Go site had
// swallowed it since inception (0 of 825,136 rows had a date), Maven never
// asked for the date at all (0 of 51,510), and when those were fixed the
// Hackage and Maven halves shipped with no test: reverting them left the
// whole suite green, which is exactly how the original bug survived.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestResolversTreatAReleaseDateFailureAsAFailure(t *testing.T) {
	// Each case serves the PRIMARY lookup normally and controls the status
	// of the date request.
	cases := []struct {
		name string
		// resolve runs the resolver against a server that answers the date
		// request with dateStatus.
		resolve func(t *testing.T, dateStatus int) (*db.LibyearRow, error)
	}{
		{
			name: "go",
			resolve: func(t *testing.T, dateStatus int) (*db.LibyearRow, error) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/@latest") {
						_, _ = io.WriteString(w, `{"Version":"v1.8.1","Time":"2024-06-22T17:02:30Z"}`)
						return
					}
					w.WriteHeader(dateStatus)
				}))
				defer srv.Close()
				defer swapBase(&goProxyBase, srv.URL)()
				return resolveGoLibyear(context.Background(),
					libyearDep{Name: "github.com/spf13/cobra", Version: "v1.8.0", Manager: "go"})
			},
		},
		// Maven asks for TWO dates — the pinned version's .pom and the
		// latest's — and each has its own guard. One case that fails both
		// masks them: either guard could be deleted alone and the suite
		// would stay green, which is the shape that let the original bug
		// ship. So each arm gets a case that fails ONLY that request.
		{
			name: "maven/pinned-version-pom",
			resolve: func(t *testing.T, dateStatus int) (*db.LibyearRow, error) {
				return resolveMavenWithPomStatus(t, "/3.12.0/", dateStatus)
			},
		},
		{
			name: "maven/latest-version-pom",
			resolve: func(t *testing.T, dateStatus int) (*db.LibyearRow, error) {
				return resolveMavenWithPomStatus(t, "/3.20.0/", dateStatus)
			},
		},
		{
			name: "hackage",
			resolve: func(t *testing.T, dateStatus int) (*db.LibyearRow, error) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/preferred") {
						_, _ = io.WriteString(w, `{"normal-version":["2.2.1.0","2.0.0.0"]}`)
						return
					}
					w.WriteHeader(dateStatus) // upload-time
				}))
				defer srv.Close()
				defer swapBase(&hackageRegistryBase, srv.URL)()
				return resolveHackageLibyear(context.Background(),
					libyearDep{Name: "aeson", Version: "2.0.0.0", Manager: "haskell"})
			},
		},
		{
			name: "swiftpm",
			resolve: func(t *testing.T, dateStatus int) (*db.LibyearRow, error) {
				gh := &fakeGitHubAPI{
					answers: map[string]string{
						"/repos/Alamofire/Alamofire/releases/latest": `{"tag_name":"5.10.2","published_at":"2024-11-14T18:00:00Z"}`,
					},
					errs: map[string]error{
						"/repos/Alamofire/Alamofire/releases/tags/5.8.0": statusAnswer(dateStatus),
					},
				}
				return resolveSwiftPMLibyear(context.Background(), gh, libyearDep{
					Name: "Alamofire", Version: "5.8.0", Manager: "swiftpm",
					Requirement: "https://github.com/Alamofire/Alamofire.git"})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A failure that says nothing: the dependency fails, and the
			// error must not be a definitive miss (those get cached).
			row, err := tc.resolve(t, http.StatusInternalServerError)
			if err == nil {
				t.Errorf("a 500 on the release-date request stored a row instead of failing: %+v", row)
			} else if isDefinitiveRegistryMiss(err) {
				t.Errorf("err = %v must not be a definitive miss — caching it would fabricate libyear 0 fleet-wide for a day", err)
			}

			// A definitive answer: the row survives, without that date.
			row, err = tc.resolve(t, http.StatusNotFound)
			if err != nil {
				t.Fatalf("a 404 on the release-date request must cost only the date: %v", err)
			}
			if row == nil || row.LatestVersion == "" {
				t.Fatalf("row = %+v, want the resolved latest version", row)
			}
			// The date the 404 cost must be the empty one; the other stays.
			lost, kept := row.CurrentReleaseDate, row.LatestReleaseDate
			lostName := "CurrentReleaseDate"
			if tc.name == "maven/latest-version-pom" {
				lost, kept = row.LatestReleaseDate, row.CurrentReleaseDate
				lostName = "LatestReleaseDate"
			}
			if lost != "" {
				t.Errorf("%s = %q, want empty", lostName, lost)
			}
			if tc.name == "maven/latest-version-pom" && kept == "" {
				t.Error("the other version's date must survive — only the 404'd request costs its date")
			}
		})
	}
}

// resolveMavenWithPomStatus serves Maven Central's metadata and every .pom
// normally EXCEPT the one under failPathSegment, which answers dateStatus.
func resolveMavenWithPomStatus(t *testing.T, failPathSegment string, dateStatus int) (*db.LibyearRow, error) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "maven-metadata.xml"):
			_, _ = io.WriteString(w, `<metadata><versioning><release>3.20.0</release>
<versions><version>3.12.0</version><version>3.20.0</version></versions></versioning></metadata>`)
		case strings.Contains(r.URL.Path, failPathSegment):
			w.WriteHeader(dateStatus)
		default:
			w.Header().Set("Last-Modified", "Fri, 26 Feb 2021 20:40:52 GMT")
		}
	}))
	defer srv.Close()
	defer swapBase(&mavenRepositoryBase, srv.URL+"/maven2")()
	return resolveMavenLibyear(context.Background(),
		libyearDep{Name: "org.apache.commons:commons-lang3", Version: "3.12.0", Manager: "maven"})
}

// swapBase points a registry base at a test server and returns the restore.
func swapBase(base *string, url string) func() {
	old := *base
	*base = url
	return func() { *base = old }
}

// statusAnswer renders a GitHub-client error the way the platform client
// classifies it: 404 is a definitive answer, 500 is not.
func statusAnswer(status int) error {
	if status == http.StatusNotFound {
		return platform.ErrNotFound
	}
	return fmt.Errorf("github: HTTP %d", status)
}
