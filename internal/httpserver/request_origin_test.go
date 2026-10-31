// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package httpserver

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// RequestOrigin is the one same-site rule (0.29.125 ASVS review, V4.3.3):
// each arm, with and without a second admitted origin.
func TestRequestOrigin(t *testing.T) {
	spa := func(origin string) bool { return strings.EqualFold(origin, "https://gui.example.org") }
	cases := []struct {
		name          string
		origin, ref   string
		secFetchSite  string
		alsoAllowed   func(string) bool
		wantOK        bool
		wantOriginOut string
	}{
		{name: "no headers is not a cross-site browser request", wantOK: true},
		{name: "same host", origin: "https://monitor.example.org", wantOK: true, wantOriginOut: "https://monitor.example.org"},
		{name: "same host, other case", origin: "https://MONITOR.example.org", wantOK: true, wantOriginOut: "https://MONITOR.example.org"},
		{name: "foreign origin", origin: "https://evil.example.net", wantOK: false, wantOriginOut: "https://evil.example.net"},
		{name: "foreign referer with no origin", ref: "https://evil.example.net/page", wantOK: false, wantOriginOut: "https://evil.example.net/page"},
		{name: "same-host referer", ref: "https://monitor.example.org/", wantOK: true, wantOriginOut: "https://monitor.example.org/"},
		{name: "malformed origin", origin: "not a url", wantOK: false, wantOriginOut: "not a url"},
		{name: "origin without a host", origin: "null", wantOK: false, wantOriginOut: "null"},
		{name: "cross-site says so, same host claimed", origin: "https://monitor.example.org", secFetchSite: "cross-site", wantOK: false, wantOriginOut: "https://monitor.example.org"},
		{name: "cross-site, admitted origin", origin: "https://gui.example.org", secFetchSite: "cross-site", alsoAllowed: spa, wantOK: true, wantOriginOut: "https://gui.example.org"},
		{name: "cross-site, admitted set but another origin", origin: "https://evil.example.net", secFetchSite: "cross-site", alsoAllowed: spa, wantOK: false, wantOriginOut: "https://evil.example.net"},
		{name: "admitted origin without the fetch header", origin: "https://gui.example.org", alsoAllowed: spa, wantOK: true, wantOriginOut: "https://gui.example.org"},
		{name: "same-site fetch header passes", origin: "https://monitor.example.org", secFetchSite: "same-origin", wantOK: true, wantOriginOut: "https://monitor.example.org"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "https://monitor.example.org/api/prioritize/1", nil)
			r.Host = "monitor.example.org"
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.ref != "" {
				r.Header.Set("Referer", tc.ref)
			}
			if tc.secFetchSite != "" {
				r.Header.Set("Sec-Fetch-Site", tc.secFetchSite)
			}
			got, ok := RequestOrigin(r, tc.alsoAllowed)
			if ok != tc.wantOK || got != tc.wantOriginOut {
				t.Errorf("RequestOrigin = (%q, %v); want (%q, %v)", got, ok, tc.wantOriginOut, tc.wantOK)
			}
		})
	}
}

// One spelling (SR-17): no other non-test source under internal/ or cmd/
// reads Sec-Fetch-Site or decides an origin's host itself; the web guard
// and the monitor call RequestOrigin.
func TestRequestOriginIsTheOnlyReaderOfSecFetchSite(t *testing.T) {
	var readers []string
	for _, root := range []string{"..", "../../cmd"} {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.Contains(string(src), `"Sec-Fetch-Site"`) {
				readers = append(readers, filepath.ToSlash(path))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(readers) != 1 || !strings.HasSuffix(readers[0], "httpserver/httpserver.go") {
		t.Errorf("Sec-Fetch-Site is read in %v; want only httpserver.go (RequestOrigin) — call it instead of a second same-site rule", readers)
	}
}
