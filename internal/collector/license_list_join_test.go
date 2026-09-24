// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/spdx"
)

// setLicenseArrays rewrites every "licenses"/"license" array in a real
// captured registry body to the given list, so the resolvers read a
// multi-license package through their normal parsing path.
func setLicenseArrays(v any, list []any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			if (k == "licenses" || k == "license") && isArray(x) {
				t[k] = list
				continue
			}
			t[k] = setLicenseArrays(x, list)
		}
	case []any:
		for i, x := range t {
			t[i] = setLicenseArrays(x, list)
		}
	}
	return v
}

func isArray(v any) bool { _, ok := v.([]any); return ok }

// TestRegistryLicenseListsAreStoredAsOR — worklist 53, decision 2
// (summary/39): RubyGems, Packagist and Hex publish a license ARRAY, which
// was stored joined with " AND ", the SPDX operator for "comply with all".
// Composer documents its array as a choice; RubyGems and Hex leave the
// relationship unstated and are stored as the common dual-licensing reading
// (the SBOM discloses that). The stored value is now a valid SPDX
// expression with OR.
func TestRegistryLicenseListsAreStoredAsOR(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		fixture  string
		base     *string
		resolver func(context.Context, libyearDep) (*db.LibyearRow, error)
		dep      libyearDep
	}{
		{"rubygems", "rubygems_rails.json", &rubygemsRegistryBase, resolveRubyGemsLibyear,
			libyearDep{Name: "rails", Version: "7.0.0", Manager: "gem"}},
		{"packagist", "packagist_monolog.json", &packagistRegistryBase, resolvePackagistLibyear,
			libyearDep{Name: "monolog/monolog", Version: "3.0.0", Manager: "composer"}},
		{"hex", "hex_phoenix.json", &hexRegistryBase, resolveHexLibyear,
			libyearDep{Name: "phoenix", Version: "1.7.0", Manager: "hex"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/registries/" + tc.fixture)
			if err != nil {
				t.Fatal(err)
			}
			var doc any
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(setLicenseArrays(doc, []any{"MIT", "Apache-2.0"}))
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(body)
			}))
			defer srv.Close()
			old := *tc.base
			*tc.base = srv.URL
			defer func() { *tc.base = old }()

			row, err := tc.resolver(ctx, tc.dep)
			if err != nil || row == nil {
				t.Fatalf("resolver: row=%v err=%v", row, err)
			}
			if row.License != "MIT OR Apache-2.0" {
				t.Errorf("stored license = %q, want %q (a license list is a choice, not a conjunction)", row.License, "MIT OR Apache-2.0")
			}
			if err := spdx.Validate(row.License); err != nil {
				t.Errorf("stored license %q is not a valid SPDX expression: %v", row.License, err)
			}
		})
	}
}

func TestJoinRegistryLicenseListEdges(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want string
	}{
		{nil, ""},
		{[]string{""}, ""},
		{[]string{"MIT"}, "MIT"},
		{[]string{" MIT ", "", "Apache-2.0"}, "MIT OR Apache-2.0"},
		{[]string{"MIT AND BSD-3-Clause"}, "MIT AND BSD-3-Clause"},                               // one entry: untouched
		{[]string{"MIT AND BSD-3-Clause", "Apache-2.0"}, "(MIT AND BSD-3-Clause) OR Apache-2.0"}, // precedence kept
		{[]string{"Ruby", "BSD-2-Clause"}, "Ruby OR BSD-2-Clause"},                               // ruby's own dual license
	} {
		if got := joinRegistryLicenseList(tc.in); got != tc.want {
			t.Errorf("joinRegistryLicenseList(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// The stored form normalizes to itself and validates.
	if got := db.NormalizeLicenseToSPDX("(MIT AND BSD-3-Clause) OR Apache-2.0"); !spdx.Valid(got) {
		t.Errorf("normalized %q is not valid SPDX", got)
	}
}

// TestJoinedListKeepsOperatorBearingSynonyms — v0.29.67 review round 2: a
// registry list entry that is a synonym with an operator word or slash
// ("zlib/libpng", the CDDL's long name) was split after the join.
func TestJoinedListKeepsOperatorBearingSynonyms(t *testing.T) {
	for _, tc := range []struct {
		list []string
		want string
	}{
		{[]string{"zlib/libpng", "MIT"}, "Zlib OR MIT"},
		{[]string{"Common Development and Distribution License", "MIT"}, "CDDL-1.0 OR MIT"},
		{[]string{"GNU Library or Lesser General Public License (LGPL)", "MIT"}, "LGPL OR MIT"},
	} {
		if got := db.NormalizeLicenseToSPDX(joinRegistryLicenseList(tc.list)); got != tc.want {
			t.Errorf("normalize(join(%q)) = %q, want %q", tc.list, got, tc.want)
		}
	}
}
