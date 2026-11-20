// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// resolveCRANLibyear resolves an R package against crandb (0.29.204;
// summary/70, 4.9). One request, /<name>/all, answers with the latest
// version and a timeline of every version CRAN published with its time.
// The name is sent as declared: crandb, like CRAN and like OSV, has the
// package under one case only, and answers 404 for another.
func resolveCRANLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx, fmt.Sprintf(cranRegistryBase+"/%s/all", dep.Name))
	if err != nil {
		return nil, err
	}
	var info struct {
		Latest   string            `json:"latest"`
		Timeline map[string]string `json:"timeline"`
		Versions map[string]struct {
			License string `json:"License"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	latestDate := info.Timeline[info.Latest]
	if info.Latest == "" || latestDate == "" {
		// Deliberately NOT typed as "the package does not exist": a 200
		// whose body does not date its own latest version is a changed
		// shape as easily as an empty package, and an unexpected shape is
		// not a "no" (SR-5). It says nothing, and is asked again.
		return nil, fmt.Errorf("crandb answer for %s names no dated latest version", dep.Name)
	}
	// R compares 1.0-7 and 1.0.7 as one version, and CRAN publishes either
	// spelling: the declared version is looked for under both.
	currentDate, onRegistry := info.Timeline[dep.Version]
	if !onRegistry && dep.Version != "" {
		want := rVersionKey(dep.Version)
		for v, date := range info.Timeline {
			if rVersionKey(v) == want {
				currentDate, onRegistry = date, true
				break
			}
		}
	}
	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "cran",
		CurrentVersion:     dep.Version,
		LatestVersion:      info.Latest,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
		License:            cranLicense(info.Versions[info.Latest].License),
		Purl:               pkgid.Purl("cran", dep.Name, dep.Version),
		VersionFound:       versionFound(dep.Version, onRegistry),
	}, nil
}

// rVersionKey writes an R version with one separator: R's own comparison
// treats '-' and '.' alike.
func rVersionKey(v string) string { return strings.ReplaceAll(v, "-", ".") }

// cranLicenseNames maps the licence names of CRAN's licence database to
// SPDX identifiers, for the names whose SPDX form is not in doubt. A
// version constraint in CRAN's form is part of the name here.
var cranLicenseNames = map[string]string{
	"MIT": "MIT", "BSD_2_clause": "BSD-2-Clause", "BSD_3_clause": "BSD-3-Clause",
	"GPL-2": "GPL-2.0-only", "GPL-3": "GPL-3.0-only",
	"GPL (>= 2)": "GPL-2.0-or-later", "GPL (>= 2.0)": "GPL-2.0-or-later", "GPL (>= 3)": "GPL-3.0-or-later", "GPL (>= 3.0)": "GPL-3.0-or-later",
	"LGPL-2": "LGPL-2.0-only", "LGPL-2.1": "LGPL-2.1-only", "LGPL-3": "LGPL-3.0-only",
	"LGPL (>= 2)": "LGPL-2.0-or-later", "LGPL (>= 2.1)": "LGPL-2.1-or-later", "LGPL (>= 3)": "LGPL-3.0-or-later",
	"AGPL-3": "AGPL-3.0-only", "AGPL (>= 3)": "AGPL-3.0-or-later",
	"Apache License 2.0": "Apache-2.0", "Apache License (== 2.0)": "Apache-2.0", "Apache License (>= 2.0)": "Apache-2.0", "Apache License (>= 2)": "Apache-2.0",
	"CC0": "CC0-1.0", "Artistic-2.0": "Artistic-2.0", "EUPL-1.2": "EUPL-1.2", "MPL-2.0": "MPL-2.0",
}

// cranFileLicense is "see the file": CRAN's way of saying the terms, or
// additions to them, are in the package's LICENSE file. It is no licence
// name.
var cranFileLicense = regexp.MustCompile(`(?i)^file\s+licen[sc]e$`)

// cranLicense turns CRAN's License field into the form the licence table
// reads. The field is alternatives separated by "|", each a name with an
// optional "+ file LICENSE". A name with an SPDX form is given it; any
// other is passed on as CRAN states it; "file LICENSE" alone is dropped.
func cranLicense(field string) string {
	var out []string
	for _, alt := range strings.Split(field, "|") {
		name, _, _ := strings.Cut(alt, "+")
		name = strings.Join(strings.Fields(name), " ")
		if name == "" || cranFileLicense.MatchString(name) {
			continue
		}
		if spdx, ok := cranLicenseNames[name]; ok {
			name = spdx
		}
		out = append(out, name)
	}
	return strings.Join(out, " OR ")
}
