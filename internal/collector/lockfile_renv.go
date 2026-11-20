// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/json"
	"net/url"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/manifest"
)

// bioconductorEcosystem is the ecosystem a Bioconductor package is recorded
// under. It is not a purl type (the purl specification has none for it) and
// not a registry this program looks versions up on.
const bioconductorEcosystem = "bioconductor"

// renvPositHosts are Posit's public package manager, which serves a CRAN
// snapshot under a path that holds "cran", and other repositories
// (Bioconductor, PyPI, a customer's own) under other paths.
var renvPositHosts = map[string]bool{"packagemanager.posit.co": true, "packagemanager.rstudio.com": true, "p3m.dev": true}

// renvCRANHosts are CRAN itself and the mirrors R offers by default.
var renvCRANHosts = map[string]bool{"cran.r-project.org": true, "cloud.r-project.org": true, "cran.rstudio.com": true}

// renvIsCRANURL reports whether a repository URL is CRAN's: CRAN or a
// default mirror, or Posit's package manager at a CRAN path.
func renvIsCRANURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if renvCRANHosts[host] {
		return true
	}
	if !renvPositHosts[host] {
		return false
	}
	for _, seg := range strings.Split(strings.ToLower(u.Path), "/") {
		if seg == "cran" {
			return true
		}
	}
	return false
}

// renvFromCRAN decides whether a "Repository" entry is a CRAN package, from
// what the lockfile itself says (SR-6: nothing is assumed of a repository
// the file does not identify):
//
//   - the repository is named CRAN: the project's own statement, whatever
//     proxy serves it;
//   - the entry gives a URL in place of a name: by that URL;
//   - another name that the file lists with a URL: by that URL;
//   - another name with no URL listed: only Posit's conventional names for
//     its CRAN snapshot (RSPM, PPM, P3M).
func renvFromCRAN(repository string, listed map[string]string) bool {
	repository = strings.TrimSpace(repository)
	switch {
	case repository == "":
		return false
	case strings.EqualFold(repository, "CRAN"):
		return true
	case strings.Contains(repository, "://"):
		return renvIsCRANURL(repository)
	}
	if u, ok := listed[repository]; ok {
		return renvIsCRANURL(u)
	}
	switch strings.ToUpper(repository) {
	case "RSPM", "PPM", "P3M":
		return true
	}
	return false
}

// parseRenvLock handles renv.lock: JSON whose "Packages" map holds one
// record per package (its version, where it comes from, and the names of
// the packages it requires) and whose "R" record lists the repositories by
// name. Direct dependencies are not distinguished.
//
// An entry from a CRAN-type repository is a CRAN package. One whose Source
// is Bioconductor is recorded under that ecosystem. Any other (a forge, a
// URL, a local path, another repository, an unknown source) is not from a
// registry this program knows, and is left out and counted.
func parseRenvLock(data []byte) (parsedLockfileData, error) {
	var doc struct {
		R struct {
			Repositories []struct {
				Name string `json:"Name"`
				URL  string `json:"URL"`
			} `json:"Repositories"`
		} `json:"R"`
		Packages map[string]struct {
			Package      string   `json:"Package"`
			Version      string   `json:"Version"`
			Source       string   `json:"Source"`
			Repository   string   `json:"Repository"`
			Requirements []string `json:"Requirements"`
			// The DESCRIPTION's own fields, which later renv versions
			// write into each record. Raw: a list in those versions, and
			// read as the DESCRIPTION's comma-separated text too.
			Depends   json.RawMessage `json:"Depends"`
			Imports   json.RawMessage `json:"Imports"`
			LinkingTo json.RawMessage `json:"LinkingTo"`
		} `json:"Packages"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	listed := map[string]string{}
	for _, r := range doc.R.Repositories {
		listed[r.Name] = r.URL
	}
	keys := make([]string, 0, len(doc.Packages))
	for k := range doc.Packages {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out parsedLockfileData
	for _, key := range keys {
		pkg := doc.Packages[key]
		name := pkg.Package
		if name == "" {
			name = key
		}
		entry := LockfileEntry{Name: name, Version: pkg.Version}
		switch {
		case pkg.Source == "Bioconductor":
			entry.Ecosystem = bioconductorEcosystem
		case pkg.Source == "Repository" && renvFromCRAN(pkg.Repository, listed):
		default:
			out.NotRegistry++
			continue
		}
		out.Entries = append(out.Entries, entry)
		if pkg.Version == "" {
			continue // left out when versions are settled; it has no edge either
		}
		// `Requirements` is renv's own list of what the package needs.
		// Where a record has none, the same is read from the fields it
		// was computed from (0.29.208); Suggests and Enhances are not
		// needed to run the package and are no edge.
		children := pkg.Requirements
		if children == nil {
			for _, raw := range []json.RawMessage{pkg.Depends, pkg.Imports, pkg.LinkingTo} {
				children = append(children, renvFieldNames(raw)...)
			}
		}
		for _, child := range children {
			if child == "" || manifest.IsRBasePackage(child) {
				continue
			}
			out.Edges = append(out.Edges, LockfileEdge{ParentName: name, ParentVersion: pkg.Version, ChildName: child})
		}
	}
	// An edge row has one ecosystem for both its ends. One with a
	// Bioconductor package at either end has two, and is not stored.
	other := map[string]bool{}
	for _, e := range out.Entries {
		if e.Ecosystem != "" {
			other[e.Name] = true
		}
	}
	kept := out.Edges[:0]
	for _, ed := range out.Edges {
		if !other[ed.ParentName] && !other[ed.ChildName] {
			kept = append(kept, ed)
		}
	}
	out.Edges = kept
	return out, nil
}

// entryEcosystem is the ecosystem one lockfile entry is stored under: its
// own where the format gives one (renv.lock's Bioconductor packages), the
// lockfile's otherwise.
func entryEcosystem(res *LockfileResult, e LockfileEntry) string {
	if e.Ecosystem != "" {
		return e.Ecosystem
	}
	return res.Ecosystem
}

// renvFieldNames are the package names in one DESCRIPTION field of a renv
// record: a JSON list of entries, or the field's comma-separated text. Each
// entry is a name with an optional constraint in parentheses, which is not
// part of the edge. A value of any other shape gives none.
func renvFieldNames(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var entries []string
	if err := json.Unmarshal(raw, &entries); err != nil {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil
		}
		entries = strings.Split(text, ",")
	}
	var out []string
	for _, e := range entries {
		name, _, _ := strings.Cut(e, "(")
		if name = strings.TrimSpace(name); name != "" {
			out = append(out, name)
		}
	}
	return out
}
