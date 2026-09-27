// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package spdx is the ONE place Aveloxis asks a question about an SPDX
// license identifier or license expression (SR-17; worklist 53, design in
// summary/39-spdx-license-expressions.md): is this an ID, is this
// expression valid, is it OSI-approved, what is its canonical spelling.
// The license table's OSI badge and both SBOM exporters go through it.
//
// The identifier data is the official SPDX license list, generated into
// spdx_data.tsv and embedded at compile time; nothing is fetched at
// runtime. Expression validation uses github.com/github/go-spdx/v2 (operator
// decision, option A); OSI status is evaluated on this package's own parsed
// tree (v0.29.67 review round 1: go-spdx's Satisfies was exponential).
package spdx

import (
	_ "embed" // spdx_data.tsv
	"regexp"
	"strings"
)

//go:embed spdx_data.tsv
var dataRaw string

type licenseInfo struct {
	id         string
	osi        bool
	deprecated bool
}

var (
	licenses      = map[string]licenseInfo{} // exact ID
	licensesLower = map[string]string{}      // lower-case ID -> canonical ID
	exceptions    = map[string]string{}      // lower-case ID -> canonical ID
	listVersion   string
)

var versionRe = regexp.MustCompile(`license list version (\S+)`)

func init() {
	for line := range strings.SplitSeq(dataRaw, "\n") {
		if strings.HasPrefix(line, "#") {
			if m := versionRe.FindStringSubmatch(line); m != nil {
				listVersion = m[1]
			}
			continue
		}
		f := strings.Split(line, "\t")
		switch {
		case len(f) == 4 && f[0] == "L":
			licenses[f[1]] = licenseInfo{id: f[1], osi: f[2] == "1", deprecated: f[3] == "1"}
			licensesLower[strings.ToLower(f[1])] = f[1]
		case len(f) == 3 && f[0] == "X":
			exceptions[strings.ToLower(f[1])] = f[1]
		}
	}
}

// ListVersion is the SPDX license list version the embedded data was
// generated from.
func ListVersion() string { return listVersion }

// IsLicenseID reports whether id is an SPDX license identifier, spelled
// exactly as the list spells it (deprecated identifiers included).
func IsLicenseID(id string) bool {
	_, ok := licenses[id]
	return ok
}

// CanonicalLicenseID returns the list's spelling of id, matched
// case-insensitively as SPDX Annex D requires.
func CanonicalLicenseID(id string) (string, bool) {
	c, ok := licensesLower[strings.ToLower(id)]
	return c, ok
}

// IsExceptionID reports whether id is an SPDX license exception
// identifier, spelled exactly as the list spells it.
func IsExceptionID(id string) bool {
	c, ok := exceptions[strings.ToLower(id)]
	return ok && c == id
}

// CanonicalExceptionID returns the list's spelling of an exception ID,
// matched case-insensitively.
func CanonicalExceptionID(id string) (string, bool) {
	c, ok := exceptions[strings.ToLower(id)]
	return c, ok
}

// IsOSIApprovedID reports the list's isOsiApproved for one license ID.
func IsOSIApprovedID(id string) bool { return licenses[id].osi }

// FamilyLabels are the house's version-less family buckets. They are not
// SPDX identifiers: the synonym map produces them for upstream metadata
// that names a family without a version (v0.28.1, v0.28.8). Every released
// version of each family is OSI-approved, so the label is too. They never
// validate as SPDX, so an SBOM never carries one.
var FamilyLabels = map[string]bool{"LGPL": true, "EPL": true, "Artistic": true}
