// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// sbom_vulns.go: what a generated SBOM says about the repository's
// vulnerability findings. The two annotators lived in internal/api beside
// the handler that calls them; they are here, beside the generators whose
// documents they extend (sbom.go), so that the one schema harness
// (sbom_schema_groundtruth_test.go) and the official validators check an
// ANNOTATED document as well as a bare one (summary/70, 4.1.4). The three
// small helpers they share with the vulnerability list handler came with
// them, and the handler calls them here.

import (
	"encoding/json"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// AnnotateCycloneDXWithVulns appends a CycloneDX `vulnerabilities`
// array to a generated SBOM, covering the repo's CURRENT (unresolved)
// findings. Components with a purl carry it as bom-ref, so affects.ref
// matches directly. A purl-less component carries an aveloxis: ref; most
// are never scanned, but a package of an ecosystem asked by name
// (Bioconductor, 0.29.206) is, and its finding is pointed at that ref
// (cdxAffectsRef). Resolved-historical findings are excluded — the
// SBOM describes the repository as it stands.
func AnnotateCycloneDXWithVulns(sbom []byte, vulns []*model.VulnerabilityRow) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(sbom, &doc); err != nil {
		return nil, err
	}
	refs := map[string]bool{}
	if comps, ok := doc["components"].([]any); ok {
		for _, c := range comps {
			if m, ok := c.(map[string]any); ok {
				if ref, ok := m["bom-ref"].(string); ok {
					refs[ref] = true
				}
			}
		}
	}
	entries := make([]map[string]any, 0, len(vulns))
	for _, v := range vulns {
		class := vulnclass.Of(v)
		if class == vulnclass.Closed {
			continue
		}
		// v0.27.29: self-advisories describe the repo's OWN releases,
		// not this SBOM's dependency components — their versionless
		// purls match no component bom-ref, so affects.ref would
		// dangle. The SBOM vulnerability array stays dependency-only.
		if class == vulnclass.Self {
			continue
		}
		osvURL, _ := AdvisoryURLs(v.VulnID, v.CVEID)
		entry := map[string]any{
			"id":     v.VulnID,
			"source": map[string]any{"name": v.Source, "url": osvURL},
			"ratings": []map[string]any{{
				"score":    v.CVSSScore,
				"severity": CDXSeverity(v.Severity),
				"method":   CVSSMethodFromVector(v.CVSSVector),
				"vector":   v.CVSSVector,
			}},
			"description": v.Summary,
			"affects":     []map[string]any{{"ref": cdxAffectsRef(v, refs)}},
		}
		if v.FixedVersion != "" {
			entry["recommendation"] = "Upgrade " + v.PackageName + " to " + v.FixedVersion + " or later"
		}
		if class == vulnclass.VersionNotDetermined {
			// The version is not determined. CycloneDX's analysis states have
			// no such value and none is invented: in_triage is the nearest
			// one the schema has, with the reason in its detail, and the
			// property says it exactly (0.29.199; summary/70, 8.2).
			entry["analysis"] = map[string]any{"state": "in_triage", "detail": vulnclass.VersionNotDeterminedDetail}
			entry["properties"] = []map[string]string{{"name": "aveloxis:version_not_determined", "value": "true"}}
		}
		entries = append(entries, entry)
	}
	doc["vulnerabilities"] = entries
	return json.MarshalIndent(doc, "", "  ")
}

// AnnotateSPDXWithVulns attaches the repo's CURRENT (unresolved)
// findings to a generated SPDX 2.3 document as package-level
// externalRefs (referenceCategory SECURITY, referenceType advisory —
// the spec's conformant vehicle; SPDX 2.3 has no native vulnerability
// section, which is why ?vulns=1 used to 400 for spdx). Findings
// match their package by purl first, then by package name; findings
// matching no package are skipped (an SPDX externalRef must hang off
// a real package — there is no dangling-ref form). Self-advisories
// and resolved-historical findings are excluded, mirroring the
// CycloneDX annotation. SPDX 3.0's security profile is the eventual
// richer home.
func AnnotateSPDXWithVulns(sbom []byte, vulns []*model.VulnerabilityRow) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(sbom, &doc); err != nil {
		return nil, err
	}
	byPurl := map[string][]*model.VulnerabilityRow{}
	byName := map[string][]*model.VulnerabilityRow{}
	for _, v := range vulns {
		if class := vulnclass.Of(v); class == vulnclass.Closed || class == vulnclass.Self {
			continue
		}
		if v.PackagePurl != "" {
			byPurl[v.PackagePurl] = append(byPurl[v.PackagePurl], v)
		}
		if v.PackageName != "" {
			byName[v.PackageName] = append(byName[v.PackageName], v)
		}
	}
	pkgs, _ := doc["packages"].([]any)
	for _, p := range pkgs {
		pkg, ok := p.(map[string]any)
		if !ok || pkg["SPDXID"] == "SPDXRef-RootPackage" {
			continue
		}
		refs, _ := pkg["externalRefs"].([]any)
		purl := ""
		for _, r := range refs {
			if ref, ok := r.(map[string]any); ok && ref["referenceType"] == "purl" {
				purl, _ = ref["referenceLocator"].(string)
			}
		}
		matched := byPurl[purl]
		if len(matched) == 0 {
			if name, _ := pkg["name"].(string); name != "" {
				matched = byName[name]
			}
		}
		seen := map[string]bool{}
		for _, v := range matched {
			osvURL, _ := AdvisoryURLs(v.VulnID, v.CVEID)
			if osvURL == "" || seen[osvURL] {
				continue
			}
			seen[osvURL] = true
			ref := map[string]any{
				"referenceCategory": "SECURITY",
				"referenceType":     "advisory",
				"referenceLocator":  osvURL,
			}
			if vulnclass.Of(v) == vulnclass.VersionNotDetermined {
				// v0.29.70: SPDX 2.3 has no status field; the comment says
				// what CycloneDX's in_triage does.
				ref["comment"] = vulnclass.VersionNotDeterminedDetail
			}
			refs = append(refs, ref)
		}
		if len(refs) > 0 {
			pkg["externalRefs"] = refs
		}
	}
	return json.MarshalIndent(doc, "", "  ")
}

// AdvisoryURLs returns the canonical description pages for a finding:
// osv.dev resolves every id OSV emits (GHSA, PYSEC, GO-, RUSTSEC-…);
// opencve is the CVE-specific page the operator pointed at.
func AdvisoryURLs(vulnID, cveID string) (osv, cve string) {
	osv = "https://osv.dev/vulnerability/" + vulnID
	if cveID != "" {
		cve = "https://app.opencve.io/cve/" + cveID
	}
	return osv, cve
}

// CDXSeverity maps OSV severity labels to CycloneDX's lowercase enum.
func CDXSeverity(s string) string {
	switch s {
	case "CRITICAL":
		return "critical"
	case "HIGH":
		return "high"
	case "MEDIUM", "MODERATE":
		return "medium"
	case "LOW":
		return "low"
	default:
		return "unknown"
	}
}

// CVSSMethodFromVector derives the CycloneDX rating method from the
// stored vector's prefix. v0.27.39 (summary/18 Phase 2 + the operator's
// standards-conformance direction): the method was hardcoded "CVSSv31"
// even when the score was computed from a v3.0 or v2 vector — wrong
// provenance in the exported document.
func CVSSMethodFromVector(vector string) string {
	switch {
	case strings.HasPrefix(vector, "CVSS:3.1"):
		return "CVSSv31"
	case strings.HasPrefix(vector, "CVSS:3.0"):
		return "CVSSv3"
	case strings.HasPrefix(vector, "CVSS:4"):
		return "CVSSv4"
	case strings.HasPrefix(vector, "AV:"): // v2 vectors have no CVSS: prefix
		return "CVSSv2"
	default:
		return "other"
	}
}

// cdxAffectsRef is the component a finding points at: its purl, which is
// its component's bom-ref; or, for a package of an ecosystem asked by name,
// whose identity (pkgid.FindingPurlByName) is not a purl and is no
// component's ref, the aveloxis: ref that component carries. The identity
// is kept as written when the document has no such component.
func cdxAffectsRef(v *model.VulnerabilityRow, refs map[string]bool) string {
	if refs[v.PackagePurl] {
		return v.PackagePurl
	}
	if _, byName := pkgid.OSVEcosystemByName(v.Ecosystem); byName {
		base := pkgid.ScopeAwareBase(v.PackagePurl)
		version := strings.TrimPrefix(v.PackagePurl[len(base):], "@")
		if ref := purlLessBOMRef(sbomGraphKey(v.Ecosystem, v.PackageName), version); refs[ref] {
			return ref
		}
	}
	return v.PackagePurl
}
