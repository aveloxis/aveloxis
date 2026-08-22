// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — sbom.go generates CycloneDX and SPDX Software Bill of
// Materials from the dependency and libyear data collected for a repository.
package collector

import (
	"context"
	"crypto/sha256"
	_ "embed" // spdx_license_ids.txt (v0.27.23)
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/google/uuid"
)

// SBOMFormat specifies the output format.
type SBOMFormat string

const (
	FormatCycloneDX SBOMFormat = "cyclonedx"
	FormatSPDX      SBOMFormat = "spdx"
)

// GenerateSBOM creates an SBOM for a repository from its collected dependency
// data and ScanCode source code analysis. ScanCode provides:
//   - Concluded license: aggregated SPDX expression from file-level detections
//   - Copyright holders: extracted from source file headers
//
// If ScanCode data is not available (tool not installed, or no scan yet),
// the SBOM is still generated with registry-only license data.
func GenerateSBOM(ctx context.Context, store *db.PostgresStore, repoID int64, format SBOMFormat) ([]byte, error) {
	return GenerateSBOMWithOptions(ctx, store, repoID, format, SBOMOptions{})
}

// SBOMOptions controls SBOM generation (v0.27.46, summary/19 P3).
type SBOMOptions struct {
	// RuntimeOnly filters the component set to runtime-scope
	// dependencies (?scope=runtime — decision #3: the full scoped
	// document is the default; the runtime filter serves consumers
	// who want only the shipped surface).
	RuntimeOnly bool
}

// GenerateSBOMWithOptions is GenerateSBOM with scope filtering.
func GenerateSBOMWithOptions(ctx context.Context, store *db.PostgresStore, repoID int64, format SBOMFormat, opts SBOMOptions) ([]byte, error) {
	repo, err := store.GetRepoForSBOM(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("repo %d not found: %w", repoID, err)
	}

	deps, err := store.GetRepoLibyearDeps(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading dependencies: %w", err)
	}

	if opts.RuntimeOnly {
		kept := deps[:0]
		for _, d := range deps {
			if model.IsRuntimeScope(d.Type) {
				kept = append(kept, d)
			}
		}
		deps = kept
	}

	// ScanCode enrichment: concluded license + copyrights from source analysis.
	// Non-fatal — if no scancode data exists, we proceed without it.
	scanData, _ := store.GetScancodeForSBOM(ctx, repoID)

	// v0.27.134 (C2 follow-through): the lockfile closure joins the
	// document when it exists. The effective gate is DATA PRESENCE —
	// scanLockfiles only stores transitive rows/edges when
	// collection.vuln_scan_transitive is on, so knob-off fleets get
	// today's SBOMs byte-identical with zero config plumbing into the
	// api process. Lookup failures are FATAL like the deps load (SR-5:
	// a lookup ERROR is not "no transitives" — silently emitting a
	// flat document on a transient DB error would make two generations
	// of the same commit disagree with nothing explaining why).
	trans, err := store.GetRepoTransitivePackages(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading lockfile transitives: %w", err)
	}
	edges, err := store.GetRepoLockfileEdges(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading lockfile edges: %w", err)
	}
	if opts.RuntimeOnly {
		kept := trans[:0]
		for _, t := range trans {
			if model.IsRuntimeScope(t.Scope) {
				kept = append(kept, t)
			}
		}
		trans = kept
	}
	var graph *sbomGraph
	if len(trans) > 0 || len(edges) > 0 {
		graph = &sbomGraph{Transitives: trans, Edges: edges}
	}

	switch format {
	case FormatCycloneDX:
		return generateCycloneDX(repo, deps, scanData, graph)
	case FormatSPDX:
		return generateSPDX(repo, deps, scanData, graph)
	default:
		return nil, fmt.Errorf("unknown format: %s", format)
	}
}

// sbomGraph carries the C2 lockfile closure (v0.27.133 tables) into
// SBOM emission: transitive packages become components/packages, and
// the requirement edges become the REAL dependency graph replacing the
// synthetic flat root→all shape. nil = pre-v0.27.134 output verbatim.
type sbomGraph struct {
	Transitives []db.RepoLockfilePackage
	Edges       []db.RepoLockfileEdge
}

// sbomGraphKey is the name-level resolution key for graph endpoints.
// Round 22: delegates to db.LockfileGraphKey — the ONE fold shared
// with the chain-attribution walk, so the two graph consumers can
// never disagree on endpoint resolution.
func sbomGraphKey(eco, name string) string {
	return db.LockfileGraphKey(eco, name)
}

// ============================================================
// CycloneDX 1.5
// ============================================================

type cycloneDX struct {
	BOMFormat    string          `json:"bomFormat"`
	SpecVersion  string          `json:"specVersion"`
	SerialNumber string          `json:"serialNumber"`
	Version      int             `json:"version"`
	Metadata     cdxMetadata     `json:"metadata"`
	Components   []cdxComponent  `json:"components"`
	Dependencies []cdxDependency `json:"dependencies,omitempty"`
}

type cdxMetadata struct {
	Timestamp string        `json:"timestamp"`
	Tools     cdxTools      `json:"tools"`
	Component *cdxComponent `json:"component,omitempty"`
}

// cdxTools uses the CycloneDX 1.5 object form (components + services)
// instead of the deprecated pre-1.5 bare array.
type cdxTools struct {
	Components []cdxToolComponent `json:"components"`
}

type cdxToolComponent struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	Author  string `json:"author,omitempty"`
}

type cdxComponent struct {
	Type      string       `json:"type"`
	Name      string       `json:"name"`
	Version   string       `json:"version,omitempty"`
	Purl      string       `json:"purl,omitempty"`
	BOMRef    string       `json:"bom-ref,omitempty"`
	Licenses  []cdxLicense `json:"licenses,omitempty"`
	Scope     string       `json:"scope,omitempty"`
	Copyright string       `json:"copyright,omitempty"`
	Evidence  *cdxEvidence `json:"evidence,omitempty"`
}

// cdxLicense models CycloneDX 1.5's licenseChoice: EITHER a license
// object (id or name) OR an expression — never both. v0.27.29 added
// the expression arm: before it, ScanCode's compound expressions
// ("MIT AND Apache-2.0") failed isSPDXLicense and fell into
// license.name as machine-unreadable free text, invisible to policy
// engines.
type cdxLicense struct {
	License    *cdxLicenseObj `json:"license,omitempty"`
	Expression string         `json:"expression,omitempty"`
}

type cdxLicenseObj struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// cdxEvidence holds CycloneDX 1.5 evidence for concluded (detected) data.
// Used to distinguish source-code-detected licenses from registry-declared ones.
type cdxEvidence struct {
	Licenses  []cdxLicense           `json:"licenses,omitempty"`
	Copyright []cdxCopyrightEvidence `json:"copyright,omitempty"`
}

type cdxCopyrightEvidence struct {
	Text string `json:"text"`
}

// cdxDependency expresses the dependency DAG. Each entry lists a component
// (by bom-ref) and the components it directly depends on.
type cdxDependency struct {
	Ref       string   `json:"ref"`
	DependsOn []string `json:"dependsOn"`
}

func generateCycloneDX(repo *db.RepoForSBOM, deps []db.SBOMDep, scanData *db.ScancodeForSBOM, graph *sbomGraph) ([]byte, error) {
	rootRef := buildPurl("generic", repo.Owner+"/"+repo.Name, "") // v0.27.29: one purl builder everywhere
	rootComp := &cdxComponent{
		Type:   "application",
		Name:   repo.Name,
		BOMRef: rootRef,
	}

	// Enrich root component with ScanCode data if available.
	if scanData != nil {
		if scanData.ConcludedLicenseSPDX != "" {
			rootComp.Evidence = &cdxEvidence{
				Licenses: makeCDXLicenses(scanData.ConcludedLicenseSPDX),
			}
		}
		if len(scanData.Copyrights) > 0 {
			if rootComp.Evidence == nil {
				rootComp.Evidence = &cdxEvidence{}
			}
			for _, c := range scanData.Copyrights {
				rootComp.Evidence.Copyright = append(rootComp.Evidence.Copyright,
					cdxCopyrightEvidence{Text: c})
			}
			// Also set the top-level copyright field with the first holder.
			rootComp.Copyright = scanData.Copyrights[0]
			if len(scanData.Copyrights) > 1 {
				rootComp.Copyright += fmt.Sprintf(" (and %d others)", len(scanData.Copyrights)-1)
			}
		}
	}

	// v0.27.23: when ScanCode evidence contributed to this document,
	// record the toolkit version that produced it. The external tools
	// are installed unpinned and auto-updated monthly, so without this
	// two SBOMs of the same commit could differ with nothing in the
	// document explaining why.
	toolComponents := []cdxToolComponent{{
		Type:    "application",
		Name:    "aveloxis",
		Version: db.ToolVersion,
		Author:  "Augur Labs",
	}}
	if scanData != nil && scanData.ScancodeVersion != "" {
		toolComponents = append(toolComponents, cdxToolComponent{
			Type:    "application",
			Name:    "scancode-toolkit-mini",
			Version: scanData.ScancodeVersion,
		})
	}

	bom := cycloneDX{
		BOMFormat:    "CycloneDX",
		SpecVersion:  "1.5",
		SerialNumber: "urn:uuid:" + uuid.New().String(),
		Version:      1,
		Metadata: cdxMetadata{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Tools:     cdxTools{Components: toolComponents},
			Component: rootComp,
		},
	}

	// Track dep bom-refs for the dependencies graph. v0.27.29:
	// bom-ref must be UNIQUE per the CycloneDX spec — two manifests
	// declaring the same package+version yield the same purl and are
	// emitted as ONE component (the audit's 1d finding: nothing
	// previously guarded against colliding refs).
	var depRefs []string
	seenRefs := map[string]bool{}
	// byName resolves graph endpoints (name-level, per the v0.27.133
	// edge model) to component bom-refs across BOTH populations.
	// byNameVer (round-19) adds a version-exact key for PARENT
	// resolution: an edge carries the parent's resolved version, and
	// attaching its children to every same-name component would hang
	// one version's dependencies off another version's node. Children
	// stay name-level — lockfiles express parent → name@range, so an
	// exact child version is not derivable.
	byName := map[string][]string{}
	byNameVer := map[string][]string{}

	for _, dep := range deps {
		if dep.Purl != "" && seenRefs[dep.Purl] {
			continue
		}
		if dep.Purl != "" {
			seenRefs[dep.Purl] = true
		}
		comp := cdxComponent{
			Type:    "library",
			Name:    dep.Name,
			Version: dep.CurrentVersion,
			Purl:    dep.Purl,
			BOMRef:  dep.Purl,
		}
		// CycloneDX scope describes runtime inclusion (v0.27.46:
		// mapping centralized in model — required for runtime,
		// optional for optional/peer, excluded for dev/test/build).
		comp.Scope = model.CycloneDXScopeForScope(dep.Type)
		if dep.License != "" {
			comp.Licenses = makeCDXLicenses(dep.License)
		}
		bom.Components = append(bom.Components, comp)

		if dep.Purl != "" {
			depRefs = append(depRefs, dep.Purl)
			k := sbomGraphKey(dep.PackageManager, dep.Name)
			byName[k] = append(byName[k], dep.Purl)
			byNameVer[k+"@"+dep.CurrentVersion] = append(byNameVer[k+"@"+dep.CurrentVersion], dep.Purl)
		}
	}

	// v0.27.134: lockfile transitives join the component list. Purls
	// come from purlForPackage — the SAME builder the vuln scan uses,
	// so cross-kind dedup against direct components works by ref.
	// Unmapped ecosystems yield "" and are honestly omitted (they can
	// never be referenced by a resolvable graph edge either). NO
	// license data — lockfiles don't carry it; absence beats guessing.
	var transRefs []string
	if graph != nil {
		for _, t := range graph.Transitives {
			purl := purlForPackage(t.Ecosystem, t.PackageName, t.ResolvedVersion)
			if purl == "" {
				continue
			}
			k := sbomGraphKey(t.Ecosystem, t.PackageName)
			if !sliceContains(byName[k], purl) {
				byName[k] = append(byName[k], purl)
			}
			kv := k + "@" + t.ResolvedVersion
			if !sliceContains(byNameVer[kv], purl) {
				byNameVer[kv] = append(byNameVer[kv], purl)
			}
			if seenRefs[purl] {
				continue // already a direct component (bom-ref must stay unique)
			}
			seenRefs[purl] = true
			bom.Components = append(bom.Components, cdxComponent{
				Type:    "library",
				Name:    t.PackageName,
				Version: t.ResolvedVersion,
				Purl:    purl,
				BOMRef:  purl,
				Scope:   model.CycloneDXScopeForScope(t.Scope),
			})
			transRefs = append(transRefs, purl)
		}
	}

	// The dependencies graph. Root depends on the DIRECT set (same as
	// the pre-v0.27.134 flat shape — direct deps were the only
	// components). With edges present, parent components carry their
	// REAL children; without them every component stays a leaf, so a
	// knob-off fleet's output is byte-identical to before.
	childrenOf := map[string]map[string]bool{}
	if graph != nil {
		for _, e := range graph.Edges {
			// Round-19: version-exact parent match first — the edge
			// names the parent's RESOLVED version, so its children
			// belong to that version's component alone. Name-level is
			// the fallback for direct deps whose recorded (manifest)
			// version differs from the lockfile-resolved one.
			pk := sbomGraphKey(e.Ecosystem, e.ParentName)
			prefs := byNameVer[pk+"@"+e.ParentVersion]
			if len(prefs) == 0 {
				prefs = byName[pk]
			}
			for _, pref := range prefs {
				for _, cref := range byName[sbomGraphKey(e.Ecosystem, e.ChildName)] {
					if cref == pref {
						continue // self-edge noise
					}
					if childrenOf[pref] == nil {
						childrenOf[pref] = map[string]bool{}
					}
					childrenOf[pref][cref] = true
				}
			}
		}
	}
	bom.Dependencies = []cdxDependency{{
		Ref:       rootRef,
		DependsOn: depRefs,
	}}
	for _, ref := range append(append([]string{}, depRefs...), transRefs...) {
		children := make([]string, 0, len(childrenOf[ref]))
		for c := range childrenOf[ref] {
			children = append(children, c)
		}
		sort.Strings(children) // deterministic output
		bom.Dependencies = append(bom.Dependencies, cdxDependency{
			Ref:       ref,
			DependsOn: children,
		})
	}

	return json.MarshalIndent(bom, "", "  ")
}

// v0.27.29 — multi-license emission semantics (the wrong-answer-tests
// audit's " AND " finding):
//
//   - REGISTRY license lists arrive stored as "A AND B" (the
//     analysis-phase joiner), but a registry listing two licenses
//     almost always means DUAL-LICENSING — a choice. Asserting AND
//     (the consumer must satisfy both) inverts the legal obligation.
//   - CycloneDX: emitted as MULTIPLE licenses[] entries — the spec's
//     honest "relationship unstated" form; each element still gets
//     id-vs-name treatment individually.
//   - SPDX licenseDeclared: each part normalized via
//     db.NormalizeLicenseToSPDX ("Apache 2.0" → Apache-2.0), then
//     validated against the embedded official id list; all-valid
//     multi-license joins with OR (dual-licensing alternatives), any
//     unmappable part → NOASSERTION (SPDX requires a parseable
//     expression, NOASSERTION, or NONE — free text is grammar-invalid).
//   - ScanCode's OWN expressions pass through untouched: the toolkit
//     emits valid SPDX expressions by construction, and its
//     whole-tree AND is semantically CORRECT (different files under
//     different licenses = conjunction). CDX carries compounds in the
//     expression field.
//
// Storage semantics (repo_deps_libyear.license keeping " AND " as the
// list separator) are deliberately unchanged tonight — flagged in the
// v0.27.29 changelog for operator review, since changing the stored
// form touches the license table display fleet-wide.

// makeCDXLicenses expands a stored license string into CycloneDX
// entries: one per " AND "-separated element (registry list), or a
// single expression entry when the string is a genuine SPDX compound
// from ScanCode (contains an operator and every token validates).
func makeCDXLicenses(raw string) []cdxLicense {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, " AND ")
	if len(parts) == 1 {
		return []cdxLicense{makeCDXLicense(raw)}
	}
	if allValidSPDXIDs(parts) {
		// A parseable compound — CDX's expression field is the
		// machine-readable home for it.
		return []cdxLicense{{Expression: raw}}
	}
	out := make([]cdxLicense, 0, len(parts))
	for _, p := range parts {
		out = append(out, makeCDXLicense(strings.TrimSpace(p)))
	}
	return out
}

func allValidSPDXIDs(parts []string) bool {
	for _, p := range parts {
		if !isSPDXLicense(strings.TrimSpace(p)) {
			return false
		}
	}
	return len(parts) > 0
}

// spdxDeclaredLicense renders a stored license string as a VALID SPDX
// licenseDeclared value: a single id, an OR-joined expression of
// normalized ids, or NOASSERTION. Never free text.
func spdxDeclaredLicense(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "NOASSERTION"
	}
	parts := strings.Split(raw, " AND ")
	ids := make([]string, 0, len(parts))
	for _, p := range parts {
		n := db.NormalizeLicenseToSPDX(strings.TrimSpace(p))
		if !isSPDXLicense(n) {
			return "NOASSERTION"
		}
		ids = append(ids, n)
	}
	if len(ids) == 1 {
		return ids[0]
	}
	return "(" + strings.Join(ids, " OR ") + ")"
}

// makeCDXLicense creates a CycloneDX license entry, using the id field for
// recognized SPDX identifiers and the name field for non-standard strings.
func makeCDXLicense(license string) cdxLicense {
	obj := &cdxLicenseObj{}
	if isSPDXLicense(license) {
		obj.ID = license
	} else if n := db.NormalizeLicenseToSPDX(license); isSPDXLicense(n) {
		// v0.27.29: registry synonyms ("Apache 2.0") normalize to
		// their SPDX id instead of demoting to free-text name.
		obj.ID = n
	} else {
		obj.Name = license
	}
	return cdxLicense{License: obj}
}

// ============================================================
// SPDX 2.3
// ============================================================

type spdxDoc struct {
	SPDXVersion       string         `json:"spdxVersion"`
	DataLicense       string         `json:"dataLicense"`
	SPDXID            string         `json:"SPDXID"`
	Name              string         `json:"name"`
	DocumentNamespace string         `json:"documentNamespace"`
	CreationInfo      spdxCreation   `json:"creationInfo"`
	Packages          []spdxPackage  `json:"packages"`
	Relationships     []spdxRelation `json:"relationships"`
}

type spdxCreation struct {
	Created  string   `json:"created"`
	Creators []string `json:"creators"`
}

type spdxPackage struct {
	SPDXID      string `json:"SPDXID"`
	Name        string `json:"name"`
	VersionInfo string `json:"versionInfo,omitempty"`
	// FilesAnalyzed is ALWAYS emitted (no omitempty) and always false
	// (v0.27.33): SPDX 2.3 §7.8 defaults an OMITTED filesAnalyzed to
	// true, and §7.9 then makes packageVerificationCode MANDATORY. We
	// never analyze package files (no files section, no verification
	// code), so omitting the field silently made every package
	// non-conformant. Declaring false is the honest, conformant state.
	FilesAnalyzed    bool              `json:"filesAnalyzed"`
	DownloadLocation string            `json:"downloadLocation"`
	LicenseConcluded string            `json:"licenseConcluded"`
	LicenseDeclared  string            `json:"licenseDeclared"`
	CopyrightText    string            `json:"copyrightText,omitempty"`
	ExternalRefs     []spdxExternalRef `json:"externalRefs,omitempty"`
}

type spdxExternalRef struct {
	ReferenceCategory string `json:"referenceCategory"`
	ReferenceType     string `json:"referenceType"`
	ReferenceLocator  string `json:"referenceLocator"`
}

type spdxRelation struct {
	SpdxElementId      string `json:"spdxElementId"`
	RelationshipType   string `json:"relationshipType"`
	RelatedSpdxElement string `json:"relatedSpdxElement"`
}

func generateSPDX(repo *db.RepoForSBOM, deps []db.SBOMDep, scanData *db.ScancodeForSBOM, graph *sbomGraph) ([]byte, error) {
	// Namespace must be unique per document (SPDX spec requirement).
	docUUID := uuid.New().String()
	doc := spdxDoc{
		SPDXVersion:       "SPDX-2.3",
		DataLicense:       "CC0-1.0",
		SPDXID:            "SPDXRef-DOCUMENT",
		Name:              repo.Name,
		DocumentNamespace: fmt.Sprintf("https://aveloxis.io/spdx/%s/%s/%s", repo.Owner, repo.Name, docUUID),
		CreationInfo: spdxCreation{
			Created:  time.Now().UTC().Format(time.RFC3339),
			Creators: []string{"Tool: aveloxis-" + db.ToolVersion},
		},
	}
	// v0.27.23: same provenance rule as CycloneDX — name the ScanCode
	// version whenever its evidence shaped licenseConcluded/copyright.
	if scanData != nil && scanData.ScancodeVersion != "" {
		doc.CreationInfo.Creators = append(doc.CreationInfo.Creators,
			"Tool: scancode-toolkit-mini-"+scanData.ScancodeVersion)
	}

	// Root package for the repo itself.
	// LicenseDeclared = from GitHub/GitLab API (what the repo claims).
	// LicenseConcluded = from ScanCode source analysis (what's actually detected).
	concludedLicense := spdxDeclaredLicense(repo.License) // v0.27.29: valid expression or NOASSERTION, never free text
	copyrightText := "NOASSERTION"
	if scanData != nil {
		if scanData.ConcludedLicenseSPDX != "" {
			concludedLicense = scanData.ConcludedLicenseSPDX
		}
		if len(scanData.Copyrights) > 0 {
			copyrightText = strings.Join(scanData.Copyrights, "\n")
		}
	}
	rootPkg := spdxPackage{
		SPDXID:           "SPDXRef-RootPackage",
		Name:             repo.Name,
		DownloadLocation: repo.GitURL,
		LicenseConcluded: concludedLicense,
		LicenseDeclared:  spdxDeclaredLicense(repo.License),
		CopyrightText:    copyrightText,
	}
	doc.Packages = append(doc.Packages, rootPkg)

	// byName resolves graph endpoints to SPDXIDs (v0.27.134 — same
	// name-level model as the CycloneDX side, with byNameVer's
	// round-19 version-exact parent resolution); seenIDs guards the
	// document-validity rule that SPDXIDs are unique (a transitive
	// row matching a direct dep's name@version hashes identically).
	byName := map[string][]string{}
	byNameVer := map[string][]string{}
	seenIDs := map[string]bool{}

	for _, dep := range deps {
		// Stable package ID based on a hash of the name+version, not loop index.
		// This ensures IDs don't change when the dep list is reordered.
		pkgID := spdxPackageID(dep.PackageManager, dep.Name, dep.CurrentVersion)
		declared := spdxDeclaredLicense(dep.License) // v0.27.29
		seenIDs[pkgID] = true
		k := sbomGraphKey(dep.PackageManager, dep.Name)
		byName[k] = append(byName[k], pkgID)
		byNameVer[k+"@"+dep.CurrentVersion] = append(byNameVer[k+"@"+dep.CurrentVersion], pkgID)

		pkg := spdxPackage{
			SPDXID:      pkgID,
			Name:        dep.Name,
			VersionInfo: dep.CurrentVersion,
			// SPDX downloadLocation requires a VCS/download URL, not a purl.
			// Purls are emitted as externalRefs below.
			DownloadLocation: "NOASSERTION",
			// LicenseConcluded requires source analysis per-dep. Without per-dep
			// scancode data, we can only assert what the registry declares.
			LicenseConcluded: "NOASSERTION",
			LicenseDeclared:  declared,
		}
		if dep.Purl != "" {
			pkg.ExternalRefs = []spdxExternalRef{{
				ReferenceCategory: "PACKAGE-MANAGER",
				ReferenceType:     "purl",
				ReferenceLocator:  dep.Purl,
			}}
		}
		doc.Packages = append(doc.Packages, pkg)

		// v0.27.46 (summary/19 P3): SPDX 2.3 typed dependency
		// relationships. Non-runtime scopes use the inverted forms
		// (pkg DEV_DEPENDENCY_OF root, etc.); runtime keeps the
		// baseline root DEPENDS_ON pkg. Mapping lives in model so
		// this file never branches on literal scope values.
		relType, inverted := model.SPDXRelationshipForScope(dep.Type)
		rel := spdxRelation{
			SpdxElementId:      "SPDXRef-RootPackage",
			RelationshipType:   relType,
			RelatedSpdxElement: pkgID,
		}
		if inverted {
			rel.SpdxElementId, rel.RelatedSpdxElement = pkgID, "SPDXRef-RootPackage"
		}
		doc.Relationships = append(doc.Relationships, rel)
	}

	// v0.27.134: lockfile transitives join the package list (licenses
	// NOASSERTION — lockfiles carry none), then the requirement edges
	// become pkg→pkg DEPENDS_ON relationships. Transitives no edge can
	// reach stay relationship-less on purpose: relating them to root
	// would fabricate directness. Root's relationships to the DIRECT
	// set above are unchanged.
	if graph != nil {
		for _, t := range graph.Transitives {
			pkgID := spdxPackageID(t.Ecosystem, t.PackageName, t.ResolvedVersion)
			k := sbomGraphKey(t.Ecosystem, t.PackageName)
			if !sliceContains(byName[k], pkgID) {
				byName[k] = append(byName[k], pkgID)
			}
			kv := k + "@" + t.ResolvedVersion
			if !sliceContains(byNameVer[kv], pkgID) {
				byNameVer[kv] = append(byNameVer[kv], pkgID)
			}
			if seenIDs[pkgID] {
				continue // SPDXID must stay unique (same name@version as a direct dep)
			}
			seenIDs[pkgID] = true
			pkg := spdxPackage{
				SPDXID:           pkgID,
				Name:             t.PackageName,
				VersionInfo:      t.ResolvedVersion,
				DownloadLocation: "NOASSERTION",
				LicenseConcluded: "NOASSERTION",
				LicenseDeclared:  "NOASSERTION",
			}
			if purl := purlForPackage(t.Ecosystem, t.PackageName, t.ResolvedVersion); purl != "" {
				pkg.ExternalRefs = []spdxExternalRef{{
					ReferenceCategory: "PACKAGE-MANAGER",
					ReferenceType:     "purl",
					ReferenceLocator:  purl,
				}}
			}
			doc.Packages = append(doc.Packages, pkg)
		}
		seenRel := map[string]bool{}
		for _, e := range graph.Edges {
			// Round-19: version-exact parent match first (see the
			// CycloneDX side) — name-level only as fallback.
			pk := sbomGraphKey(e.Ecosystem, e.ParentName)
			pIDs := byNameVer[pk+"@"+e.ParentVersion]
			if len(pIDs) == 0 {
				pIDs = byName[pk]
			}
			for _, pID := range pIDs {
				for _, cID := range byName[sbomGraphKey(e.Ecosystem, e.ChildName)] {
					if pID == cID || seenRel[pID+">"+cID] {
						continue
					}
					seenRel[pID+">"+cID] = true
					doc.Relationships = append(doc.Relationships, spdxRelation{
						SpdxElementId:      pID,
						RelationshipType:   "DEPENDS_ON",
						RelatedSpdxElement: cID,
					})
				}
			}
		}
	}

	// Document describes root package.
	doc.Relationships = append(doc.Relationships, spdxRelation{
		SpdxElementId:      "SPDXRef-DOCUMENT",
		RelationshipType:   "DESCRIBES",
		RelatedSpdxElement: "SPDXRef-RootPackage",
	})

	return json.MarshalIndent(doc, "", "  ")
}

// spdxPackageID generates a stable SPDX package identifier. Uses a
// truncated SHA-256 hash to ensure stability across regenerations
// regardless of dep ordering. Round-24: the hash input is the
// ECOSYSTEM-SCOPED graph key (db.LockfileGraphKey — alias-folded, so
// the same real package reached via the gem/rubygems vocabulary split
// still deduplicates) + "@" + version. A bare name@version hash
// collided npm/foo@1.0.0 with pypi/foo@1.0.0: seenIDs dropped the
// second package while its graph key kept the shared ID, silently
// pointing one ecosystem's relationships at the other's purl. IDs are
// document-scoped per the SPDX spec, so the scheme change only means
// regenerated documents carry new (still deterministic) SPDXIDs.
func spdxPackageID(eco, name, version string) string {
	h := sha256.Sum256([]byte(db.LockfileGraphKey(eco, name) + "@" + version))
	return fmt.Sprintf("SPDXRef-Package-%x", h[:8])
}

func orNoAssertion(s string) string {
	if s == "" {
		return "NOASSERTION"
	}
	return s
}

// isSPDXLicense checks whether a license string is a recognized SPDX license
// identifier. CycloneDX and SPDX tools require the exact SPDX ID for
// machine-readable policy enforcement.
func isSPDXLicense(license string) bool {
	_, ok := spdxLicenses[license]
	return ok
}

// spdxLicenseIDsRaw is the official SPDX license identifier list,
// embedded at compile time (v0.27.23). It replaces a hand-maintained
// ~70-entry allowlist that drifted monotonically from the real list
// (733 identifiers) — valid-but-unlisted ids were demoted from
// license.id to license.name, which downstream policy engines don't
// match on. Refresh procedure is in the file's header comment; the
// //go:embed follows the v0.25.4 NumFocus-catalog precedent (data
// ships inside the binary, no network at runtime).
//
//go:embed spdx_license_ids.txt
var spdxLicenseIDsRaw string

// spdxLicenses is the parsed identifier set. Lines starting with '#'
// are header comments in the generated file.
var spdxLicenses = func() map[string]bool {
	set := make(map[string]bool, 800)
	for line := range strings.SplitSeq(spdxLicenseIDsRaw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		set[line] = true
	}
	return set
}()

// StoreSBOM saves the generated SBOM JSON to repo_sbom_scans.
func StoreSBOM(ctx context.Context, store *db.PostgresStore, repoID int64, sbomJSON []byte) error {
	return store.InsertSBOM(ctx, repoID, sbomJSON)
}

// GenerateAndStoreSBOMs generates both CycloneDX and SPDX SBOMs for a repo
// and stores them in the database. Called at the end of each collection run.
// Errors are non-fatal — if SBOM generation fails, collection still succeeds.
func GenerateAndStoreSBOMs(ctx context.Context, store *db.PostgresStore, repoID int64, logger *slog.Logger) {
	for _, spec := range []struct {
		format  SBOMFormat
		name    string
		version string
	}{
		{FormatCycloneDX, "cyclonedx", "1.5"},
		{FormatSPDX, "spdx", "2.3"},
	} {
		data, err := GenerateSBOM(ctx, store, repoID, spec.format)
		if err != nil {
			logger.Debug("SBOM generation skipped", "repo_id", repoID, "format", spec.name, "error", err)
			continue
		}
		if err := store.InsertSBOMWithFormat(ctx, repoID, data, spec.name, spec.version); err != nil {
			logger.Warn("failed to store SBOM", "repo_id", repoID, "format", spec.name, "error", err)
		}
	}
}
