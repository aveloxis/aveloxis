// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — sbom.go generates CycloneDX and SPDX Software Bill of
// Materials from the dependency and libyear data collected for a repository.
package collector

import (
	"context"
	"crypto/sha256"
	_ "embed" // cdx_license_ids_1_7.txt
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/spdx"
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
	trans, err := store.GetRepoTransitivePackagesWithPaths(ctx, repoID)
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

// purlLessBOMRef is the CycloneDX bom-ref of a component that has no
// purl: "aveloxis:" + graph key + "@" + version. The prefix cannot
// collide with a purl ("pkg:") or the root ref, and the graph key is the
// same identity SPDX hashes into its package id, so the two formats name
// one package the same way (v0.29.58).
func purlLessBOMRef(graphKey, version string) string {
	return "aveloxis:" + graphKey + "@" + version
}

// sbomGraphIndex resolves edge endpoints WITH lockfile provenance —
// v0.27.151 (round 30), the round-19 per-lockfile rule applied to the
// SBOM side: a monorepo's lockfiles are independent resolved graphs,
// and a repo-wide name index let an edge from apps/a attach children
// resolved from apps/b's same-name entries, fabricating dependency
// relationships in both formats. Transitive refs partition per
// lockfile (an edge resolves inside its OWN lockfile first); DIRECT
// components stay a repo-wide fallback — manifest rows carry no path
// column (the round-20 carve-out), and they cover Go-declared deps
// and manifest-version-differs parents. ONE resolver shared by both
// generators (SR-17) — the ref vocabulary (purl vs SPDXID) is the
// only per-format difference.
type sbomGraphIndex struct {
	directByName    map[string][]string
	directByNameVer map[string][]string
	lockByName      map[string]map[string][]string
	lockByNameVer   map[string]map[string][]string
}

func newSBOMGraphIndex() *sbomGraphIndex {
	return &sbomGraphIndex{
		directByName:    map[string][]string{},
		directByNameVer: map[string][]string{},
		lockByName:      map[string]map[string][]string{},
		lockByNameVer:   map[string]map[string][]string{},
	}
}

func (x *sbomGraphIndex) addDirect(k, version, ref string) {
	x.directByName[k] = append(x.directByName[k], ref)
	x.directByNameVer[k+"@"+version] = append(x.directByNameVer[k+"@"+version], ref)
}

func (x *sbomGraphIndex) addTransitive(lockfile, k, version, ref string) {
	if x.lockByName[lockfile] == nil {
		x.lockByName[lockfile] = map[string][]string{}
		x.lockByNameVer[lockfile] = map[string][]string{}
	}
	if !sliceContains(x.lockByName[lockfile][k], ref) {
		x.lockByName[lockfile][k] = append(x.lockByName[lockfile][k], ref)
	}
	kv := k + "@" + version
	if !sliceContains(x.lockByNameVer[lockfile][kv], ref) {
		x.lockByNameVer[lockfile][kv] = append(x.lockByNameVer[lockfile][kv], ref)
	}
}

// parentRefs — round-19 version-exact-first, now within the edge's own
// lockfile first: lockfile version-exact → lockfile name-level →
// direct version-exact → direct name-level.
func (x *sbomGraphIndex) parentRefs(e db.RepoLockfileEdge) []string {
	pk := sbomGraphKey(e.Ecosystem, e.ParentName)
	if refs := x.lockByNameVer[e.LockfilePath][pk+"@"+e.ParentVersion]; len(refs) > 0 {
		return refs
	}
	if refs := x.lockByName[e.LockfilePath][pk]; len(refs) > 0 {
		return refs
	}
	if refs := x.directByNameVer[pk+"@"+e.ParentVersion]; len(refs) > 0 {
		return refs
	}
	return x.directByName[pk]
}

// childRefs — children stay name-level (lockfiles express
// parent → name@range), resolved inside the edge's own lockfile with
// the direct set as the only fallback.
func (x *sbomGraphIndex) childRefs(e db.RepoLockfileEdge) []string {
	ck := sbomGraphKey(e.Ecosystem, e.ChildName)
	if refs := x.lockByName[e.LockfilePath][ck]; len(refs) > 0 {
		return refs
	}
	return x.directByName[ck]
}

// ============================================================
// CycloneDX 1.7 (worklist 53, decision 6: 1.5 until v0.29.67)
// ============================================================

// cdxSpecVersion and cdxSchemaURL name the one CycloneDX version the
// export targets; cdx_license_ids_1_7.txt is that version's license-ID enum.
const (
	cdxSpecVersion = "1.7"
	cdxSchemaURL   = "http://cyclonedx.org/schema/bom-1.7.schema.json"
)

type cycloneDX struct {
	Schema       string          `json:"$schema"`
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

// cdxTools uses the CycloneDX object form (components + services),
// introduced in 1.5, instead of the deprecated bare array.
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

// cdxLicense models CycloneDX's licenseChoice: EITHER a license
// object (id or name) OR an expression — never both. v0.27.29 added
// the expression arm: before it, ScanCode's compound expressions
// ("MIT AND Apache-2.0") failed isSPDXLicense and fell into
// license.name as machine-unreadable free text, invisible to policy
// engines.
type cdxLicense struct {
	License    *cdxLicenseObj `json:"license,omitempty"`
	Expression string         `json:"expression,omitempty"`
	// Acknowledgement (CycloneDX 1.6+) on the expression arm: "declared"
	// for a registry's license, "concluded" for ScanCode's.
	Acknowledgement string `json:"acknowledgement,omitempty"`
}

type cdxLicenseObj struct {
	ID              string `json:"id,omitempty"`
	Name            string `json:"name,omitempty"`
	Acknowledgement string `json:"acknowledgement,omitempty"`
}

// cdxEvidence holds CycloneDX evidence for concluded (detected) data.
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
		// Evidence only when there is some (review round 10: a no-license
		// conclusion left an empty "evidence": {}).
		if lics := makeCDXLicenses(scanData.ConcludedLicenseSPDX, "concluded"); lics != nil {
			rootComp.Evidence = &cdxEvidence{Licenses: lics}
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
		Schema:       cdxSchemaURL,
		BOMFormat:    "CycloneDX",
		SpecVersion:  cdxSpecVersion,
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
	// v0.27.155 (round 34): the same duplicate-scope fold as the SPDX
	// side — the component's single Scope field must carry the
	// strongest observation, not whichever manifest the walk met
	// first (a dev-first order marked a RUNTIME dep "excluded").
	//
	// v0.29.58 (review rounds 2 and 3): one component per graph key +
	// version, the identity SPDX has always used (spdxPackageID). Two
	// passes: the first folds every row of a package into one entry —
	// scope by StrongerScope, the purl from whichever row carries one
	// (a purl-less row and a purl-ful row of one package are the SAME
	// package), the first license seen — and the second emits it. A
	// package with no purl on any row gets an aveloxis: bom-ref
	// (purlLessBOMRef), so it can carry edges and a transitive of the
	// same package can dedupe against it.
	type cdxDirect struct {
		gk, version, name, purl, scope, license string
	}
	directByKeyVer := map[string]*cdxDirect{}
	var directOrder []string
	for _, dep := range deps {
		gk := sbomGraphKey(dep.PackageManager, dep.Name)
		kv := gk + "@" + dep.CurrentVersion
		d, ok := directByKeyVer[kv]
		if !ok {
			d = &cdxDirect{gk: gk, version: dep.CurrentVersion, name: dep.Name, purl: dep.Purl, scope: dep.Type, license: dep.License}
			directByKeyVer[kv] = d
			directOrder = append(directOrder, kv)
			continue
		}
		d.scope = model.StrongerScope(d.scope, dep.Type)
		if d.purl == "" {
			d.purl = dep.Purl
		}
		if d.license == "" {
			d.license = dep.License
		}
	}
	// v0.27.151 (round 30): endpoint resolution goes through the
	// per-lockfile sbomGraphIndex — see its doc for the fabrication
	// class the old repo-wide maps produced.
	gidx := newSBOMGraphIndex()
	directRefByKeyVer := map[string]string{}
	for _, kv := range directOrder {
		d := directByKeyVer[kv]
		ref := d.purl
		if ref == "" {
			ref = purlLessBOMRef(d.gk, d.version)
		}
		directRefByKeyVer[kv] = ref
		if seenRefs[ref] {
			continue // two graph keys folding to one purl: bom-ref must stay unique
		}
		seenRefs[ref] = true
		comp := cdxComponent{
			Type:    "library",
			Name:    d.name,
			Version: d.version,
			Purl:    d.purl,
			BOMRef:  ref,
		}
		// CycloneDX scope describes runtime inclusion (v0.27.46:
		// mapping centralized in model — required for runtime,
		// optional for optional/peer, excluded for dev/test/build).
		comp.Scope = model.CycloneDXScopeForScope(d.scope)
		if d.license != "" {
			comp.Licenses = makeCDXLicenses(d.license, "declared")
		}
		bom.Components = append(bom.Components, comp)

		depRefs = append(depRefs, ref)
		gidx.addDirect(d.gk, d.version, ref)
	}

	// v0.27.134: lockfile transitives join the component list. Purls
	// come from purlForPackage — the SAME builder the vuln scan uses,
	// so cross-kind dedup against direct components works by ref. A
	// transitive with no valid purl (an unmapped ecosystem, or a
	// namespace-required type without one, v0.29.58) is still a
	// component: it takes the direct component's ref when the same
	// package@version was declared directly, else an aveloxis: ref
	// (purlLessBOMRef), and joins the graph index either way so its
	// edges resolve. NO license data — lockfiles don't carry it;
	// absence beats guessing.
	var transRefs []string
	if graph != nil {
		for _, t := range graph.Transitives {
			purl := purlForPackage(t.Ecosystem, t.Namespace, t.PackageName, t.ResolvedVersion)
			if purl == "" {
				// v0.29.58 (review rounds 1 and 2): a package with no valid
				// purl is still a component of the software — SPDX lists
				// it without a locator, so CycloneDX does too. Its ref is
				// the direct component's when the same package@version was
				// declared directly (one identity, as SPDX has), else an
				// aveloxis: ref; it joins the graph index so the edges that
				// name it resolve. Never scanned (no purl to send).
				gk := sbomGraphKey(t.Ecosystem, t.PackageName)
				ref, declared := directRefByKeyVer[gk+"@"+t.ResolvedVersion]
				if !declared {
					ref = purlLessBOMRef(gk, t.ResolvedVersion)
				}
				gidx.addTransitive(t.LockfilePath, gk, t.ResolvedVersion, ref)
				if seenRefs[ref] {
					continue
				}
				seenRefs[ref] = true
				bom.Components = append(bom.Components, cdxComponent{
					Type:    "library",
					Name:    t.PackageName,
					Version: t.ResolvedVersion,
					BOMRef:  ref,
					Scope:   model.CycloneDXScopeForScope(t.Scope),
				})
				transRefs = append(transRefs, ref)
				continue
			}
			gidx.addTransitive(t.LockfilePath, sbomGraphKey(t.Ecosystem, t.PackageName), t.ResolvedVersion, purl)
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
			// Round-19: version-exact parent first; round-30: both
			// endpoints resolve inside the edge's OWN lockfile, with
			// the direct set as the only repo-wide fallback.
			for _, pref := range gidx.parentRefs(e) {
				for _, cref := range gidx.childRefs(e) {
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

// License emission (v0.27.29, rewritten in v0.29.67 for worklist 53; design
// in summary/39):
//
//   - The stored value is one SPDX expression. Registry license LISTS are
//     joined with OR when they are written (joinRegistryLicenseList), so a
//     stored AND is a real conjunction and is never rewritten.
//   - SPDX: the normalized expression when it validates (internal/spdx),
//     NOASSERTION otherwise; never free text. Every LicenseRef- used is
//     declared in hasExtractedLicensingInfos.
//   - CycloneDX: at most one licenseChoice entry (none for a no-license
//     value), license.id / expression / license.name by cdxLicenseFor's
//     rules.
//   - ScanCode's per-file expressions are joined with AND through
//     spdx.JoinExpressions (different files under different licenses is a
//     conjunction), with each file's own expression parenthesized.

// makeCDXLicenses renders a stored license as a CycloneDX licenseChoice: at
// most ONE entry (Aveloxis's contract; see cdxLicenseFor for which arm), and
// none for an empty or no-license value.
func makeCDXLicenses(raw, acknowledgement string) []cdxLicense {
	raw = strings.TrimSpace(raw)
	// A no-license sentinel (NOASSERTION, NONE, N/A) normalizes to
	// "Unknown": no entry, as SPDX says NOASSERTION (review round 9; an
	// entry named "Unknown" read as a license).
	if raw == "" {
		return nil
	}
	n := db.NormalizeLicenseToSPDX(raw) // once (mcp-gopls review A6)
	if n == "Unknown" {
		return nil
	}
	return []cdxLicense{cdxLicenseForNormalized(n, acknowledgement, inCDXLicenseEnum)}
}

// cdxLicenseFor picks the licenseChoice arm for one stored license, after
// normalization (db.NormalizeLicenseToSPDX):
//   - license.id for an SPDX ID the target schema's enum lists (inEnum);
//   - expression for any other valid SPDX expression: a compound, or a
//     single ID newer than the enum (a one-term expression is valid in every
//     schema version, and the enum is frozen per version, decision 6);
//   - license.name for anything else (free text, house family labels).
func cdxLicenseFor(raw, acknowledgement string, inEnum func(string) bool) cdxLicense {
	return cdxLicenseForNormalized(db.NormalizeLicenseToSPDX(raw), acknowledgement, inEnum)
}

// cdxLicenseForNormalized is the production path (makeCDXLicenses normalizes
// once); it applies the arm rules documented on cdxLicenseFor, which remains
// the raw-input convenience the tests use.
func cdxLicenseForNormalized(n, acknowledgement string, inEnum func(string) bool) cdxLicense {
	if spdx.IsLicenseID(n) && inEnum(n) {
		return cdxLicense{License: &cdxLicenseObj{ID: n, Acknowledgement: acknowledgement}}
	}
	if spdx.Valid(n) {
		return cdxLicense{Expression: n, Acknowledgement: acknowledgement}
	}
	return cdxLicense{License: &cdxLicenseObj{Name: n, Acknowledgement: acknowledgement}}
}

// spdxDeclaredLicense renders a stored license as a VALID SPDX license field
// (licenseDeclared, and licenseConcluded from ScanCode): the normalized
// expression when it validates, NOASSERTION otherwise, never free text.
// Until v0.29.67 it split on " AND " and required bare IDs, which turned every
// OR, WITH and parenthesized license into NOASSERTION and rewrote a real
// AND as OR (worklist 53, D3). A DocumentRef- names a license in ANOTHER
// document this one would have to reference, so it is NOASSERTION too.
func spdxDeclaredLicense(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "NOASSERTION"
	}
	n := db.NormalizeLicenseToSPDX(raw)
	if !spdx.Valid(n) || strings.Contains(strings.ToLower(n), "documentref-") {
		return "NOASSERTION"
	}
	return n
}

// assumedChoiceComment is the SPDX licenseComments for a license list whose
// registry does not state how the licenses combine (RubyGems, Hex): the
// writer stored it as OR (joinRegistryLicenseList, decision 2), and the
// document says that was an assumption. Composer documents its list as a
// choice, so it needs no note.
func assumedChoiceComment(packageManager, declared string) string {
	if !strings.Contains(declared, " OR ") {
		return ""
	}
	for eco, registry := range map[string]string{"rubygems": "RubyGems", "hex": "Hex"} {
		if db.LockfileGraphKey(packageManager, "x") == db.LockfileGraphKey(eco, "x") {
			return "The " + registry + " registry records a package's licenses as a list that does not state how they combine; " +
				"Aveloxis reads such a list as a choice (OR), the common dual-licensing reading."
		}
	}
	return ""
}

// extractedLicensesFor declares every LicenseRef- the document's packages
// use (SPDX 2.3 section 10: a LicenseRef- without a hasExtractedLicensingInfos
// entry makes the document invalid; ScanCode's LicenseRef-scancode-* were
// emitted that way until v0.29.67). Aveloxis never captures the license text,
// so extractedText says so and names the source.
func extractedLicensesFor(pkgs []spdxPackage) []spdxExtractedLicense {
	seen := map[string]bool{}
	var out []spdxExtractedLicense
	for _, p := range pkgs {
		for _, field := range []string{p.LicenseDeclared, p.LicenseConcluded} {
			for _, tok := range strings.FieldsFunc(field, func(r rune) bool { return r == ' ' || r == '(' || r == ')' }) {
				if !strings.HasPrefix(tok, "LicenseRef-") || seen[tok] {
					continue
				}
				seen[tok] = true
				source := "the package registry"
				if strings.HasPrefix(tok, "LicenseRef-scancode-") {
					source = "ScanCode source analysis"
				}
				out = append(out, spdxExtractedLicense{
					LicenseID:     tok,
					Name:          strings.TrimPrefix(tok, "LicenseRef-"),
					ExtractedText: "The license text was not captured. Aveloxis recorded this identifier from " + source + ".",
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LicenseID < out[j].LicenseID })
	return out
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
	// ExtractedLicenses declares each LicenseRef- the packages use.
	ExtractedLicenses []spdxExtractedLicense `json:"hasExtractedLicensingInfos,omitempty"`
}

type spdxExtractedLicense struct {
	LicenseID     string `json:"licenseId"`
	ExtractedText string `json:"extractedText"`
	Name          string `json:"name"`
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
	LicenseComments  string            `json:"licenseComments,omitempty"`
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
			concludedLicense = spdxDeclaredLicense(scanData.ConcludedLicenseSPDX)
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

	// Graph-endpoint resolution goes through the per-lockfile
	// sbomGraphIndex (v0.27.151, round 30 — same resolver as the
	// CycloneDX side, SPDXID vocabulary); seenIDs guards the
	// document-validity rule that SPDXIDs are unique (a transitive
	// row matching a direct dep's name@version hashes identically).
	gidx := newSBOMGraphIndex()
	seenIDs := map[string]bool{}

	// v0.27.155 (round 34): duplicates of ONE package can carry
	// DIFFERENT scopes across a monorepo's manifests (runtime in one,
	// dev in another). Package emission dedupes by ID (round 33), so
	// the emitted relationship uses the STRONGEST scope across all
	// duplicates (model.StrongerScope — fail toward visibility),
	// making the document independent of manifest-walk order instead
	// of silently discarding the later DEPENDS_ON/DEV_DEPENDENCY_OF.
	scopeFor := map[string]string{}
	for _, dep := range deps {
		id := spdxPackageID(dep.PackageManager, dep.Name, dep.CurrentVersion)
		if cur, ok := scopeFor[id]; ok {
			scopeFor[id] = model.StrongerScope(cur, dep.Type)
		} else {
			scopeFor[id] = dep.Type
		}
	}

	for _, dep := range deps {
		// Stable package ID based on a hash of the name+version, not loop index.
		// This ensures IDs don't change when the dep list is reordered.
		pkgID := spdxPackageID(dep.PackageManager, dep.Name, dep.CurrentVersion)
		if seenIDs[pkgID] {
			// v0.27.154 (round 33): repo_deps_libyear has no unique and
			// the manifest walk appends from EVERY manifest — a monorepo
			// declaring the same eco/name/version twice emitted duplicate
			// SPDXIDs (an invalid document). First occurrence wins,
			// matching the transitive guard below; its package and root
			// relationship already exist.
			continue
		}
		declared := spdxDeclaredLicense(dep.License) // v0.27.29
		seenIDs[pkgID] = true
		gidx.addDirect(sbomGraphKey(dep.PackageManager, dep.Name), dep.CurrentVersion, pkgID)

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
			LicenseComments:  assumedChoiceComment(dep.PackageManager, declared),
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
		relType, inverted := model.SPDXRelationshipForScope(scopeFor[pkgID])
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
			gidx.addTransitive(t.LockfilePath, sbomGraphKey(t.Ecosystem, t.PackageName), t.ResolvedVersion, pkgID)
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
			if purl := purlForPackage(t.Ecosystem, t.Namespace, t.PackageName, t.ResolvedVersion); purl != "" {
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
			// Round-19: version-exact parent first; round-30: both
			// endpoints resolve inside the edge's OWN lockfile (see
			// sbomGraphIndex).
			for _, pID := range gidx.parentRefs(e) {
				for _, cID := range gidx.childRefs(e) {
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

	doc.ExtractedLicenses = extractedLicensesFor(doc.Packages)
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

// isSPDXLicense reports whether a string is an SPDX license identifier, as
// spelled by the official list. The list is internal/spdx's (worklist 53,
// SR-17); the collector's own embedded copy (v0.27.23) was retired in
// v0.29.67.
func isSPDXLicense(license string) bool { return spdx.IsLicenseID(license) }

// cdxLicenseIDsRaw is the CycloneDX 1.7 schema's license-ID enum; see the
// file's header for why it is not the SPDX list.
//
//go:embed cdx_license_ids_1_7.txt
var cdxLicenseIDsRaw string

// cdxLicenseIDs is the parsed enum. Lines starting with '#' are the
// generated file's header.
var cdxLicenseIDs = func() map[string]bool {
	set := make(map[string]bool, 900)
	for line := range strings.SplitSeq(cdxLicenseIDsRaw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		set[line] = true
	}
	return set
}()

func inCDXLicenseEnum(id string) bool { return cdxLicenseIDs[id] }

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
		{FormatCycloneDX, "cyclonedx", cdxSpecVersion},
		{FormatSPDX, "spdx", "2.3"},
	} {
		data, err := GenerateSBOM(ctx, store, repoID, spec.format)
		if err != nil {
			logger.Debug("SBOM generation skipped", "repo_id", repoID, "format", spec.name, "error", err)
			continue
		}
		err = store.InsertSBOMWithFormat(ctx, repoID, data, spec.name, spec.version)
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure (pass 35)
		}
		if err != nil {
			logger.Warn("failed to store SBOM", "repo_id", repoID, "format", spec.name, "error", err)
		}
	}
}
