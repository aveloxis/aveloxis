// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package pkgid is the one place that says what a package IS: its key (how two
// records are known to name the same package) and its purl (how it is written
// for OSV, for SBOM consumers and in a stored finding). The two are separate
// outputs with separate rules, and neither is derived from the other here.
//
// It is a leaf package (summary/70, section 3). Everything in it was spread
// over internal/collector (purl.go, vuln_targets.go, lockfile_parser.go) and
// internal/db (lockfile_store.go), and moved with no change in output: the
// golden in testdata was written by the old code over a sample of production's
// dependency names, and the purl specification's own cases are beside it.
//
// Two invariants hold everything else up:
//
//   - A purl is byte-identical to the one built before for the same input. A
//     finding's identity is (repo_id, vuln_id, package_purl); one changed byte
//     opens a new finding and closes the old one.
//   - A key is byte-identical to the one built before. Keys join lockfile
//     edges, direct-dependency sets and SBOM graphs; two spellings of one
//     fold drop edges without an error.
//
// A change of fold or of canonical form is therefore never a tidy-up. It is
// its own red-first step that lists every reader of the value.
//
// Reading a purl back is here too (purlparse.go): where its version begins,
// the same purl at another version, and the wire gate that says whether OSV
// would accept it.
//
// Five vocabularies name an ecosystem in Aveloxis: the libyear writers'
// package_manager ("rubygems", "go"), the lockfile roster's ecosystem
// ("packagist", "swiftpm"), the distribution subsystem's ("elixir", "dart"),
// the purl type ("gem", "golang") and OSV's. The functions here convert the
// first three to a purl type or to the folded ecosystem of a key.
package pkgid

import "strings"

// Per-type purl rules implemented (github.com/package-url/purl-spec,
// PURL-TYPES.md; the specification's own test cases are in
// testdata/purl_spec_cases.json):
//   - pypi: lowercase, '_' becomes '-'
//   - npm, golang, composer, nuget: lowercase
//   - all types: '/'-separated segments percent-encode the reserved
//     characters ('@' becomes %40, the npm scope case; '%', '?', '#', space);
//     the version likewise.
//
// OSV normalizes on its side (verified live in v0.27.29), so a non-canonical
// purl lost no finding; the canonical form matters to SBOM consumers that do
// not normalize, and to the finding's identity.

// purlReserved encodes the characters the purl specification reserves inside
// a namespace or name segment. '%' is first so nothing is encoded twice.
var purlReserved = strings.NewReplacer(
	"%", "%25",
	"@", "%40",
	"?", "%3F",
	"#", "%23",
	" ", "%20",
)

// EscapeSegment percent-encodes the characters the purl specification
// reserves inside one namespace, name or version segment. It is deliberately
// minimal: only the characters that collide with purl syntax.
func EscapeSegment(s string) string {
	return purlReserved.Replace(s)
}

// Purl assembles a canonical purl from a purl type, a name and a version.
// version may be empty (a versionless purl asks OSV for every advisory the
// package has). name may hold '/' separators (an npm scope, a Go module path,
// a Maven group and artifact once ':' has become '/'); each segment is
// escaped on its own so the separators survive. An empty type or name yields
// "": no purl, which every caller treats as "cannot be sent".
func Purl(typ, name, version string) string {
	if typ == "" || name == "" {
		return ""
	}
	switch typ {
	case "pypi":
		name = strings.ReplaceAll(strings.ToLower(name), "_", "-")
	case "npm", "golang", "composer", "nuget":
		name = strings.ToLower(name)
	}
	segs := strings.Split(name, "/")
	for i, s := range segs {
		segs[i] = EscapeSegment(s)
	}
	p := "pkg:" + typ + "/" + strings.Join(segs, "/")
	if version != "" {
		p += "@" + EscapeSegment(version)
	}
	return p
}

// managerPurlTypes maps a package-manager or lockfile-ecosystem string to its
// purl type. Both vocabularies are here on purpose: the lockfile roster emits
// "rubygems", "packagist", "swiftpm" and "hackage", and before v0.27.133 a
// missing alias dropped every such transitive package from the scan without a
// word.
var managerPurlTypes = map[string]string{
	"npm":       "npm",
	"pypi":      "pypi",
	"go":        "golang",
	"cargo":     "cargo",
	"gem":       "gem",
	"maven":     "maven",
	"composer":  "composer",
	"hex":       "hex",
	"nuget":     "nuget",
	"pub":       "pub",
	"swift":     "swift",
	"haskell":   "hackage",
	"rubygems":  "gem",
	"packagist": "composer",
	"swiftpm":   "swift",
	"hackage":   "hackage",
	// R (0.29.204). The purl keeps the name's case: OSV matches
	// pkg:cran/<name> by case (Purl folds only the types listed there).
	"cran": "cran",
}

// distributionPurlTypes maps the distribution subsystem's ecosystem strings
// (deps.dev, ecosyste.ms and manifest flavours: a different vocabulary from
// the package managers') to purl types. An ecosystem that is not here (conda,
// julia, cpp) has no purl and is left out, not guessed.
var distributionPurlTypes = map[string]string{
	"npm":      "npm",
	"pypi":     "pypi",
	"go":       "golang",
	"cargo":    "cargo",
	"rubygems": "gem",
	"gem":      "gem",
	"maven":    "maven",
	"composer": "composer",
	"elixir":   "hex",
	"hex":      "hex",
	"nuget":    "nuget",
	"dart":     "pub",
	"pub":      "pub",
	"swift":    "swift",
	// What ecosyste.ms calls it, and so what the distribution subsystem
	// stores (0.29.206: without this no Swift publisher had a purl).
	"swiftpm": "swift",
	"haskell": "hackage",
	// The same two registries under ecosyste.ms's names (0.29.208): every
	// spelling of the package managers' table is one here too
	// (TestEveryRegistrySpellingHasADistributionPurlType).
	"hackage":   "hackage",
	"packagist": "composer",
	// R (0.29.206): a repository that publishes to CRAN is asked about its
	// own package. See SelfNameKeepsCase.
	"cran": "cran",
}

// selfNameKeepsCase are the purl types whose own-package name is used as
// stored: the types whose names OSV matches BY CASE (asked live on
// 2026-11-19; the table is in the test). The store's query folded every
// own-package name to lower case from 0.27.29, which for these types asks
// about a package that does not exist whenever the real name has a capital
// (pkg:maven/xerces/xercesImpl has five advisories, .../xercesimpl none).
// Keeping the case re-keys nothing: a folded name with a capital found
// nothing, so no finding exists at such an identity (checked on both
// production databases).
//
// The types that are NOT here are folded as before: OSV ignores case for
// them (pypi, cargo, nuget, golang, composer), their identities exist in
// lower case, and Purl folds most of them itself; npm is folded by Purl.
var selfNameKeepsCase = map[string]bool{
	"cran": true, "swift": true, "maven": true, "gem": true, "hex": true, "pub": true, "hackage": true,
}

// SelfNameKeepsCase reports whether an own-package name of one of the
// distribution subsystem's ecosystems is used as stored, case kept.
func SelfNameKeepsCase(ecosystem string) bool {
	// An ecosystem asked by name (Bioconductor) is asked by the name as
	// stored: R package names are case-sensitive.
	if _, byName := osvEcosystemsByName[strings.ToLower(ecosystem)]; byName {
		return true
	}
	typ, ok := PurlTypeOfDistribution(ecosystem)
	return ok && selfNameKeepsCase[typ]
}

// PurlTypeOfManager is the purl type of a package-manager or
// lockfile-ecosystem string, and whether there is one. The lookup is exact:
// these strings are written by Aveloxis's own parsers, in lower case.
func PurlTypeOfManager(ecosystem string) (string, bool) {
	typ, ok := managerPurlTypes[ecosystem]
	return typ, ok
}

// PurlTypeOfDistribution is the purl type of one of the distribution
// subsystem's ecosystem strings, and whether there is one. The lookup folds
// case: those strings come from outside.
func PurlTypeOfDistribution(ecosystem string) (string, bool) {
	typ, ok := distributionPurlTypes[strings.ToLower(ecosystem)]
	return typ, ok
}

// namespaceRequired lists the purl types whose specification makes the
// namespace mandatory: maven (the groupId), swift (the package's host and
// owner path) and composer (the vendor). OSV enforces it, and one purl
// without its namespace gets a repository's whole batch refused ("namespace
// is required", four repositories in the 2026-09-22 log).
var namespaceRequired = map[string]bool{
	"maven":    true,
	"swift":    true,
	"composer": true,
}

// NamespaceRequired reports whether a purl of this type must carry a
// namespace.
func NamespaceRequired(purlType string) bool {
	return namespaceRequired[purlType]
}

// NameHasNamespace reports whether a purl name (the part after the type and
// before any version) has a non-empty namespace segment ahead of a non-empty
// name segment.
func NameHasNamespace(name string) bool {
	ns, rest, ok := strings.Cut(name, "/")
	return ok && ns != "" && rest != ""
}

// PurlForPackage builds the purl of a package at a version, from a
// package-manager or lockfile-ecosystem string. namespace is the purl
// namespace a lockfile supplied (a SwiftPM pin's host and owner), "" where
// there is none; when present it goes ahead of the name. A Maven name arrives
// as "group:artifact" and becomes group/artifact.
//
// It returns "" (the package is not sent) for an ecosystem with no purl type,
// for an empty name or version, and for a type that requires a namespace
// when the name has none: a Package.resolved pin without a repository URL
// ("alamofire"), a composer platform package ("php"), a Gradle shorthand
// without its group. Leaving one out is honest; a malformed purl costs the
// repository its whole batch.
func PurlForPackage(ecosystem, namespace, name, version string) string {
	typ, ok := managerPurlTypes[ecosystem]
	if !ok || name == "" || version == "" {
		return ""
	}
	if typ == "maven" {
		name = strings.Replace(name, ":", "/", 1)
	}
	if namespace != "" {
		name = strings.Trim(namespace, "/") + "/" + name
	}
	if namespaceRequired[typ] && !NameHasNamespace(name) {
		return ""
	}
	return Purl(typ, name, version)
}

// SelfPurl builds the versionless purl of a package a repository publishes,
// from one of the distribution subsystem's ecosystem strings. A versionless
// purl asks OSV for every advisory of the package. It returns "" under the
// same rules as PurlForPackage.
func SelfPurl(ecosystem, name string) string {
	typ, ok := PurlTypeOfDistribution(ecosystem)
	if !ok || name == "" {
		return ""
	}
	if typ == "maven" {
		name = strings.Replace(name, ":", "/", 1)
	}
	if namespaceRequired[typ] && !NameHasNamespace(name) {
		return ""
	}
	return Purl(typ, name, "")
}

// FoldEcosystem collapses the ecosystem vocabularies onto one spelling for a
// key: "rubygems" and "gem" are one ecosystem, as are "packagist" and
// "composer", "swiftpm" and "swift", "elixir" and "hex", "dart" and "pub",
// "golang" and "go", "haskell" and "hackage". Anything else is lower-cased.
// The chain walk once had no fold while the SBOM side had a private one, and
// lost every rubygems, packagist and swiftpm chain.
func FoldEcosystem(eco string) string {
	switch strings.ToLower(eco) {
	case "rubygems":
		return "gem"
	case "packagist":
		return "composer"
	case "swiftpm":
		return "swift"
	case "elixir":
		return "hex"
	case "dart":
		return "pub"
	case "golang":
		return "go"
	case "haskell":
		return "hackage"
	}
	return strings.ToLower(eco)
}

// pep503 folds '_' and '.' to '-': PEP 503 treats flask_sqlalchemy,
// flask.sqlalchemy and flask-sqlalchemy as one name.
var pep503 = strings.NewReplacer("_", "-", ".", "-")

// Key is THE key of a package: the folded ecosystem, '|', and the name
// lower-cased and trimmed, with PyPI's '_' and '.' folded to '-'. Every
// producer and every consumer of a key (chain attribution, SBOM dependency
// graphs, direct-dependency sets, the storage filter) uses this one function;
// two folds on two sides drop valid edges silently.
func Key(eco, name string) string {
	e := FoldEcosystem(eco)
	return e + "|" + KeyName(e, name)
}

// KeyName is the name half of Key for an ecosystem that is ALREADY folded.
// It is separate so that a SQL prefilter can be built from the same fold
// without spelling it again.
func KeyName(foldedEco, name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	if foldedEco == "pypi" {
		n = pep503.Replace(n)
	}
	return n
}

// MatchKey is the key the lockfile scan uses to match a lockfile entry to a
// declared dependency of the SAME ecosystem string. It folds the name exactly
// as Key does and does not fold the ecosystem, so its strings differ from
// Key's ("swiftpm|x" against "swift|x") while the pairs it matches within one
// ecosystem string are the same. It is kept, not merged into Key, because its
// callers compare a manifest's package manager with a lockfile's ecosystem as
// written; making them one is a change in what matches, and would be its own
// step.
func MatchKey(ecosystem, name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	if ecosystem == "pypi" {
		n = pep503.Replace(n)
	}
	return ecosystem + "|" + n
}

// osvEcosystemsByName are the ecosystems whose packages OSV is asked about
// by name and ecosystem, because neither the purl specification nor OSV has
// a purl for them: the stored ecosystem, and OSV's name for it. (GitHub
// Actions are asked by name too; their identity carries a ref, not a
// version, and is built where a workflow is read.)
var osvEcosystemsByName = map[string]string{
	"bioconductor": "Bioconductor",
}

// OSVEcosystemByName is OSV's name for an ecosystem that is asked by name,
// and whether it is one.
func OSVEcosystemByName(ecosystem string) (string, bool) {
	osv, ok := osvEcosystemsByName[strings.ToLower(ecosystem)]
	return osv, ok
}

// FindingPurlByName is the identity of a finding for a package of an
// ecosystem that is asked by name: pkg:<ecosystem>/<name>[@<version>], the
// name as written. It is this program's own, as a GitHub Action's is, and
// is NOT a purl of the package: PurlForPackage and SelfPurl give none for
// such an ecosystem, so it is never sent to OSV and no SBOM component
// carries it (the SBOM's vulnerability entry points at the component's own
// reference instead: collector's cdxAffectsRef). It returns "" for any
// other ecosystem.
func FindingPurlByName(ecosystem, name, version string) string {
	ecosystem = strings.ToLower(ecosystem)
	if _, ok := osvEcosystemsByName[ecosystem]; !ok {
		return ""
	}
	return Purl(ecosystem, name, version)
}
