// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — lockfile_parser.go: pure parsers for committed
// dependency lockfiles (v0.27.11). When a repo commits a lockfile, the
// resolved version of each direct dependency is KNOWN, so the
// vulnerability scan can use that version instead of the manifest
// range's floor.
//
// Every parser is best-effort: a malformed lockfile returns an error
// the analysis walk logs at WARN and skips — it never fails the
// analysis phase. Parsers are pure (bytes in, entries out) so each
// format is fixture-testable in isolation.
//
// Deliberately NOT in the roster:
//   - requirements.txt — NEVER a lockfile, even fully hash-pinned
//     (operator ruling 2026-07-15: "it usually includes all the same
//     ambiguities and is often their source"). Its == pins classify
//     'exact' per finding, but it never contributes lockfile
//     certainty. TestRequirementsTxtIsNeverALockfile pins this.
//   - go.sum / go.mod — Go needs no lockfile: go.mod versions are
//     exact under MVS, so Go deps classify 'locked' by construction.
package collector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"gopkg.in/yaml.v3"
)

// LockfileEntry is one resolved package from a lockfile. Direct is
// meaningful only when the owning LockfileResult has DirectKnown=true
// (not every format distinguishes direct from transitive).
type LockfileEntry struct {
	Name    string
	Version string
	Direct  bool
	// Namespace is the purl namespace the entry's ecosystem requires and
	// the lockfile can supply — for SwiftPM the package repository's host
	// and owner ("github.com/Alamofire"), read from the pin's location
	// (v0.29.59, worklist 46). Empty where the format has none; such an
	// entry of a namespace-required type never becomes a purl.
	Namespace string
	// Ecosystem (0.29.205) overrides the lockfile's ecosystem for this one
	// entry. Empty for every format but renv.lock, the one lockfile that
	// holds packages of two ecosystems: a Bioconductor package is recorded
	// as "bioconductor", never as the CRAN package of its name (SR-6).
	Ecosystem string
	// Scope (v0.27.21 C1) — 'dev' when the format flags the entry as
	// development-only (package-lock v2/3 `dev`, poetry `category`),
	// '' otherwise (unknown/runtime). Refined per-format extraction is
	// C2 scope (summary/13 R5).
	Scope string
}

// LockfileEdge is one parent→child dependency edge from a lockfile
// (v0.27.133 C2 — parent-chain attribution). ParentVersion may be ""
// for formats that key parents by name only; Constraint is the raw
// declared range (informational — child RESOLUTION is derived at read
// time by joining the package set, which keeps edge extraction
// format-uniform and honest).
type LockfileEdge struct {
	ParentName    string
	ParentVersion string
	ChildName     string
	Constraint    string
}

// LockfileResult is a parsed lockfile.
type LockfileResult struct {
	Kind        string // roster filename, e.g. "package-lock.json"
	Ecosystem   string // package-manager string matching repo_deps_libyear: npm/pypi/cargo/…
	Entries     []LockfileEntry
	Edges       []LockfileEdge // parent→child edges; empty for edge-less formats (documented per format)
	DirectKnown bool           // whether Direct flags are meaningful for this format
	// v0.29.175: entries whose "version" was not a version. Aliased were
	// recorded under their real package; NotRegistry (a git or URL source)
	// and NotAVersion (a requirement string) were left out.
	Aliased     int
	NotRegistry int
	NotAVersion int
}

// parsedLockfileData is a parser's raw output before roster metadata is
// attached (v0.27.133 — the signature carries Edges now).
type parsedLockfileData struct {
	Entries     []LockfileEntry
	Edges       []LockfileEdge
	DirectKnown bool
	// NotRegistry counts the entries a parser left out because the file
	// itself says they are not from a registry (renv.lock's Source).
	NotRegistry int
}

type lockfileSpec struct {
	ecosystem string
	// binaryOnly formats (bun.lockb) are DETECTED for the inventory
	// (kind marker, zero entries) but never parsed.
	binaryOnly bool
	parse      func(data []byte) (parsedLockfileData, error)
}

// lockfileKinds is the roster: base filename → parser. The analysis
// walk consults it to decide what counts as a lockfile.
var lockfileKinds = map[string]lockfileSpec{
	// JavaScript / npm-registry ecosystems.
	"package-lock.json": {ecosystem: "npm", parse: parsePackageLockJSON},
	"yarn.lock":         {ecosystem: "npm", parse: parseYarnLock},
	"pnpm-lock.yaml":    {ecosystem: "npm", parse: parsePnpmLock},
	"bun.lock":          {ecosystem: "npm", parse: parseBunLock},
	"bun.lockb":         {ecosystem: "npm", binaryOnly: true},
	// Python.
	"poetry.lock":  {ecosystem: "pypi", parse: parsePoetryStyleTOML},
	"Pipfile.lock": {ecosystem: "pypi", parse: parsePipfileLock},
	"uv.lock":      {ecosystem: "pypi", parse: parsePoetryStyleTOML},
	"pdm.lock":     {ecosystem: "pypi", parse: parsePoetryStyleTOML},
	// Others.
	"Cargo.lock":         {ecosystem: "cargo", parse: parsePoetryStyleTOML},
	"Gemfile.lock":       {ecosystem: "rubygems", parse: parseGemfileLock},
	"composer.lock":      {ecosystem: "packagist", parse: parseComposerLock},
	"mix.lock":           {ecosystem: "hex", parse: parseMixLock},
	"pubspec.lock":       {ecosystem: "pub", parse: parsePubspecLock},
	"Package.resolved":   {ecosystem: "swiftpm", parse: parsePackageResolved},
	"packages.lock.json": {ecosystem: "nuget", parse: parseDotnetPackagesLock},
	"gradle.lockfile":    {ecosystem: "maven", parse: parseGradleLockfile},
	"stack.yaml.lock":    {ecosystem: "hackage", parse: parseStackYamlLock},
	renvLockKind:         {ecosystem: "cran", parse: parseRenvLock},
}

// renvLockKind is the one lockfile whose entries may be of another ecosystem
// than the lockfile's (LockfileEntry.Ecosystem). lockfileScopeDev is the one
// scope a parser gives an entry.
const (
	renvLockKind     = "renv.lock"
	lockfileScopeDev = "dev"
)

// ParseLockfile dispatches on the roster filename. Unknown kinds are
// an error (the walk should never call it with one — this is the
// backstop). Binary-only kinds return an inventory-marker result with
// zero entries.
func ParseLockfile(kind string, data []byte) (*LockfileResult, error) {
	spec, ok := lockfileKinds[kind]
	if !ok {
		return nil, fmt.Errorf("%q is not a recognized lockfile kind", kind)
	}
	res := &LockfileResult{Kind: kind, Ecosystem: spec.ecosystem}
	if spec.binaryOnly {
		return res, nil
	}
	parsed, err := spec.parse(data)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", kind, err)
	}
	res.Entries = parsed.Entries
	res.Edges = parsed.Edges
	res.DirectKnown = parsed.DirectKnown
	res.NotRegistry = parsed.NotRegistry
	settleLockedVersions(res)
	return res, nil
}

// settleLockedVersions is the one pass, after every parser, over what a
// lockfile wrote in its version field when that is not a version (v0.29.175;
// summary/70, 4.3). The parsers store the field as written.
//
//   - An npm alias ("npm:strip-ansi@6.0.1" under the name "strip-ansi-cjs")
//     is recorded under the real name and version. The alias is a name the
//     lockfile invented; scanned under it, the real package was never
//     checked. Edges that name the alias as parent follow it.
//   - A git or URL source ("github:owner/repo#commit", "git+https://...",
//     "file:", "link:", "workspace:") is not the registry package of that
//     name (SR-6): no entry, counted.
//   - A requirement string (">= 2.3.0") is not a locked version: no entry,
//     counted.
func settleLockedVersions(res *LockfileResult) {
	type renamed struct{ name, version string }
	aliases := map[string]renamed{} // "alias\x00version as written" -> the real package
	kept := res.Entries[:0:0]
	for _, e := range res.Entries {
		if real, version, ok := npmAliasTarget(res.Ecosystem, e.Version); ok {
			aliases[e.Name+"\x00"+e.Version] = renamed{real, version}
			e.Name, e.Version = real, version
			res.Aliased++
		}
		switch {
		case isNonRegistrySource(e.Version):
			res.NotRegistry++
			continue
		case !isLockedVersion(e.Version):
			res.NotAVersion++
			continue
		}
		kept = append(kept, e)
	}
	res.Entries = kept
	for i, ed := range res.Edges {
		if r, ok := aliases[ed.ParentName+"\x00"+ed.ParentVersion]; ok {
			res.Edges[i].ParentName, res.Edges[i].ParentVersion = r.name, r.version
		}
	}
}

// npmAliasTarget reads "npm:<name>@<version>": the real package an npm alias
// stands for. The name may be scoped ("npm:@scope/name@1.0.0"), so the
// version starts at the LAST '@'.
func npmAliasTarget(ecosystem, v string) (name, version string, ok bool) {
	rest, isAlias := strings.CutPrefix(v, "npm:")
	if ecosystem != "npm" || !isAlias {
		return "", "", false
	}
	at := strings.LastIndexByte(rest, '@')
	if at <= 0 || at == len(rest)-1 {
		return "", "", false
	}
	return rest[:at], rest[at+1:], true
}

// nonRegistrySourcePrefixes are how a lockfile writes a dependency that does
// not come from its registry.
var nonRegistrySourcePrefixes = []string{
	"github:", "gitlab:", "bitbucket:", "gist:", "git+", "git:", "git@",
	"file:", "link:", "workspace:", "portal:", "patch:", "npm:",
}

// isNonRegistrySource reports whether a version field names a source, not a
// version: one of the prefixes above, or any URL. (A registry tarball URL of
// the package itself was already read as its version by npmLockVersion; a
// URL that reaches here is another package's tarball or another host's
// file. A leftover "npm:" is an alias that could not be read.)
func isNonRegistrySource(v string) bool {
	if strings.Contains(v, "://") {
		return true
	}
	for _, p := range nonRegistrySourcePrefixes {
		if strings.HasPrefix(v, p) {
			return true
		}
	}
	return false
}

// isLockedVersion reports whether a version field is one version, as a
// lockfile locks it, and not a requirement: it is not empty, does not start
// with a comparison or range operator or a bracket, and holds no space, no
// "||" and no wildcard on its own. It deliberately does not ask for digits:
// a Composer "dev-master" is what that lockfile locked.
func isLockedVersion(v string) bool {
	if v == "" || v == "*" || strings.ContainsAny(v, " \t") || strings.Contains(v, "||") {
		return false
	}
	switch v[0] {
	case '<', '>', '=', '~', '^', '[', '(', '*':
		return false
	}
	// "!=" is a requirement; "1!2.0" (a Python epoch) is a version.
	return !strings.HasPrefix(v, "!")
}

// ============================================================
// JavaScript
// ============================================================

// parsePackageLockJSON handles npm's package-lock.json: v1 (nested
// "dependencies" objects) and v2/v3 (flat "packages" map keyed by
// node_modules path). v2/v3 distinguish direct deps via the root ""
// entry's dependencies/devDependencies; v1 does not.
func parsePackageLockJSON(data []byte) (parsedLockfileData, error) {
	var doc struct {
		LockfileVersion int `json:"lockfileVersion"`
		Packages        map[string]struct {
			Version         string            `json:"version"`
			Link            bool              `json:"link"`
			Dev             bool              `json:"dev"`
			Dependencies    map[string]string `json:"dependencies"`
			DevDependencies map[string]string `json:"devDependencies"`
		} `json:"packages"`
		Dependencies map[string]json.RawMessage `json:"dependencies"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}

	if len(doc.Packages) > 0 {
		// v2/v3: root "" entry declares the direct deps.
		direct := map[string]bool{}
		if root, ok := doc.Packages[""]; ok {
			for name := range root.Dependencies {
				direct[name] = true
			}
			for name := range root.DevDependencies {
				direct[name] = true
			}
		}
		var out parsedLockfileData
		for path, pkg := range doc.Packages {
			if path == "" || pkg.Link || pkg.Version == "" {
				continue
			}
			// The package name is everything after the LAST
			// "node_modules/" (nested trees + scoped names both work).
			name := path
			if idx := strings.LastIndex(path, "node_modules/"); idx >= 0 {
				name = path[idx+len("node_modules/"):]
			}
			pkg.Version = npmLockVersion(name, pkg.Version)
			if name == "" {
				continue
			}
			scope := ""
			if pkg.Dev {
				scope = lockfileScopeDev
			}
			out.Entries = append(out.Entries, LockfileEntry{Name: name, Version: pkg.Version, Direct: direct[name], Scope: scope})
			// v0.27.133 C2: the per-entry dependency maps were decoded
			// and DISCARDED since v0.27.21 — keep them as edges.
			for child, constraint := range pkg.Dependencies {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: name, ParentVersion: pkg.Version, ChildName: child, Constraint: constraint})
			}
			for child, constraint := range pkg.DevDependencies {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: name, ParentVersion: pkg.Version, ChildName: child, Constraint: constraint})
			}
		}
		out.DirectKnown = true
		return out, nil
	}

	// v1: walk the nested dependencies tree; direct not distinguished
	// (top-level entries are the HOISTED set, not the declared set).
	// C2: the parent is in scope at each recursion level — record the
	// edge (constraint unknown in the v1 nested shape; the child value
	// IS the resolution).
	//
	// 0.29.213: the tree is read once (npmV1Tree). It was decoded level by
	// level, each node's text again from the level above, which costs the
	// depth times the size: 190 KB of one nested chain took 1.5 seconds.
	seen := map[string]bool{}
	var out parsedLockfileData
	var walk func(parentName, parentVersion string, deps []npmV1Dep)
	walk = func(parentName, parentVersion string, deps []npmV1Dep) {
		for _, dep := range deps {
			name, node := dep.name, dep.node
			if node == nil {
				continue // not a dependency's object: left out, as it was
			}
			node.version = npmLockVersion(name, node.version)
			if node.version != "" && !seen[name+"@"+node.version] {
				seen[name+"@"+node.version] = true
				out.Entries = append(out.Entries, LockfileEntry{Name: name, Version: node.version})
			}
			if parentName != "" {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: parentName, ParentVersion: parentVersion, ChildName: name})
			}
			if len(node.dependencies) > 0 {
				walk(name, node.version, node.dependencies)
			}
		}
	}
	walk("", "", npmV1Tree(data))
	return out, nil
}

// npmV1Node is a dependency's object in a package-lock.json v1: its version
// and the dependencies nested in it.
type npmV1Node struct {
	version      string
	dependencies []npmV1Dep
}

// npmV1Dep is one name of a "dependencies" object. node is nil when its
// value is not a dependency's object (a string, a number, an object whose
// version is not a string or whose dependencies are not an object): such a
// name was always left out, with everything under it.
type npmV1Dep struct {
	name string
	node *npmV1Node
}

// npmV1Tree reads the top-level "dependencies" of a document that
// encoding/json has already accepted, in one pass over its tokens. It reads
// as encoding/json read the same text into structs: a field's name matches
// without regard to case, a null leaves a field as it was, a name given
// twice in one object keeps its last value, and a "dependencies" given twice
// adds to the first.
func npmV1Tree(data []byte) []npmV1Dep {
	dec := json.NewDecoder(bytes.NewReader(data))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil
	}
	var deps []npmV1Dep
	index := map[string]int{}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return deps
		}
		if name, _ := key.(string); strings.EqualFold(name, "dependencies") {
			var ok bool
			if deps, ok = npmV1Deps(dec, deps, index); !ok {
				return deps
			}
			continue
		}
		if !jsonSkipValue(dec) {
			return deps
		}
	}
	return deps
}

// npmV1Deps reads a "dependencies" value into deps: an object's names, each
// with its node; a null adds nothing. wellFormed is false when the value is
// anything else (it has been read past all the same), and when the text
// ended.
func npmV1Deps(dec *json.Decoder, deps []npmV1Dep, index map[string]int) (_ []npmV1Dep, wellFormed bool) {
	tok, err := dec.Token()
	if err != nil {
		return deps, false
	}
	if tok == nil {
		return deps, true
	}
	if tok != json.Delim('{') {
		jsonSkipRest(dec, tok)
		return deps, false
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return deps, false
		}
		name, _ := key.(string)
		node, ok := npmV1NodeOf(dec)
		if !ok {
			return deps, false
		}
		if at, again := index[name]; again {
			deps[at].node = node
			continue
		}
		index[name] = len(deps)
		deps = append(deps, npmV1Dep{name, node})
	}
	_, err = dec.Token() // the closing brace
	return deps, err == nil
}

// npmV1NodeOf reads one dependency's value. The node is nil when the value
// is not a dependency's object; read is false only when the text ended.
func npmV1NodeOf(dec *json.Decoder) (node *npmV1Node, read bool) {
	tok, err := dec.Token()
	if err != nil {
		return nil, false
	}
	if tok == nil {
		return &npmV1Node{}, true // a null: an object with nothing in it
	}
	if tok != json.Delim('{') {
		return nil, jsonSkipRest(dec, tok)
	}
	node = &npmV1Node{}
	good := true
	index := map[string]int{}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, false
		}
		switch name, _ := key.(string); {
		case strings.EqualFold(name, "version"):
			tok, err := dec.Token()
			if err != nil {
				return nil, false
			}
			switch v := tok.(type) {
			case string:
				node.version = v
			case nil:
			default:
				good = false
				if !jsonSkipRest(dec, tok) {
					return nil, false
				}
			}
		case strings.EqualFold(name, "dependencies"):
			deps, wellFormed := npmV1Deps(dec, node.dependencies, index)
			node.dependencies = deps
			if !wellFormed {
				good = false
			}
		default:
			if !jsonSkipValue(dec) {
				return nil, false
			}
		}
	}
	if _, err := dec.Token(); err != nil {
		return nil, false
	}
	if !good {
		return nil, true
	}
	return node, true
}

// jsonSkipValue reads past the next value; jsonSkipRest, past the rest of a
// value whose first token is tok. False: the text ended.
func jsonSkipValue(dec *json.Decoder) bool {
	tok, err := dec.Token()
	return err == nil && jsonSkipRest(dec, tok)
}

func jsonSkipRest(dec *json.Decoder, tok json.Token) bool {
	if d, ok := tok.(json.Delim); !ok || d == '}' || d == ']' {
		return true
	}
	for depth := 1; depth > 0; {
		tok, err := dec.Token()
		if err != nil {
			return false
		}
		switch tok {
		case json.Delim('{'), json.Delim('['):
			depth++
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
	}
	return true
}

// parseYarnLock dispatches between yarn's classic v1 text format and
// the berry (v2+) YAML flavor — same filename, sniffed on the
// `__metadata:` header berry always writes. Pre-v0.27.133 the v1
// scanner silently yielded ZERO entries on berry files (berry writes
// `version: X`, the v1 scanner matched only `version "X"`), so berry
// repos got an inventory row with entry_count=0 and no packages.
func parseYarnLock(data []byte) (parsedLockfileData, error) {
	if strings.Contains(string(data), "__metadata:") {
		return parseYarnBerry(data)
	}
	return parseYarnLockV1(data)
}

// parseYarnLockV1 handles yarn's classic (v1) text format: blocks
// keyed by one or more "name@range" selectors, each with an indented
// `version "X"` line and an optional indented `dependencies:` block
// (v0.27.133 C2: those blocks — previously unreachable dead lines to
// the scanner — become edges). Direct deps are not distinguished.
func parseYarnLockV1(data []byte) (parsedLockfileData, error) {
	var out parsedLockfileData
	currentName, currentVersion := "", ""
	inDeps := false
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") || strings.TrimSpace(line) == "" {
			continue
		}
		// Block header: non-indented, ends with ":".
		if !strings.HasPrefix(line, " ") && strings.HasSuffix(strings.TrimSpace(line), ":") {
			header := strings.TrimSuffix(strings.TrimSpace(line), ":")
			firstKey := strings.TrimSpace(strings.Split(header, ",")[0])
			firstKey = strings.Trim(firstKey, `"`)
			// name@range — the name may itself start with @scope/, and the
			// range may be a URL or file: reference (manifest.SplitNPMSpec).
			currentName, _, _ = manifest.SplitNPMSpec(firstKey)
			currentVersion = ""
			inDeps = false
			continue
		}
		trimmed := strings.TrimSpace(line)
		switch {
		case currentName != "" && strings.HasPrefix(trimmed, "version "):
			currentVersion = npmLockVersion(currentName, strings.Trim(strings.TrimPrefix(trimmed, "version "), `" `))
			if currentVersion != "" {
				out.Entries = append(out.Entries, LockfileEntry{Name: currentName, Version: currentVersion})
			}
			inDeps = false
		case currentName != "" && (trimmed == "dependencies:" || trimmed == "optionalDependencies:"):
			inDeps = true
		case inDeps && strings.HasPrefix(line, "    "):
			// 4-space child line: `name "range"` (name may be quoted).
			if child, constraint, ok := yarnDepLine(trimmed, " "); ok {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: currentName, ParentVersion: currentVersion, ChildName: child, Constraint: constraint})
			}
		default:
			inDeps = false
		}
	}
	return out, nil
}

// parseYarnBerry handles yarn v2+ ("berry") lockfiles: YAML documents
// whose entries are keyed `"name@npm:range"[, ...]:` with `version:`
// and `dependencies:` fields. The workspace entry
// (`"proj@workspace:."`) carries the DIRECT dependency set and is not
// itself a package. v0.27.133 — the 36-file fleet cohort that
// previously parsed to zero entries.
func parseYarnBerry(data []byte) (parsedLockfileData, error) {
	var out parsedLockfileData
	direct := map[string]bool{}
	currentName, currentVersion := "", ""
	isWorkspace := false
	inDeps := false
	flushable := false
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || trimmed == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") && strings.HasSuffix(trimmed, ":") {
			header := strings.TrimSuffix(trimmed, ":")
			firstKey := strings.Trim(strings.TrimSpace(strings.Split(header, ",")[0]), `"`)
			isWorkspace = strings.Contains(firstKey, "@workspace:")
			currentName, currentVersion = "", ""
			if firstKey == "__metadata" {
				inDeps, flushable = false, false
				continue
			}
			currentName, _, _ = manifest.SplitNPMSpec(firstKey)
			inDeps = false
			flushable = currentName != "" && !isWorkspace
			continue
		}
		switch {
		case strings.HasPrefix(trimmed, "version:"):
			currentVersion = npmLockVersion(currentName, strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "version:")), `"`))
			if flushable && currentVersion != "" {
				out.Entries = append(out.Entries, LockfileEntry{Name: currentName, Version: currentVersion})
			}
		case trimmed == "dependencies:" || trimmed == "optionalDependencies:":
			inDeps = true
			continue
		case inDeps && strings.HasPrefix(line, "    "):
			if child, constraint, ok := yarnDepLine(trimmed, ":"); ok {
				if isWorkspace {
					direct[child] = true
				} else if currentName != "" {
					out.Edges = append(out.Edges, LockfileEdge{ParentName: currentName, ParentVersion: currentVersion, ChildName: child, Constraint: constraint})
				}
			}
			continue
		default:
			if !strings.HasPrefix(line, "    ") {
				inDeps = false
			}
		}
	}
	for i := range out.Entries {
		out.Entries[i].Direct = direct[out.Entries[i].Name]
	}
	out.DirectKnown = len(direct) > 0
	return out, nil
}

// yarnDepLine splits one dependency child line. v1 uses `name "range"`
// (sep " "); berry uses `name: range` / `"@scope/name": range`
// (sep ":").
func yarnDepLine(trimmed, sep string) (name, constraint string, ok bool) {
	if trimmed == "" {
		return "", "", false
	}
	if strings.HasPrefix(trimmed, `"`) {
		// Quoted name: find the closing quote.
		if end := strings.Index(trimmed[1:], `"`); end >= 0 {
			name = trimmed[1 : 1+end]
			constraint = strings.TrimPrefix(strings.TrimSpace(trimmed[2+end:]), sep)
		}
	} else {
		i := strings.Index(trimmed, sep)
		if i <= 0 {
			return "", "", false
		}
		name = trimmed[:i]
		constraint = trimmed[i+len(sep):]
	}
	name = strings.TrimSuffix(strings.TrimSpace(name), ":")
	constraint = strings.Trim(strings.TrimSpace(constraint), `"`)
	if name == "" {
		return "", "", false
	}
	return name, constraint, true
}

// parsePnpmLock handles pnpm-lock.yaml (v5 through v9). Resolved
// packages come from the top-level "packages" map keys; direct deps
// from the "importers" section (or the legacy top-level
// dependencies/devDependencies on very old lockfiles).
func parsePnpmLock(data []byte) (parsedLockfileData, error) {
	// Read through the node walk of lockfile_yaml.go, not yaml.Unmarshal
	// into maps, which is quadratic in a mapping's keys.
	root, err := yamlLockRoot(data)
	if err != nil {
		return parsedLockfileData{}, err
	}
	directNames := map[string]bool{}
	// The names of a dependencies mapping; its values (a version, or a
	// specifier and a version) are not read.
	collectDirect := func(n *yaml.Node) error {
		deps, err := yamlPairs(n)
		for _, d := range deps {
			directNames[d.key] = true
		}
		return err
	}
	importers, err := yamlPairs(yamlField(root, "importers"))
	if err != nil {
		return parsedLockfileData{}, err
	}
	for _, imp := range importers {
		fields, err := yamlPairs(imp.val)
		if err != nil {
			return parsedLockfileData{}, err
		}
		if err := errors.Join(collectDirect(yamlField(fields, "dependencies")), collectDirect(yamlField(fields, "devDependencies"))); err != nil {
			return parsedLockfileData{}, err
		}
	}
	if err := errors.Join(collectDirect(yamlField(root, "dependencies")), collectDirect(yamlField(root, "devDependencies"))); err != nil {
		return parsedLockfileData{}, err
	}

	packages, err := yamlPairs(yamlField(root, "packages"))
	if err != nil {
		return parsedLockfileData{}, err
	}
	var out parsedLockfileData
	for _, pkg := range packages {
		name, version := splitPnpmPackageKey(pkg.key)
		if name == "" || version == "" {
			continue
		}
		out.Entries = append(out.Entries, LockfileEntry{Name: name, Version: version, Direct: directNames[name]})
	}
	// v0.27.133 C2: edges. v5/v6 carry per-package dependencies inside
	// `packages`; v9 moved them to the parallel `snapshots` section
	// (values there are RESOLVED versions).
	collectEdges := func(m []yamlPair) error {
		for _, pkg := range m {
			fields, err := yamlPairs(pkg.val)
			if err != nil {
				return err
			}
			pName, pVersion := splitPnpmPackageKey(pkg.key)
			for _, section := range []string{"dependencies", "optionalDependencies"} {
				children, constraints, err := yamlStringMap(yamlField(fields, section))
				if err != nil {
					return err
				}
				if pName == "" {
					continue // read, as it always was, and not a package's edges
				}
				for i, child := range children {
					out.Edges = append(out.Edges, LockfileEdge{ParentName: pName, ParentVersion: pVersion, ChildName: child.key, Constraint: constraints[i]})
				}
			}
		}
		return nil
	}
	snapshots, err := yamlPairs(yamlField(root, "snapshots"))
	if err != nil {
		return parsedLockfileData{}, err
	}
	if err := errors.Join(collectEdges(packages), collectEdges(snapshots)); err != nil {
		return parsedLockfileData{}, err
	}
	out.DirectKnown = true
	return out, nil
}

// splitPnpmPackageKey handles the historical pnpm key shapes: v9
// "name@1.2.3", v6 "/name@1.2.3(peer)", v5 "/name/1.2.3" with an optional
// "_peer@x" suffix, and v5 keys prefixed with the registry host
// ("registry.npmjs.org/@jest/transform/26.0.1"). A key whose version is
// not a registry version (git commit, tarball file, file:/link: path)
// returns ("", "") and is skipped.
//
// v0.29.56: the name was cut at the LAST '@', so a registry-host key
// became name "registry.npmjs.org/" with version "jest/transform/26.0.1"
// (361 production rows) and a v5 peer suffix left "react-dom/18.2.0_react"
// as the name. Such a row's purl has no name, and one of them fails the
// repository's whole OSV batch.
func splitPnpmPackageKey(key string) (name, version string) {
	if idx := strings.IndexByte(key, '('); idx >= 0 {
		key = key[:idx]
	}
	segs := strings.Split(strings.TrimPrefix(key, "/"), "/")
	last := segs[len(segs)-1]
	var nameSegs []string
	if at := strings.IndexByte(last, '@'); at > 0 && !pnpmV5PeerSuffixed(last) {
		// v6/v9: the last segment is "name@version".
		nameSegs = append(segs[:len(segs)-1:len(segs)-1], last[:at])
		version = last[at+1:]
	} else if len(segs) >= 2 {
		// v5: the last segment is the version, maybe "_peer" suffixed.
		nameSegs = segs[:len(segs)-1]
		version = last
		if u := strings.IndexByte(version, '_'); u > 0 {
			version = version[:u]
		}
	}
	name = npmNameFromPathSegments(nameSegs)
	if name == "" || !npmRegistryVersionRe.MatchString(version) {
		return "", ""
	}
	return name, version
}

// pnpmV5PeerSuffixed reports a v5 version segment with a peer suffix
// ("18.2.0_react@18.2.0"): a version, then '_' before any '@'.
func pnpmV5PeerSuffixed(seg string) bool {
	u := strings.IndexByte(seg, '_')
	at := strings.IndexByte(seg, '@')
	return u > 0 && at > u && npmRegistryVersionRe.MatchString(seg[:u])
}

// npmNameFromPathSegments takes the path segments that precede a version
// and returns the package name: "@scope/name" when the last two segments
// are a scope and a name, otherwise the last segment (an unscoped name
// never contains '/', so anything before it is a registry host). A
// segment with ':' is a reference (file:, link:), not a name.
func npmNameFromPathSegments(segs []string) string {
	n := len(segs)
	if n == 0 || segs[n-1] == "" || strings.HasPrefix(segs[n-1], "@") || strings.Contains(segs[n-1], ":") {
		return ""
	}
	if n >= 2 && len(segs[n-2]) > 1 && strings.HasPrefix(segs[n-2], "@") {
		return segs[n-2] + "/" + segs[n-1]
	}
	return segs[n-1]
}

// npmRegistryVersionRe matches the start of a registry version
// (major.minor…). Git commits, tarball file names and path references do
// not match.
var npmRegistryVersionRe = regexp.MustCompile(`^\d+\.\d+`)

// npmTarballRe reads a registry tarball URL's package path (the segments
// before "/-/", a scope's "/" possibly encoded as %2f) and its file-name
// stem (".../@s/utils/-/utils-2.4.2.tgz" → "@s/utils", "utils-2.4.2").
var npmTarballRe = regexp.MustCompile(`/((?:@[^/]+(?:/|%2[fF]))?[^/@]+)/-/([^/]+)\.tgz$`)

// npmSemverRe is a whole semver version (prerelease and build metadata
// allowed): what remains of a tarball stem after "<name>-".
var npmSemverRe = regexp.MustCompile(`^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$`)

// npmLockVersion is the ONE reading (SR-17) of an npm lockfile's version
// field: a dependency installed from a registry tarball URL is locked with
// the URL as its version (package-lock.json v1 and v3, yarn.lock v1 and
// berry, bun.lock), and its version is the tarball's file name
// (v0.29.71: the URL became the purl's version — the 2026-09-28 log's
// malformed npm purls). The tarball must be the package's own (whole-branch
// review C3, fix review V1): the package path in the URL equals name, and
// the file name is its unscoped name, "-", then a whole version — a package
// installed from another package's tarball (another scope's, or a name that
// merely shares the stem) is not that version of itself (SR-6). Any other
// value is returned unchanged, and the wire gate drops it.
func npmLockVersion(name, v string) string {
	m := npmTarballRe.FindStringSubmatch(v)
	if m == nil || name == "" {
		return v
	}
	pkg := strings.NewReplacer("%2f", "/", "%2F", "/").Replace(m[1])
	if pkg != name {
		return v
	}
	unscoped := name
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		unscoped = name[i+1:]
	}
	if rest, ok := strings.CutPrefix(m[2], unscoped+"-"); ok && npmSemverRe.MatchString(rest) {
		return rest
	}
	return v
}

// parseBunLock handles Bun ≥1.2's TEXT lockfile (bun.lock) — JSONC:
// JSON with comments and trailing commas. The binary bun.lockb is
// detect-only (see the roster). Direct deps come from the root
// workspace's dependencies/devDependencies.
func parseBunLock(data []byte) (parsedLockfileData, error) {
	cleaned := stripJSONC(data)
	var doc struct {
		Workspaces map[string]struct {
			Dependencies    map[string]string `json:"dependencies"`
			DevDependencies map[string]string `json:"devDependencies"`
		} `json:"workspaces"`
		Packages map[string]json.RawMessage `json:"packages"`
	}
	if err := json.Unmarshal(cleaned, &doc); err != nil {
		return parsedLockfileData{}, err
	}

	directNames := map[string]bool{}
	for _, ws := range doc.Workspaces {
		for name := range ws.Dependencies {
			directNames[name] = true
		}
		for name := range ws.DevDependencies {
			directNames[name] = true
		}
	}

	var out parsedLockfileData
	for installName, raw := range doc.Packages {
		// Each value is a tuple array whose first element is
		// "name@version"; tuple[2] (when present) carries the entry's own
		// dependencies map (v0.27.133 C2 — previously decoded as
		// RawMessage and discarded).
		var tuple []json.RawMessage
		if err := json.Unmarshal(raw, &tuple); err != nil || len(tuple) == 0 {
			continue
		}
		var spec string
		if err := json.Unmarshal(tuple[0], &spec); err != nil {
			continue
		}
		name, version, ok := manifest.SplitNPMSpec(spec)
		if !ok {
			continue
		}
		// A package fetched by tarball URL carries its version in the file
		// name; any other non-registry reference (file:, link:, git) has no
		// registry version and is skipped.
		version = npmLockVersion(name, version)
		if !npmRegistryVersionRe.MatchString(version) {
			continue
		}
		out.Entries = append(out.Entries, LockfileEntry{
			Name:    name,
			Version: version,
			Direct:  directNames[installName],
		})
		if len(tuple) > 2 {
			var meta struct {
				Dependencies         map[string]string `json:"dependencies"`
				OptionalDependencies map[string]string `json:"optionalDependencies"`
			}
			if err := json.Unmarshal(tuple[2], &meta); err == nil {
				for child, constraint := range meta.Dependencies {
					out.Edges = append(out.Edges, LockfileEdge{ParentName: name, ParentVersion: version, ChildName: child, Constraint: constraint})
				}
				for child, constraint := range meta.OptionalDependencies {
					out.Edges = append(out.Edges, LockfileEdge{ParentName: name, ParentVersion: version, ChildName: child, Constraint: constraint})
				}
			}
		}
	}
	out.DirectKnown = true
	return out, nil
}

// stripJSONC removes // and /* */ comments plus trailing commas so
// bun.lock parses with encoding/json. String-aware: comment markers
// inside quoted strings are preserved.
func stripJSONC(data []byte) []byte {
	var out []byte
	inString := false
	escaped := false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if inString {
			out = append(out, c)
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		switch {
		case c == '"':
			inString = true
			out = append(out, c)
		case c == '/' && i+1 < len(data) && data[i+1] == '/':
			for i < len(data) && data[i] != '\n' {
				i++
			}
			if i < len(data) {
				out = append(out, '\n')
			}
		case c == '/' && i+1 < len(data) && data[i+1] == '*':
			i += 2
			for i+1 < len(data) && !(data[i] == '*' && data[i+1] == '/') {
				i++
			}
			i++ // skip the trailing '/'
		default:
			out = append(out, c)
		}
	}
	// Trailing commas: ",  }" / ",\n]" → "}" / "]".
	trailing := regexp.MustCompile(`,\s*([}\]])`)
	return trailing.ReplaceAll(out, []byte("$1"))
}
