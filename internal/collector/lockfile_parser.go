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
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// LockfileEntry is one resolved package from a lockfile. Direct is
// meaningful only when the owning LockfileResult has DirectKnown=true
// (not every format distinguishes direct from transitive).
type LockfileEntry struct {
	Name    string
	Version string
	Direct  bool
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
}

// parsedLockfileData is a parser's raw output before roster metadata is
// attached (v0.27.133 — the signature carries Edges now).
type parsedLockfileData struct {
	Entries     []LockfileEntry
	Edges       []LockfileEdge
	DirectKnown bool
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
}

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
	return res, nil
}

// lockfileMatchKey normalizes a package name for matching lockfile
// entries against declared manifest dependencies. Lowercased for every
// ecosystem; PyPI additionally folds '_' and '.' to '-' (PEP 503:
// flask_sqlalchemy == flask.sqlalchemy == flask-sqlalchemy).
func lockfileMatchKey(ecosystem, name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	if ecosystem == "pypi" {
		n = strings.NewReplacer("_", "-", ".", "-").Replace(n)
	}
	return ecosystem + "|" + n
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
			if name == "" {
				continue
			}
			scope := ""
			if pkg.Dev {
				scope = "dev"
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
	seen := map[string]bool{}
	var out parsedLockfileData
	var walk func(parentName, parentVersion string, deps map[string]json.RawMessage)
	walk = func(parentName, parentVersion string, deps map[string]json.RawMessage) {
		for name, raw := range deps {
			var node struct {
				Version      string                     `json:"version"`
				Dependencies map[string]json.RawMessage `json:"dependencies"`
			}
			if err := json.Unmarshal(raw, &node); err != nil {
				continue
			}
			if node.Version != "" && !seen[name+"@"+node.Version] {
				seen[name+"@"+node.Version] = true
				out.Entries = append(out.Entries, LockfileEntry{Name: name, Version: node.Version})
			}
			if parentName != "" {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: parentName, ParentVersion: parentVersion, ChildName: name})
			}
			if len(node.Dependencies) > 0 {
				walk(name, node.Version, node.Dependencies)
			}
		}
	}
	walk("", "", doc.Dependencies)
	return out, nil
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
			// name@range — the name may itself start with @scope/.
			if at := strings.LastIndex(firstKey, "@"); at > 0 {
				currentName = firstKey[:at]
			} else {
				currentName = ""
			}
			currentVersion = ""
			inDeps = false
			continue
		}
		trimmed := strings.TrimSpace(line)
		switch {
		case currentName != "" && strings.HasPrefix(trimmed, "version "):
			currentVersion = strings.Trim(strings.TrimPrefix(trimmed, "version "), `" `)
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
			if at := strings.LastIndex(firstKey, "@"); at > 0 {
				currentName = firstKey[:at]
			}
			inDeps = false
			flushable = currentName != "" && !isWorkspace
			continue
		}
		switch {
		case strings.HasPrefix(trimmed, "version:"):
			currentVersion = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "version:")), `"`)
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
	type pnpmPkg struct {
		Dependencies         map[string]string `yaml:"dependencies"`
		OptionalDependencies map[string]string `yaml:"optionalDependencies"`
	}
	var doc struct {
		Packages  map[string]pnpmPkg `yaml:"packages"`
		Snapshots map[string]pnpmPkg `yaml:"snapshots"` // v9: edges moved here
		Importers map[string]struct {
			Dependencies    map[string]yaml.Node `yaml:"dependencies"`
			DevDependencies map[string]yaml.Node `yaml:"devDependencies"`
		} `yaml:"importers"`
		Dependencies    map[string]yaml.Node `yaml:"dependencies"`
		DevDependencies map[string]yaml.Node `yaml:"devDependencies"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}

	directNames := map[string]bool{}
	collectDirect := func(deps map[string]yaml.Node) {
		for name := range deps {
			directNames[name] = true
		}
	}
	for _, imp := range doc.Importers {
		collectDirect(imp.Dependencies)
		collectDirect(imp.DevDependencies)
	}
	collectDirect(doc.Dependencies)
	collectDirect(doc.DevDependencies)

	var out parsedLockfileData
	for key := range doc.Packages {
		name, version := splitPnpmPackageKey(key)
		if name == "" || version == "" {
			continue
		}
		out.Entries = append(out.Entries, LockfileEntry{Name: name, Version: version, Direct: directNames[name]})
	}
	// v0.27.133 C2: edges. v5/v6 carry per-package dependencies inside
	// `packages`; v9 moved them to the parallel `snapshots` section
	// (values there are RESOLVED versions).
	collectEdges := func(m map[string]pnpmPkg) {
		for key, pkg := range m {
			pName, pVersion := splitPnpmPackageKey(key)
			if pName == "" {
				continue
			}
			for child, constraint := range pkg.Dependencies {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: pName, ParentVersion: pVersion, ChildName: child, Constraint: constraint})
			}
			for child, constraint := range pkg.OptionalDependencies {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: pName, ParentVersion: pVersion, ChildName: child, Constraint: constraint})
			}
		}
	}
	collectEdges(doc.Packages)
	collectEdges(doc.Snapshots)
	out.DirectKnown = true
	return out, nil
}

// splitPnpmPackageKey handles the three historical pnpm key shapes:
// v9 "name@1.2.3", v6 "/name@1.2.3(peer)", v5 "/name/1.2.3". Peer
// suffixes in parentheses are stripped.
func splitPnpmPackageKey(key string) (name, version string) {
	if idx := strings.IndexByte(key, '('); idx >= 0 {
		key = key[:idx]
	}
	key = strings.TrimPrefix(key, "/")
	// "name@version" (the name may start with @scope/): split at the
	// LAST @ past position 0.
	if at := strings.LastIndex(key, "@"); at > 0 {
		return key[:at], key[at+1:]
	}
	// v5 "/name/1.2.3" (scoped: "/@scope/name/1.2.3") — version is the
	// last path segment.
	if slash := strings.LastIndex(key, "/"); slash > 0 {
		return key[:slash], key[slash+1:]
	}
	return "", ""
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
		at := strings.LastIndex(spec, "@")
		if at <= 0 {
			continue
		}
		name, version := spec[:at], spec[at+1:]
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
