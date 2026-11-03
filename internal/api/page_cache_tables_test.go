// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// cacheStateTables are tables a cached handler reads that are not its
// answer's content and not the admission's (authorizeRepo's, derived below),
// each with its reason — the request's own bookkeeping, say — so their
// writers need no stamp. Empty today: repos, the one state table every
// answer names, is read by the admission too.
var cacheStateTables = map[string]string{}

// pageCacheTableList reads internal/db's list of the tables the cached
// answers read (testdata/page_cache_tables.txt: one per line, "#" comments).
func pageCacheTableList(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, line := range strings.Split(srctest.Read(t, "internal/db/testdata/page_cache_tables.txt"), "\n") {
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		if line = strings.TrimSpace(line); line != "" {
			out[line] = true
		}
	}
	srctest.MinCount(t, "page-cache tables listed", len(out), 1)
	return out
}

// pkgSource is a package's functions and methods by name (comment
// stripped, several of one name joined), which of those names are methods,
// and its package-level values.
type pkgSource struct {
	funcs    map[string]string
	isMethod map[string]bool
	values   map[string]string
}

func readPkgSource(t *testing.T, dir string, minFiles int) pkgSource {
	t.Helper()
	p := pkgSource{funcs: map[string]string{}, isMethod: map[string]bool{}, values: map[string]string{}}
	for _, src := range srctest.PackageFiles(t, dir, minFiles) {
		src = srctest.StripGoComments(src)
		for fn, part := range srctest.FuncParts(t, src) {
			if fn == srctest.PackageLevel {
				continue
			}
			p.funcs[fn] += part
		}
		for fn := range srctest.MethodNames(t, src) {
			p.isMethod[fn] = true
		}
		for n, v := range srctest.PackageValues(t, src) {
			p.values[n] = v
		}
	}
	return p
}

var (
	selectorCallRE = regexp.MustCompile(`\.([A-Za-z_]\w*)\s*\(`)
	plainCallRE    = regexp.MustCompile(`(?:^|[^.\w])([A-Za-z_]\w*)\s*\(`)
	funcValueRE    = regexp.MustCompile(`[(,]\s*(s\.)?([A-Za-z_]\w*)\s*`) // followed by , or ) — checked, not consumed, so neighbours both match
	identRE        = regexp.MustCompile(`\b[A-Za-z_]\w*\b`)
)

// next is what text can run or read in p: a method called through a
// selector (.name(), a function called plainly (name(), a function or
// method passed as a value (, name) / , s.name)), and a package-level value
// named. A selector call of a name that is a function here is another
// package's (errors.New is not this package's New).
func (p pkgSource) next(text string) []string {
	var out []string
	for _, m := range selectorCallRE.FindAllStringSubmatch(text, -1) {
		if p.isMethod[m[1]] {
			out = append(out, m[1])
		}
	}
	for _, m := range plainCallRE.FindAllStringSubmatch(text, -1) {
		if _, ok := p.funcs[m[1]]; ok && !p.isMethod[m[1]] {
			out = append(out, m[1])
		}
	}
	for _, loc := range funcValueRE.FindAllStringSubmatchIndex(text, -1) {
		if loc[1] >= len(text) || (text[loc[1]] != ',' && text[loc[1]] != ')') {
			continue
		}
		name, selector := text[loc[4]:loc[5]], loc[2] >= 0
		if _, ok := p.funcs[name]; ok && selector == p.isMethod[name] {
			out = append(out, name)
		}
	}
	for _, id := range identRE.FindAllString(text, -1) {
		if _, ok := p.values[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// internalImports maps each import name the package at dir uses for one of
// this module's packages to that package's directory.
func internalImports(t *testing.T, dir string) map[string]string {
	t.Helper()
	mod := srctest.ModulePath(t)
	re := regexp.MustCompile(`(?m)^\s*(?:import\s+)?(\w+\s+)?"` + regexp.QuoteMeta(mod) + `/([\w/]+)"`)
	out := map[string]string{}
	for _, src := range srctest.PackageFiles(t, dir, 20) {
		for _, m := range re.FindAllStringSubmatch(src, -1) {
			name := strings.TrimSpace(m[1])
			if name == "" {
				name = m[2][strings.LastIndex(m[2], "/")+1:]
			}
			out[name] = m[2]
		}
	}
	if out["db"] != "internal/db" {
		t.Fatalf("internalImports(%s) did not find internal/db: %v", dir, out)
	}
	return out
}

// closure is every function body and value p reaches from roots, joined.
func (p pkgSource) closure(roots []string) string {
	var b strings.Builder
	seen := map[string]bool{}
	queue := append([]string(nil), roots...)
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if seen[name] {
			continue
		}
		seen[name] = true
		text, ok := p.funcs[name]
		if !ok {
			text, ok = p.values[name]
		}
		if !ok {
			continue
		}
		b.WriteString(text)
		b.WriteString("\n")
		queue = append(queue, p.next(text)...)
	}
	return b.String()
}

// tablesReachedFrom is every table the statements read that the API
// functions roots reach: through this package's functions, one package deep
// into this module's other packages (collector.GenerateSBOM… builds the SBOM
// from the store; db.Func(…) is internal/db's own), to the store methods
// called, through internal/db's functions and named SQL.
func tablesReachedFrom(t *testing.T, roots []string, apiPkg, dbPkg pkgSource, otherPkgs map[string]pkgSource, imported map[string]string) map[string]bool {
	t.Helper()
	apiText := apiPkg.closure(roots)
	texts := []string{apiText}
	var dbRoots []string
	for _, m := range regexp.MustCompile(`\b([a-z]\w*)\.([A-Z]\w*)\s*\(`).FindAllStringSubmatch(apiText, -1) {
		dir, ok := imported[m[1]]
		if !ok {
			continue
		}
		if dir == "internal/db" {
			if _, isFunc := dbPkg.funcs[m[2]]; isFunc && !dbPkg.isMethod[m[2]] {
				dbRoots = append(dbRoots, m[2])
			}
			continue
		}
		other, read := otherPkgs[dir]
		if !read {
			other = readPkgSource(t, dir, 1)
			otherPkgs[dir] = other
		}
		if _, isFunc := other.funcs[m[2]]; isFunc && !other.isMethod[m[2]] {
			texts = append(texts, other.closure([]string{m[2]}))
		}
	}
	for _, text := range texts {
		for _, m := range selectorCallRE.FindAllStringSubmatch(text, -1) {
			if dbPkg.isMethod[m[1]] {
				dbRoots = append(dbRoots, m[1])
			}
		}
	}
	srctest.MinCount(t, "store functions reached", len(dbRoots), 1)
	// Every schema-qualified name the statements use (quoted or not), not
	// only the one after FROM or JOIN: a comma join, FROM ONLY and LATERAL
	// name tables too (L10 round: those reads were invisible). A name called
	// as a function (aveloxis_data.is_automation_email(...)) is not a table.
	out := map[string]bool{}
	for _, m := range regexp.MustCompile(`"?aveloxis_\w+"?\."?(\w+)"?(\s*\()?`).FindAllStringSubmatch(dbPkg.closure(dbRoots), -1) {
		if m[2] == "" {
			out[m[1]] = true
		}
	}
	return out
}

// Whole-branch review of 0.29.119–0.29.123, A11: the list of tables whose
// writers must stamp was written by hand and had missed one
// (contributors_aliases, read by top contributors' daily commit arm). The
// set is derived here, from every cached handler to every table its
// statements read (tablesReachedFrom). The admission decision cachedRepoGET
// makes before any lookup — authorizeRepo: the caller's scope, the
// Shared-with-Me auto-add and its capacity check
// (TestCachedRepoGETAuthorizesBeforeAnyLookup) — decides who is answered,
// not what, so the tables it reads are derived the same way and left out.
// Every other table read must be listed (its writers are then checked) or
// reviewed as cache state, and every listed table must still be read.
//
// What the walk cannot see (L10 round of 0.29.124; none hides a read
// today, each proven by a mutant): a method value held in a local or in a
// Server field (f := s.store.X; f(…) — the homeLoader/repoStates shape), a
// call two packages deep, a method on another package's type, a table name
// built at run time (fmt.Sprintf), and a plain call of a name this package
// also declares as a method. A new read written in one of those shapes
// escapes; write the read as a call, or list its table.
func TestPageCacheTablesAreWhatTheCachedHandlersRead(t *testing.T) {
	apiPkg := readPkgSource(t, "internal/api", 20)
	dbPkg := readPkgSource(t, "internal/db", 20)
	otherPkgs := map[string]pkgSource{}
	imported := internalImports(t, "internal/api")
	var handlers []string
	for _, reg := range repoGETRegistrations(t) {
		if reg.cached {
			handlers = append(handlers, reg.handler)
		}
	}
	srctest.MinCount(t, "cached handlers", len(handlers), 1)
	if _, ok := apiPkg.funcs["authorizeRepo"]; !ok {
		t.Fatal("authorizeRepo, cachedRepoGET's admission decision, is not found: the scope tables cannot be told apart")
	}
	read := tablesReachedFrom(t, handlers, apiPkg, dbPkg, otherPkgs, imported)
	scope := tablesReachedFrom(t, []string{"authorizeRepo"}, apiPkg, dbPkg, otherPkgs, imported)
	listed := pageCacheTableList(t)
	var unlisted, unread []string
	for tbl := range read {
		if !listed[tbl] && !scope[tbl] && cacheStateTables[tbl] == "" {
			unlisted = append(unlisted, tbl)
		}
	}
	for tbl := range listed {
		if !read[tbl] {
			unread = append(unread, tbl)
		}
	}
	for tbl := range cacheStateTables {
		if !read[tbl] || scope[tbl] {
			unread = append(unread, tbl+" (cacheStateTables: no longer read, or already the admission's)")
		}
	}
	sort.Strings(unlisted)
	sort.Strings(unread)
	for _, tbl := range unlisted {
		t.Errorf("a cached handler reads %s, which is neither in internal/db/testdata/page_cache_tables.txt (its writers must stamp the repository) nor reviewed in cacheStateTables", tbl)
	}
	for _, tbl := range unread {
		t.Errorf("%s is listed but no cached handler reads it — remove it", tbl)
	}
}
