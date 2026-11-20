// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestNoUnboundedResponseBodyRead — O5 (v0.29.71): a response body read
// whole (io.ReadAll(x.Body)) lets a broken or hostile upstream exhaust a
// worker's memory. An ERROR body is read through platform.ReadErrorBody; a
// read wrapped in io.LimitReader is bounded. The remaining unbounded reads
// are DATA bodies, listed here by file and function: their limit is an
// operator decision (no success-body size has been measured), and an entry
// that stops matching fails as stale. The denominator is every io.ReadAll
// examined.
func TestNoUnboundedResponseBodyRead(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	found := map[string]int{}
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			src, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			for _, fn := range unboundedBodyReads(t, rel, string(src), &examined) {
				found[srctest.SiteKey(srctest.SiteDir(filepath.ToSlash(rel)), fn)]++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range unboundedReadMismatches(unboundedDataBodyReads, found) {
		t.Error(m)
	}
	srctest.MinCount(t, "io.ReadAll calls examined", examined, 15)
}

// unboundedDataBodyReads are the data-body reads still unbounded, pending
// the operator's success-body limit (summary/43 §2 item 5), with the exact
// number of such reads in each function: keyed by function alone, an error
// body in the same function could go back to io.ReadAll unnoticed (v0.29.71
// review round 1 F2).
var unboundedDataBodyReads = map[string]struct {
	reads  int
	reason string
}{
	srctest.SiteKey("internal/importers/apache", "fetchJSON"):  {1, "O5 data body: limit pending the operator decision"},
	srctest.SiteKey("internal/importers/cncf", "Fetch"):        {1, "O5 data body: limit pending the operator decision"},
	srctest.SiteKey("internal/importers/numfocus", "crawlURL"): {1, "O5 data body: limit pending the operator decision"},
	srctest.SiteKey("internal/mailinglist", "get"):             {1, "O5 data body: limit pending the operator decision"},
	srctest.SiteKey("internal/mailinglist", "FetchMonth"):      {1, "O5 data body: limit pending the operator decision (a month's mbox)"},
	srctest.SiteKey("internal/platform", "GraphQLAt"):          {1, "O5 data body: limit pending the operator decision (a GraphQL page; giant repositories)"},
	srctest.SiteKey("internal/web", "fetchOAuthUser"):          {1, "O5 data body: limit pending the operator decision (the OAuth user answer; the one head of both callbacks since 0.29.102)"},
	srctest.SiteKey("internal/mailinglist", "parseRFC822"):     {1, "not a network response: a mail message already parsed from bytes held in memory"},
}

// unboundedReadMismatches reports every function whose unbounded body
// reads differ from its reviewed count: an unlisted read, an extra one in a
// listed function, or an entry that matched fewer (stale).
func unboundedReadMismatches(allowed map[string]struct {
	reads  int
	reason string
}, found map[string]int) []string {
	var out []string
	for key, n := range found {
		if want := allowed[key].reads; n > want {
			out = append(out, fmt.Sprintf("%s: %d unbounded io.ReadAll of a response body, %d reviewed — use platform.ReadErrorBody for an error body, io.LimitReader for data", key, n, want))
		}
	}
	for key, e := range allowed {
		if found[key] < e.reads {
			out = append(out, fmt.Sprintf("entry %q states %d reads and matched %d — correct or delete it", key, e.reads, found[key]))
		}
	}
	sort.Strings(out)
	return out
}

// TestUnboundedBodyReadCorpus proves the scan and the count both ways.
func TestUnboundedBodyReadCorpus(t *testing.T) {
	src := `package p
import "io"
func data(resp *http.Response) { _, _ = io.ReadAll(resp.Body) }
func both(resp *http.Response) { _, _ = io.ReadAll(resp.Body); _, _ = io.ReadAll(resp.Body) }
func bounded(resp *http.Response) { _, _ = io.ReadAll(io.LimitReader(resp.Body, 10)) }
func notBody(r io.Reader) { _, _ = io.ReadAll(r) }
`
	n := 0
	found := map[string]int{}
	for _, fn := range unboundedBodyReads(t, "corpus.go", src, &n) {
		found["corpus.go "+fn]++
	}
	if n != 5 || found["corpus.go data"] != 1 || found["corpus.go both"] != 2 || found["corpus.go bounded"] != 0 || found["corpus.go notBody"] != 0 {
		t.Errorf("examined %d, found %v; want 5 examined, data 1, both 2", n, found)
	}
	allowed := map[string]struct {
		reads  int
		reason string
	}{"corpus.go data": {1, "r"}, "corpus.go both": {1, "r"}, "corpus.go gone": {1, "r"}}
	got := unboundedReadMismatches(allowed, found)
	if len(got) != 2 || !strings.Contains(strings.Join(got, "|"), "corpus.go both: 2") || !strings.Contains(strings.Join(got, "|"), `"corpus.go gone" states 1 reads and matched 0`) {
		t.Errorf("mismatches = %q; want the extra read in both and the stale gone", got)
	}
}

// unboundedBodyReads names the enclosing function of every io.ReadAll
// whose argument is a .Body selector (a response body) not wrapped in a
// bounding reader.
func unboundedBodyReads(t testing.TB, name, src string, examined *int) []string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	var out []string
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "ReadAll" {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "io" {
				return true
			}
			*examined++
			if arg, ok := call.Args[0].(*ast.SelectorExpr); ok && arg.Sel.Name == "Body" {
				out = append(out, fd.Name.Name)
			}
			return true
		})
	}
	return out
}

// TestUnboundedDataReadsLogTheirSize — operator, 2026-09-30: data bodies get
// no size limit, but the largest are logged. Every function allowlisted above
// for a network data read notes the size it read (platform.NoteResponseSize,
// or reads through platform.CountResponseBody). What this checks is the
// io.ReadAll reads; streamed decodes are counted where the body is large (the
// forge REST client, deps.dev, ecosyste.ms, Jira, the OSV batch and detail
// answers) and not for small fixed-shape answers (a rate-limit probe, one
// commit, a release record, the OAuth e-mail list, CLI listings). A decode
// site is not scanned (review round 1 F6, recorded).
func TestUnboundedDataReadsLogTheirSize(t *testing.T) {
	for key, e := range unboundedDataBodyReads {
		if !strings.HasPrefix(e.reason, "O5 data body") {
			continue // not a network response
		}
		dir, fn, _ := strings.Cut(key, "|")
		if !funcCallsAny(t, dir, fn, "NoteResponseSize", "CountResponseBody") {
			t.Errorf("%s: an unbounded data read that does not log its size (platform.NoteResponseSize)", key)
		}
	}
}

// funcCallsAny reports whether every declaration named fn in the package
// at dir that reads a response body (an `io.ReadAll(x.Body)` call — the
// registry's subject) calls any of names (a bare or package-qualified
// call). Two methods may share a name on different receivers
// (mailinglist's two FetchMonth: Pony Mail reads HTTP, public-inbox reads
// git blobs); the first version checked whichever the map walk reached
// first, so the pin flipped between runs (stage 1 of summary/59, found on
// the way). A declaration without a body read is not the registry's
// subject and is not checked.
func funcCallsAny(t *testing.T, dir, fn string, names ...string) bool {
	t.Helper()
	var decls []ast.Decl
	for file, src := range srctest.PackageFiles(t, dir, 1) {
		f, err := parser.ParseFile(token.NewFileSet(), file, src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		decls = append(decls, f.Decls...)
	}
	seen, readers := 0, 0
	for _, d := range decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != fn || fd.Body == nil {
			continue
		}
		seen++
		readsBody, found := false, false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "ReadAll" && len(call.Args) == 1 {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "io" {
					if arg, ok := call.Args[0].(*ast.SelectorExpr); ok && arg.Sel.Name == "Body" {
						readsBody = true
					}
				}
			}
			var name string
			switch f := call.Fun.(type) {
			case *ast.Ident:
				name = f.Name
			case *ast.SelectorExpr:
				name = f.Sel.Name
			}
			for _, want := range names {
				if name == want {
					found = true
				}
			}
			return true
		})
		if !readsBody {
			continue
		}
		readers++
		if !found {
			return false
		}
	}
	if seen == 0 {
		t.Fatalf("function %s not found in %s", fn, dir)
	}
	if readers == 0 {
		t.Fatalf("no declaration named %s in %s reads a response body — the registry entry lost its subject", fn, dir)
	}
	return true
}
