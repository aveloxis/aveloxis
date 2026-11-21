// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The worker reads no file of a clone (0.29.214).
//
// Review round 7 found the worker itself parsing every .go file of the clone
// after the child had returned (classifyGoModTestOnlyDeps): 12 GiB for one
// 128 MiB file. The pin then in place listed five function names and where
// each may be called from, and that function was not on the list. This one
// is by OPERATION and by reach: every function of the package that calls
// something that reads a file's content or a directory's entries must be
// reachable only from the child (ManifestChildMain), or be registered below
// with what it reads and why that is bounded. A new reader, anywhere in the
// worker, fails here until it is one or the other.
//
// What it cannot see, said plainly: a read made by another package on the
// worker's behalf (it sees this package's calls only, and holds every call
// into internal/manifest to a list of the ones that read nothing), and
// subprocesses (git, go, scc, scorecard, scancode), which are bounded where
// they are started.

// cloneReadingCalls are the calls that read a file's content or a
// directory's entries. A stat is not one: it reads no content.
var cloneReadingCalls = map[string]bool{
	"os.ReadFile": true, "os.Open": true, "os.OpenFile": true, "os.ReadDir": true, "os.ReadLink": true,
	"os.DirFS": true, "filepath.Walk": true, "filepath.WalkDir": true, "filepath.Glob": true,
	"ioutil.ReadFile": true, "ioutil.ReadDir": true, "ioutil.ReadAll": true,
	"parser.ParseFile": true, "parser.ParseDir": true, "fs.WalkDir": true, "fs.ReadFile": true, "fs.ReadDir": true,
}

// manifestCallsThatReadNothing are the functions of internal/manifest the
// worker may call: each works on a value it is handed. Any other call into
// that package is taken to read a file.
var manifestCallsThatReadNothing = map[string]bool{
	"manifest.CleanVersion": true, "manifest.NormalizeParsedVersion": true, "manifest.ParseSwiftPackageURL": true,
	"manifest.SplitNPMSpec": true, "manifest.IsRBasePackage": true, "manifest.ParseRemotePom": true,
	"manifest.Roster": true, "manifest.RemoteOutcome": true,
}

// workerFileReaders are the functions the worker runs that read a file or a
// directory, each with what it reads and why that cannot be a repository
// owner's doing without bound.
var workerFileReaders = map[string]string{
	"AnalysisCollector.scanGoModGraph": "walks the clone for the NAMES of go.mod files and reads none: what it costs is the number of directory entries, which git wrote when it made the clone",
	"FacadeCollector.ensureClone":      "reads the bare clone's own `config`, which git wrote when it cloned, not a file of the repository's tree",
	"sweepScancodeDir":                 "lists the directory of scancode's working clones (one entry for a repository), not a clone",
	// NOT bounded, and said so: scancode's output file holds a record for
	// every file of the repository and is read whole. It is the scancode
	// worker's, not the analysis's, and is outside what 0.29.214 moved;
	// reported in that release's review notes as the next reader to bound.
	"ingestScancodeOutput":      "reads scancode's own output file whole (a record for each file of the repository): NOT bounded here; the scancode worker's, outside the analysis",
	"salvageScancodeOutput":     "the same output file, read whole to parse its headers: NOT bounded here; the scancode worker's, outside the analysis",
	"readToolCheckTimestamp":    "a timestamp file of this host that this program wrote (~/.aveloxis-tool-check), not a file of a clone",
	"ensurePythonUserBinOnPath": "the operator's shell profile on this host, read and appended to by install-tools, not a file of a clone",
	"typecodePairInVenv":        "globs scancode's own Python virtualenv on this host for its libmagic files, not a clone",
}

func TestTheCloneIsReadOnlyByTheChild(t *testing.T) {
	fset := token.NewFileSet()
	type fn struct {
		name    string
		calls   map[string]bool // names of this package's functions it refers to
		reads   []string        // the reading calls it makes
		pkgInit bool            // a package-level initializer, not a function
	}
	funcs := map[string]*fn{}
	var files []*ast.File
	for name, src := range srctest.PackageFiles(t, "internal/collector", 40) {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	declName := func(d *ast.FuncDecl) string {
		if d.Recv == nil || len(d.Recv.List) == 0 {
			return d.Name.Name
		}
		rt := d.Recv.List[0].Type
		if s, ok := rt.(*ast.StarExpr); ok {
			rt = s.X
		}
		if id, ok := rt.(*ast.Ident); ok {
			return id.Name + "." + d.Name.Name
		}
		return d.Name.Name
	}
	// Every function, and every package-level initializer (a function
	// literal assigned to a variable is the worker's until shown otherwise).
	byShort := map[string][]string{}
	for _, f := range files {
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				n := declName(d)
				funcs[n] = &fn{name: n, calls: map[string]bool{}}
				byShort[d.Name.Name] = append(byShort[d.Name.Name], n)
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if vs, ok := spec.(*ast.ValueSpec); ok && len(vs.Values) > 0 {
						for _, id := range vs.Names {
							n := "var " + id.Name
							funcs[n] = &fn{name: n, calls: map[string]bool{}, pkgInit: true}
						}
					}
				}
			}
		}
	}
	// What each refers to. A reference by name is taken for a call of every
	// function of that name (a method value, a callback): more reach, never
	// less.
	scan := func(owner *fn, body ast.Node) {
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok {
						call := pkg.Name + "." + sel.Sel.Name
						if cloneReadingCalls[call] || (pkg.Name == "manifest" && !manifestCallsThatReadNothing[call]) {
							owner.reads = append(owner.reads, call)
						}
					}
					// walk.Read(...) and the like: a method of a
					// manifest.Walk reads.
					if sel.Sel.Name == "Read" || sel.Sel.Name == "SettleMavenManaged" {
						if id, ok := sel.X.(*ast.Ident); ok && id.Name == "walk" {
							owner.reads = append(owner.reads, "manifest.Walk."+sel.Sel.Name)
						}
					}
				}
			case *ast.Ident:
				for _, full := range byShort[x.Name] {
					owner.calls[full] = true
				}
				if _, ok := funcs["var "+x.Name]; ok {
					owner.calls["var "+x.Name] = true
				}
			case *ast.SelectorExpr:
				for _, full := range byShort[x.Sel.Name] {
					owner.calls[full] = true
				}
			}
			return true
		})
	}
	for _, f := range files {
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					owner := funcs[declName(d)]
					scan(owner, d.Body)
					delete(owner.calls, owner.name) // its own name in its body is recursion
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if vs, ok := spec.(*ast.ValueSpec); ok {
						for i, id := range vs.Names {
							if i < len(vs.Values) {
								scan(funcs["var "+id.Name], vs.Values[i])
							}
						}
					}
				}
			}
		}
	}
	if len(funcs) < 800 {
		t.Fatalf("%d functions and initializers read in internal/collector: the package has more", len(funcs))
	}
	// The child is ManifestChildMain and what only it reaches. The worker is
	// everything reachable from any function that the child's own entry
	// points do not account for: start from every function nothing in the
	// package refers to (an entry point: exported, or called from another
	// package or a test) and every package-level initializer, less the
	// child's two entry points.
	childEntry := map[string]bool{"ManifestChildMain": true, "RunManifestChildIfAsked": true}
	referred := map[string]bool{}
	for _, f := range funcs {
		for c := range f.calls {
			referred[c] = true
		}
	}
	reach := func(roots []string, stopAt map[string]bool) map[string]bool {
		seen := map[string]bool{}
		for len(roots) > 0 {
			n := roots[len(roots)-1]
			roots = roots[:len(roots)-1]
			if seen[n] || stopAt[n] {
				continue
			}
			seen[n] = true
			for c := range funcs[n].calls {
				roots = append(roots, c)
			}
		}
		return seen
	}
	var workerRoots []string
	for n, f := range funcs {
		if childEntry[n] {
			continue
		}
		if !referred[n] || f.pkgInit || ast.IsExported(strings.TrimPrefix(n[strings.LastIndex(n, ".")+1:], "var ")) {
			workerRoots = append(workerRoots, n)
		}
	}
	worker := reach(workerRoots, childEntry)
	child := reach([]string{"ManifestChildMain"}, nil)
	if !child["readManifestDeclarations"] || !child["walkLockfiles"] || !child["readManifestNames"] || !child["classifyGoModTestOnlyDeps"] {
		t.Fatal("the child does not reach its own passes: the reading of the package's calls is broken")
	}
	var bad []string
	readers := 0
	used := map[string]bool{}
	for n, f := range funcs {
		if len(f.reads) == 0 {
			continue
		}
		readers++
		if !worker[n] {
			continue // the child's alone
		}
		if _, ok := workerFileReaders[n]; ok {
			used[n] = true
			continue
		}
		bad = append(bad, n+" calls "+strings.Join(f.reads, ", "))
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Errorf("%s, and is reachable in the worker: a clone's files are read by the child (manifest_child.go). Move the reading into a pass the child serves, or register the function in workerFileReaders with what it reads and why that is bounded", b)
	}
	for n, why := range workerFileReaders {
		if !used[n] {
			t.Errorf("workerFileReaders lists %s, which reads no file in the worker now: delete the entry", n)
		}
		if len(why) < 40 {
			t.Errorf("workerFileReaders[%s] does not say what it reads and why that is bounded", n)
		}
	}
	if readers < 8 {
		t.Fatalf("%d functions that read a file were found: the package has more", readers)
	}

	// And the analysis takes what the child read, once.
	analyze := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) AnalyzeRepo("))
	if strings.Count(analyze, "ac.readClone(") != 1 || !strings.Contains(analyze, "read.lockfiles()") {
		t.Error("AnalyzeRepo reads its clone once, through readClone, and takes the lockfiles from that reading")
	}
}
