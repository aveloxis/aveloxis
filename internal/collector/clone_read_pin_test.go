// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The worker holds no amount of repository-determined data that a stated
// limit does not bound (0.29.215).
//
// Round 7 found the worker parsing every .go file of a clone; the pin then
// written was by operation ("reads a file") and by the spelling of the
// call. Round 8 found both wrong. The spelling let an import alias, a
// function value and os.OpenRoot through. And the operation was the wrong
// thing to hold: the hazard is a property of DATA, that its size is the
// repository's to choose, and a subprocess's output has it as much as a
// file does (`go mod graph` printed nine million edges for 228 KB of
// go.mod, taken whole).
//
// So this pin is by the type checker's OBJECTS, not names, and it covers
// everything by which such data enters the process: a file's content, a
// directory's entries, a subprocess's output (its pipes, Output and
// CombinedOutput, and whatever is assigned to Cmd.Stdout or Cmd.Stderr),
// and the readers and decoders put over them. Every function of the package
// that uses one must be reachable only from the child (ManifestChildMain),
// or be registered below with its BOUND: what limits what it takes, and
// the helpers that enforce it, which the function must be seen to use.
//
// What it cannot see, said plainly: a read made by another package on the
// worker's behalf (it sees this package's uses only, and holds every use of
// internal/manifest to a list of the ones that read nothing); and what a
// function does with data once it has it (a registered function could
// still build an unbounded value from a bounded stream: its registration
// says why it does not, and a reviewer reads that).

// dataEntryFuncs are package-level functions through which a file's
// content, a directory's entries or a subprocess's output enters the
// process, by import path. A stat is not one: it reads no content.
var dataEntryFuncs = map[string][]string{
	"os":            {"ReadFile", "Open", "OpenFile", "ReadDir", "Readlink", "DirFS", "OpenRoot", "OpenInRoot", "CopyFS"},
	"io":            {"ReadAll"},
	"io/ioutil":     {"ReadFile", "ReadDir", "ReadAll"},
	"io/fs":         {"ReadFile", "ReadDir", "WalkDir", "Glob"},
	"path/filepath": {"Walk", "WalkDir", "Glob"},
	"bufio":         {"NewReader", "NewReaderSize", "NewScanner"},
	"go/parser":     {"ParseFile", "ParseDir"},
	"archive/zip":   {"OpenReader"},
}

// dataEntryMethods are the methods, by the receiver's package and type.
// "*" is every method of the type.
var dataEntryMethods = map[string][]string{
	"os.File":     {"Read", "ReadAt", "ReadFrom", "WriteTo", "ReadDir", "Readdir", "Readdirnames"},
	"os.Root":     {"*"},
	"os/exec.Cmd": {"Output", "CombinedOutput", "StdoutPipe", "StderrPipe"},
}

// dataEntryFields are fields whose ASSIGNMENT decides where a subprocess's
// output goes. An assignment is an entry unless what is assigned cannot
// hold more than a fixed amount: one of the package's bounded writers
// (boundedWriters), or a file (the terminal for install-tools, a pipe).
var dataEntryFields = map[string][]string{
	"os/exec.Cmd": {"Stdout", "Stderr"},
}

// boundedWriters are the package's writers that keep a fixed amount
// whatever is written to them. Each is pinned by its own test.
var boundedWriters = map[string]bool{
	"stderrCapture": true, "boundedCapture": true, "cappedOutput": true,
	"capWriter": true, "tailBuffer": true, "headTailBuffer": true, "cappedBuffer": true,
}

// manifestUsesThatReadNothing are the functions of internal/manifest the
// worker may use: each works on a value it is handed. Any other use of
// that package's functions is taken to read a file.
var manifestUsesThatReadNothing = map[string]bool{
	"CleanVersion": true, "NormalizeParsedVersion": true, "ParseSwiftPackageURL": true,
	"SplitNPMSpec": true, "IsRBasePackage": true, "ParseRemotePom": true,
	"Roster": true, "DeclaredKind": true,
}

// A workerBound registers one function the worker runs that takes such
// data: what bounds it, and the package's own helpers that enforce the
// bound, each of which the function must use (so that a registration
// cannot outlive the code it describes).
type workerBound struct {
	Bound string
	Via   []string
}

var workerDataEntries = map[string]workerBound{
	// The bounded helpers themselves: what every other entry goes through.
	"boundedLines":         {Bound: "one line at a time (at most boundedLineBytes), stopping the subprocess past the caller's byte bound; nothing of the output is kept by it", Via: []string{"startSweptCommand", "stderrCapture"}},
	"readToolOutputWithin": {Bound: "a file no longer than the caller's bound, measured before it is read, into one buffer of that size; refused past it", Via: nil},
	"walkBounded":          {Bound: "walkChunk directory entries at a time, never a directory's whole listing, and at most the caller's count of entries in all", Via: nil},
	"lockfileSpool.each":   {Bound: "reads the worker's own spool file back one lockfile at a time; the file's size is bounded when it is written (lockfileWireFactor times the lockfiles' size on disk)", Via: nil},
	"runManifestChild":     {Bound: "the reader child's frames, each at most manifestFrameLimit, checked on arrival; what is kept of them is bounded where it is kept (the spool; the names and declarations by the child's own memory limit)", Via: []string{"cappedBuffer"}},

	// Subprocess output decided by a repository.
	"FacadeCollector.parseGitLog":      {Bound: "git log as a stream, a line at a time (at most 10 MiB a line), parsed into commits that are flushed in batches; never the whole log", Via: []string{"startSweptCommand", "stderrCapture"}},
	"parseWhitespaceLog":               {Bound: "git log as a stream through a 1 MiB reader, a record at a time; counts are kept, not the text", Via: nil},
	"CommitResolver.loadDefaultBranch": {Bound: "git rev-list as a stream, a hash a line; only the hashes this run asked about are kept (bounded by the run's own batch)", Via: []string{"startSweptCommand", "stderrCapture"}},
	"ScancodeWorker.executeScan":       {Bound: "scancode's stdout and stderr go to a tail and a head-and-tail buffer of fixed sizes through io.MultiWriter; its report goes to a file, read by decodeScancodeReport under scancodeOutputMaxBytes", Via: []string{"tailBuffer", "headTailBuffer"}},

	// Subprocess output of a fixed shape, whatever the repository holds.
	"FacadeCollector.runWhitespaceWalk": {Bound: "cmd.Output of `git rev-parse <branch>`: one object name; the walk itself is streamed by parseWhitespaceLog", Via: []string{"parseWhitespaceLog"}},
	"FacadeCollector.syncDefaultBranch": {Bound: "cmd.Output of `git ls-remote --symref origin HEAD` and `git symbolic-ref HEAD`: git prints the one ref asked for, whatever the remote advertises", Via: nil},
	"defaultBranchIsEmpty":              {Bound: "cmd.Output of `git rev-parse --verify --quiet`: at most one object name", Via: nil},
	"resolveDefaultBranch":              {Bound: "cmd.Output of `git symbolic-ref HEAD`: one ref name", Via: nil},
	"FacadeCollector.ensureClone":       {Bound: "the bare clone's own `config` file, which git wrote on this host when it cloned (not a file of the repository's tree), and the output of `git remote set-url`, which prints nothing on success", Via: nil},

	// This host's files and tools, not a repository's.
	"goEnvValues":               {Bound: "cmd.Output of `go env` for a fixed list of variables: this host's toolchain settings", Via: nil},
	"readToolCheckTimestamp":    {Bound: "a timestamp file of this host that this program wrote (~/.aveloxis-tool-check)", Via: nil},
	"ensurePythonUserBinOnPath": {Bound: "the operator's shell profile on this host and `python3 -m site --user-base`, during install-tools", Via: nil},
	"typecodePairInVenv":        {Bound: "globs scancode's own Python virtualenv on this host for its libmagic files", Via: nil},
	"sweepScancodeDir":          {Bound: "lists the directory of scancode's working clones: one entry for a repository being scanned, at most the scancode worker count", Via: nil},
	"runLoggedCommand":          {Bound: "the output of a remediation command on this host (pip, brew, apt) during scancode's self-repair: a package manager's log, not a repository's", Via: nil},

	// The network, bounded where it is read.
	"fetchRegistryJSON": {Bound: "a registry's answer through io.LimitReader at the caller's byte limit, plus one byte to tell that it was passed", Via: nil},
	"probeSCCVersion":   {Bound: "scc --version through io.LimitReader at sccVersionOutputMaxBytes", Via: []string{"startSweptCommand"}},
}

const collectorImportPath = "github.com/aveloxis/aveloxis/internal/collector"

var (
	pinImporterOnce sync.Once
	pinImporter     types.ImporterFrom
	pinFset         = token.NewFileSet()
)

type pinRootImporter struct {
	from types.ImporterFrom
	dir  string
}

func (r pinRootImporter) Import(path string) (*types.Package, error) {
	return r.from.ImportFrom(path, r.dir, 0)
}

// workerDataEntryReport type-checks the package's non-test files for this
// platform, with extra files added (fixtures), and returns: every function
// reachable in the worker that takes data by an entry above and is not
// registered or does not use the helpers its registration names; the
// registrations that cover nothing; and how many functions take data at
// all (the denominator).
func workerDataEntryReport(t *testing.T, extra map[string]string) (bad, stale []string, takers int) {
	t.Helper()
	root := srctest.Root(t)
	dir := filepath.Join(root, "internal", "collector")
	pinImporterOnce.Do(func() {
		pinImporter = importer.ForCompiler(pinFset, "source", nil).(types.ImporterFrom)
	})
	var files []*ast.File
	n := 0
	for name, src := range srctest.PackageFiles(t, "internal/collector", 40) {
		if ok, err := build.Default.MatchFile(dir, filepath.Base(name)); err != nil || !ok {
			continue
		}
		f, err := parser.ParseFile(pinFset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
		n++
	}
	for name, src := range extra {
		f, err := parser.ParseFile(pinFset, filepath.Join(dir, name), src, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	if n < 40 {
		t.Fatalf("%d files of internal/collector read: the package has more", n)
	}
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}}
	var typeErrs []string
	conf := types.Config{Importer: pinRootImporter{pinImporter, root}, Error: func(err error) { typeErrs = append(typeErrs, err.Error()) }}
	pkg, _ := conf.Check(collectorImportPath, pinFset, files, info)
	if len(typeErrs) > 0 {
		t.Fatalf("internal/collector does not type-check here, so nothing can be said of it: %s", strings.Join(typeErrs[:min(3, len(typeErrs))], "; "))
	}

	entryFunc := map[string]bool{}
	for path, names := range dataEntryFuncs {
		for _, name := range names {
			entryFunc[path+"."+name] = true
		}
	}
	recvKey := func(recv types.Type) string {
		if p, ok := recv.(*types.Pointer); ok {
			recv = p.Elem()
		}
		named, ok := recv.(*types.Named)
		if !ok || named.Obj().Pkg() == nil {
			return ""
		}
		return named.Obj().Pkg().Path() + "." + named.Obj().Name()
	}
	inList := func(list []string, name string) bool {
		for _, l := range list {
			if l == "*" || l == name {
				return true
			}
		}
		return false
	}
	// entry says whether using obj takes data, and under what name.
	entry := func(obj types.Object) (string, bool) {
		switch o := obj.(type) {
		case *types.Func:
			if o.Pkg() == nil {
				return "", false
			}
			sig := o.Type().(*types.Signature)
			if sig.Recv() == nil {
				if entryFunc[o.Pkg().Path()+"."+o.Name()] {
					return o.Pkg().Name() + "." + o.Name(), true
				}
				if strings.HasSuffix(o.Pkg().Path(), "/internal/manifest") && !manifestUsesThatReadNothing[o.Name()] {
					return "manifest." + o.Name(), true
				}
				return "", false
			}
			key := recvKey(sig.Recv().Type())
			if inList(dataEntryMethods[key], o.Name()) {
				return key + "." + o.Name(), true
			}
			if strings.HasSuffix(key, "/internal/manifest.Walk") {
				return "manifest.Walk." + o.Name(), true
			}
		case *types.Var:
			if o.IsField() && o.Pkg() != nil {
				for key, fields := range dataEntryFields {
					if strings.HasPrefix(key, o.Pkg().Path()+".") && inList(fields, o.Name()) {
						return key + "." + o.Name() + " (assigned)", true
					}
				}
			}
		}
		return "", false
	}

	type fn struct {
		name  string
		uses  map[string]bool // this package's functions, methods and package variables it refers to
		takes []string
		root  bool
	}
	funcs := map[string]*fn{}
	nameOf := func(obj types.Object) string {
		switch o := obj.(type) {
		case *types.Func:
			sig := o.Type().(*types.Signature)
			if sig.Recv() != nil {
				key := recvKey(sig.Recv().Type())
				return key[strings.LastIndex(key, ".")+1:] + "." + o.Name()
			}
			return o.Name()
		case *types.Var:
			return "var " + o.Name()
		}
		return ""
	}
	// A method called through an interface reaches every method of that
	// name in the package: more reach, never less.
	methodsNamed := map[string][]string{}
	for _, f := range files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok {
				obj := info.Defs[fd.Name]
				if obj == nil {
					continue
				}
				n := nameOf(obj)
				funcs[n] = &fn{name: n, uses: map[string]bool{}}
				if fd.Recv != nil {
					methodsNamed[fd.Name.Name] = append(methodsNamed[fd.Name.Name], n)
				}
			}
			if gd, ok := d.(*ast.GenDecl); ok {
				for _, spec := range gd.Specs {
					if vs, ok := spec.(*ast.ValueSpec); ok && len(vs.Values) > 0 {
						for _, id := range vs.Names {
							if id.Name != "_" {
								funcs["var "+id.Name] = &fn{name: "var " + id.Name, uses: map[string]bool{}, root: true}
							}
						}
					}
				}
			}
		}
	}
	scan := func(owner *fn, body ast.Node) {
		// Assignments to a Cmd's Stdout or Stderr, by the field's object:
		// an entry unless what is assigned is bounded by its type.
		boundedSink := func(e ast.Expr) bool {
			t := info.TypeOf(e)
			if t == nil {
				return false
			}
			if p, ok := t.(*types.Pointer); ok {
				t = p.Elem()
			}
			named, ok := t.(*types.Named)
			if !ok || named.Obj().Pkg() == nil {
				return false
			}
			if named.Obj().Pkg() == pkg {
				return boundedWriters[named.Obj().Name()]
			}
			return named.Obj().Pkg().Path() == "os" && named.Obj().Name() == "File"
		}
		field := func(e ast.Expr) (string, bool) {
			var id *ast.Ident
			switch x := e.(type) {
			case *ast.SelectorExpr:
				id = x.Sel
			case *ast.Ident:
				id = x
			}
			if id == nil {
				return "", false
			}
			if v, ok := info.Uses[id].(*types.Var); ok && v.IsField() {
				return entry(v)
			}
			return "", false
		}
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for i, l := range x.Lhs {
					what, ok := field(l)
					if !ok {
						continue
					}
					if len(x.Rhs) == len(x.Lhs) && boundedSink(x.Rhs[i]) {
						continue
					}
					owner.takes = append(owner.takes, what)
				}
			case *ast.KeyValueExpr:
				if what, ok := field(x.Key); ok && !boundedSink(x.Value) {
					owner.takes = append(owner.takes, what)
				}
			}
			return true
		})
		ast.Inspect(body, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			obj := info.Uses[id]
			if obj == nil {
				return true
			}
			if what, ok := entry(obj); ok {
				if _, isField := obj.(*types.Var); !isField {
					owner.takes = append(owner.takes, what)
				}
			}
			if obj.Pkg() != pkg {
				return true
			}
			switch o := obj.(type) {
			case *types.Func:
				sig := o.Type().(*types.Signature)
				if sig.Recv() != nil {
					if _, isIface := sig.Recv().Type().Underlying().(*types.Interface); isIface {
						for _, m := range methodsNamed[o.Name()] {
							owner.uses[m] = true
						}
						return true
					}
				}
				owner.uses[nameOf(o)] = true
			case *types.Var:
				if o.Parent() == pkg.Scope() {
					owner.uses["var "+o.Name()] = true
				}
			}
			return true
		})
	}
	for _, f := range files {
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Body == nil || info.Defs[d.Name] == nil {
					continue
				}
				owner := funcs[nameOf(info.Defs[d.Name])]
				scan(owner, d.Body)
				delete(owner.uses, owner.name)
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if vs, ok := spec.(*ast.ValueSpec); ok {
						for i, id := range vs.Names {
							if i < len(vs.Values) && funcs["var "+id.Name] != nil {
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
	childEntry := map[string]bool{"ManifestChildMain": true, "RunManifestChildIfAsked": true}
	referred := map[string]bool{}
	for _, f := range funcs {
		for u := range f.uses {
			referred[u] = true
		}
	}
	reach := func(roots []string, stopAt map[string]bool) map[string]bool {
		seen := map[string]bool{}
		for len(roots) > 0 {
			n := roots[len(roots)-1]
			roots = roots[:len(roots)-1]
			if seen[n] || stopAt[n] || funcs[n] == nil {
				continue
			}
			seen[n] = true
			for u := range funcs[n].uses {
				roots = append(roots, u)
			}
		}
		return seen
	}
	var workerRoots []string
	for n, f := range funcs {
		if childEntry[n] {
			continue
		}
		last := strings.TrimPrefix(n[strings.LastIndex(n, ".")+1:], "var ")
		if !referred[n] || f.root || ast.IsExported(last) {
			workerRoots = append(workerRoots, n)
		}
	}
	worker := reach(workerRoots, childEntry)
	child := reach([]string{"ManifestChildMain"}, nil)
	for _, must := range []string{"readManifestDeclarations", "walkLockfiles", "readManifestNames", "classifyGoModTestOnlyDeps"} {
		if !child[must] {
			t.Fatalf("the child does not reach %s: the reading of the package's uses is broken", must)
		}
	}
	used := map[string]bool{}
	for n, f := range funcs {
		if len(f.takes) == 0 {
			continue
		}
		takers++
		if !worker[n] {
			continue // the child's alone
		}
		reg, ok := workerDataEntries[n]
		if !ok {
			sort.Strings(f.takes)
			bad = append(bad, n+" uses "+strings.Join(compactStrings(f.takes), ", "))
			continue
		}
		used[n] = true
		for _, via := range reg.Via {
			if !f.uses[via] && !f.uses["var "+via] && !usesType(info, files, n, via, nameOf) {
				bad = append(bad, n+" is registered as bounded through "+via+" and does not use it")
			}
		}
	}
	for n := range workerDataEntries {
		if !used[n] {
			stale = append(stale, n)
		}
	}
	sort.Strings(bad)
	sort.Strings(stale)
	return bad, stale, takers
}

// usesType reports whether the function named fn refers to the package
// type named typ (a registration may name a type: boundedCapture).
func usesType(info *types.Info, files []*ast.File, fn, typ string, nameOf func(types.Object) string) bool {
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil || info.Defs[fd.Name] == nil || nameOf(info.Defs[fd.Name]) != fn {
				continue
			}
			found := false
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if tn, ok := info.Uses[id].(*types.TypeName); ok && tn.Name() == typ {
						found = true
					}
				}
				return !found
			})
			return found
		}
	}
	return false
}

func compactStrings(s []string) []string {
	var out []string
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func TestTheWorkerTakesNoUnboundedRepositoryData(t *testing.T) {
	bad, stale, takers := workerDataEntryReport(t, nil)
	for _, b := range bad {
		t.Errorf("%s, and is reachable in the worker. What a repository's files say is read by the child (manifest_child.go); a subprocess's output and a tool's report are taken through the bounded helpers (bounded_output.go). Move it, or register the function in workerDataEntries with its bound and the helpers that enforce it", b)
	}
	for _, s := range stale {
		t.Errorf("workerDataEntries lists %s, which takes no such data in the worker now: delete the entry", s)
	}
	for n, reg := range workerDataEntries {
		if len(reg.Bound) < 40 {
			t.Errorf("workerDataEntries[%s] does not say what bounds it", n)
		}
	}
	if takers < 20 {
		t.Fatalf("%d functions that take a file's content, a directory's entries or a subprocess's output were found: the package has more", takers)
	}

	// And the analysis takes what the child read, once.
	analyze := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) AnalyzeRepo("))
	if strings.Count(analyze, "ac.readClone(") != 1 || !strings.Contains(analyze, "read.lockfiles()") {
		t.Error("AnalyzeRepo reads its clone once, through readClone, and takes the lockfiles from that reading")
	}
}

// The spellings round 8's reviewer got past the pin's predecessor, each a
// worker function that reads a file of the clone or takes a subprocess's
// output whole. Each must be reported.
func TestThePinSeesEverySpellingOfARead(t *testing.T) {
	dir := filepath.Join(srctest.Root(t), "internal", "collector", "testdata", "pin_evasions")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	// All six in ONE reading of the package (each declares its own
	// PinEvasion function): the package was type-checked once per fixture,
	// twenty seconds of it under the race detector.
	seen := 0
	fixtures, wanted := map[string]string{}, map[string]string{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go.txt") {
			continue
		}
		seen++
		src, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		fixtures["zz_"+strings.TrimSuffix(e.Name(), ".txt")] = string(src)
		// The fixture x.go.txt declares PinEvasionX.
		wanted[e.Name()] = "PinEvasion" + strings.ToUpper(strings.TrimSuffix(e.Name(), ".go.txt"))
	}
	bad, _, _ := workerDataEntryReport(t, fixtures)
	for name, fn := range wanted {
		if !strings.Contains(fixtures["zz_"+strings.TrimSuffix(name, ".txt")], "func "+fn+"(") {
			t.Fatalf("%s does not declare %s: the fixture and this test disagree about its function's name", name, fn)
		}
		found := false
		for _, b := range bad {
			// The whole name: PinEvasionA must not answer for PinEvasionAB.
			if rest, ok := strings.CutPrefix(b, fn); ok && (rest == "" || !isIdentByte(rest[0])) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: a worker function that takes repository data this way (%s) is not reported", name, fn)
		}
	}
	if seen < 6 {
		t.Fatalf("%d evasion fixtures read: there are six", seen)
	}
}

// isIdentByte: c can continue a Go identifier (ASCII is all these names use).
func isIdentByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
