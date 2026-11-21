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

// What a worker takes of a repository's data is LISTED here, each place with
// its bound or with the plain statement that it has none.
//
// 0.29.215 claimed more: "the worker holds no amount of repository-determined
// data that a stated limit does not bound". Review round 9 showed that
// false (a Go expansion held at 3.7 GiB with every bound respected; the
// lockfile rows streamed in were read back out whole by the SBOM; scanSCC,
// which holds a row for every file, was not seen at all). The claim is
// withdrawn. What stands instead (operator, 2026-11-21: "restate the claim
// honestly, cap the worst, and build it in chunk 3"):
//
//   - every function of this package that takes a file's content, a
//     directory's entries or a subprocess's output in the worker is
//     registered below, and its registration begins "bounded:" and gives the
//     number and where it is enforced, or begins "NOT BOUNDED" and says what
//     the amount grows with;
//   - the worst are capped: what the Go expansion holds for a repository,
//     the rows of one lockfile snapshot (db.LockfileSnapshotRows), the rows
//     scanSCC holds;
//   - bounding a worker's memory as a whole (the SBOM, the scan's targets,
//     the carry, scc's ingestion, the git log's batches, what the parent
//     holds of the child's result) is the vulnerability program's chunk 3.
//
// How the pin sees. Round 7 found the worker parsing every .go file of a
// clone; the pin then written was by operation ("reads a file") and by the
// spelling of the call. Round 8 found both wrong: an import alias, a
// function value and os.OpenRoot got through, and the hazard is a property
// of DATA (its size is the repository's to choose), which a subprocess's
// output has as much as a file does. So this pin is by the type checker's
// OBJECTS, not names, and it covers a file's content, a directory's
// entries, a subprocess's output (its pipes, Output and CombinedOutput,
// whatever is assigned to Cmd.Stdout or Cmd.Stderr, and the stream the
// package's own startSweptCommand hands back), temporary files and pipes
// made to be read back, and the readers put over them. Every function of
// the package that uses one must be reachable only from the child
// (ManifestChildMain), or be registered with its statement, the helpers
// that enforce it (which the function must be seen to use) and the reads
// it makes (all of them: a read added to a registered function is
// reported).
//
// What it cannot see, said plainly:
//
//   - data that enters another way: rows read back from the database and
//     bodies read from the network. The places known to hold an amount the
//     repository decides are named in workerHoldsOutsideThePin, each with
//     its statement; that list is kept by review, not by the type checker;
//   - a read made by another package on the worker's behalf (it sees this
//     package's uses only, and holds every use of internal/manifest to a
//     list of the ones that read nothing);
//   - what a function does with data once it has it: a registered function
//     can build an unbounded value from a bounded stream, which is why a
//     registration must SAY "NOT BOUNDED" when it does, and a reviewer reads
//     that.

// dataEntryFuncs are package-level functions through which a file's
// content, a directory's entries or a subprocess's output enters the
// process, by import path. A stat is not one: it reads no content.
var dataEntryFuncs = map[string][]string{
	"os":            {"ReadFile", "Open", "OpenFile", "ReadDir", "Readlink", "DirFS", "OpenRoot", "OpenInRoot", "CopyFS", "Pipe", "CreateTemp", "Create", "NewFile"},
	"syscall":       {"Open", "Openat", "Read", "Pread", "ReadDirent", "Getdents"},
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
	// Reads are the entries the function uses, sorted, as the pin names
	// them: all of them, and no other.
	Reads []string
}

var workerDataEntries = map[string]workerBound{
	// The bounded helpers themselves: what every other entry goes through.
	"startSweptCommand":    {Bound: "bounded: holds nothing of the output; it makes the pipe and hands its read end to the caller, and every caller is an entry of this list for what it takes from it", Reads: []string{"os.Pipe", "os/exec.Cmd.Stdout (assigned)"}},
	"boundedLines":         {Bound: "bounded: one line at a time, at most boundedLineBytes (1 MiB), stopping the subprocess past the caller's byte bound; nothing of the output is kept by it", Via: []string{"startSweptCommand", "stderrCapture"}, Reads: []string{"bufio.NewScanner", "startSweptCommand (its stream)"}},
	"readToolOutputWithin": {Bound: "bounded: a file no longer than the caller's bound, measured before it is read, into one buffer of that size; refused past it (readMeasured)", Via: []string{"readMeasured"}, Reads: []string{"os.Open"}},
	"walkBounded":          {Bound: "bounded: walkChunk (1,024) directory entries at a time, never a directory's whole listing, and at most the caller's count of entries in all", Reads: []string{"os.File.ReadDir", "os.Open"}},
	"newLockfileSpool":     {Bound: "bounded: creates the worker's own temporary file and reads nothing; what is written to it is bounded where it is written (lockfileWireFactor times the lockfiles' size on disk)", Reads: []string{"os.CreateTemp"}},

	// The reader child's output.
	"lockfileSpool.each": {Bound: "NOT BOUNDED by a number of its own: it reads the spool back one lockfile at a time, and holds that one lockfile's parsed entries and edges, which are as many as the child's memory limit let it parse (measured at the 1,024 MiB default before 0.29.216: 556 MiB for a 60 MB pnpm-lock.yaml; the largest pnpm-lock.yaml read is now 22 MiB). Grows with the largest lockfile; chunk 3", Reads: []string{"bufio.NewReader"}},
	"runManifestChild":   {Bound: "NOT BOUNDED by a number of its own: each frame is at most manifestFrameLimit, checked on arrival, and the lockfiles go to the spool, but the manifest NAMES and the DECLARATIONS are held whole by the worker until the analysis ends, as many as the child's memory limit let it read (measured at the default: 507 MiB of names, 347 MiB of declarations). Grows with the count of manifests and declarations; chunk 3", Via: []string{"cappedBuffer"}, Reads: []string{"bufio.NewReader", "os.Pipe", "startSweptCommand (its stream)"}},

	// Subprocess output decided by a repository.
	"AnalysisCollector.scanSCC":        {Bound: "bounded: scc's report as a stream, a file's record at a time; one row a file is held until the snapshot is written, at most sccLaborRowsLimit (cloneWalkEntries, 5,000,000) rows, about 1 GiB at the 215 bytes a row measured; past it the scan fails and nothing is written. PROVISIONAL", Via: []string{"streamSCCLabor", "var sccLaborRowsLimit"}, Reads: []string{"startSweptCommand (its stream)"}},
	"FacadeCollector.parseGitLog":      {Bound: "NOT BOUNDED in bytes: git log as a stream, a line at a time (at most 10 MiB a line), parsed into commits that are flushed 500 at a time; a commit holds a row for every file it touched, and nothing bounds the files of one commit or of 500 (measured: 963 MiB for one batch). Grows with the files changed per commit; chunk 3", Via: []string{"startSweptCommand", "stderrCapture"}, Reads: []string{"bufio.NewScanner", "startSweptCommand (its stream)"}},
	"parseWhitespaceLog":               {Bound: "bounded: git log as a stream through a 1 MiB reader, a record at a time; counts are kept, not the text", Reads: []string{"bufio.NewReaderSize"}},
	"CommitResolver.loadDefaultBranch": {Bound: "bounded: git rev-list as a stream, a hash a line; only the hashes this run asked about are kept, and those are the resolver's own batch", Via: []string{"startSweptCommand", "stderrCapture"}, Reads: []string{"bufio.NewScanner", "startSweptCommand (its stream)"}},
	"ScancodeWorker.executeScan":       {Bound: "bounded: scancode's stdout and stderr go to a tail and a head-and-tail buffer of fixed sizes through io.MultiWriter; its report goes to a file, read by decodeScancodeReport under scancodeOutputMaxBytes (256 MiB, held about two and a half times over while it is ingested; PROVISIONAL, from an estimate)", Via: []string{"tailBuffer", "headTailBuffer"}, Reads: []string{"os/exec.Cmd.Stderr (assigned)", "os/exec.Cmd.Stdout (assigned)"}},

	// Subprocess output of a fixed shape, whatever the repository holds.
	"FacadeCollector.runWhitespaceWalk": {Bound: "bounded: cmd.Output of `git rev-parse <branch>`, one object name; the walk itself is a stream handed to parseWhitespaceLog", Via: []string{"parseWhitespaceLog"}, Reads: []string{"os/exec.Cmd.Output", "startSweptCommand (its stream)"}},
	"FacadeCollector.syncDefaultBranch": {Bound: "bounded: cmd.Output of `git ls-remote --symref origin HEAD` and `git symbolic-ref HEAD`; git prints the one ref asked for, whatever the remote advertises", Reads: []string{"os/exec.Cmd.Output"}},
	"defaultBranchIsEmpty":              {Bound: "bounded: cmd.Output of `git rev-parse --verify --quiet`, at most one object name", Reads: []string{"os/exec.Cmd.Output"}},
	"resolveDefaultBranch":              {Bound: "bounded: cmd.Output of `git symbolic-ref HEAD`, one ref name", Reads: []string{"os/exec.Cmd.Output"}},
	"FacadeCollector.ensureClone":       {Bound: "bounded: the bare clone's own `config` file, which git wrote on this host when it cloned (not a file of the repository's tree), and the output of `git remote set-url`, which prints nothing on success", Reads: []string{"os.ReadFile", "os/exec.Cmd.CombinedOutput"}},

	// This host's files and tools, not a repository's.
	"goEnvValues":               {Bound: "bounded: cmd.Output of `go env` for a fixed list of variables, this host's toolchain settings", Reads: []string{"os/exec.Cmd.Output"}},
	"readToolCheckTimestamp":    {Bound: "bounded: a timestamp file of this host that this program wrote (~/.aveloxis-tool-check)", Reads: []string{"os.ReadFile"}},
	"ensurePythonUserBinOnPath": {Bound: "bounded: the operator's shell profile on this host and `python3 -m site --user-base`, during install-tools", Reads: []string{"os.OpenFile", "os.ReadFile", "os/exec.Cmd.Output"}},
	"typecodePairInVenv":        {Bound: "bounded: globs scancode's own Python virtualenv on this host for its libmagic files", Reads: []string{"filepath.Glob"}},
	"sweepScancodeDir":          {Bound: "bounded: lists the directory of scancode's working clones, one entry for a repository being scanned, at most the scancode worker count", Reads: []string{"os.ReadDir"}},
	"runLoggedCommand":          {Bound: "bounded: the output of a remediation command on this host (pip, brew, apt) during scancode's self-repair, a package manager's log and not a repository's", Reads: []string{"os/exec.Cmd.CombinedOutput"}},
	"installScorecardBinary":    {Bound: "bounded: creates a temporary file on this host for the scorecard release it downloads during install-tools; nothing of a repository's", Reads: []string{"os.CreateTemp"}},

	// The network, bounded where it is read.
	"fetchRegistryJSON": {Bound: "bounded: a registry's answer through io.LimitReader at the caller's byte limit, plus one byte to tell that it was passed", Reads: []string{"io.ReadAll"}},
	"probeSCCVersion":   {Bound: "bounded: scc --version through io.LimitReader at sccVersionOutputMaxBytes", Via: []string{"startSweptCommand"}, Reads: []string{"io.ReadAll", "startSweptCommand (its stream)"}},
}

// workerHoldsOutsideThePin are the named places where a worker holds an
// amount the repository decides and that this pin CANNOT see, because the
// data does not enter by a file, a directory or a subprocess: rows read
// back from the database, and bodies read from the network by another
// package. They are listed so that the claim at the top of this file is
// read with them; each must exist (a rename fails the test), and each says
// what bounds it or that nothing does.
var workerHoldsOutsideThePin = []struct{ Dir, Func, Holds string }{
	{"internal/collector", "func GenerateSBOMWithOptions(", "NOT BOUNDED in bytes: every stored lockfile package and edge of the repository read back whole, and the document built from them; the rows are at most db.LockfileSnapshotRows (2,000,000) for a snapshot written since 0.29.216, at which an SPDX document allocates about 3 GiB (measured 1.6 GiB at 1,000,000 rows). Grows with the stored rows; chunk 3"},
	{"internal/scantarget", "func Build(", "NOT BOUNDED in bytes: the repository's declared dependencies and stored lockfile packages read whole to make the vulnerability scan's targets (measured: 396 MiB for 240 MB of lockfiles); at most db.LockfileSnapshotRows lockfile rows since 0.29.216. Grows with the stored rows; chunk 3"},
	{"internal/collector", "func (ac *AnalysisCollector) storedLockfileRows(", "NOT BOUNDED in bytes: the stored lockfile packages of the repository read whole when its lockfiles went unread; at most db.LockfileSnapshotRows rows since 0.29.216. Grows with the stored rows; chunk 3"},
	{"internal/collector", "func (ac *AnalysisCollector) scanLockfiles(", "NOT BOUNDED in bytes: the previous rows of the lockfiles that failed this cycle (the carry) and the Go closure are held whole and handed to the store in one piece; the carry is at most db.LockfileSnapshotRows rows since 0.29.216 (measured before it: 665 MiB), the closure is bounded by goModHeldBytes and goModGraphLines. Grows with the stored rows; chunk 3"},
	{"internal/collector", "func (ac *AnalysisCollector) scanGoModGraph(", "bounded: the Go module expansion's lines, at most goModGraphLines (2,000,000) lines and goModHeldBytes (128 MiB) of their text for one repository over all its modules and both commands, a line at most goModLineBytes (8 KiB); about 480 MiB held at both bounds. PROVISIONAL"},
	{"internal/platform", "func (c *HTTPClient) GraphQLAt(", "NOT BOUNDED by this program: a forge's answer to one GraphQL request is read whole (io.ReadAll); its size is the forge's page, which the query's own page sizes decide. Not a repository's file; listed because its size is not this process's to choose"},
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
				// The package's own way of starting a subprocess hands
				// back its standard output as a stream (round 9: scanSCC
				// decoded one whole and was not seen).
				if o.Pkg().Path() == collectorImportPath && o.Name() == "startSweptCommand" {
					return "startSweptCommand (its stream)", true
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
			// A file is a bounded sink only when it is this process's own
			// standard output or error (install-tools, to the terminal). A
			// pipe or a temporary file is read back by someone (round 9).
			if sel, ok := e.(*ast.SelectorExpr); ok && named.Obj().Pkg().Path() == "os" && named.Obj().Name() == "File" {
				if v, ok := info.Uses[sel.Sel].(*types.Var); ok && v.Pkg() != nil && v.Pkg().Path() == "os" && (v.Name() == "Stdout" || v.Name() == "Stderr") {
					return true
				}
			}
			return false
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
		// The registration answers for the reads it NAMES: a read added to
		// a registered function is a new read (round 9: a registered
		// function was exempt whole, whatever it came to take).
		sort.Strings(f.takes)
		if got := compactStrings(f.takes); strings.Join(got, "; ") != strings.Join(reg.Reads, "; ") {
			bad = append(bad, n+" is registered for the reads ["+strings.Join(reg.Reads, "; ")+"] and makes ["+strings.Join(got, "; ")+"]")
		}
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

func TestWhatAWorkerTakesOfARepositoryIsRegisteredWithItsBound(t *testing.T) {
	bad, stale, takers := workerDataEntryReport(t, nil)
	for _, b := range bad {
		t.Errorf("%s, and is reachable in the worker. What a repository's files say is read by the child (manifest_child.go); a subprocess's output and a tool's report are taken through the bounded helpers (bounded_output.go). Move it, or register the function in workerDataEntries with its bound and the helpers that enforce it", b)
	}
	for _, s := range stale {
		t.Errorf("workerDataEntries lists %s, which takes no such data in the worker now: delete the entry", s)
	}
	// Each registration says one of two things, and says it at length; the
	// same for the places the pin cannot see, each of which must exist.
	said := func(where, text string) {
		t.Helper()
		bounded, not := strings.HasPrefix(text, "bounded: "), strings.HasPrefix(text, "NOT BOUNDED")
		if (!bounded && !not) || len(text) < 60 {
			t.Errorf("%s does not say whether what it holds is bounded: it begins \"bounded: \" and gives the number and where it is enforced, or begins \"NOT BOUNDED\" and says what the amount grows with", where)
		}
		if not && !strings.Contains(text, "Grows with") && !strings.Contains(text, "not this process's to choose") {
			t.Errorf("%s says NOT BOUNDED and not what the amount grows with", where)
		}
	}
	unbounded := 0
	for n, reg := range workerDataEntries {
		said("workerDataEntries["+n+"]", reg.Bound)
		if strings.HasPrefix(reg.Bound, "NOT BOUNDED") {
			unbounded++
		}
	}
	for _, h := range workerHoldsOutsideThePin {
		said(h.Dir+" "+h.Func, h.Holds)
		_ = srctest.PackageFunc(t, h.Dir, h.Func) // fails the test when the function is gone
	}
	// The count of places that say NOT BOUNDED only goes down: chunk 3
	// bounds them, and a new one is a decision, not a registration.
	if unbounded != 3 {
		t.Errorf("%d registered functions say NOT BOUNDED; there were 3 when the claim was restated (lockfileSpool.each, runManifestChild, FacadeCollector.parseGitLog). Fewer: lower this number. More: bound the new one", unbounded)
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

// The spellings round 8's reviewer got past the pin's predecessor, and the
// six round 9's got past the pin (the package's own swept pipe decoded or
// copied whole, a pipe made by hand, a temporary file read back, a decoder
// over a file, a read by system call), each a worker function that reads a
// file of the clone or takes a subprocess's output whole. Each must be
// reported.
func TestThePinSeesEverySpellingOfARead(t *testing.T) {
	dir := filepath.Join(srctest.Root(t), "internal", "collector", "testdata", "pin_evasions")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	// All of them in ONE reading of the package (each declares its own
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
	if seen < 12 {
		t.Fatalf("%d evasion fixtures read: there are twelve", seen)
	}
}

// isIdentByte: c can continue a Go identifier (ASCII is all these names use).
func isIdentByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
