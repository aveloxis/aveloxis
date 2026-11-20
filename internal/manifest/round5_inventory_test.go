// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// Review round 5, the memory rule's pin. Five rounds each found another
// structure that a walk kept and nothing counted. The rule is now that
// EVERY field of a struct that lives as long as a walk, and that can hold
// memory (a map, a slice, a pointer, a string), is in the table below with
// how it is bounded: registered with the walk's account (walkAccount.keep),
// or bounded by something stated. The structs are found, not listed: Walk,
// and every struct of this package reachable from it through a field. A new
// field, or a new struct behind one, fails here until it is classified.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// walkFieldInventory: "Type.field" -> how what it holds is bounded.
// "accounted:" names where it is registered with the walk's account;
// "bounded:" says by what, and where that is shown.
var walkFieldInventory = map[string]string{
	// --- the walk
	"Walk.acct":   "bounded: one walkAccount, of fixed size",
	"Walk.cargo":  "bounded: one cargoWorkspaceIndex; its fields below",
	"Walk.gradle": "bounded: one gradleTree; its fields below",
	"Walk.pom":    "bounded: one pomTree; its fields below",

	"walkAccount.logger": "bounded: the caller's logger",
	"walkAccount.now":    "bounded: a function value",

	// --- the Cargo workspace index. Not a build-tree reader: its files are
	// not in the account's bytes read, and nothing here is a product. Each
	// structure is linear in the Cargo.toml files of the directories above
	// the manifests read, each opened once (seen).
	"cargoWorkspaceIndex.byDir":      "bounded: one table per directory, of that directory's own Cargo.toml [workspace.dependencies] (an entry per line of it), read once per walk (workspaceAt, seen)",
	"cargoWorkspaceIndex.logger":     "bounded: the caller's logger",
	"cargoWorkspaceIndex.root":       "bounded: the clone's path",
	"cargoWorkspaceIndex.seen":       "bounded: one flag per directory on the path above a Cargo.toml that was read",
	"cargoWorkspaceIndex.unreadable": "bounded: one flag per such directory",
	"tomlDepEntry.Name":              "bounded: a part of the line of the Cargo.toml it was read from (cargoWorkspaceIndex.byDir)",
	"tomlDepEntry.Raw":               "bounded: as Name",
	"tomlDepEntry.Section":           "bounded: as Name",
	"tomlDepEntry.Version":           "bounded: as Name",

	// --- the Maven tree
	"pomTree.acct":          "bounded: the walk's one account",
	"pomTree.logger":        "bounded: the caller's logger",
	"pomTree.remote":        "bounded: the caller's source of poms",
	"pomTree.root":          "bounded: the clone's path",
	"pomTree.work":          "bounded: one counter",
	"pomTree.models":        "accounted: pomTree.model registers pomModel.bytes and the path, twice, before the model is kept; refused, the pom is not kept and is named in the log; a nil entry stays for a path that could not be read, which is a pom.xml of the clone or a path a pom of the clone names as its parent or module, built from that pom's own text",
	"pomTree.noRoom":        "bounded: one flag for a pom that was read and not kept; its key is that pom's path, which the walk read from a directory",
	"pomTree.pomFiles":      "accounted: the path of each pom.xml of the clone, registered in pomTree.model (the second of its two path charges); a pom that is not kept leaves its path, which is a path the walk read from a directory",
	"pomTree.byCoord":       "accounted: indexTree registers each key (the pom's interpolated coordinates) and its entry; refused, the pom is not indexed by coordinates and is named in the log",
	"pomTree.contexts":      "accounted: pomTree.context registers pomContextBytes and the chain's pointers; refused, the effective model is built again for each question",
	"pomTree.remoteAnswers": "accounted: pomTree.remotePom registers the key and, for a pom obtained, pomModel.bytes; refused, a negative answer is asked for again (a counted request) and a pom obtained is reported as a failed lookup, by name",
	"remoteAnswer.model":    "accounted: with its answer (pomTree.remoteAnswers)",

	"pomModel.path":         "accounted: pomModel.bytes",
	"pomModel.group":        "accounted: pomModel.bytes",
	"pomModel.artifact":     "accounted: pomModel.bytes",
	"pomModel.version":      "accounted: pomModel.bytes",
	"pomModel.servedBy":     "accounted: pomModel.bytes",
	"pomModel.parent":       "accounted: pomModel.bytes (pomModelBytes holds the struct, the strings are added)",
	"pomModel.props":        "accounted: pomModel.bytes (memStringMap)",
	"pomModel.managed":      "accounted: pomModel.bytes (pomEntryBytes and the coordinates of each entry)",
	"pomModel.modules":      "accounted: pomModel.bytes (memStrings)",
	"pomModel.repositories": "accounted: pomModel.bytes (five string headers and the address of each)",
	"pomModel.fetchable":    "accounted: pomModel.bytes (the fifth header of each repository; the addresses are the repositories' own strings)",
	"pomModel.idx":          "accounted: pomModel.bytes registers the index with the model, before it is built: pomEntryBytes holds an entry's place in it and twice the entry's coordinates its key",

	"pomParentRef.group":        "accounted: pomModel.bytes",
	"pomParentRef.artifact":     "accounted: pomModel.bytes",
	"pomParentRef.version":      "accounted: pomModel.bytes",
	"pomParentRef.relativePath": "accounted: pomModel.bytes",
	"pomManaged.group":          "accounted: pomModel.bytes",
	"pomManaged.artifact":       "accounted: pomModel.bytes",
	"pomManaged.version":        "accounted: pomModel.bytes",

	"DeclaredRepository.URL":          "accounted: pomModel.bytes for a pom's; gradleTree.noteRepositories for a Gradle file's",
	"DeclaredRepository.ManifestPath": "accounted: as URL (empty in what a tree keeps; set in the list handed to the caller)",
	"DeclaredRepository.Kind":         "bounded: one of two constants",
	"DeclaredRepository.NotFetched":   "bounded: one of the NotFetched constants",

	"pomIndex.plain":         "accounted: pomModel.bytes (pomEntryBytes and the key, per entry)",
	"pomIndex.dynamic":       "accounted: pomModel.bytes (a word per entry, in pomEntryBytes)",
	"pomIndex.dynByArtifact": "accounted: pomModel.bytes (a word and a key per entry)",
	"pomIndex.dynByGroup":    "accounted: pomModel.bytes (a word and a key per entry)",
	"pomIndex.dynBoth":       "accounted: pomModel.bytes (a word per entry)",
	"pomIndex.imports":       "accounted: pomModel.bytes (a word per import)",
	"pomIndex.importKeys":    "accounted: pomModel.bytes (a header and the coordinates per import)",
	"pomIndex.live":          "accounted: pomModel.bytes (a word per import; compact only shortens it)",
	"pomIndex.dead":          "accounted: pomModel.bytes (a byte per import)",
	"pomIndex.deadReason":    "accounted: pomModel.bytes (a header per import; the reasons are constants)",
	"pomIndex.gapBefore":     "accounted: pomModel.bytes (a header per import; the reasons are constants)",

	"pomContext.tree":         "bounded: the tree",
	"pomContext.chain":        "accounted: pomTree.context (a word for each pom of the chain, at most pomChainLimit)",
	"pomContext.group":        "bounded: a string of the model's or of its parent reference, by reference",
	"pomContext.artifact":     "bounded: the model's string, by reference",
	"pomContext.version":      "bounded: as group",
	"pomContext.parentGroup":  "bounded: the parent reference's string, by reference",
	"pomContext.parentVer":    "bounded: the parent reference's string, by reference",
	"pomContext.chainGap":     "bounded: one of the reason constants",
	"pomContext.declaredList": "accounted: the declaring pom's own list by reference (pomModel.fetchable), or a merged list registered in pomContext.declaredFor; refused, it is written out again for each request, which is charged",

	// --- the Gradle tree
	"gradleTree.acct":         "bounded: the walk's one account",
	"gradleTree.logger":       "bounded: the caller's logger",
	"gradleTree.pom":          "bounded: the walk's Maven tree",
	"gradleTree.root":         "bounded: the clone's path",
	"gradleTree.work":         "bounded: one counter",
	"gradleTree.dirs":         "accounted: gradleTree.keepKnown registers gradleKnownBytes, the directory's path and what its files added; refused, a tombstone of gradleKnownBytes stays for a directory on the path of a file that was read, and the build files under it are refused by name",
	"gradleTree.belows":       "accounted: gradleTree.keepKnown, as dirs",
	"gradleTree.repos":        "accounted: gradleTree.noteRepositories; refused, the file's repositories are not recorded and the file is named in the log",
	"gradleTree.memo":         "accounted: gradleTree.readApplied registers each reading kept, inside the memo's own allowance (memoLimit); refused, the reading is made again, which is charged",
	"gradleTree.bundles":      "accounted: gradleTree.keepBundle registers each run kept, inside the memo's allowance; refused, the run is built again, which is charged",
	"gradleTree.toks":         "accounted: gradleTree.scan registers a file's tokens and text; refused, the file is tokenised again for each reading, which is charged its bytes",
	"gradleTree.applying":     "bounded: the path of the script being read, by reference",
	"gradleTree.counted":      "bounded: one flag and path per Gradle file read; the path's bytes are in the account's bytes read and walkMemPerFile is for such entries",
	"gradleTree.pastBound":    "bounded: one flag and path per Gradle file read, as counted",
	"gradleTree.readCost":     "bounded: one number and path per applied script read, as counted",
	"gradleTree.roots":        "bounded: one entry per directory an apply statement was resolved from, a directory of a file that was read; the value is another such directory's path",
	"gradleTree.unreadParts":  "bounded: a string header for each time a file was found unread: once per reading of a file that depends on it, each of which is an apply statement or a file that was read",
	"gradleTree.unreadWarned": "bounded: one flag per file found unread",
	"gradleWalkWork.acct":     "bounded: the walk's one account",

	"gradleKnown.vars":      "accounted: gradleTree.keepKnown (the layers this directory's files added; the rest is the directory above's, by reference)",
	"gradleKnown.libs":      "accounted: gradleTree.keepKnown",
	"gradleKnown.platforms": "accounted: gradleTree.keepKnown",
	"gradleKnown.shared":    "accounted: gradleTree.keepKnown",
	"gradleKnown.unread":    "bounded: the path of a file or directory of the walk, by reference",

	"gradleVars.own":    "accounted: gradleTree.keepKnown for a layer a directory keeps; gradleTree.keepBundle for a layer of a kept run; a layer in neither is dropped with the reading of the file it was built for",
	"gradleVars.gone":   "accounted: as own",
	"gradleVars.parent": "accounted: as own (four words a layer)",
	"gradleVars.seg":    "accounted: gradleTree.keepBundle (the run a link stands for is a kept run)",
	"gradleLibs.own":    "accounted: gradleTree.keepKnown",
	"gradleLibs.parent": "accounted: gradleTree.keepKnown",
	"gradleList.before": "accounted: gradleTree.keepKnown",
	"gradleList.after":  "accounted: gradleTree.keepKnown",
	"gradleList.parent": "accounted: gradleTree.keepKnown",

	"gradleDecl.config":   "accounted: gradleTree.keepKnown (gradleDeclBytes; a part of the file's text, or a constant)",
	"gradleDecl.group":    "accounted: gradleTree.keepKnown",
	"gradleDecl.artifact": "accounted: gradleTree.keepKnown",
	"gradleDecl.version":  "accounted: gradleTree.keepKnown",
	"gradleDecl.written":  "accounted: gradleTree.keepKnown",
	"gradleDecl.source":   "bounded: empty in a declaration a tree keeps (set on a file's own declarations when a platform answers, which are the caller's)",
	"gradleDecl.hosts":    "bounded: as source",
	"gradleDecl.reason":   "bounded: as source",

	"gradleMemo.reads":   "accounted: gradleTree.readApplied (the names and values, with gradleMemoEntryBytes)",
	"gradleMemo.own":     "accounted: gradleTree.readApplied",
	"gradleMemo.gone":    "accounted: gradleTree.readApplied",
	"gradleMemo.applies": "accounted: gradleTree.readApplied",
	"gradleRead.value":   "accounted: with the reading or run that holds it (readApplied, keepBundle)",

	"gradleBundle.reads":   "accounted: gradleTree.keepBundle",
	"gradleBundle.seg":     "accounted: gradleTree.keepBundle (gradleBundleLinkBytes a link, and each link's assignments)",
	"gradleBundle.scripts": "accounted: gradleTree.keepBundle (gradleMemoRefBytes a script)",
	"gradleBundle.has":     "accounted: gradleTree.keepBundle (gradleMemoRefBytes a script)",

	"gradleKeptTokens.toks": "accounted: gradleTree.scan (gradleTokBytes a token)",
	"gradleTok.text":        "accounted: gradleTree.scan (a part of the file's text, which is registered whole)",
}

// walkStructs parses the package and returns its struct types by name.
func walkStructs(t *testing.T) map[string]*ast.StructType {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*ast.StructType{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if ts, ok := n.(*ast.TypeSpec); ok {
				if st, ok := ts.Type.(*ast.StructType); ok {
					out[ts.Name.Name] = st
				}
			}
			return true
		})
	}
	return out
}

// holdsMemory reports whether a field of this type can hold memory of its
// own: a map, a slice, a pointer, a string, a function or an interface.
func holdsMemory(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.MapType, *ast.StarExpr, *ast.FuncType, *ast.InterfaceType, *ast.ChanType:
		return true
	case *ast.ArrayType:
		return true
	case *ast.Ident:
		return x.Name == "string" || x.Name == "PomSource"
	}
	return false
}

func TestEveryWalkLifetimeFieldIsAccounted(t *testing.T) {
	structs := walkStructs(t)
	if structs["Walk"] == nil {
		t.Fatal("the Walk struct was not found: the inventory has no root")
	}
	reached := map[string]bool{}
	var visit func(name string)
	visit = func(name string) {
		st := structs[name]
		if st == nil || reached[name] {
			return
		}
		reached[name] = true
		for _, f := range st.Fields.List {
			ast.Inspect(f.Type, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					visit(id.Name)
				}
				return true
			})
		}
	}
	visit("Walk")
	found := map[string]bool{}
	var missing []string
	fields := 0
	for name := range reached {
		for _, f := range structs[name].Fields.List {
			if !holdsMemory(f.Type) {
				// A struct held by value is walked into above; a number or
				// a flag holds nothing.
				continue
			}
			names := f.Names
			if len(names) == 0 {
				names = []*ast.Ident{{Name: "(embedded)"}}
			}
			for _, n := range names {
				fields++
				key := name + "." + n.Name
				found[key] = true
				how := walkFieldInventory[key]
				if !strings.HasPrefix(how, "accounted: ") && !strings.HasPrefix(how, "bounded: ") {
					missing = append(missing, key)
				}
			}
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("%d fields of walk-lifetime structs are not classified in walkFieldInventory (accounted: <where> or bounded: <by what>):\n%s", len(missing), strings.Join(missing, "\n"))
	}
	for key := range walkFieldInventory {
		if !found[key] {
			t.Errorf("walkFieldInventory lists %s, which is not a field of a walk-lifetime struct any more: remove it", key)
		}
	}
	// The denominator: the walk reaches structs and fields at all.
	if len(reached) < 15 || fields < 60 {
		t.Errorf("only %d structs and %d fields were examined: the walk over the package's types is not finding them", len(reached), fields)
	}
	t.Logf("%d structs reachable from Walk, %d fields that can hold memory, all classified", len(reached), fields)
}
