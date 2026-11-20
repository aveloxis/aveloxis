// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"bytes"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 3 of the vulnerability program's first part. Two rounds had
// bounded each FILE; the cost that was left is a product across files: what
// is built per file from what files share. The rule since 0.29.210: a
// walk's work and what it retains are linear in the bytes it reads.
// scaling_test.go measures that; these pin the mechanisms and the answers.

// What is known from outside a file is a chain of layers, looked up nearest
// first, with the meaning the copied maps had.
func TestGradleVarsAreLayersWithTheMeaningOfTheCopies(t *testing.T) {
	var base *gradleVars
	if _, ok, layers := base.get("x"); ok || layers != 0 {
		t.Error("nothing known: a name was found, or a layer counted")
	}
	one := base.push(map[string]string{"a": "1", "b": "1"}, nil)
	if one.push(nil, nil) != one {
		t.Error("a source that assigns nothing added a layer")
	}
	// A nearer layer overrides; a name its source left with two values is
	// not known below it, whatever a layer above says ...
	two := one.push(map[string]string{"a": "2"}, map[string]bool{"b": true})
	for name, want := range map[string]string{"a": "2", "b": "", "c": ""} {
		v, ok, _ := two.get(name)
		if v != want || ok != (want != "") {
			t.Errorf("%s = %q (%v), want %q", name, v, ok, want)
		}
	}
	// ... until a nearer source assigns it again.
	three := two.push(map[string]string{"b": "3"}, nil)
	if v, ok, layers := three.get("b"); v != "3" || !ok || layers != 1 {
		t.Errorf("b after a later assignment = %q (%v) in %d layers", v, ok, layers)
	}
	if _, _, layers := three.get("zz"); layers != 3 {
		t.Errorf("a name nobody sets was looked for in %d layers, want all 3", layers)
	}
	// The layers above are untouched: every directory shares them.
	if v, ok, _ := one.get("b"); v != "1" || !ok {
		t.Error("a layer in front changed the one behind it")
	}

	// What a file leaves below it: its assignments, less the names it
	// assigned twice differently; nothing when it was not read whole.
	b := readGradleBuild("ext.a = '1'\next.c = '1'\next.c = '2'\n", one)
	own, gone := b.result()
	if !reflect.DeepEqual(own, map[string]string{"a": "1"}) || !reflect.DeepEqual(gone, map[string]bool{"c": true}) {
		t.Errorf("result = %v, %v", own, gone)
	}
	b.overBound = true
	if own, gone := b.result(); own != nil || gone != nil {
		t.Error("a file not read whole left assignments")
	}
}

// The lists a build shares with the projects below it are linked, not
// copied, and read out in the order the copies had.
func TestGradleListKeepsItsOrder(t *testing.T) {
	var l *gradleList[string]
	if l.len() != 0 || l.flat() != nil {
		t.Error("an empty list is not empty")
	}
	root := l.with(nil, []string{"settings"})                   // a settings file's: after
	sub := root.with([]string{"near1", "near2"}, nil)           // a build file above: in front
	deeper := sub.with([]string{"nearest"}, []string{"deeper"}) // both
	if got, want := deeper.flat(), []string{"nearest", "near1", "near2", "settings", "deeper"}; !reflect.DeepEqual(got, want) || deeper.len() != 5 {
		t.Errorf("flat = %v (len %d), want %v", got, deeper.len(), want)
	}
	if sub.with(nil, nil) != sub {
		t.Error("adding nothing made a link")
	}
	if got := root.flat(); !reflect.DeepEqual(got, []string{"settings"}) {
		t.Errorf("a link below changed the list above: %v", got)
	}
}

// A script that many projects apply is read once for all of them, when what
// it takes from outside is the same for each.
func TestAScriptAppliedByManyProjectsIsReadOnce(t *testing.T) {
	const projects = 60
	files := map[string]string{
		"gradle.properties":      "base=1\n",
		"gradle/versions.gradle": "ext.v = \"${base}.5\"\next.w = '2.0'\n",
	}
	for i := 0; i < projects; i++ {
		// Each project has a variable of its own that the script does not
		// look at: their scopes differ, and it does not matter.
		files[fmt.Sprintf("p%d/gradle.properties", i)] = fmt.Sprintf("mine=%d\n", i)
		files[fmt.Sprintf("p%d/build.gradle", i)] = "apply from: \"$rootDir/gradle/versions.gradle\"\ndependencies { implementation \"g:a:$v\" }\n"
	}
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	for i := 0; i < projects; i++ {
		got := w.Read(filepath.Join(root, fmt.Sprintf("p%d/build.gradle", i)), Options{})
		if got.Failure != nil || len(got.Deps) != 1 || got.Deps[0].Version != "1.5" {
			t.Fatalf("project %d: %+v, failure %v", i, got.Deps, got.Failure)
		}
	}
	// Each build file is read twice (for what it applies, then for what it
	// declares) and the script once.
	if want := 2*projects + 1; w.gradle.fullReads != want {
		t.Errorf("%d readings for %d projects and one script; want %d: the script is read again for each project", w.gradle.fullReads, projects, want)
	}
	// A project whose scope answers differently for a name the script
	// looks up gets its own reading, and its own answer.
	files["other/gradle.properties"] = "base=7\n"
	files["other/build.gradle"] = files["p0/build.gradle"]
	root2 := writeTree(t, files)
	w2 := NewWalk(root2, nil)
	w2.Read(filepath.Join(root2, "p0", "build.gradle"), Options{})
	got := w2.Read(filepath.Join(root2, "other", "build.gradle"), Options{})
	if got.Failure != nil || len(got.Deps) != 1 || got.Deps[0].Version != "7.5" {
		t.Errorf("a project with another value for what the script reads: %+v, failure %v; want 7.5", got.Deps, got.Failure)
	}
}

// A real-shaped multi-project build is far inside the walk's allowance,
// with the case the memo cannot help in: three hundred modules that each
// apply one publishing script which reads a property every module sets
// differently, so the script is read for each of them.
func TestARealShapedGradleBuildIsFarFromTheWalksAllowance(t *testing.T) {
	const modules = 300
	var script strings.Builder
	script.WriteString("group = GROUP\nversion = VERSION_NAME\next.artifact = \"$POM_ARTIFACT_ID\"\n")
	for script.Len() < 5<<10 { // a publishing script of 5 KB
		fmt.Fprintf(&script, "ext.pomField%d = 'value %d'\n", script.Len(), script.Len())
	}
	files := map[string]string{
		"settings.gradle":           "rootProject.name = 'big'\n",
		"gradle.properties":         "GROUP=com.example\nVERSION_NAME=1.2.3\nkotlin=1.9.0\n",
		"build.gradle":              "subprojects {\n repositories { mavenCentral() }\n}\n",
		"gradle/mvn-publish.gradle": script.String(),
	}
	for i := 0; i < modules; i++ {
		files[fmt.Sprintf("m%d/gradle.properties", i)] = fmt.Sprintf("POM_ARTIFACT_ID=module-%d\nPOM_NAME=Module %d\n", i, i)
		files[fmt.Sprintf("m%d/build.gradle", i)] = "apply plugin: 'java-library'\napply from: rootProject.file('gradle/mvn-publish.gradle')\n" +
			"dependencies {\n  implementation \"org.jetbrains.kotlin:kotlin-stdlib:$kotlin\"\n  api 'com.google.guava:guava:33.0.0-jre'\n  testImplementation 'junit:junit:4.13.2'\n}\n"
	}
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	for i := 0; i < modules; i++ {
		got := w.Read(filepath.Join(root, fmt.Sprintf("m%d/build.gradle", i)), Options{})
		if got.Failure != nil || len(got.Deps) != 3 {
			t.Fatalf("module %d: %d dependencies, failure %v", i, len(got.Deps), got.Failure)
		}
	}
	used, limit := w.gradle.work.used, w.gradle.work.limit
	t.Logf("work %d of %d allowed; %d readings", used, limit, w.gradle.fullReads)
	if used*pomHonestMargin > limit {
		t.Errorf("a real-shaped build used %d of %d; the walk's allowance has no %d-fold margin", used, limit, pomHonestMargin)
	}
}

// The walk's allowance ends a walk whose files, each inside its own bound,
// cost a product: projects times a script that must be read for each. The
// projects past it are reported, by name; what was read before stands.
func TestTheWalksAllowanceEndsAProductAcrossFiles(t *testing.T) {
	const projects = 400
	var script strings.Builder
	script.WriteString("ext.name = \"lib-$POM_ARTIFACT\"\n")
	for i := 0; i < 3000; i++ {
		fmt.Fprintf(&script, "ext.v%d = '1.%d'\n", i, i)
	}
	files := map[string]string{"gradle/publish.gradle": script.String()}
	for i := 0; i < projects; i++ {
		files[fmt.Sprintf("p%d/gradle.properties", i)] = fmt.Sprintf("POM_ARTIFACT=a%d\n", i)
		files[fmt.Sprintf("p%d/build.gradle", i)] = "apply from: \"$rootDir/gradle/publish.gradle\"\ndependencies { implementation \"g:a:$v1\" }\n"
	}
	root := writeTree(t, files)
	var log bytes.Buffer
	w := NewWalk(root, slog.New(slog.NewTextHandler(&log, nil)))
	// The mechanism, at a size a test can run: the walk's own factor
	// (gradleWalkFactor) lets ten times as many readings through first.
	w.gradle.walkFactor = gradleWorkFactor
	read, refused := 0, 0
	for i := 0; i < projects; i++ {
		got := w.Read(filepath.Join(root, fmt.Sprintf("p%d/build.gradle", i)), Options{})
		switch {
		case got.Failure == nil && len(got.Deps) == 1 && got.Deps[0].Version == "1.1":
			// (One may be read after another was refused: every project
			// adds its own bytes to the allowance, and enough of them pay
			// for one more reading of the script. The sum stays inside.)
			read++
		case got.Failure != nil && len(got.Deps) == 0:
			refused++
		default:
			t.Fatalf("project %d: %+v, failure %v; want it read whole or refused", i, got.Deps, got.Failure)
		}
	}
	if read == 0 || refused == 0 {
		t.Fatalf("%d read, %d refused; the shape must pass the allowance part of the way", read, refused)
	}
	if w.gradle.work.used > w.gradle.work.limit+gradleWorkFactor*(script.Len()+1) {
		t.Errorf("work %d went past the allowance %d by more than one reading", w.gradle.work.used, w.gradle.work.limit)
	}
	// Past the allowance the script is not even opened: a project that is
	// refused costs its own two files and nothing of the script's size.
	if most := 2*projects + read + 2; w.gradle.opens > most {
		t.Errorf("%d files opened; %d projects of two files each and %d readings of the script are at most %d", w.gradle.opens, projects, read, most)
	}
	// What the memo of readings holds is bounded by the files read: every
	// project here reads the script differently, and the readings are not
	// all kept.
	if w.gradle.memoBytes > w.gradle.memoLimit || w.gradle.memoBytes == 0 {
		t.Errorf("the memo holds %d bytes of text; it may hold what the walk has read, %d", w.gradle.memoBytes, w.gradle.memoLimit)
	}
	// The script is named in the log once, however many projects it failed.
	if n := strings.Count(log.String(), "a file of the Gradle build could not be read within the reader's bounds"); n != 1 {
		t.Errorf("the script was reported %d times in the log, want once", n)
	}
	// And a reading of a file never costs the walk more than the file adds
	// to its allowance, so one file that uses its whole bound leaves the
	// others theirs.
	hostile := strings.Repeat("} a (", 50_000) + strings.Repeat(")", 50_000)
	shared := &gradleWalkWork{limit: gradleWorkFactor * (len(hostile) + 1)}
	if b := readGradleBuildWith(hostile, nil, nil, shared); !b.overBound || b.walkBound || shared.used > shared.limit {
		t.Errorf("a file past its own bound: overBound %v, walkBound %v, charged %d of the %d it added", b.overBound, b.walkBound, shared.used, shared.limit)
	}
}

// A pom's reasons do not depend on what was read before it (round 3): the
// effective model of a pom was cached with the reasons of every question
// ever put to it, so a pom read after another that imports it took that
// other's.
func TestAPomsReasonsDoNotDependOnReadOrder(t *testing.T) {
	files := map[string]string{
		// It manages g:pinned itself (at a version that cannot be worked
		// out) and imports a BOM that no repository has.
		"bom/pom.xml": `<project><groupId>g</groupId><artifactId>bom</artifactId><version>1</version><dependencyManagement><dependencies>
<dependency><groupId>g</groupId><artifactId>pinned</artifactId><version>${undefined.version}</version></dependency>
<dependency><groupId>ext</groupId><artifactId>missing-bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>
</dependencies></dependencyManagement><dependencies>
<dependency><groupId>g</groupId><artifactId>pinned</artifactId></dependency>
<dependency><groupId>g</groupId><artifactId>other</artifactId></dependency></dependencies></project>`,
		"app/pom.xml": `<project><groupId>g</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>
<dependency><groupId>g</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>
</dependencies></dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>elsewhere</artifactId></dependency></dependencies></project>`,
	}
	reasons := func(order ...string) map[string]string {
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		w.SetPomSource(&declaredRecorder{})
		out := map[string]string{}
		// What a question could not obtain is the question's. Nothing of
		// it may be left on a pom's effective model, which every later
		// file shares: both chains here are whole, and stay so.
		defer func() {
			for _, c := range w.pom.contexts {
				if c.chainGap != "" {
					t.Errorf("after reading %v, the effective model of %s carries the reason %q", order, c.chain[0].path, c.chainGap)
				}
			}
		}()
		for _, rel := range order {
			got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
			if rel != "bom/pom.xml" {
				continue
			}
			for _, d := range got.Deps {
				if !d.ManagedEntry {
					out[d.Name] = d.VersionReason
				}
			}
		}
		return out
	}
	alone, after := reasons("bom/pom.xml"), reasons("app/pom.xml", "bom/pom.xml")
	// g:pinned is answered by the pom's own entry before any import is
	// looked at: no pom that might have answered was missing. g:other is
	// looked for in the missing BOM.
	want := map[string]string{"g:pinned": "", "g:other": model.VersionReasonInheritedNotFound}
	if !reflect.DeepEqual(alone, want) || !reflect.DeepEqual(after, want) {
		t.Errorf("read alone %v; read after the pom that imports it %v; want %v both times", alone, after, want)
	}
}

// A context holds its chain and nothing that grows with what the chain
// manages; a pom's entries are indexed once, on the pom.
func TestAPomsEntriesAreIndexedOnceAndShared(t *testing.T) {
	var managed strings.Builder
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&managed, "<dependency><groupId>org.x</groupId><artifactId>a%d</artifactId><version>1.%d</version></dependency>", i, i)
	}
	// One entry whose group is a property the CHILD sets: what it manages
	// depends on the model it is read through. It is written before the
	// plain entry for the same coordinate, so it wins there.
	files := map[string]string{"pom.xml": `<project><groupId>g</groupId><artifactId>root</artifactId><version>1</version><properties><grp>org.none</grp></properties><dependencyManagement><dependencies>
<dependency><groupId>${grp}</groupId><artifactId>a7</artifactId><version>9.9</version></dependency>` + managed.String() + `</dependencies></dependencyManagement></project>`}
	child := func(name, props string) string {
		return `<project><parent><groupId>g</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>` + name + `</artifactId>` + props + `<dependencies><dependency><groupId>org.x</groupId><artifactId>a7</artifactId></dependency></dependencies></project>`
	}
	files["plain/pom.xml"] = child("plain", "")
	files["override/pom.xml"] = child("override", "<properties><grp>org.x</grp></properties>")
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	version := func(rel string) string {
		for _, d := range w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{}).Deps {
			if d.Name == "org.x:a7" && !d.ManagedEntry {
				return d.Version
			}
		}
		return "(absent)"
	}
	if got := version("plain/pom.xml"); got != "1.7" {
		t.Errorf("a child that leaves the property alone: org.x:a7 at %q, want 1.7", got)
	}
	if got := version("override/pom.xml"); got != "9.9" {
		t.Errorf("a child that sets the property: org.x:a7 at %q, want 9.9 (the entry written first, which now names it)", got)
	}
	parent := w.pom.model(filepath.Join(root, "pom.xml"))
	idx := parent.idx
	if idx == nil || len(idx.plain) != 500 || len(idx.dynamic) != 1 {
		t.Fatalf("the parent's index: %+v", idx)
	}
	for _, rel := range []string{"plain/pom.xml", "override/pom.xml"} {
		c := w.pom.context(w.pom.model(filepath.Join(root, filepath.FromSlash(rel))))
		if len(c.chain) != 2 || c.chain[1] != parent || c.chain[1].idx != idx {
			t.Errorf("%s: its context does not share the parent's model and index", rel)
		}
	}
	// The struct itself: a field that could hold a copy of what the chain
	// says (a map) would bring the product back.
	typ := reflect.TypeOf(pomContext{})
	for i := 0; i < typ.NumField(); i++ {
		if k := typ.Field(i).Type.Kind(); k == reflect.Map {
			t.Errorf("pomContext.%s is a map: an effective model holds its chain, not a copy of what the chain says", typ.Field(i).Name)
		}
	}
}

// A file's allowance for the pom tree's looks is its own bytes and those of
// the poms its questions reach. Each part is needed by a build that is not
// hostile (round 3: each could be deleted with the suite green).
func TestEachPartOfAFilesPomAllowanceIsNeeded(t *testing.T) {
	// (a) A pom whose BOMs are all FETCHED: 400 version-less dependencies
	// that none of 400 BOMs manages are 160,000 looks, more than the pom's
	// own 100 KB allows, and well inside what the BOMs' entries stand for.
	const k = 400
	var imports, deps strings.Builder
	for i := 0; i < k; i++ {
		fmt.Fprintf(&imports, "<dependency><groupId>far</groupId><artifactId>bom%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", i)
		fmt.Fprintf(&deps, "<dependency><groupId>x</groupId><artifactId>d%d</artifactId></dependency>", i)
	}
	pom := pomOf("<dependencyManagement><dependencies>" + imports.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies>")
	if k*k < len(pom) {
		t.Fatalf("the premise: %d looks must outnumber the pom's %d bytes", k*k, len(pom))
	}
	root := writeTree(t, map[string]string{"pom.xml": pom})
	w := NewWalk(root, nil)
	w.SetPomSource(fetchedBOMs{entries: 100})
	if got := w.Read(filepath.Join(root, "pom.xml"), Options{}); got.Failure != nil {
		t.Errorf("a pom whose BOMs are all fetched was refused: %v (a fetched pom's entries must count toward the allowance, the file's and the walk's)", got.Failure)
	}

	// (b) A Gradle build file against platforms of the clone, each a few
	// bytes: its own bytes are what allow its looks.
	const d, p = 300, 20
	files := map[string]string{}
	var gradle strings.Builder
	gradle.WriteString("dependencies {\n")
	for i := 0; i < p; i++ {
		files[fmt.Sprintf("boms/%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>b</groupId><artifactId>b%d</artifactId><version>1</version></project>", i)
		fmt.Fprintf(&gradle, " implementation platform('b:b%d:1')\n", i)
	}
	for i := 0; i < d; i++ {
		fmt.Fprintf(&gradle, " implementation 'x:d%d'\n", i)
	}
	files["build.gradle"] = gradle.String() + "}\n"
	bomBytes := 0
	for rel, body := range files {
		if rel != "build.gradle" {
			bomBytes += len(body) + 1
		}
	}
	if d*p < bomBytes || d*p > len(files["build.gradle"]) {
		t.Fatalf("the premise: %d looks must outnumber the platforms' %d bytes and fit the build file's %d", d*p, bomBytes, len(files["build.gradle"]))
	}
	rootG := writeTree(t, files)
	if got := NewWalk(rootG, nil).Read(filepath.Join(rootG, "build.gradle"), Options{}); got.Failure != nil || len(got.Deps) != d+p {
		t.Errorf("a Gradle build of %d dependencies and %d platforms: %d rows, failure %v (the build file's own bytes must count toward its allowance, the file's and the walk's)", d, p, len(got.Deps), got.Failure)
	}
}

// fetchedBOMs is a PomSource that has every BOM asked for, each managing
// `entries` artifacts nobody asks about.
type fetchedBOMs struct{ entries int }

func (f fetchedBOMs) RemotePom(group, artifact, version string, _ []string) (*RemotePom, RemoteOutcome) {
	pom := &RemotePom{Group: group, Artifact: artifact, Version: version, ServedBy: "repo.example"}
	for i := 0; i < f.entries; i++ {
		pom.Managed = append(pom.Managed, RemoteManaged{Group: "managed", Artifact: fmt.Sprintf("%s-%d", artifact, i), Version: "1"})
	}
	return pom, RemoteFound
}

// The pass that decides which Maven trees are whole looks at every BOM
// import of every pom: the poms times the imports they inherit. It is
// counted against the walk, and past the allowance a tree is not known to
// be whole: its managed entries stay inventoried.
func TestTheWholeTreePassIsCountedAgainstTheWalk(t *testing.T) {
	const k = 1500
	files := map[string]string{}
	var imports strings.Builder
	for i := 0; i < k; i++ {
		files[fmt.Sprintf("b/%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>b</groupId><artifactId>b%d</artifactId><version>1</version></project>", i)
		fmt.Fprintf(&imports, "<dependency><groupId>b</groupId><artifactId>b%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", i)
	}
	files["pom.xml"] = `<project><groupId>g</groupId><artifactId>root</artifactId><version>1</version><dependencyManagement><dependencies>
<dependency><groupId>g</groupId><artifactId>managed</artifactId><version>1</version></dependency>` + imports.String() + `</dependencies></dependencyManagement></project>`
	for i := 0; i < k; i++ {
		files[fmt.Sprintf("m%d/pom.xml", i)] = fmt.Sprintf(`<project><parent><groupId>g</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m%d</artifactId></project>`, i)
	}
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	entry := model.LibyearDep{Name: "g:managed", Version: "1", Manager: "maven", ManagedEntry: true, ManifestPath: "pom.xml"}
	res := w.SettleMavenManaged([]model.LibyearDep{entry})
	if w.pom.work.used > w.pom.work.limit {
		t.Errorf("the pass used %d looks of %d allowed", w.pom.work.used, w.pom.work.limit)
	}
	if k*k <= w.pom.work.limit {
		t.Fatalf("the premise: %d poms times %d imports must pass the allowance of %d", k, k, w.pom.work.limit)
	}
	if len(res.Kept) != 1 || len(res.Unused) != 0 || len(res.NotWhole) != 1 || !strings.Contains(res.NotWhole[0].Reason, "bound") {
		t.Errorf("kept %d, unused %d, not whole %+v; want the entry kept and its tree not whole for the bound", len(res.Kept), len(res.Unused), res.NotWhole)
	}
}

// A file that passes its own bound is read ONCE, so it costs the walk what
// it added to the walk's allowance and no more, and a project that does not
// depend on it is read as before. (Each file was read twice, for what it
// applies and then for what it says: a second reading of a file that uses
// its whole bound took the allowance of the files after it.)
func TestAFilePastItsBoundIsReadOnce(t *testing.T) {
	hostile := strings.Repeat("} a (", 50_000) + strings.Repeat(")", 50_000)
	app := "dependencies {\n  implementation 'c:d:1'\n}\n"
	for what, c := range map[string]struct {
		files map[string]string
		// readings of files in full, when a/x/build.gradle has been read:
		// the file past its bound once, and (when it is another file)
		// a/x/build.gradle once, for what it applies.
		readings int
	}{
		"a build file above":  {map[string]string{"a/build.gradle": hostile, "a/x/build.gradle": app}, 2},
		"a settings file":     {map[string]string{"a/settings.gradle": hostile, "a/x/build.gradle": app}, 2},
		"a buildSrc source":   {map[string]string{"a/buildSrc/src/main/kotlin/V.kt": hostile, "a/x/build.gradle": app}, 2},
		"the build file read": {map[string]string{"a/x/build.gradle": hostile}, 1},
	} {
		files := c.files
		files["b/y/build.gradle"] = app
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		if got := w.Read(filepath.Join(root, "a", "x", "build.gradle"), Options{}); got.Failure == nil {
			t.Fatalf("%s past its bound: the build file that depends on it was read (the premise)", what)
		}
		if w.gradle.fullReads != c.readings {
			t.Errorf("%s past its bound: %d readings in full, want %d: the file was read again", what, w.gradle.fullReads, c.readings)
		}
		if w.gradle.work.used > gradleWorkFactor*(len(hostile)+1)+gradleWorkFactor*1000 {
			t.Errorf("%s past its bound cost the walk %d; one reading of it may cost %d", what, w.gradle.work.used, gradleWorkFactor*(len(hostile)+1))
		}
		if got := w.Read(filepath.Join(root, "b", "y", "build.gradle"), Options{}); got.Failure != nil || len(got.Deps) != 1 {
			t.Errorf("%s past its bound: a project that does not depend on it: %d dependencies, failure %v; want it read", what, len(got.Deps), got.Failure)
		}
	}
}

// A coordinate is not built past what a coordinate can be (round 3: the
// cut could be deleted with the suite green).
func TestACoordinateIsNotBuiltPastItsLength(t *testing.T) {
	long := "dependencies {\n implementation 'g:" + strings.Repeat("a", 100) + "'" + strings.Repeat(" + '"+strings.Repeat("b", 100)+"'", 4*gradleInterpolatedLimit/100+10) + " + ':1'\n implementation 'c:d:1'\n}\n"
	root := writeTree(t, map[string]string{"build.gradle": long})
	got := NewWalk(root, nil).Read(filepath.Join(root, "build.gradle"), Options{})
	if got.Failure != nil {
		t.Fatalf("failure %v", got.Failure)
	}
	for _, d := range got.Deps {
		if len(d.Name)+len(d.Version)+len(d.Requirement) > 4*gradleInterpolatedLimit {
			t.Errorf("a dependency of %d bytes was built from a concatenation", len(d.Name)+len(d.Version)+len(d.Requirement))
		}
	}
	if len(got.Deps) != 1 || got.Deps[0].Name != "c:d" {
		t.Errorf("dependencies = %d; want the ordinary one beside it, and nothing from the long one", len(got.Deps))
	}
}
