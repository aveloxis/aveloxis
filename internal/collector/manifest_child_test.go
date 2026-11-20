// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The boundary between an analysis and the child that reads its manifests
// (0.29.213). These tests start the REAL child: this test binary is one when
// it is started with ManifestChildArg (TestMain), exactly as cmd/aveloxis is.

// The child an analysis starts is this program, with the one argument, and
// nothing in its environment.
func TestTheManifestChildIsThisProgram(t *testing.T) {
	exe, args, err := manifestChildCommand()
	self, _ := os.Executable()
	if err != nil || exe != self || !reflect.DeepEqual(args, []string{ManifestChildArg}) {
		t.Errorf("the child's command is %q %v (%v); want this program (%q) with %q", exe, args, err, self, ManifestChildArg)
	}
	if manifestParentVersion != db.ToolVersion {
		t.Errorf("an analysis asks the child as version %q; want this program's, %q", manifestParentVersion, db.ToolVersion)
	}
	if env := manifestChildEnv(); env == nil || len(env) != 0 {
		t.Errorf("the child's environment is %v; want one that is empty and not nil (nil is the parent's own)", env)
	}
	// And cmd/aveloxis is the child when it is started so.
	cmd := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func manifestReadCmd("))
	for _, needle := range []string{"collector.ManifestChildArg", "collector.ManifestChildMain(os.Stdin, os.Stdout)", "Hidden: true"} {
		if !strings.Contains(cmd, needle) {
			t.Errorf("cmd/aveloxis's manifestReadCmd does not hold %q", needle)
		}
	}
	if root := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func newRootCmd(")); !strings.Contains(root, "manifestReadCmd()") {
		t.Error("cmd/aveloxis does not register manifestReadCmd")
	}
}

// The parent's bound by the clock: a minute, and four times the CPU limit.
func TestTheParentsClockAllowance(t *testing.T) {
	if got := manifestWallAllowance(2 * time.Minute); got != time.Minute+4*2*time.Minute {
		t.Errorf("the allowance for two minutes of CPU is %s; want the floor and four times it", got)
	}
	if manifestWallCheckEvery != time.Second {
		t.Errorf("the clock is looked at every %s", manifestWallCheckEvery)
	}
}

func writeClone(t *testing.T, files map[string]string) string {
	t.Helper()
	// The child is told an absolute path; a temporary directory on macOS is
	// a symbolic link away from its real one, and the walks reject links.
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// recorded is a logger that keeps its records' level and message.
type recorded struct {
	mu    sync.Mutex
	lines []string
}

func (r *recorded) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		// The time differs between two runs; the rest is the record.
		line := string(p)
		if i := strings.Index(line, " level="); i >= 0 {
			line = line[i+1:]
		}
		r.lines = append(r.lines, strings.TrimSpace(line))
		return len(p), nil
	}), nil))
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// mapPomSource answers from a map, and counts.
type mapPomSource struct {
	poms  map[string]*manifest.RemotePom
	other manifest.RemoteOutcome
	asked []string
}

func (s *mapPomSource) RemotePom(g, a, v string, declared []string) (*manifest.RemotePom, manifest.RemoteOutcome) {
	s.asked = append(s.asked, g+":"+a+":"+v+" "+strings.Join(declared, ","))
	if p, ok := s.poms[g+":"+a+":"+v]; ok {
		return p, manifest.RemoteFound
	}
	return nil, s.other
}

func testLimits() ManifestLimits { return ManifestLimits{CPU: 30 * time.Second, HeapBytes: 512 << 20} }

func normalDeclared(d *manifestDeclared) *manifestDeclared {
	c := *d
	if len(c.Deps) == 0 {
		c.Deps = nil
	}
	if len(c.Unused) == 0 {
		c.Unused = nil
	}
	if len(c.Failed) == 0 {
		c.Failed = nil
	}
	if len(c.TreeHosts) == 0 {
		c.TreeHosts = nil
	}
	if len(c.NotWhole) == 0 {
		c.NotWhole = nil
	}
	if len(c.Repositories) == 0 {
		c.Repositories = nil
	}
	// A manifest read from a JSON object gives its declarations in no fixed
	// order (composer.json, in either process): the order is not part of a
	// result.
	c.Deps, c.Unused = append([]model.LibyearDep(nil), c.Deps...), append([]model.LibyearDep(nil), c.Unused...)
	for _, list := range [][]model.LibyearDep{c.Deps, c.Unused} {
		for i := range list {
			if len(list[i].VersionHosts) == 0 {
				list[i].VersionHosts = nil
			}
		}
		sort.SliceStable(list, func(i, j int) bool { return fmt.Sprintf("%+v", list[i]) < fmt.Sprintf("%+v", list[j]) })
	}
	return &c
}

// What the child returns is what the pass returns in this process: the
// declarations, the coverage parts, the declared repositories, the requests
// for poms and the log, for the manifest corpora and for trees that reach
// each part of the result.
func TestTheChildReturnsWhatThePassReturns(t *testing.T) {
	remoteParent := &manifest.RemotePom{Group: "far", Artifact: "away", Version: "3", Properties: map[string]string{"junit.version": "5.10.0"},
		Managed: []manifest.RemoteManaged{{Group: "org.x", Artifact: "managed", Version: "2.5"}, {Group: "org.b", Artifact: "bom", Version: "1", Import: true}}, ServedBy: "repo.example"}
	bom := &manifest.RemotePom{Group: "org.b", Artifact: "bom", Version: "1", Managed: []manifest.RemoteManaged{{Group: "org.y", Artifact: "frombom", Version: "9"}}, ServedBy: "other.example"}
	trees := map[string]map[string]string{
		"a Maven tree with a fetched parent, a BOM, a module, repositories and a pom that does not parse": {
			"pom.xml": `<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>3</version><relativePath/></parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version><packaging>pom</packaging>
<modules><module>app</module></modules><repositories><repository><url>https://nexus.example/m2</url></repository><repository><url>http://plain.example/m2</url></repository></repositories>
<dependencyManagement><dependencies><dependency><groupId>org.z</groupId><artifactId>unused</artifactId><version>4</version></dependency></dependencies></dependencyManagement></project>`,
			"app/pom.xml": `<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>app</artifactId><dependencies>
<dependency><groupId>org.x</groupId><artifactId>managed</artifactId></dependency><dependency><groupId>org.y</groupId><artifactId>frombom</artifactId></dependency>
<dependency><groupId>org.junit</groupId><artifactId>junit</artifactId><version>${junit.version}</version></dependency><dependency><groupId>org.q</groupId><artifactId>nobody</artifactId></dependency></dependencies></project>`,
			"broken/package.json": "{this is not JSON",
		},
		"a Gradle build with a platform that is not found, and a settings file": {
			"settings.gradle":  "rootProject.name = 'r'\ndependencyResolutionManagement { repositories { maven { url 'https://gradle.example/m2' } } }\n",
			"build.gradle":     "ext.v = '1.2.3'\n",
			"app/build.gradle": "dependencies {\n  implementation platform('org.gone:bom:7')\n  implementation \"a:b:$v\"\n  implementation 'c:d'\n}\n",
		},
		"several ecosystems, a manifest that is not UTF-8, and vendor directories": {
			"package.json":                `{"dependencies":{"left-pad":"^1.3.0"},"devDependencies":{"jest":"29.0.0"}}`,
			"requirements.txt":            "requests==2.31.0\nbad\xffname==1.0\n# c\nflask>=2\n",
			"Gemfile":                     "source 'https://rubygems.org'\ngem 'rails', '~> 7.0'\ngem 'bad\xffgem', '1.0'\ngroup :test do\n  gem 'rspec'\nend\n",
			"go.mod":                      "module x\n\ngo 1.22\n\nrequire github.com/a/b v1.2.3\n",
			"node_modules/x/package.json": `{"dependencies":{"never":"1"}}`,
			"vendor/requirements.txt":     "never==1\n",
			"sub/Cargo.toml":              "[package]\nname=\"s\"\nversion=\"0.1.0\"\n[dependencies]\nserde = \"1.0\"\n",
		},
		"an empty clone": {},
	}
	dirs := map[string]string{}
	for name, files := range trees {
		dirs[name] = writeClone(t, files)
	}
	for _, corpus := range []string{"../manifest/testdata/manifest_corpus"} {
		abs, err := filepath.Abs(corpus)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(abs); err != nil {
			t.Fatalf("the corpus %s is not there: %v", corpus, err)
		}
		dirs["the corpus "+corpus] = abs
	}
	compared := 0
	for name, dir := range dirs {
		for _, opts := range []manifest.Options{{}, {DevBuildDeps: true, GitHubActionsDeps: true}} {
			newSrc := func() *mapPomSource {
				return &mapPomSource{poms: map[string]*manifest.RemotePom{"far:away:3": remoteParent, "org.b:bom:1": bom}, other: manifest.RemoteNotFound}
			}
			hereLog, hereSrc := &recorded{}, newSrc()
			here, err := readManifestDeclarations(dir, opts, hereSrc, maxRemotePomCoordinates, hereLog.logger(), 7)
			if err != nil {
				t.Fatalf("%s: in this process: %v", name, err)
			}
			childLog, childSrc := &recorded{}, newSrc()
			run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RepoID: 7,
				DevBuildDeps: opts.DevBuildDeps, GitHubActionsDeps: opts.GitHubActionsDeps, RemotePomLimit: maxRemotePomCoordinates}, testLimits(), childSrc, childLog.logger())
			if err != nil {
				t.Fatalf("%s: in the child: %v", name, err)
			}
			if got, want := normalDeclared(run.decl), normalDeclared(here); !reflect.DeepEqual(got, want) {
				t.Errorf("%s (%+v): the child's result is not the pass's: %s", name, opts, firstDifference(got, want))
			}
			if !reflect.DeepEqual(childSrc.asked, hereSrc.asked) {
				t.Errorf("%s: the child asked for %v; the pass in this process for %v", name, childSrc.asked, hereSrc.asked)
			}
			if !reflect.DeepEqual(childLog.lines, hereLog.lines) {
				t.Errorf("%s: the child's log\n%s\nthe pass's\n%s", name, strings.Join(childLog.lines, "\n"), strings.Join(hereLog.lines, "\n"))
			}
			compared += len(here.Deps) + len(here.Unused)

			// The name pass.
			names, err := readManifestNames(dir)
			if err != nil {
				t.Fatal(err)
			}
			nrun, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassNames}, Dir: dir}, testLimits(), nil, childLog.logger())
			if err != nil {
				t.Fatalf("%s: names in the child: %v", name, err)
			}
			// A language whose manifests name nothing is no row either way.
			for lang, counts := range names {
				if len(counts) == 0 {
					delete(names, lang)
				}
			}
			if !reflect.DeepEqual(nrun.names, names) {
				t.Errorf("%s: the child's names %v; the pass's %v", name, nrun.names, names)
			}
		}
	}
	// Guard the denominator: the trees above reach each part of a result.
	if compared < 100 {
		t.Errorf("only %d declarations were compared", compared)
	}
	sample, err := readManifestDeclarations(dirs["a Maven tree with a fetched parent, a BOM, a module, repositories and a pom that does not parse"], manifest.Options{},
		&mapPomSource{poms: map[string]*manifest.RemotePom{"far:away:3": remoteParent, "org.b:bom:1": bom}}, 10, slog.New(slog.NewTextHandler(io.Discard, nil)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(sample.Unused) == 0 || len(sample.Failed) != 1 || len(sample.TreeHosts) == 0 || len(sample.Repositories) != 2 || sample.Found != 3 || sample.Parsed != 2 {
		t.Errorf("the Maven tree does not reach every part of a result: %+v", sample)
	}
	bad := false
	for _, d := range mustDeclared(t, dirs["several ecosystems, a manifest that is not UTF-8, and vendor directories"]).Deps {
		if strings.Contains(d.Name, "\xff") || strings.Contains(d.Requirement, "\xff") {
			bad = true
		}
		if d.Name == "never" {
			t.Error("a manifest under node_modules or vendor was read")
		}
	}
	if !bad {
		t.Error("no declaration with a name that is not UTF-8 crossed the boundary: the fixture does not test that bytes arrive as written")
	}
}

// firstDifference says where two results part.
func firstDifference(got, want *manifestDeclared) string {
	g, w := reflect.ValueOf(*got), reflect.ValueOf(*want)
	for i := 0; i < g.NumField(); i++ {
		name := g.Type().Field(i).Name
		a, b := g.Field(i), w.Field(i)
		if reflect.DeepEqual(a.Interface(), b.Interface()) {
			continue
		}
		if a.Kind() != reflect.Slice {
			return fmt.Sprintf("%s is %v, want %v", name, a.Interface(), b.Interface())
		}
		if a.Len() != b.Len() {
			return fmt.Sprintf("%s has %d entries, want %d", name, a.Len(), b.Len())
		}
		for j := 0; j < a.Len(); j++ {
			if !reflect.DeepEqual(a.Index(j).Interface(), b.Index(j).Interface()) {
				return fmt.Sprintf("%s[%d] is %+q, want %+q", name, j, fmt.Sprintf("%+v", a.Index(j).Interface()), fmt.Sprintf("%+v", b.Index(j).Interface()))
			}
		}
	}
	return "(no difference found)"
}

func mustDeclared(t *testing.T, dir string) *manifestDeclared {
	t.Helper()
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 10}, testLimits(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return run.decl
}

// Every string of a frame arrives as the bytes it was sent as: JSON alone
// would turn bytes that are not UTF-8 into the replacement character, and a
// name the sanitizer must refuse would arrive as one it accepts.
func TestAFrameCarriesEveryStringExactly(t *testing.T) {
	hard := []string{"", "plain", "bad\xffbytes", "\x00starts with the mark", "\x00", "tab\tquote\"back\\slash", "line\nbreak", strings.Repeat("é", 500), "\xed\xa0\x80"}
	// Every string field of every type a frame holds, by reflection: a field
	// added to one of them is covered without a line here.
	n := 0
	var fill func(v reflect.Value)
	fill = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.String:
			v.SetString(hard[n%len(hard)])
			n++
		case reflect.Bool:
			v.SetBool(true)
		case reflect.Int, reflect.Int64:
			v.SetInt(int64(3 + n))
		case reflect.Pointer:
			v.Set(reflect.New(v.Type().Elem()))
			fill(v.Elem())
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).IsExported() {
					fill(v.Field(i))
				}
			}
		case reflect.Slice:
			v.Set(reflect.MakeSlice(v.Type(), 3, 3))
			for i := 0; i < 3; i++ {
				fill(v.Index(i))
			}
		case reflect.Map:
			v.Set(reflect.MakeMap(v.Type()))
			for i := 0; i < 3; i++ {
				k, e := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
				fill(k)
				fill(e)
				v.SetMapIndex(k, e)
			}
		}
	}
	var f manifestFrame
	fill(reflect.ValueOf(&f).Elem())
	// What the wire deliberately carries another way, or not at all.
	f.PomAnswer.Pom.ServedBy = ""
	for i := range f.Part.Failed {
		f.Part.Failed[i].ManifestFailure.NoCarry = false
	}
	before := fmt.Sprintf("%#v", f.Part.Deps)
	var buf bytes.Buffer
	if err := writeManifestFrame(&buf, &f); err != nil {
		t.Fatal(err)
	}
	if after := fmt.Sprintf("%#v", f.Part.Deps); after != before {
		t.Error("writing a frame changed the caller's values")
	}
	got, size, err := readManifestFrame(&buf)
	if err != nil || size == 0 {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, &f) {
		t.Errorf("a frame did not arrive as it was sent:\n got %+v\nwant %+v", got.Part, f.Part)
	}
	if n < 60 {
		t.Errorf("only %d strings were sent: the frame's types were not walked", n)
	}
	// What a failed manifest's own JSON leaves out crosses beside it: the
	// instruction to carry none of its previous rows.
	var carried bytes.Buffer
	if err := writeManifestFrame(&carried, &manifestFrame{T: framePart, Part: &manifestPart{Failed: []manifestWireFailure{
		{ManifestFailure: model.ManifestFailure{Path: "a/pom.xml", Kind: "pom.xml", Reason: model.ManifestFailedRead}, NoCarry: true},
		{ManifestFailure: model.ManifestFailure{Path: "b/pom.xml", Kind: "pom.xml", Reason: model.ManifestFailedParse}},
	}}}); err != nil {
		t.Fatal(err)
	}
	back, _, err := readManifestFrame(&carried)
	if err != nil {
		t.Fatal(err)
	}
	run := newManifestPending(manifestPassDeclarations)
	run.add(back.Part)
	if len(run.decl.Failed) != 2 || !run.decl.Failed[0].NoCarry || run.decl.Failed[1].NoCarry {
		t.Errorf("NoCarry did not cross the boundary: %+v", run.decl.Failed)
	}
	// A marked string that is not base64 is refused, not passed on.
	var raw bytes.Buffer
	body := []byte(`{"t":"log","log":{"level":0,"msg":"\u0000not base64!"}}`)
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(body)))
	raw.Write(head[:])
	raw.Write(body)
	if _, _, err := readManifestFrame(&raw); err == nil {
		t.Error("a string marked as base64 that is not base64 was accepted")
	}
}

// The child's watchdog, with its three reasons to end the process, each by
// its own exit status; and none while there is none.
func TestTheChildsWatchdogEndsItForEachReason(t *testing.T) {
	run := func(cpu time.Duration, heap int64, orphaned bool) int {
		stop, exited := make(chan struct{}), make(chan int, 1)
		var peak atomic.Int64
		go manifestWatchdog(cpu, heap, stop, func(code int) { exited <- code }, &peak, func() bool { return orphaned })
		select {
		case code := <-exited:
			return code
		case <-time.After(5 * manifestWatchEvery):
			close(stop)
			if peak.Load() == 0 {
				t.Error("the watchdog looked at nothing")
			}
			return -1
		}
	}
	if got := run(time.Hour, 1<<40, false); got != -1 {
		t.Errorf("inside every limit the watchdog ended the process with %d", got)
	}
	if got := run(time.Nanosecond, 1<<40, false); got != manifestExitCPU {
		t.Errorf("past the CPU limit: %d, want %d", got, manifestExitCPU)
	}
	if got := run(time.Hour, 1, false); got != manifestExitMemory {
		t.Errorf("past the memory limit: %d, want %d", got, manifestExitMemory)
	}
	if got := run(time.Hour, 1<<40, true); got != manifestExitOrphan {
		t.Errorf("with its parent gone: %d, want %d", got, manifestExitOrphan)
	}
	// The three statuses and the ones a Go program gives itself are apart.
	seen := map[int]bool{0: true, 1: true, 2: true}
	for _, code := range []int{manifestExitProtocol, manifestExitCPU, manifestExitMemory, manifestExitOrphan} {
		if seen[code] {
			t.Errorf("exit status %d is used twice", code)
		}
		seen[code] = true
	}
	// And the real child is given the real check: its parent's id as it
	// was when it started.
	if main := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func ManifestChildMain(")); !strings.Contains(main, "os.Getppid() != req.ParentPID") {
		t.Error("ManifestChildMain does not watch for its parent going away")
	}
}

// There is no child without limits, and none that serves anything but a
// request: each is refused with the protocol's exit status before a file is
// read.
func TestAChildWithoutLimitsOrARequestServesNothing(t *testing.T) {
	frame := func(f *manifestFrame) *bytes.Buffer {
		var b bytes.Buffer
		if err := writeManifestFrame(&b, f); err != nil {
			t.Fatal(err)
		}
		return &b
	}
	dir := writeClone(t, map[string]string{"requirements.txt": "requests==2.31.0\n"})
	limits := func(r manifestRequest) *manifestRequest {
		r.Dir, r.Version, r.CPULimitNanos, r.HeapLimitBytes = dir, db.ToolVersion, int64(time.Minute), 1<<30
		return &r
	}
	noCPU, noHeap, otherVersion, noVersion := limits(manifestRequest{Passes: []string{manifestPassNames}}), limits(manifestRequest{Passes: []string{manifestPassNames}}),
		limits(manifestRequest{Passes: []string{manifestPassNames}}), limits(manifestRequest{Passes: []string{manifestPassNames}})
	noCPU.CPULimitNanos, noHeap.HeapLimitBytes, otherVersion.Version, noVersion.Version = 0, 0, db.ToolVersion+".1", ""
	// A request of another version that this program could not even read as
	// a request (a field it does not have) is still another version's.
	unknown := `{"t":"request","request":{"version":"9.9.9","a_field_of_the_future":true}}`
	var future bytes.Buffer
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(unknown)))
	future.Write(head[:])
	future.WriteString(unknown)
	for name, c := range map[string]struct {
		in   *bytes.Buffer
		want int
	}{
		"no CPU limit":      {frame(&manifestFrame{T: frameRequest, Request: noCPU}), manifestExitProtocol},
		"no memory limit":   {frame(&manifestFrame{T: frameRequest, Request: noHeap}), manifestExitProtocol},
		"not a request":     {frame(&manifestFrame{T: frameDone, Done: &manifestDone{}}), manifestExitVersion}, // it names no version: not this program's
		"no such pass":      {frame(&manifestFrame{T: frameRequest, Request: limits(manifestRequest{Passes: []string{"everything"}})}), manifestExitProtocol},
		"no pass":           {frame(&manifestFrame{T: frameRequest, Request: limits(manifestRequest{})}), manifestExitProtocol},
		"nothing at all":    {&bytes.Buffer{}, manifestExitProtocol},
		"not a frame":       {bytes.NewBufferString("GET / HTTP/1.1\r\n\r\n"), manifestExitProtocol},
		"another version":   {frame(&manifestFrame{T: frameRequest, Request: otherVersion}), manifestExitVersion},
		"no version":        {frame(&manifestFrame{T: frameRequest, Request: noVersion}), manifestExitVersion},
		"a future's fields": {&future, manifestExitVersion},
	} {
		var out bytes.Buffer
		if code := ManifestChildMain(c.in, &out); code != c.want {
			t.Errorf("%s: the child ended with %d, want %d", name, code, c.want)
		}
		if name != "no such pass" && out.Len() != 0 {
			t.Errorf("%s: the child wrote %d bytes", name, out.Len())
		}
	}
	// With its own version and its limits, the same request is served.
	var out bytes.Buffer
	if code := ManifestChildMain(frame(&manifestFrame{T: frameRequest, Request: limits(manifestRequest{Passes: []string{manifestPassNames}})}), &out); code != manifestExitOK || out.Len() == 0 {
		t.Errorf("a request of this version, with limits: ended with %d having written %d bytes", code, out.Len())
	}
}

// What starting the child costs an analysis: a process, twice (the names
// and the declarations). Logged, and held to a bound that says it is small
// beside cloning a repository.
func TestWhatStartingTheChildCosts(t *testing.T) {
	dir := writeClone(t, map[string]string{"requirements.txt": "requests==2.31.0\n"})
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	const runs = 20
	began := time.Now()
	for i := 0; i < runs; i++ {
		if _, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5}, testLimits(), nil, quiet); err != nil {
			t.Fatal(err)
		}
	}
	each := time.Since(began) / runs
	t.Logf("one child for a clone of one small manifest: %s by the clock (this test binary; the aveloxis binary is larger)", each.Round(time.Millisecond))
	if each > 2*time.Second {
		t.Errorf("starting the child takes %s", each)
	}
}

// The analysis tells the child which process started it: that is what the
// child watches for going away.
func TestTheParentTellsTheChildWhoItIs(t *testing.T) {
	misbehaving(t, "parent-pid")
	if _, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassNames}, Dir: writeClone(t, nil)}, testLimits(), nil, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Errorf("the child was not told its parent's process id: %v", err)
	}
}

// A child whose parent is gone ends itself (manifestExitOrphan). The real
// child is started by a helper process that exits at once; the child is then
// in the middle of a pass, waiting for the answer to a pom on a pipe this
// test holds open, so nothing but the orphan check can end it: it has used
// no CPU time to speak of, holds little, and its input has not ended.
//
// What is asserted is that it ENDS, within a bound (its output closes). Its
// exit status cannot be read here: only a process's parent can wait for it,
// its parent is gone, and adopting it (a subreaper) is Linux's alone. The
// status for each reason is TestTheChildsWatchdogEndsItForEachReason's.
func TestAChildWhoseParentIsGoneEndsItself(t *testing.T) {
	dir := writeClone(t, map[string]string{
		"pom.xml": "<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>3</version><relativePath/></parent><groupId>t</groupId><artifactId>a</artifactId><version>1</version>" +
			"<dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId></dependency></dependencies></project>",
	})
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer inW.Close()
	defer outR.Close()
	exe, _ := os.Executable()
	helper := exec.Command(exe, misbehaveArg, "orphan-helper")
	helper.ExtraFiles = []*os.File{inR, outW}
	if err := helper.Run(); err != nil {
		t.Fatalf("the helper: %v", err)
	}
	// The helper is gone, and so are this process's copies of the child's
	// ends of the pipes: the child alone holds them.
	inR.Close()
	outW.Close()
	if err := writeManifestFrame(inW, &manifestFrame{T: frameRequest, Request: &manifestRequest{Version: db.ToolVersion, Passes: []string{manifestPassDeclarations}, Dir: dir,
		PomSource: true, RemotePomLimit: 5, CPULimitNanos: int64(time.Hour), HeapLimitBytes: 1 << 30, ParentPID: helper.ProcessState.Pid()}}); err != nil {
		t.Fatal(err)
	}
	// It is at work: it asks for the parent pom, and is not answered.
	if err := outR.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	out := bufio.NewReader(outR)
	for {
		f, _, err := readManifestFrame(out)
		if err != nil {
			t.Fatalf("the child ended, or said nothing, before it asked for the pom: %v", err)
		}
		if f.T == framePom {
			break
		}
	}
	began := time.Now()
	if _, _, err := readManifestFrame(out); !errors.Is(err, io.EOF) {
		t.Fatalf("the child did not end: %v (it should have, about %s after its parent went)", err, manifestWatchEvery)
	}
	t.Logf("the child ended %s after it was last heard from", time.Since(began).Round(time.Millisecond))
}

// --- children that misbehave -------------------------------------------

const misbehaveArg = "manifest-misbehave"

// runMisbehavingManifestChildIfAsked makes this test binary a child that
// does one wrong thing, named by its second argument.
func runMisbehavingManifestChildIfAsked() {
	if len(os.Args) != 3 || os.Args[1] != misbehaveArg {
		return
	}
	out := os.Stdout
	frame := func(f *manifestFrame) {
		if err := writeManifestFrame(out, f); err != nil {
			os.Exit(90)
		}
	}
	raw := func(n uint32, body string) {
		var head [4]byte
		binary.BigEndian.PutUint32(head[:], n)
		out.Write(head[:])
		out.WriteString(body)
	}
	req, _, _ := readManifestFrame(os.Stdin)
	// end ends the pass in hand, as the real child does: by name.
	passes := []string{""}
	if req != nil && req.Request != nil && len(req.Request.Passes) > 0 {
		passes = req.Request.Passes
	}
	end := func(d manifestDone) {
		d.Pass = passes[0]
		frame(&manifestFrame{T: frameDone, Done: &d})
		if len(passes) > 1 {
			passes = passes[1:]
		}
	}
	lock := func(l manifestWireLockfile, entries []manifestWireEntry, edges []manifestWireEdge) {
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Lockfiles: []manifestWireLockfile{l}, Entries: entries, Edges: edges}})
	}
	yarn := manifestWireLockfile{Path: "web/yarn.lock", Kind: "yarn.lock", Ecosystem: "npm", Entries: 1}
	entry := manifestWireEntry{Name: "left-pad", Version: "1.3.0"}
	dep := func(path string) model.LibyearDep {
		return model.LibyearDep{Name: "n", Manager: "npm", ManifestPath: path}
	}
	switch os.Args[2] {
	case "oversized":
		raw(uint32(manifestFrameLimit)+1, "x")
	case "truncated":
		raw(100, "{\"t\":")
	case "not-json":
		raw(9, "not json!")
	case "garbage-first":
		out.WriteString("hello, I am a banner\n")
		end(manifestDone{})
	case "unknown-field":
		body := `{"t":"done","done":{},"extra":1}`
		raw(uint32(len(body)), body)
	case "outside":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Deps: []model.LibyearDep{dep("../../etc/passwd")}}})
		end(manifestDone{})
	case "absolute":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Deps: []model.LibyearDep{dep("/etc/passwd")}}})
		end(manifestDone{})
	case "unknown-manager":
		d := dep("package.json")
		d.Manager = "made-up"
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Deps: []model.LibyearDep{d}}})
		end(manifestDone{})
	case "unknown-kind":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Failed: []manifestWireFailure{{ManifestFailure: model.ManifestFailure{Path: "x", Kind: "made-up", Reason: model.ManifestFailedParse}}}}})
		end(manifestDone{Found: 1})
	case "counts":
		end(manifestDone{Found: 5, Parsed: 1})
	case "too-much":
		// More than its own memory limit, in frames that each fit.
		big := strings.Repeat("x", 1<<20)
		for sent := int64(0); sent <= req.Request.HeapLimitBytes+(2<<20); sent += 1 << 20 {
			d := dep("package.json")
			d.Requirement = big
			frame(&manifestFrame{T: framePart, Part: &manifestPart{Deps: []model.LibyearDep{d}}})
		}
		end(manifestDone{})
	case "silent":
		os.Exit(0)
	case "wrong-frame":
		frame(&manifestFrame{T: framePomAnswer, PomAnswer: &manifestPomAnswer{}})
	case "names-in-declarations":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Names: []manifestNameCount{{"Python", "x", 1}}}})
		end(manifestDone{})
	case "too-many-poms":
		for i := 0; ; i++ {
			frame(&manifestFrame{T: framePom, Pom: &manifestPomAsk{Group: "g", Artifact: fmt.Sprint("a", i), Version: "1"}})
			if _, _, err := readManifestFrame(os.Stdin); err != nil {
				os.Exit(0)
			}
		}
	case "orphan-helper":
		// Start the REAL child on the two pipes this process was handed
		// (descriptors 3 and 4), and go away without waiting for it.
		exe, _ := os.Executable()
		child := exec.Command(exe, ManifestChildArg)
		child.Env = manifestChildEnv()
		child.Stdin, child.Stdout = os.NewFile(3, "child-stdin"), os.NewFile(4, "child-stdout")
		if err := child.Start(); err != nil {
			os.Exit(91)
		}
		os.Exit(0)
	case "parent-pid":
		// A child that checks who it was told its parent is.
		if req == nil || req.Request == nil || req.Request.ParentPID != os.Getppid() {
			os.Exit(9)
		}
		end(manifestDone{})
	case "lock-outside":
		l := yarn
		l.Path = "../other/yarn.lock"
		lock(l, []manifestWireEntry{entry}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-not-its-kind":
		l := yarn
		l.Path = "web/package.json"
		lock(l, []manifestWireEntry{entry}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-unknown-kind":
		lock(manifestWireLockfile{Path: "requirements.txt", Kind: "requirements.txt", Ecosystem: "pypi", Entries: 1}, []manifestWireEntry{entry}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-ecosystem":
		l := yarn
		l.Ecosystem = "pypi"
		lock(l, []manifestWireEntry{entry}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-twice":
		lock(yarn, []manifestWireEntry{entry}, nil)
		second := entry
		second.L = 1
		lock(yarn, []manifestWireEntry{second}, nil)
		end(manifestDone{Lockfiles: 2})
	case "lock-twice-in-one-piece":
		second := entry
		second.L = 1
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Lockfiles: []manifestWireLockfile{yarn, yarn}, Entries: []manifestWireEntry{entry, second}}})
		end(manifestDone{Lockfiles: 2})
	case "lock-count-fewer":
		lock(yarn, []manifestWireEntry{entry}, nil)
		end(manifestDone{})
	case "pom-in-the-lockfiles":
		frame(&manifestFrame{T: framePom, Pom: &manifestPomAsk{Group: "g", Artifact: "a", Version: "1"}})
		end(manifestDone{})
	case "lock-entry-of-none":
		stray := entry
		stray.L = 3
		lock(yarn, []manifestWireEntry{stray}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-edge-of-none":
		lock(yarn, []manifestWireEntry{entry}, []manifestWireEdge{{L: -1, ParentName: "a", ChildName: "b"}})
		end(manifestDone{Lockfiles: 1})
	case "lock-more-than-said":
		lock(yarn, []manifestWireEntry{entry}, nil)
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Entries: []manifestWireEntry{entry}}})
		end(manifestDone{Lockfiles: 1})
	case "lock-more-edges-than-said":
		lock(yarn, []manifestWireEntry{entry}, []manifestWireEdge{{ParentName: "a", ChildName: "b"}})
		end(manifestDone{Lockfiles: 1})
	case "lock-fewer-than-said":
		l := yarn
		l.Entries = 2
		lock(l, []manifestWireEntry{entry}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-count":
		lock(yarn, []manifestWireEntry{entry}, nil)
		end(manifestDone{Lockfiles: 2})
	case "lock-negative":
		l := yarn
		l.NotAVersion = -1
		lock(l, []manifestWireEntry{entry}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-binary-with-entries":
		lock(manifestWireLockfile{Path: "bun.lockb", Kind: "bun.lockb", Ecosystem: "npm", Entries: 1}, []manifestWireEntry{entry}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-scope":
		e := entry
		e.Scope = "made-up"
		lock(yarn, []manifestWireEntry{e}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-entry-ecosystem":
		e := entry
		e.Ecosystem = bioconductorEcosystem
		lock(yarn, []manifestWireEntry{e}, nil)
		end(manifestDone{Lockfiles: 1})
	case "lock-failed-and-parsed":
		lock(yarn, []manifestWireEntry{entry}, nil)
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Failed: []manifestWireFailure{{ManifestFailure: model.ManifestFailure{Path: yarn.Path, Kind: yarn.Kind, Reason: model.ManifestFailedParse}}}}})
		end(manifestDone{Lockfiles: 1})
	case "lock-failed-carrying":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Failed: []manifestWireFailure{{ManifestFailure: model.ManifestFailure{Path: yarn.Path, Kind: yarn.Kind, Reason: model.ManifestFailedParse, RowsCarried: 4}}}}})
		end(manifestDone{})
	case "lock-failed-reason":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Failed: []manifestWireFailure{{ManifestFailure: model.ManifestFailure{Path: yarn.Path, Kind: yarn.Kind, Reason: "tired"}}}}})
		end(manifestDone{})
	case "lock-with-declarations":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Deps: []model.LibyearDep{dep("package.json")}}})
		end(manifestDone{})
	case "lockfiles-in-declarations":
		empty := yarn
		empty.Entries = 0
		lock(empty, nil, nil)
		end(manifestDone{})
	case "lock-entries-in-declarations":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Entries: []manifestWireEntry{entry}}})
		end(manifestDone{})
	case "lock-edges-in-declarations":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Edges: []manifestWireEdge{{ParentName: "a", ChildName: "b"}}}})
		end(manifestDone{})
	case "wrong-pass-ended":
		frame(&manifestFrame{T: frameDone, Done: &manifestDone{Pass: manifestPassLockfiles}})
	case "one-pass-of-three":
		// The names whole, then gone.
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Names: []manifestNameCount{{"Python", "requests", 2}}}})
		end(manifestDone{})
		os.Exit(0)
	case "two-passes-then-the-memory-limit":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Names: []manifestNameCount{{"Python", "requests", 2}}}})
		end(manifestDone{})
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Deps: []model.LibyearDep{dep("package.json")}}})
		end(manifestDone{Found: 1, Parsed: 1})
		l := yarn
		l.Entries = 2
		lock(l, []manifestWireEntry{entry}, nil)
		os.Exit(manifestExitMemory)
	case "three-passes-then-rubbish":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Names: []manifestNameCount{{"Python", "requests", 2}}}})
		end(manifestDone{})
		end(manifestDone{})
		lock(yarn, []manifestWireEntry{entry}, nil)
		raw(9, "not json!")
	case "crash":
		panic("the reader fell over")
	case "hang":
		time.Sleep(time.Hour)
	case "partial-then-die":
		frame(&manifestFrame{T: framePart, Part: &manifestPart{Deps: []model.LibyearDep{dep("package.json")}}})
		os.Exit(3)
	}
	os.Exit(0)
}

func misbehaving(t *testing.T, mode string) {
	t.Helper()
	saved := manifestChildCommand
	manifestChildCommand = func() (string, []string, error) {
		exe, err := os.Executable()
		return exe, []string{misbehaveArg, mode}, err
	}
	t.Cleanup(func() { manifestChildCommand = saved })
}

// Whatever a child does that the protocol does not, the analysis gets a
// named failure and NOTHING of what the child sent.
func TestAChildThatMisbehavesIsANamedFailure(t *testing.T) {
	dir := writeClone(t, map[string]string{"package.json": `{"dependencies":{"a":"1"}}`})
	for mode, want := range map[string]string{
		"oversized":                    "larger than the limit",
		"truncated":                    "ended without a result",
		"not-json":                     "not what this protocol sends",
		"garbage-first":                "larger than the limit", // the banner's first four bytes read as a length
		"unknown-field":                "not what this protocol sends",
		"outside":                      "not a path inside the clone",
		"absolute":                     "not a path inside the clone",
		"unknown-manager":              "package manager the roster does not have",
		"unknown-kind":                 "not one the pass records",
		"counts":                       "do not add up",
		"too-much":                     "its own memory limit",
		"silent":                       "ended without a result",
		"wrong-frame":                  "a child does not send",
		"names-in-declarations":        "sent name counts",
		"too-many-poms":                "more than the 5 poms",
		"crash":                        "the reader fell over",
		"partial-then-die":             "exit status 3",
		"lockfiles-in-declarations":    "a manifest pass sent lockfiles",
		"lock-entries-in-declarations": "a manifest pass sent lockfiles",
		"lock-edges-in-declarations":   "a manifest pass sent lockfiles",
		"wrong-pass-ended":             "while on the",
	} {
		misbehaving(t, mode)
		limits := ManifestLimits{CPU: 20 * time.Second, HeapBytes: 8 << 20}
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5},
			limits, &mapPomSource{other: manifest.RemoteNotFound}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		var child *errManifestChild
		if run.anyFinished() || !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadFailed || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: a pass finished: %v, error %v; want no result and a failed read that says %q", mode, run.anyFinished(), err, want)
		}
	}
}

// A child that does not end is ended by its parent's clock, and by the
// analysis's context: in both cases it is gone when runManifestChild
// returns (Wait has returned, and the group was killed).
func TestAChildThatHangsIsStoppedByTheClockAndByAShutdown(t *testing.T) {
	dir := writeClone(t, nil)
	savedAllow, savedEvery := manifestWallAllowance, manifestWallCheckEvery
	manifestWallAllowance = func(time.Duration) time.Duration { return 300 * time.Millisecond }
	manifestWallCheckEvery = 50 * time.Millisecond
	t.Cleanup(func() { manifestWallAllowance, manifestWallCheckEvery = savedAllow, savedEvery })
	misbehaving(t, "hang")
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	began := time.Now()
	_, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassNames}, Dir: dir}, testLimits(), nil, quiet)
	var child *errManifestChild
	if !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadTimedOut {
		t.Fatalf("a child that hangs: %v; want it stopped by the clock", err)
	}
	if took := time.Since(began); took > 20*time.Second {
		t.Errorf("stopping it took %s", took)
	}
	if !strings.Contains(err.Error(), "manifest_read_cpu_seconds") {
		t.Errorf("the error does not name the setting: %v", err)
	}

	// A shutdown: the context's error, not a failure of the reader.
	manifestWallAllowance = func(time.Duration) time.Duration { return time.Hour }
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	began = time.Now()
	_, err = runManifestChild(ctx, manifestRequest{Passes: []string{manifestPassNames}, Dir: dir}, testLimits(), nil, quiet)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("a shutdown while the child ran: %v; want the context's own error", err)
	}
	if took := time.Since(began); took > 20*time.Second {
		t.Errorf("the shutdown took %s: the child was waited for", took)
	}
}

// --- the limits, from the configuration to the stopped child ------------

// slowBuild is a clone whose reading takes tens of seconds of CPU time for
// about a megabyte of poms: what the child's CPU limit is for. k modules
// under a parent with k BOM imports whose coordinates hold a reference each
// module answers differently, padded by a property of 2,600 bytes: every
// module's every import is another coordinate, each interpolated. (It is one
// of the products the readers do not pretend to bound: scalingNotLinear in
// internal/manifest. Until 0.29.213 this fixture was a build file of
// unclosed calls, which the Gradle reader now reads in milliseconds.)
func slowBuild() map[string]string {
	const k = 1600
	var parent strings.Builder
	parent.WriteString("<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging><properties><L>" +
		strings.Repeat("L", 2600) + "</L></properties><dependencyManagement><dependencies>")
	for i := 0; i < k; i++ {
		fmt.Fprintf(&parent, "<dependency><groupId>${L}org.x.${project.artifactId}</groupId><artifactId>${L}b%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", i)
	}
	parent.WriteString("</dependencies></dependencyManagement></project>")
	files := map[string]string{"pom.xml": parent.String()}
	for m := 0; m < k; m++ {
		files[fmt.Sprintf("m%d/pom.xml", m)] = fmt.Sprintf("<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>m%d</artifactId>"+
			"<dependencies><dependency><groupId>g</groupId><artifactId>a</artifactId></dependency></dependencies></project>", m)
	}
	return files
}

// hugeRequirements is a manifest whose declarations take some hundreds of
// mebibytes to hold.
func hugeRequirements() string {
	var sb strings.Builder
	for i := 0; sb.Len() < 24<<20; i++ {
		fmt.Fprintf(&sb, "package-number-%d==1.%d.0\n", i, i)
	}
	return sb.String()
}

func limitsFromConfig(t *testing.T, collection string) ManifestLimits {
	t.Helper()
	path := filepath.Join(t.TempDir(), "aveloxis.json")
	if err := os.WriteFile(path, []byte(`{"collection":{`+collection+`}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("%s: %v", collection, err)
	}
	return ManifestLimits{CPU: cfg.Collection.ManifestReadCPULimit(), HeapBytes: cfg.Collection.ManifestReadMemoryLimit()}
}

// collection.manifest_read_cpu_seconds, from the file to the child that is
// stopped at it; and the same for collection.manifest_read_memory_mb.
func TestTheConfiguredLimitsStopTheRealChild(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Run("cpu", func(t *testing.T) {
		files := slowBuild()
		files["package.json"] = `{"dependencies":{"a":"1"}}`
		dir := writeClone(t, files)
		limits := limitsFromConfig(t, `"manifest_read_cpu_seconds": 2`)
		if limits.CPU != 2*time.Second {
			t.Fatalf("the configured limit is %s", limits.CPU)
		}
		began := time.Now()
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5}, limits, nil, quiet)
		took := time.Since(began)
		var child *errManifestChild
		if run.anyFinished() || !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadCPULimit {
			t.Fatalf("a build that takes tens of seconds, under a limit of two seconds: result %v, error %v; want it stopped at the CPU limit", run.anyFinished(), err)
		}
		if !strings.Contains(err.Error(), "collection.manifest_read_cpu_seconds allows (2s)") {
			t.Errorf("the error does not give the setting and its effective value: %v", err)
		}
		// Stopped AT the limit, not long after. (By the clock it can be
		// sooner than the limit: CPU time is every thread's, and the
		// garbage collector's threads work beside the reader.)
		if took > 15*time.Second {
			t.Errorf("it was stopped after %s by the clock", took)
		}
		t.Logf("stopped after %s", took)
	})
	t.Run("memory", func(t *testing.T) {
		dir := writeClone(t, map[string]string{"requirements.txt": hugeRequirements()})
		limits := limitsFromConfig(t, `"manifest_read_memory_mb": 48`)
		if limits.HeapBytes != 48<<20 {
			t.Fatalf("the configured limit is %d", limits.HeapBytes)
		}
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5}, limits, nil, quiet)
		var child *errManifestChild
		if run.anyFinished() || !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadMemoryLimit {
			t.Fatalf("a manifest of a million declarations under a limit of 48 MiB: result %v, error %v; want it stopped at the memory limit", run.anyFinished(), err)
		}
		if !strings.Contains(err.Error(), "collection.manifest_read_memory_mb allows (48 MiB)") {
			t.Errorf("the error does not give the setting and its effective value: %v", err)
		}
		// With room, the same clone is read.
		roomy := limitsFromConfig(t, `"manifest_read_memory_mb": 2048`)
		run, err = runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5}, roomy, nil, quiet)
		if err != nil || len(run.decl.Deps) < 500_000 {
			t.Fatalf("the same clone under 2 GiB: %v", err)
		}
	})
	// There is no child without limits, and no configuration without them.
	for _, bad := range []string{`"manifest_read_cpu_seconds": 0`, `"manifest_read_cpu_seconds": -1`, `"manifest_read_memory_mb": 0`, `"manifest_read_memory_mb": -5`,
		`"manifest_read_cpu_seconds": 9223372037`, `"manifest_read_memory_mb": 8796093022208`} {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(`{"collection":{`+bad+`}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := config.Load(path); err == nil || !strings.Contains(err.Error(), "manifest_read_") {
			t.Errorf("%s: loaded (%v); want it refused, naming the key", bad, err)
		}
	}
	if d := (ManifestLimits{}).effective(); d.CPU != config.DefaultManifestReadCPUSeconds*time.Second || d.HeapBytes != config.DefaultManifestReadMemoryMB<<20 {
		t.Errorf("a collector given no limits uses %+v; want the configuration's defaults", d)
	}
}

// The whole phase: an analysis whose manifests pass the limit writes
// nothing, keeps the previous inventory, and records why.
func TestAnAnalysisStoppedAtTheLimitKeepsItsInventoryAndSaysWhy(t *testing.T) {
	f := newWholeReplaceFixture(t, "limit", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(npmAnswer)) })
	ctx := context.Background()
	f.manifest(t, map[string]string{"limit-kept-a": "1.0.0"})
	var result AnalysisResult
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, f.ac.readClone(ctx, f.repoID, f.dir), nil, &result); err != nil {
		t.Fatal(err)
	}
	before := f.inventory(t)
	if len(before) != 1 {
		t.Fatalf("the first analysis stored %v", before)
	}
	// The next analysis meets a build that takes tens of seconds.
	for rel, content := range slowBuild() {
		p := filepath.Join(f.dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	SetManifestLimits(limitsFromConfig(t, `"manifest_read_cpu_seconds": 1`))
	t.Cleanup(func() { installedManifestLimits.Store(nil) })
	err := f.ac.scanLibyear(ctx, f.repoID, f.dir, f.ac.readClone(ctx, f.repoID, f.dir), nil, &result)
	var child *errManifestChild
	if !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadCPULimit {
		t.Fatalf("the analysis returned %v; want the CPU limit", err)
	}
	if after := f.inventory(t); !reflect.DeepEqual(after, before) {
		t.Errorf("the inventory changed: %v, was %v", after, before)
	}
	cov, found, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil || !found || cov.LastAttemptResult != model.InventoryAttemptReadCPULimit {
		t.Errorf("the coverage record says %q (found %v, %v); want %q", cov.LastAttemptResult, found, err, model.InventoryAttemptReadCPULimit)
	}
	// And the store takes every reason the model lists.
	for _, r := range model.InventoryAttemptResults() {
		if err := f.store.RecordInventoryAttempt(ctx, f.repoID, r); err != nil {
			t.Errorf("the store refuses the attempt result %q: %v", r, err)
		}
	}
	if err := f.store.RecordInventoryAttempt(ctx, f.repoID, "made_up"); err == nil {
		t.Error("the store took an attempt result the model does not list")
	}
}

// The knobs of the analysis reach the child's readers, and its walks do not
// follow a symbolic link out of the clone.
func TestTheChildReadsUnderTheAnalysisKnobs(t *testing.T) {
	dir := writeClone(t, map[string]string{
		".github/workflows/ci.yml": "jobs:\n  b:\n    steps:\n      - uses: actions/checkout@v4\n",
		"requirements-dev.txt":     "pytest==8.0.0\n",
		"requirements.txt":         "requests==2.31.0\n",
	})
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	names := func(req manifestRequest) []string {
		req.Passes, req.Dir, req.RemotePomLimit = []string{manifestPassDeclarations}, dir, 5
		run, err := runManifestChild(context.Background(), req, testLimits(), nil, quiet)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, d := range run.decl.Deps {
			out = append(out, d.Name)
		}
		sort.Strings(out)
		return out
	}
	if got := names(manifestRequest{}); !reflect.DeepEqual(got, []string{"requests"}) {
		t.Errorf("both knobs off: %v", got)
	}
	if got := names(manifestRequest{DevBuildDeps: true}); !reflect.DeepEqual(got, []string{"pytest", "requests"}) {
		t.Errorf("dev_build_deps on: %v", got)
	}
	if got := names(manifestRequest{GitHubActionsDeps: true}); !reflect.DeepEqual(got, []string{"actions/checkout", "requests"}) {
		t.Errorf("github_actions_deps on: %v", got)
	}
}

func TestTheManifestWalksDoNotFollowSymlinks(t *testing.T) {
	outside := filepath.Join(writeClone(t, map[string]string{"secret.txt": "stolen-package==6.6.6\n"}), "secret.txt")
	dir := writeClone(t, map[string]string{"sub/requirements.txt": "real-package==1.0\n"})
	if err := os.Symlink(outside, filepath.Join(dir, "requirements.txt")); err != nil {
		t.Fatalf("a symbolic link could not be made: %v", err)
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5}, testLimits(), nil, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if len(run.decl.Deps) != 1 || run.decl.Deps[0].Name != "real-package" {
		t.Errorf("the declarations: %+v; want the one inside the clone", run.decl.Deps)
	}
	nrun, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassNames}, Dir: dir}, testLimits(), nil, quiet)
	if err != nil {
		t.Fatal(err)
	}
	for _, counts := range nrun.names {
		if counts["stolen-package"] != 0 || counts["real-package"] != 1 {
			t.Errorf("the names: %v", nrun.names)
		}
	}
}

// The limit on the poms one analysis asks for reaches the child's walk: a
// build whose imports are another coordinate in every module asks for that
// many and no more, and its tree is reported not whole.
func TestTheLimitOnPomsAskedForReachesTheChildsWalk(t *testing.T) {
	const k, limit = 12, 7
	files := map[string]string{}
	var parent strings.Builder
	parent.WriteString("<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>")
	for i := 0; i < k; i++ {
		fmt.Fprintf(&parent, "<dependency><groupId>org.x.${project.artifactId}</groupId><artifactId>b%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", i)
	}
	parent.WriteString("</dependencies></dependencyManagement></project>")
	files["pom.xml"] = parent.String()
	for m := 0; m < k; m++ {
		files[fmt.Sprintf("m%d/pom.xml", m)] = fmt.Sprintf("<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>m%d</artifactId>"+
			"<dependencies><dependency><groupId>g</groupId><artifactId>a</artifactId></dependency></dependencies></project>", m)
	}
	dir := writeClone(t, files)
	src := &mapPomSource{other: manifest.RemoteNotFound}
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: limit}, testLimits(), src, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if len(src.asked) != limit || run.decl.RemoteNotAsked == 0 || len(run.decl.NotWhole) != 1 {
		t.Errorf("the analysis was asked for %d poms (the limit is %d); %d were not asked for; %d trees not whole (want 1)", len(src.asked), limit, run.decl.RemoteNotAsked, len(run.decl.NotWhole))
	}
	// A dependency behind an import that was not asked about has no version
	// and says the lookup failed: not asked is not "not found".
	reasons := map[string]int{}
	for _, d := range run.decl.Deps {
		if d.Name == "g:a" {
			reasons[d.VersionReason]++
		}
	}
	if reasons[model.VersionReasonInheritedLookupFailed] == 0 {
		t.Errorf("no dependency says its lookup failed: %v", reasons)
	}
	// production gives the walk the collector's own limit.
	if code := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) readClone(")); !strings.Contains(code, "RemotePomLimit: maxRemotePomCoordinates") {
		t.Error("readClone does not give the manifest reader maxRemotePomCoordinates")
	}
}

// A clone whose root cannot be read is the root's failure, as it was.
func TestAMissingCloneIsARootFailure(t *testing.T) {
	missing := filepath.Join(writeClone(t, nil), "no-such-directory")
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: missing, RemotePomLimit: 5}, testLimits(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("the reader itself failed: %v", err)
	}
	for _, pass := range manifestPassesAll {
		var root *errManifestRoot
		if err := run.passErr(pass, nil); !errors.As(err, &root) {
			t.Errorf("%s of a clone that is not there: %v; want the root's failure", pass, err)
		}
	}
}

// anyFinished: the run holds a pass's result.
func (r *manifestRun) anyFinished() bool { return r != nil && len(r.finished) > 0 }
