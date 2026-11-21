// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/manifest"
)

// The largest honest builds the review built, read by the real child under
// the DEFAULT limits. They are what the defaults are derived from
// (config.DefaultManifestReadCPUSeconds, DefaultManifestReadMemoryMB), and
// what the allowances of 0.29.210 to 0.29.212 refused parts of.

// honestReactor is a Maven reactor of n modules under twenty aggregators,
// whose root inherits from a fetched parent that manages a thousand
// artifacts by property and imports twenty BOMs, and manages its own modules
// by reference; each module declares fifteen dependencies with no version.
func honestReactor(n int) (files map[string]string, src *mapPomSource, managed int) {
	src = &mapPomSource{poms: map[string]*manifest.RemotePom{}, other: manifest.RemoteNotFound}
	parent := &manifest.RemotePom{Group: "org.boot", Artifact: "boot-dependencies", Version: "3.2.0", Properties: map[string]string{}, ServedBy: "repo.example"}
	for i := 0; i < 300; i++ {
		parent.Properties[fmt.Sprintf("lib%d.version", i)] = fmt.Sprintf("%d.0.%d", i%9+1, i)
	}
	for i := 0; i < 1000; i++ {
		parent.Managed = append(parent.Managed, manifest.RemoteManaged{Group: fmt.Sprintf("org.lib%d", i%300), Artifact: fmt.Sprintf("art%d", i), Version: fmt.Sprintf("${lib%d.version}", i%300)})
	}
	for b := 0; b < 20; b++ {
		v := fmt.Sprintf("%d.0.%d", b%9+1, b)
		parent.Managed = append(parent.Managed, manifest.RemoteManaged{Group: fmt.Sprintf("org.bom%d", b), Artifact: "bom", Version: fmt.Sprintf("${lib%d.version}", b), Import: true})
		bom := &manifest.RemotePom{Group: fmt.Sprintf("org.bom%d", b), Artifact: "bom", Version: v, ServedBy: "repo.example"}
		for i := 0; i < 150; i++ {
			bom.Managed = append(bom.Managed, manifest.RemoteManaged{Group: bom.Group, Artifact: fmt.Sprintf("m%d", i), Version: v})
		}
		src.poms[bom.Group+":bom:"+v] = bom
	}
	src.poms["org.boot:boot-dependencies:3.2.0"] = parent

	files = map[string]string{}
	var root strings.Builder
	root.WriteString("<project><parent><groupId>org.boot</groupId><artifactId>boot-dependencies</artifactId><version>3.2.0</version><relativePath/></parent>" +
		"<groupId>com.acme</groupId><artifactId>acme-parent</artifactId><version>4.5.6</version><packaging>pom</packaging><modules>")
	for a := 0; a < 20; a++ {
		fmt.Fprintf(&root, "<module>agg%d</module>", a)
	}
	root.WriteString("</modules><dependencyManagement><dependencies>")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&root, "<dependency><groupId>${project.groupId}</groupId><artifactId>mod%d</artifactId><version>${project.version}</version></dependency>", i)
	}
	root.WriteString("</dependencies></dependencyManagement></project>")
	files["pom.xml"] = root.String()
	for a := 0; a < 20; a++ {
		var agg strings.Builder
		fmt.Fprintf(&agg, "<project><parent><groupId>com.acme</groupId><artifactId>acme-parent</artifactId><version>4.5.6</version></parent><artifactId>agg%d</artifactId><packaging>pom</packaging><modules>", a)
		for i := a; i < n; i += 20 {
			fmt.Fprintf(&agg, "<module>mod%d</module>", i)
		}
		agg.WriteString("</modules></project>")
		files[fmt.Sprintf("agg%d/pom.xml", a)] = agg.String()
	}
	dep := func(g, a string) string {
		return "<dependency><groupId>" + g + "</groupId><artifactId>" + a + "</artifactId></dependency>"
	}
	for i := 0; i < n; i++ {
		var m strings.Builder
		fmt.Fprintf(&m, "<project><parent><groupId>com.acme</groupId><artifactId>agg%d</artifactId><version>4.5.6</version></parent><artifactId>mod%d</artifactId><dependencies>", i%20, i)
		for j := 0; j < 8; j++ {
			k := (i*11 + j*17) % 1000
			m.WriteString(dep(fmt.Sprintf("org.lib%d", k%300), fmt.Sprintf("art%d", k)))
			managed++
		}
		for j := 0; j < 6; j++ {
			m.WriteString(dep(fmt.Sprintf("org.bom%d", (i+j)%20), fmt.Sprintf("m%d", (i*3+j)%150)))
			managed++
		}
		if i > 0 {
			m.WriteString(dep("com.acme", fmt.Sprintf("mod%d", i-1)))
			managed++
		}
		m.WriteString("</dependencies></project>")
		files[fmt.Sprintf("agg%d/mod%d/pom.xml", i%20, i)] = m.String()
	}
	return files, src, managed
}

// honestGradle is n Gradle projects under a root whose subprojects block
// applies one platform, a pom of the clone that imports thirty BOMs of the
// clone; each project declares two dependencies the platform's BOMs manage
// and eight that nothing manages. (Review round 6: under the allowances,
// 1,486 of 3,000 such build files were refused; before them, none.)
func honestGradle(n int) (files map[string]string, managed, unmanaged int) {
	files = map[string]string{
		"settings.gradle": "rootProject.name = 'big'\n",
		"build.gradle":    "subprojects {\n  dependencies {\n    implementation platform('com.acme:platform:1.0')\n  }\n}\n",
	}
	var platform strings.Builder
	platform.WriteString("<project><groupId>com.acme</groupId><artifactId>platform</artifactId><version>1.0</version><packaging>pom</packaging><dependencyManagement><dependencies>")
	for b := 0; b < 30; b++ {
		fmt.Fprintf(&platform, "<dependency><groupId>com.acme</groupId><artifactId>bom%d</artifactId><version>1.0</version><type>pom</type><scope>import</scope></dependency>", b)
		var bom strings.Builder
		fmt.Fprintf(&bom, "<project><groupId>com.acme</groupId><artifactId>bom%d</artifactId><version>1.0</version><packaging>pom</packaging><dependencyManagement><dependencies>", b)
		for i := 0; i < 50; i++ {
			fmt.Fprintf(&bom, "<dependency><groupId>org.b%d</groupId><artifactId>a%d</artifactId><version>%d.%d</version></dependency>", b, i, b, i)
		}
		bom.WriteString("</dependencies></dependencyManagement></project>")
		files[fmt.Sprintf("platform/bom%d/pom.xml", b)] = bom.String()
	}
	platform.WriteString("</dependencies></dependencyManagement></project>")
	files["platform/pom.xml"] = platform.String()
	for p := 0; p < n; p++ {
		var b strings.Builder
		b.WriteString("dependencies {\n")
		for j := 0; j < 2; j++ {
			fmt.Fprintf(&b, "  implementation 'org.b%d:a%d'\n", (p+j)%30, (p*7+j)%50)
			managed++
		}
		for j := 0; j < 8; j++ {
			fmt.Fprintf(&b, "  implementation 'org.nobody%d:thing%d'\n", j, p)
			unmanaged++
		}
		b.WriteString("}\n")
		files[fmt.Sprintf("p%d/build.gradle", p)] = b.String()
	}
	return files, managed, unmanaged
}

func TestTheLargestHonestBuildsAreFarInsideTheDefaultLimits(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	defaults := ManifestLimits{}.effective()
	check := func(name string, run *manifestRun, took time.Duration) {
		t.Helper()
		t.Logf("%s: %d manifests, %d declarations; the child used %s of CPU and at most %d MiB of heap, in %s by the clock",
			name, run.decl.Found, len(run.decl.Deps), run.cpu.Round(time.Millisecond), run.heapPeak>>20, took.Round(time.Millisecond))
		if len(run.decl.Failed) != 0 || run.decl.Found != run.decl.Parsed {
			t.Errorf("%s: %d of %d manifests were not read: %+v", name, len(run.decl.Failed), run.decl.Found, run.decl.Failed[:min(3, len(run.decl.Failed))])
		}
		// The margins the defaults are derived with. (Not under the race
		// detector, whose slowdown is several times and not the build's.)
		if manifestRaceEnabled {
			return
		}
		if run.cpu*honestCPUMargin > defaults.CPU {
			t.Errorf("%s used %s of CPU time: more than a %dth of the default limit (%s)", name, run.cpu, honestCPUMargin, defaults.CPU)
		}
		if run.heapPeak*honestHeapMargin > defaults.HeapBytes {
			t.Errorf("%s held %d MiB: more than a %dth of the default limit (%d MiB)", name, run.heapPeak>>20, honestHeapMargin, defaults.HeapBytes>>20)
		}
	}

	files, src, wantManaged := honestReactor(2000)
	dir := writeClone(t, files)
	began := time.Now()
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: maxRemotePomCoordinates}, ManifestLimits{}, src, quiet)
	if err != nil {
		t.Fatalf("the reactor: %v", err)
	}
	check("a Maven reactor of 2,000 modules", run, time.Since(began))
	got := 0
	for _, d := range run.decl.Deps {
		if d.VersionSource == manifest.SourceManaged && d.Version != "" {
			got++
		}
	}
	if got != wantManaged || len(run.decl.NotWhole) != 0 || run.decl.RemoteNotAsked != 0 {
		t.Errorf("the reactor: %d dependencies took a managed version, want %d; %d trees not whole; %d poms not asked for", got, wantManaged, len(run.decl.NotWhole), run.decl.RemoteNotAsked)
	}
	// What an honest build asks repositories for is far inside the limit on
	// that too.
	if len(src.asked)*honestRequestMargin > maxRemotePomCoordinates {
		t.Errorf("the reactor asked for %d poms: more than a %dth of the %d one analysis may ask for", len(src.asked), honestRequestMargin, maxRemotePomCoordinates)
	}
	t.Logf("the reactor asked for %d poms", len(src.asked))

	gfiles, gManaged, gUnmanaged := honestGradle(3000)
	dir = writeClone(t, gfiles)
	began = time.Now()
	run, err = runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: maxRemotePomCoordinates}, ManifestLimits{}, nil, quiet)
	if err != nil {
		t.Fatalf("the Gradle build: %v", err)
	}
	check("a Gradle build of 3,000 projects", run, time.Since(began))
	with, without := 0, 0
	for _, d := range run.decl.Deps {
		if d.Manager != "maven" || strings.HasPrefix(d.ManifestPath, "platform/") || d.ManifestPath == "build.gradle" {
			continue
		}
		if d.Version != "" {
			with++
		} else {
			without++
		}
	}
	if with != gManaged || without != gUnmanaged {
		t.Errorf("the Gradle build: %d dependencies have a version and %d have none; want %d and %d", with, without, gManaged, gUnmanaged)
	}
	if defaults.CPU != config.DefaultManifestReadCPUSeconds*time.Second {
		t.Errorf("the defaults in force are %+v", defaults)
	}
}

// The margins: an honest build of the largest size measured uses no more
// than a fiftieth of the default CPU limit, a tenth of the default memory
// limit and a tenth of the limit on poms asked for.
const (
	honestCPUMargin     = 50
	honestHeapMargin    = 10
	honestRequestMargin = 10
)

// What a lockfile costs the child to hold, as a multiple of its size
// (0.29.213). The memory default is argued from what the formats cost per
// byte (config.DefaultManifestReadMemoryMB): the limit reads a lockfile of
// the limit divided by its format's multiple. A parser that comes to hold
// twice as much per byte halves what the default reads, and fails here.
//
// The claim is tested in its own terms: an honest lockfile is read by the
// real child under a memory limit of lockfileHeapFactors times the file's
// size, and must be read. The pnpm-lock.yaml is 8 MB; the package-lock.json
// is 24 MB, because 8 MB of it is read in less than the tenth of a second
// between two looks of the child's watchdog, and a parser that held twice
// as much was stopped two times in three. (Not "the heap peak under the default
// limit is below a number": that peak is the largest of ten samples a
// second, of a heap in which nothing presses the collector, and for these
// files it came out anywhere from 1.0 to 1.9 times the file for the same
// reading. Under a limit the collector is pressed to it, and what decides
// is what the parser really holds.)
//
// PROVISIONAL (summary/71, section 8). Each factor is the least multiple
// under which the file was read three times of three (an Apple M-series
// machine), and a quarter more, rounded up:
//
//	pnpm-lock.yaml     read at 13 times its size, stopped at 11; factor 16.
//	                   Holding the parsed document twice needs about 25.
//	package-lock.json  read at 2 times its size, stopped at 1; factor 2.5.
//	                   Holding the decoded document twice needs about 3
//	                   (the text, and the document twice over).
var lockfileHeapFactors = map[string]float64{
	"pnpm-lock.yaml":    16,
	"package-lock.json": 2.5,
}

func honestLockfile(kind string, size int) string {
	switch kind {
	case "pnpm-lock.yaml":
		var snaps strings.Builder
		snaps.WriteString("\nsnapshots:\n")
		packages := fill(size*5/8, "lockfileVersion: '9.0'\n\nimporters:\n  .:\n    dependencies:\n      pkg-0:\n        specifier: ^1.0.0\n        version: 1.0.0\n\npackages:\n", "", func(i int) string {
			fmt.Fprintf(&snaps, "\n  '@scope%d/pkg-%d@%d.%d.%d':\n    dependencies:\n      pkg-%d: 1.2.3\n      '@scope%d/pkg-%d': 2.0.0\n", i%500, i, i%9, i%20, i%30, i+1, i%500, i+2)
			return fmt.Sprintf("\n  '@scope%d/pkg-%d@%d.%d.%d':\n    resolution: {integrity: sha512-%064d%022d==}\n    engines: {node: '>=14'}\n", i%500, i, i%9, i%20, i%30, i, i)
		})
		return packages + snaps.String()
	case "package-lock.json":
		return fill(size, "{\n  \"name\": \"app\",\n  \"lockfileVersion\": 3,\n  \"packages\": {\n    \"\": {\"name\":\"app\",\"dependencies\":{\"pkg-0\":\"^1.0.0\"}}", "\n  }\n}\n", func(i int) string {
			return fmt.Sprintf(",\n    \"node_modules/@scope%d/pkg-%d\": {\n      \"version\": \"%d.%d.%d\",\n      \"resolved\": \"https://registry.npmjs.org/@scope%d/pkg-%d/-/pkg-%d-%d.%d.%d.tgz\",\n      \"integrity\": \"sha512-%064d%022d==\",\n      \"dev\": %v,\n      \"license\": \"MIT\",\n      \"dependencies\": {\n        \"pkg-%d\": \"^1.2.3\",\n        \"@scope%d/pkg-%d\": \"~2.0.0\",\n        \"pkg-%d\": \">=3\"\n      },\n      \"engines\": {\n        \"node\": \">=14\"\n      }\n    }",
				i%500, i, i%9, i%20, i%30, i%500, i, i, i%9, i%20, i%30, i, i, i%2 == 0, i+1, i%500, i+2, i+3)
		})
	}
	panic("no such lockfile: " + kind)
}

func TestAnHonestLockfileIsHeldInAKnownMultipleOfItsSize(t *testing.T) {
	sizes := map[string]int{"pnpm-lock.yaml": 8 << 20, "package-lock.json": 24 << 20}
	wantPackages := 10_000
	if manifestRaceEnabled {
		// The race detector makes this reading thirty times slower and
		// puts its own memory in the heap, so nothing is judged under it:
		// the same path is run on an eighth of each file.
		for kind := range sizes {
			sizes[kind] /= 8
		}
		wantPackages /= 8
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for kind, factor := range lockfileHeapFactors {
		body := honestLockfile(kind, sizes[kind])
		dir := writeClone(t, map[string]string{kind: body})
		read := func(what string, limits ManifestLimits) {
			t.Helper()
			began := time.Now()
			run, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: maxRemotePomCoordinates}, limits, nil, quiet)
			if err != nil {
				t.Errorf("%s of %.1f MB, %s: %v", kind, float64(len(body))/(1<<20), what, err)
				return
			}
			if lerr := run.passErr(manifestPassLockfiles, nil); lerr != nil || len(heldLockfiles(*run.locks)) != 1 || len(heldLockfiles(*run.locks)[0].Result.Entries) < wantPackages {
				t.Fatalf("%s, %s: %v, %d lockfiles", kind, what, lerr, len(heldLockfiles(*run.locks)))
			}
			t.Logf("%s of %.1f MB, %d packages, %s: heap samples at most %.1f times the file, %d MiB resident at most, %s of CPU; %s by the clock",
				kind, float64(len(body))/(1<<20), len(heldLockfiles(*run.locks)[0].Result.Entries), what, float64(run.heapPeak)/float64(len(body)), run.maxRSS>>20, run.cpu.Round(time.Millisecond), time.Since(began).Round(time.Millisecond))
		}
		read("under the default limits", ManifestLimits{})
		if manifestRaceEnabled {
			continue // the race detector's own memory is in the heap that is limited
		}
		read(fmt.Sprintf("under a memory limit of %.1f times its size", factor), ManifestLimits{HeapBytes: int64(factor * float64(len(body)))})
	}
}
