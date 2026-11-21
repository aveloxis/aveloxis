// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Review round 6 (M6-10): a reference for the Gradle tree that shares
// NOTHING with the reader it checks but the text of the files.
//
// The reference of round 5 (round5_oracle_test.go) reads a tree naively but
// reads each FILE with the reader itself, so a mistake in reading a file, a
// catalogue or a properties file is one it makes too, and it knows nothing
// of platforms. This one reads a small dialect of Gradle, which its own
// generator writes and nothing else does, with line patterns of its own:
//
//	gradle.properties            name=value
//	settings.gradle[.kts]        marks the root of a build
//	gradle/libs.versions.toml    [versions] and [libraries], plain and rich
//	buildSrc/.../X.kt            object X { const val name = "value" }
//	*.gradle                     ext.name = 'value' | "…${name}…"
//	                             apply from: "$rootDir/gradle/s.gradle" | 'rel.gradle'
//	                             subprojects { dependencies { implementation platform(…) } }
//	                             dependencies { implementation … }
//
// and it works everything out again for every file: a scope is a map,
// copied; nothing is remembered. What a declaration's version is, it
// decides by the rules as Gradle users know them, stated where they are
// applied below. Platforms are BOMs, read by the Maven reference
// (oraclePoms), which is independent too.

// refScope is what is known where a file is read.
type refScope struct {
	vars map[string]string     // name -> value
	libs map[string]refLibrary // accessor -> library
}

type refLibrary struct{ group, artifact, version string }

func (s refScope) copy() refScope {
	out := refScope{vars: map[string]string{}, libs: map[string]refLibrary{}}
	for k, v := range s.vars {
		out.vars[k] = v
	}
	for k, v := range s.libs {
		out.libs[k] = v
	}
	return out
}

type refGradle struct {
	root string
	poms *oraclePoms
	// withheld counts the answers a platform gave that were not taken
	// because one before it could not be read.
	withheld int
}

func (g *refGradle) text(path string) (string, bool) {
	data, err := os.ReadFile(path)
	return string(data), err == nil
}

var (
	refAssign   = regexp.MustCompile(`^ext\.(\w+) = (?:'([^']*)'|"([^"]*)")$`)
	refApply    = regexp.MustCompile(`^apply from: (?:'([^']*)'|"([^"]*)")$`)
	refDep      = regexp.MustCompile(`^implementation (.+)$`)
	refQuoted   = regexp.MustCompile(`^(?:'([^']*)'|"([^"]*)")$`)
	refPlatform = regexp.MustCompile(`^platform\((.+)\)$`)
	refRef      = regexp.MustCompile(`\$\{([^}]+)\}|\$(\w+)`)
	refConst    = regexp.MustCompile(`^const val (\w+) = "([^"]*)"$`)
)

// interpolate replaces $name and ${name}; ok is false when a name is not
// known. A single-quoted string is not interpolated (the caller says).
func (s refScope) interpolate(text string) (string, bool) {
	ok := true
	out := refRef.ReplaceAllStringFunc(text, func(ref string) string {
		name := strings.TrimSuffix(strings.Trim(ref, "${}"), ".get()")
		v, known := s.vars[name]
		if !known {
			ok = false
		}
		return v
	})
	return out, ok
}

// rootOf is the root of the build dir is in: the nearest directory at or
// above it with a settings file, or the clone's root.
func (g *refGradle) rootOf(dir string) string {
	for d := dir; ; d = filepath.Dir(d) {
		for _, name := range []string{"settings.gradle", "settings.gradle.kts"} {
			if _, err := os.Stat(filepath.Join(d, name)); err == nil {
				return d
			}
		}
		if d == g.root {
			return g.root
		}
	}
}

// refApplyDepth is how deep a script applied by a script is followed (the
// reader documents eight).
const refApplyDepth = 8

// run reads the statements of one .gradle file into the scope, in order,
// and returns the lines of its dependencies blocks: its own, and the ones
// it gives every project below (subprojects).
//
// The rules: the scripts a file applies are read first, in the order
// written, each once per file; a script's own applied scripts before its
// own assignments. An assignment whose value names something not known
// assigns nothing. An assignment is known to the whole file it is in (a
// build file's dependencies may be written above it).
func (g *refGradle) run(path string, s *refScope, depth int, applied map[string]bool) (own, forSubprojects []string) {
	content, ok := g.text(path)
	if !ok {
		return nil, nil
	}
	dir := filepath.Dir(path)
	lines := strings.Split(content, "\n")
	for _, raw := range lines {
		m := refApply.FindStringSubmatch(strings.TrimSpace(raw))
		if m == nil || depth >= refApplyDepth {
			continue
		}
		written := m[1] + m[2]
		var target string
		if rest, isRoot := strings.CutPrefix(written, "$rootDir/"); isRoot {
			target = filepath.Join(g.rootOf(dir), filepath.FromSlash(rest))
		} else {
			target = filepath.Join(dir, filepath.FromSlash(written))
		}
		if applied[target] {
			continue
		}
		applied[target] = true
		g.run(target, s, depth+1, applied)
	}
	block := ""
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		switch {
		case line == "subprojects {" || line == "dependencies {":
			block += "/" + strings.TrimSuffix(line, " {")
		case line == "}":
			block = block[:strings.LastIndex(block, "/")]
		case block == "":
			if m := refAssign.FindStringSubmatch(line); m != nil {
				value, known := m[2], true
				if m[3] != "" || strings.HasSuffix(line, `""`) {
					value, known = s.interpolate(m[3])
				}
				if known {
					s.vars[m[1]] = value
				}
			}
		case block == "/dependencies":
			own = append(own, line)
		case block == "/subprojects/dependencies":
			forSubprojects = append(forSubprojects, line)
		}
	}
	return own, forSubprojects
}

// catalogue reads gradle/libs.versions.toml of dir into the scope.
func (g *refGradle) catalogue(dir string, s *refScope) {
	content, ok := g.text(filepath.Join(dir, "gradle", "libs.versions.toml"))
	if !ok {
		return
	}
	// A rich version is the strict one, else the required one, else the
	// preferred one.
	rich := func(table string) string {
		for _, key := range []string{"strictly", "require", "prefer"} {
			if m := regexp.MustCompile(key + ` = "([^"]*)"`).FindStringSubmatch(table); m != nil {
				return m[1]
			}
		}
		return ""
	}
	section := ""
	versions := map[string]string{}
	var libraries []string
	for _, raw := range strings.Split(content, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "[") {
			section = line
			continue
		}
		alias, value, found := strings.Cut(line, " = ")
		if !found {
			continue
		}
		switch section {
		case "[versions]":
			if strings.HasPrefix(value, "{") {
				versions[alias] = rich(value)
			} else {
				versions[alias] = strings.Trim(value, `"`)
			}
			s.vars["libs.versions."+alias] = versions[alias]
		case "[libraries]":
			libraries = append(libraries, line)
		}
	}
	for _, line := range libraries {
		alias, value, _ := strings.Cut(line, " = ")
		var lib refLibrary
		if !strings.HasPrefix(value, "{") {
			parts := strings.Split(strings.Trim(value, `"`), ":")
			lib = refLibrary{group: parts[0], artifact: parts[1]}
			if len(parts) > 2 {
				lib.version = parts[2]
			}
		} else {
			module := regexp.MustCompile(`module = "([^"]*)"`).FindStringSubmatch(value)[1]
			lib.group, lib.artifact, _ = strings.Cut(module, ":")
			switch {
			case strings.Contains(value, "version.ref"):
				lib.version = versions[regexp.MustCompile(`version\.ref = "([^"]*)"`).FindStringSubmatch(value)[1]]
			case strings.Contains(value, "version = {"):
				lib.version = rich(value[strings.Index(value, "version = {"):])
			}
		}
		s.libs["libs."+alias] = lib
	}
}

// buildSrc reads the constants of dir's buildSrc: X.name for `const val
// name` in `object X`. A constant may be built from another, of any file.
func (g *refGradle) buildSrc(dir string, s *refScope) {
	files, _ := filepath.Glob(filepath.Join(dir, "buildSrc", "src", "main", "kotlin", "*.kt"))
	sort.Strings(files)
	raw := map[string]string{}
	var names []string
	for _, f := range files {
		content, _ := g.text(f)
		object := ""
		for _, l := range strings.Split(content, "\n") {
			l = strings.TrimSpace(l)
			if rest, ok := strings.CutPrefix(l, "object "); ok {
				object = strings.TrimSuffix(rest, " {")
			} else if m := refConst.FindStringSubmatch(l); m != nil {
				raw[object+"."+m[1]] = m[2]
				names = append(names, object+"."+m[1])
			}
		}
	}
	// Until nothing more settles: a constant whose references are known.
	for changed := true; changed; {
		changed = false
		for _, name := range names {
			if _, done := s.vars[name]; done {
				continue
			}
			if v, ok := s.interpolate(raw[name]); ok {
				s.vars[name] = v
				changed = true
			}
		}
	}
}

// at is what is known in dir before its own build file is read, and the
// platform lines the build files above give it: for each directory from the
// clone's root down to dir, its buildSrc, its gradle.properties and its
// catalogue; and between a directory and the next, the directory's own
// build file (with the scripts that applies), whose assignments the
// projects below see.
func (g *refGradle) at(dir string) (refScope, []string) {
	s := refScope{vars: map[string]string{}, libs: map[string]refLibrary{}}
	var inherited []string
	rel, _ := filepath.Rel(g.root, dir)
	steps := []string{g.root}
	if rel != "." {
		d := g.root
		for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
			d = filepath.Join(d, part)
			steps = append(steps, d)
		}
	}
	for i, d := range steps {
		g.buildSrc(d, &s)
		if content, ok := g.text(filepath.Join(d, "gradle.properties")); ok {
			for _, line := range strings.Split(content, "\n") {
				if name, value, found := strings.Cut(line, "="); found {
					s.vars[name] = value
				}
			}
		}
		g.catalogue(d, &s)
		if i == len(steps)-1 {
			break
		}
		// The build file of a directory above: read with what is known
		// there, and what it assigns is known below. Its subprojects'
		// platform lines are read with ITS scope.
		above := s.copy()
		_, lines := g.run(filepath.Join(d, "build.gradle"), &above, 0, map[string]bool{})
		for _, line := range lines {
			inherited = append(inherited, g.resolvePlatformLine(line, above))
		}
		s = above
	}
	return s, inherited
}

// resolvePlatformLine is a platform line as coordinates, written out: the
// scope it was written in decides what a catalogue accessor in it means.
func (g *refGradle) resolvePlatformLine(line string, s refScope) string {
	m := refDep.FindStringSubmatch(line)
	if m == nil {
		return ""
	}
	p := refPlatform.FindStringSubmatch(m[1])
	if p == nil {
		return ""
	}
	if q := refQuoted.FindStringSubmatch(p[1]); q != nil {
		return q[1] + q[2]
	}
	if lib, ok := s.libs[p[1]]; ok {
		return lib.group + ":" + lib.artifact + ":" + lib.version
	}
	return ""
}

// refNotOneVersion: a range, or nothing, is not a version to take.
func refNotOneVersion(v string) bool {
	return v == "" || strings.ContainsAny(v, "[]()+,")
}

// declared is what the build file at rel declares: every dependency of
// group g, in the order written.
func (g *refGradle) declared(rel string) []oracleAnswer {
	path := filepath.Join(g.root, filepath.FromSlash(rel))
	scope, inherited := g.at(filepath.Dir(path))
	lines, forSubprojects := g.run(path, &scope, 0, map[string]bool{})
	// The file's own platforms come first (those it gives its subprojects
	// are its own too), then the ones from above.
	var platforms []string
	for _, line := range append(append([]string{}, lines...), forSubprojects...) {
		if p := g.resolvePlatformLine(line, scope); p != "" {
			platforms = append(platforms, p)
		}
	}
	platforms = append(platforms, inherited...)

	// Each platform is a BOM: a pom of the clone with those coordinates, or
	// one a repository has. unknown[i] is why platform i's content is not
	// known ("" when it is, or when it is simply on no repository).
	type platform struct {
		ctx     *oracleCtx
		missing string
	}
	var boms []platform
	worst := ""
	for _, coord := range platforms {
		parts := strings.Split(coord, ":")
		if len(parts) != 3 || parts[2] == "" {
			continue
		}
		bom, ambiguous := g.poms.byCoordinates(parts[0], parts[1], parts[2])
		reason, unknown := "", ""
		if ambiguous {
			unknown = "inherited_not_found"
		} else if bom == nil {
			bom, reason, unknown = g.poms.remote(parts[0], parts[1], parts[2])
		}
		worst = oracleWorse(worst, reason)
		if bom == nil {
			if unknown != "" {
				boms = append(boms, platform{missing: unknown})
			}
			continue
		}
		c := g.poms.ctx(bom)
		// Everything that could not be obtained anywhere in this BOM.
		q := &oracleQuestion{searched: map[string]bool{}, gap: c.gap}
		c.lookup(q, "\x00", "\x00", 0)
		worst = oracleWorse(worst, q.gap)
		boms = append(boms, platform{ctx: c})
	}

	var out []oracleAnswer
	add := func(group, artifact, version string, written bool) {
		if group != "g" {
			return
		}
		a := oracleAnswer{Name: group + ":" + artifact, Version: version, Hosts: []string{}}
		if refNotOneVersion(a.Version) {
			a.Version = ""
		}
		if a.Version == "" && !written {
			// No version written: the platforms, all of them together
			// (round 7). Gradle takes the highest version any of them
			// recommends, so one whose answer is not known withholds the
			// answer wherever it is written, and so does one that manages
			// the dependency at something that is not one version.
			unknown, notOne := "", false
			var versions []string
			hosts := map[string][]string{}
			for _, p := range boms {
				if p.ctx == nil {
					unknown = oracleWorse(unknown, p.missing)
					continue
				}
				q := &oracleQuestion{searched: map[string]bool{}, gap: p.ctx.gap}
				v, found, via, stop := p.ctx.lookup(q, group, artifact, 0)
				unknown = oracleWorse(unknown, q.before)
				switch {
				case stop, found && refNotOneVersion(v):
					notOne = true
				case found:
					if _, seen := hosts[v]; !seen {
						versions = append(versions, v)
					}
					hosts[v] = oracleHosts(p.ctx.chain, append(append([]string{}, via...), hosts[v]...))
				}
			}
			switch {
			case unknown != "" && (len(versions) > 0 || notOne):
				a.Reason = unknown
				g.withheld++
			case notOne, len(versions) == 0:
				a.Reason = worst
			default:
				if v, ok := refHighest(versions); ok {
					a.Version, a.Source = v, "managed"
					a.Hosts = append([]string{}, hosts[v]...)
				} else {
					a.Reason = worst
				}
			}
		}
		out = append(out, a)
	}
	for _, line := range lines {
		m := refDep.FindStringSubmatch(line)
		if m == nil || refPlatform.MatchString(m[1]) {
			continue
		}
		arg := m[1]
		if q := refQuoted.FindStringSubmatch(arg); q != nil {
			// A coordinate: its version, when written, interpolated in a
			// double-quoted string; one that names something unknown is no
			// version, and no platform's either (it was written).
			coord, double := q[1]+q[2], q[2] != ""
			parts := strings.SplitN(coord, ":", 3)
			version, written := "", len(parts) == 3
			if written {
				version = parts[2]
				if double {
					v, known := scope.interpolate(version)
					if version = v; !known {
						version = ""
					}
				}
			}
			add(parts[0], parts[1], version, written)
			continue
		}
		// A name: a catalogue's library, or a constant that holds a
		// coordinate.
		if lib, ok := scope.libs[arg]; ok {
			add(lib.group, lib.artifact, lib.version, lib.version != "")
			continue
		}
		if v, ok := scope.vars[arg]; ok && strings.Contains(v, ":") {
			parts := strings.SplitN(v, ":", 3)
			version := ""
			if len(parts) == 3 {
				version = parts[2]
			}
			add(parts[0], parts[1], version, len(parts) == 3)
		}
	}
	return out
}

// refGradleTree writes one random clone in the dialect, with its source of
// poms, and names its build files.
func refGradleTree(rng *rand.Rand) (files map[string]string, builds []string, src *oracleSource) {
	pick := func(s []string) string { return s[rng.Intn(len(s))] }
	names := []string{"va", "vb", "vc", "vd", "ve"}
	literals := []string{"1.0", "2.0", "3.1", "4.2.0"}
	scripts := []string{"s1", "s2", "s3", "s4"}
	files = map[string]string{}
	maybe := func(p float64, rel, content string) {
		if rng.Float64() < p {
			files[rel] = content
		}
	}
	// A file assigns each name at most once, and never one it reads: what
	// a name means in a file is then one thing, whatever the order of its
	// lines.
	body := func(up string, withDeps bool) string {
		var sb strings.Builder
		perm := rng.Perm(len(names))
		assigned := map[string]bool{}
		for _, i := range perm[:rng.Intn(3)] {
			assigned[names[i]] = true
		}
		var read []string
		for _, n := range names {
			if !assigned[n] {
				read = append(read, n)
			}
		}
		for i, n := 0, rng.Intn(3); i < n; i++ {
			s := pick(scripts)
			switch rng.Intn(3) {
			case 0:
				sb.WriteString("apply from: \"$rootDir/gradle/" + s + ".gradle\"\n")
			case 1:
				sb.WriteString("apply from: '" + up + "gradle/" + s + ".gradle'\n")
			default:
				sb.WriteString("apply from: \"" + up + "gradle/" + s + ".gradle\"\n")
			}
		}
		var assigns []string
		for n := range assigned {
			assigns = append(assigns, n)
		}
		sort.Strings(assigns)
		writeAssigns := func(list []string) {
			for _, n := range list {
				switch rng.Intn(3) {
				case 0:
					sb.WriteString("ext." + n + " = '" + pick(literals) + "'\n")
				case 1:
					sb.WriteString("ext." + n + " = \"" + pick(literals) + "\"\n")
				default:
					sb.WriteString("ext." + n + " = \"${" + pick(read) + "}." + fmt.Sprint(rng.Intn(9)) + "\"\n")
				}
			}
		}
		half := len(assigns) / 2
		writeAssigns(assigns[:half])
		if withDeps {
			sb.WriteString("dependencies {\n")
			if rng.Intn(3) == 0 {
				sb.WriteString("  implementation platform('t:" + pick([]string{"bomA", "bomB", "gone1", "rb1", "bomR"}) + ":1')\n")
			}
			if rng.Intn(4) == 0 {
				sb.WriteString("  implementation platform('t:" + pick([]string{"bomB", "bomA", "gone2"}) + ":1')\n")
			}
			if rng.Intn(9) == 0 {
				// Round 7: a platform that is never asked for.
				sb.WriteString("  implementation platform('t:snap:1-SNAPSHOT')\n")
			}
			for i, n := range names {
				switch rng.Intn(5) {
				case 0:
					fmt.Fprintf(&sb, "  implementation \"g:d%d:$%s\"\n", i, n)
				case 1:
					fmt.Fprintf(&sb, "  implementation \"g:e%d:${%s}\"\n", i, n)
				case 2:
					fmt.Fprintf(&sb, "  implementation 'g:m%d'\n", i)
				case 3:
					fmt.Fprintf(&sb, "  implementation 'g:lit%d:%s'\n", i, pick(literals))
				}
			}
			if rng.Intn(2) == 0 {
				sb.WriteString("  implementation libs." + pick([]string{"x", "y", "rich", "nover"}) + "\n")
				sb.WriteString("  implementation \"g:z:${libs.versions.k.get()}\"\n")
			}
			if rng.Intn(3) == 0 {
				sb.WriteString("  implementation A.coord\n")
			}
			sb.WriteString("}\n")
		}
		writeAssigns(assigns[half:])
		return sb.String()
	}
	catalogue := func() string {
		version := pick([]string{`"` + pick(literals) + `"`, `{ require = "` + pick(literals) + `", prefer = "9.9" }`, `{ strictly = "` + pick(literals) + `", require = "8.8" }`, `{ prefer = "` + pick(literals) + `" }`})
		return "[versions]\nk = " + version + "\n[libraries]\nx = { module = \"g:x\", version.ref = \"k\" }\ny = \"g:y:" + pick(literals) + "\"\n" +
			"rich = { module = \"g:rich\", version = { strictly = \"" + pick(literals) + "\" } }\nnover = { module = \"g:m9\" }\nbom = \"t:" + pick([]string{"bomA", "bomB", "gone1"}) + ":1\"\n"
	}
	properties := func() string {
		var sb strings.Builder
		for _, n := range names {
			if rng.Intn(3) == 0 {
				sb.WriteString(n + "=" + pick(literals) + "\n")
			}
		}
		return sb.String()
	}
	maybe(0.7, "gradle.properties", properties())
	maybe(0.7, pick([]string{"settings.gradle", "settings.gradle.kts"}), "rootProject.name = 'r'\n")
	maybe(0.6, "gradle/libs.versions.toml", catalogue())
	rootBuild := body("", false)
	if rng.Intn(2) == 0 {
		rootBuild += "subprojects {\n  dependencies {\n    implementation platform(" + pick([]string{"'t:bomA:1'", "'t:bomB:1'", "libs.bom", "'t:gone1:1'", "'t:rb1:1'"}) + ")\n  }\n}\n"
	}
	maybe(0.8, "build.gradle", rootBuild)
	for _, s := range scripts {
		maybe(0.75, "gradle/"+s+".gradle", body("../", false))
	}
	if rng.Intn(3) == 0 {
		files["buildSrc/src/main/kotlin/A.kt"] = "object A {\n  const val coord = \"g:bs:${Z.v}\"\n}\n"
		maybe(0.8, "buildSrc/src/main/kotlin/Z.kt", "object Z {\n  const val v = \""+pick(literals)+"\"\n}\n")
	}
	for _, d := range []string{"pa", "pb", "pa/sub", "deep/x", "pc"} {
		up := strings.Repeat("../", strings.Count(d, "/")+1)
		maybe(0.3, d+"/gradle.properties", properties())
		files[d+"/build.gradle"] = body(up, true)
		builds = append(builds, d+"/build.gradle")
	}
	maybe(0.2, "pb/gradle/libs.versions.toml", catalogue())
	maybe(0.2, "pa/"+pick([]string{"settings.gradle", "settings.gradle.kts"}), "rootProject.name = 'nested'\n")
	maybe(0.3, "pa/gradle/s1.gradle", body("../../", false))
	sort.Strings(builds)

	// The platforms: BOMs of the clone, and what repositories have.
	entry := func(i int, version string) string { return r4dep("g", fmt.Sprintf("m%d", i), version) }
	bom := func(artifact string, extra string) string {
		s := "<project><groupId>t</groupId><artifactId>" + artifact + "</artifactId><version>1</version><dependencyManagement><dependencies>"
		for i := range names {
			if rng.Intn(2) == 0 {
				s += entry(i, pick([]string{"5.0", "6.1", "[1,2)", "7.0"}))
			}
		}
		if rng.Intn(3) == 0 {
			s += entry(9, "9.0")
		}
		return s + extra + "</dependencies></dependencyManagement></project>"
	}
	files["platforms/bomA/pom.xml"] = bom("bomA", "")
	imported := ""
	if rng.Intn(2) == 0 {
		imported = r4import("t", pick([]string{"bomA", "gone1", "rb1"}), "1")
	}
	files["platforms/bomB/pom.xml"] = bom("bomB", imported)
	if rng.Intn(5) == 0 {
		return files, builds, nil // no source: nothing outside the clone is asked
	}
	fetched := func(name string) *RemotePom {
		r := &RemotePom{Group: "t", Artifact: name, Version: "1", ServedBy: pick([]string{"repo.example", "other.example"})}
		for i := range names {
			if rng.Intn(2) == 0 {
				r.Managed = append(r.Managed, RemoteManaged{Group: "g", Artifact: fmt.Sprintf("m%d", i), Version: pick([]string{"8.0", "8.5"})})
			}
		}
		return r
	}
	src = &oracleSource{poms: map[string]*RemotePom{"t:rb1:1": fetched("rb1"), "t:bomR:1": fetched("bomR")}, outcomes: map[string]RemoteOutcome{}}
	for _, k := range []string{"t:gone1:1", "t:gone2:1", "t:rb1:1", "t:bomR:1"} {
		switch rng.Intn(6) {
		case 0, 1:
			src.outcomes[k] = RemoteFailed
		case 2:
			src.outcomes[k] = RemoteRefused
		case 3:
			src.outcomes[k] = RemoteNotFound
		case 4:
			src.outcomes[k] = RemoteNotAsked // round 7
		}
	}
	return files, builds, src
}

// refGradleTrees is how many random clones the test compares, each in
// three read orders.
const refGradleTrees = 600

func TestTheGradleTreeAnswersAsTheIndependentReferenceDoes(t *testing.T) {
	rng := rand.New(rand.NewSource(20261122))
	root := t.TempDir()
	readings, versions, managed, withheld, fromPlatformsAbove := 0, 0, 0, 0, 0
	for it := 0; it < refGradleTrees; it++ {
		files, builds, src := refGradleTree(rng)
		oracleWrite(t, root, files)
		ref := &refGradle{root: root, poms: &oraclePoms{root: root, decoded: map[string]*oraclePom{}}}
		if src != nil {
			ref.poms.src = src
		}
		want := map[string]string{}
		for _, rel := range builds {
			answers := ref.declared(rel)
			for _, a := range answers {
				if a.Version != "" {
					versions++
				}
				if a.Source != "" {
					managed++
				}
			}
			want[rel] = oracleJSON(answers)
		}
		withheld += ref.withheld
		if strings.Contains(files["build.gradle"], "subprojects {") {
			fromPlatformsAbove++
		}
		for order := 0; order < oracleOrders; order++ {
			seq := append([]string(nil), builds...)
			if order > 0 {
				rng.Shuffle(len(seq), func(i, j int) { seq[i], seq[j] = seq[j], seq[i] })
			}
			w := NewWalk(root, nil)
			if src != nil {
				w.SetPomSource(src)
			}
			for _, rel := range seq {
				got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
				readings++
				if got.Failure != nil {
					t.Fatalf("tree %d, read order %v: %s was not read: %v\n%s", it, seq, rel, got.Failure, oracleDump(files))
				}
				var answers []oracleAnswer
				for _, d := range got.Deps {
					if strings.HasPrefix(d.Name, "g:") {
						answers = append(answers, oracleAnswer{d.Name, d.Version, d.VersionSource, d.VersionReason, append([]string{}, d.VersionHosts...)})
					}
				}
				if g := oracleJSON(answers); g != want[rel] {
					t.Fatalf("tree %d, read order %v: %s\n the tree reader gives      %s\n the independent reference %s\n%s", it, seq, rel, g, want[rel], oracleDump(files))
				}
			}
		}
	}
	t.Logf("%d trees: %d readings compared; the reference read %d versions, %d of them from a platform, and withheld %d; %d trees give their projects a platform from above", refGradleTrees, readings, versions, managed, withheld, fromPlatformsAbove)
	if versions < 5*refGradleTrees || managed < refGradleTrees/2 || fromPlatformsAbove < refGradleTrees/4 || withheld < refGradleTrees/20 {
		t.Errorf("too little to compare: %d versions, %d from platforms, %d withheld, %d trees with a platform from above, over %d trees", versions, managed, withheld, fromPlatformsAbove, refGradleTrees)
	}
}

// refHighest is the highest of several versions when each is digits in
// groups separated by dots, compared as numbers group by group; a version
// the longer one begins with is the lower. Anything else is not ordered.
func refHighest(versions []string) (string, bool) {
	if len(versions) == 1 {
		return versions[0], true
	}
	num := func(v string) ([]int, bool) {
		var out []int
		for _, part := range strings.Split(v, ".") {
			n, err := strconv.Atoi(part)
			if err != nil || n < 0 || strings.TrimLeft(part, "0123456789") != "" {
				return nil, false
			}
			out = append(out, n)
		}
		return out, true
	}
	best := -1
	var bestN []int
	for i, v := range versions {
		n, ok := num(v)
		if !ok {
			return "", false
		}
		less := func(a, b []int) bool {
			for k := 0; k < len(a) && k < len(b); k++ {
				if a[k] != b[k] {
					return a[k] < b[k]
				}
			}
			return len(a) < len(b)
		}
		if best < 0 || less(bestN, n) {
			best, bestN = i, n
		}
	}
	return versions[best], true
}
