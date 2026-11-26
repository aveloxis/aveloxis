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
//
// Round 8: WHAT THE GRADLE REFERENCE BELIEVES about versions that BOMs
// manage. Until now the reference had the reader's own rule (one mechanism,
// the highest of plain numbers), and four findings were beliefs the two
// shared. The rules are written from the Gradle user manual ("Sharing
// dependency versions between projects: platforms", "Declaring versions and
// ranges", "Dependency constraints and conflict resolution") and the Spring
// dependency-management plugin's reference, AS REMEMBERED: Gradle cannot be
// run where this suite runs, and nothing here was checked against a build.
//
//	G1 VERIFIED-BY-DOCS-FROM-MEMORY. platform(bom) adds the BOM's managed
//	   versions as constraints (recommendations). Gradle settles all the
//	   versions asked of one module together and the HIGHEST wins, whatever
//	   the order written.
//	G2 VERIFIED-BY-DOCS-FROM-MEMORY. enforcedPlatform(bom) forces its
//	   versions over every other, a written one included. UNSURE: two
//	   enforced platforms that differ (taken here as not determined).
//	G3 VERIFIED-BY-DOCS-FROM-MEMORY. A version written on the dependency
//	   takes part in the same settling: a platform that recommends a higher
//	   one raises it, and an enforced platform's replaces it. Round 9:
//	   NEITHER is done. Which platform reaches which configuration, and a
//	   strict ("1.0!!") declaration, are not read, and a platform that is
//	   not known could raise it too; the written version is kept as what
//	   the build DECLARES (the inventory's meaning for every written
//	   version), as until 0.29.214. A known difference from what Gradle
//	   would resolve (DISAGREEMENT E1).
//	G4 VERIFIED-BY-DOCS-FROM-MEMORY. Version ordering: a version is split at
//	   . - _ + and where digits meet letters; numbers compare as numbers; a
//	   number is above a word; a version with a further numeric part is
//	   above, with a further word below ("1.0-rc" < "1.0" < "1.0.1");
//	   separators do not count. UNSURE: the full table of special words
//	   (dev lowest; rc < snapshot < final < ga < release < sp highest,
//	   without regard to case) and that other words compare by case-
//	   sensitive text.
//	G5 VERIFIED-BY-DOCS-FROM-MEMORY. Spring's dependency-management plugin
//	   is not G1: mavenBom imports are applied in order and a LATER one
//	   overrides an earlier one for the same artifact. A version given
//	   outright (dependencies { dependency 'g:a:v' }) overrides every import.
//	G6 VERIFIED-BY-DOCS-FROM-MEMORY. With the Spring Boot plugin and the
//	   dependency-management plugin both applied, spring-boot-dependencies
//	   of the Boot plugin's version is imported, when the plugins are
//	   applied: before the build's own imports, wherever the blocks are.
//	G7 UNSURE. The plugin leaves a version written on the dependency alone
//	   (its default; overrideable in the plugin's configuration, not read).
//	G8 UNSURE. When a platform and the plugin both manage a dependency with
//	   no version: the plugin sets the version, and the platform's
//	   constraint then takes part in conflict resolution, so the higher
//	   would win. That is not implemented on either side: the same version
//	   from both is taken, anything else is "platforms_disagree"
//	   (DISAGREEMENT E2: withheld where Gradle would answer).
//
//	G9 VERIFIED-BY-DOCS-FROM-MEMORY (the Spring Boot Gradle plugin's
//	   reference, "Customizing managed versions": ext['slf4j.version'] =
//	   '1.7.20'). Spring's plugin reads a BOM with the project's properties
//	   over the BOM's own, so a property of the project changes every
//	   version the BOM writes with it, and the version of a BOM it imports
//	   with it. Not so for platform(...) of the same BOM. UNSURE: that a
//	   property set in gradle.properties counts (taken here to: the
//	   dependency-management plugin's reference names it with the command
//	   line, as recalled); that the property reaches the BOMs a BOM
//	   imports (taken here not to: only the BOM named is read with it).
//	   Round 10: the READER takes a property the file itself sets only
//	   from an assignment at the file's top, made once, with a literal;
//	   set in any other way it gives the rows managed through it no
//	   version (value_not_read). The trees generated here set such a
//	   property only at the top, once, with a literal, so the reference
//	   and the reader agree on them; the other shapes are held by
//	   TestASpringPropertyIsTakenOnlyWhereTheFileSurelySetsIt alone.
//	G10 VERIFIED-BY-DOCS-FROM-MEMORY. What a subprojects { } block of a
//	   build file declares is for the projects below, not for the file's
//	   own project: a platform declared there does not manage the file's
//	   own dependencies. (Until round 9 both sides gave it to them.)
//	S1 VERIFIED-BY-DOCS-FROM-MEMORY for the plain case, which is all the
//	   dialect has (every assignment is "ext.name = ..." at the top of its
//	   file, every "apply from:" a line at the top of its file): Gradle
//	   runs a build file from the top, so a script applied AFTER the file
//	   assigned a variable assigns over it. The reference takes the
//	   script's value for a variable the file assigns in front of its
//	   FIRST "apply from:" line and a script it applies assigns. UNSURE,
//	   and not decided: an assignment between two "apply from:" lines
//	   (which script assigns is not looked at: the file's own value is
//	   kept, as it is after the last of them, where it is right). Only the
//	   build files (the one read and the ones above it) are read so; a
//	   script's own assignments stand over those of the scripts it applies
//	   (UNSURE: the same order would hold inside a script).
//
// What is not known withholds exactly where it could change the answer:
// any platform for G1 (it could be the highest), an enforced one for G2, and
// for G5 only a BOM imported AFTER the one that answers.
//
// DISAGREEMENTS that remain between the reference and the reader: none in
// the trees generated. Of both with Gradle: E1 and E2 above; E3: the
// configuration a platform is declared on is not looked at (a platform on
// testImplementation manages implementation dependencies here); E6: S1's
// undecided cases, and every assignment or "apply from:" that is under a
// condition, inside a block or not a property of the project, keep the
// file's own value, where Gradle may read the script's; E7: the plugin's
// overriddenByDependencies = false, which lets its version replace a written
// one, is not read; E8: a dotted property that is in truth a key of a map
// variable ("ext.x = [version: '1']" read as x.version) is taken for a
// property of the project by the reader (the dialect has no maps); E4: a
// build's own resolutionStrategy, constraints { } blocks, strictly/prefer on
// a declaration and BOM imports of a parent project's dependencyManagement
// are not read; E5: mavenBom imports given to subprojects from above are
// ordered before the project's own, which is right for subprojects { } and
// not checked for other ways of sharing.

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
	// notOrdered counts the variables S1 gave a script's value over the
	// file's own; overridden the
	// references a property of the project answered inside a BOM (G9).
	notOrdered, overridden int
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
	// Round 8.
	refEnforced   = regexp.MustCompile(`^implementation enforcedPlatform\('([^']*)'\)$`)
	refMavenBom   = regexp.MustCompile(`^dependencyManagement \{ imports \{ mavenBom '([^']*)' \} \}$`)
	refManagedDep = regexp.MustCompile(`^dependencyManagement \{ dependencies \{ dependency '([^:']*):([^:']*):([^']*)' \} \}$`)
	refBootPlugin = regexp.MustCompile(`^plugins \{ id 'org.springframework.boot' version '([^']*)' \}$`)
	refProperty   = regexp.MustCompile(`^ext\['([\w.\-]+)'\] = '([^']*)'$`)
	refPart       = regexp.MustCompile(`[0-9]+|[^0-9._+\-]+`)
	refRef        = regexp.MustCompile(`\$\{([^}]+)\}|\$(\w+)`)
	refConst      = regexp.MustCompile(`^const val (\w+) = "([^"]*)"$`)
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
//
// S1: byScripts collects what the scripts assign (the last value each name
// is given); a build file (depth 0) is read with it.
func (g *refGradle) run(path string, s *refScope, depth int, applied map[string]bool, byScripts map[string]string) (own, forSubprojects []string) {
	content, ok := g.text(path)
	if !ok {
		return nil, nil
	}
	dir := filepath.Dir(path)
	lines := strings.Split(content, "\n")
	firstApply := -1
	for i, raw := range lines {
		m := refApply.FindStringSubmatch(strings.TrimSpace(raw))
		if m != nil && firstApply < 0 {
			firstApply = i
		}
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
		g.run(target, s, depth+1, applied, byScripts)
	}
	block := ""
	for i, raw := range lines {
		line := strings.TrimSpace(raw)
		switch {
		case block == "" && (strings.HasPrefix(line, "dependencyManagement { ") || strings.HasPrefix(line, "plugins { ")):
			// One-line statements of the Spring plugin (G5, G6), kept in
			// the order written.
			own = append(own, line)
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
				script, by := byScripts[m[1]]
				switch {
				case !known:
				case depth > 0:
					s.vars[m[1]] = value
					byScripts[m[1]] = value
				case by && i < firstApply:
					s.vars[m[1]] = script // S1: the script ran after it
					if script != value {
						g.notOrdered++
					}
				default:
					s.vars[m[1]] = value
				}
			} else if m := refProperty.FindStringSubmatch(line); m != nil {
				s.vars[m[1]] = m[2] // G9: a property of the project
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
		_, lines := g.run(filepath.Join(d, "build.gradle"), &above, 0, map[string]bool{}, map[string]string{})
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
	lines, _ := g.run(path, &scope, 0, map[string]bool{}, map[string]string{})
	// The BOMs of the build, by mechanism. Gradle's platforms (G1, G2): the
	// file's own and those from above; their
	// order does not matter. The Spring plugin's (G5, G6): the Boot
	// plugin's first, then the imports as written.
	type bomRef struct {
		coord    string
		enforced bool
	}
	var platformRefs, springRefs []bomRef
	explicit := map[string]string{}
	boot, management := "", false
	// G10: the lines of the file's subprojects block are not for its own
	// project.
	for _, line := range lines {
		if m := refEnforced.FindStringSubmatch(line); m != nil {
			platformRefs = append(platformRefs, bomRef{m[1], true})
		} else if p := g.resolvePlatformLine(line, scope); p != "" {
			platformRefs = append(platformRefs, bomRef{p, false})
		}
		if m := refMavenBom.FindStringSubmatch(line); m != nil {
			springRefs = append(springRefs, bomRef{coord: m[1]})
		}
		if m := refManagedDep.FindStringSubmatch(line); m != nil {
			explicit[m[1]+":"+m[2]] = m[3]
		}
		if m := refBootPlugin.FindStringSubmatch(line); m != nil {
			boot = m[1]
		}
		management = management || line == "plugins { id 'io.spring.dependency-management' version '1.1.4' }"
	}
	for _, p := range inherited {
		platformRefs = append(platformRefs, bomRef{p, false})
	}
	if boot != "" && management {
		springRefs = append([]bomRef{{coord: "org.springframework.boot:spring-boot-dependencies:" + boot}}, springRefs...)
	}

	// Each is a BOM: a pom of the clone with those coordinates, or one a
	// repository has (the Maven reference's obtain). missing is why its
	// content is not known ("" for one that is simply on no repository,
	// which says nothing and is left out).
	type platform struct {
		ctx      *oracleCtx
		missing  string
		enforced bool
	}
	worst := ""
	// G9: the project's properties, for the BOMs Spring's plugin reads: the
	// names no variable of a build file can have.
	properties := map[string]string{}
	for name, value := range scope.vars {
		if strings.ContainsAny(name, ".-") {
			properties[name] = value
		}
	}
	load := func(refs []bomRef, spring bool) []platform {
		var boms []platform
		for _, r := range refs {
			parts := strings.Split(r.coord, ":")
			if len(parts) != 3 || parts[2] == "" {
				continue
			}
			bom, reason, unknown := g.poms.obtain(parts[0], parts[1], parts[2], nil, nil)
			worst = oracleWorse(worst, reason)
			if bom == nil {
				if unknown != "" {
					boms = append(boms, platform{missing: unknown, enforced: r.enforced})
				}
				continue
			}
			c := g.poms.ctx(bom)
			if spring {
				c.over, c.overHits = properties, &g.overridden
			}
			// Everything that could not be obtained anywhere in this BOM.
			q := &oracleQuestion{searched: map[string]bool{}, gap: c.gap}
			c.lookup(q, "\x00", "\x00", 0)
			worst = oracleWorse(worst, q.gap)
			boms = append(boms, platform{ctx: c, enforced: r.enforced})
		}
		return boms
	}
	springBoms := load(springRefs, true)
	platforms := load(platformRefs, false)

	// said is what a set of platforms says of one module.
	type said struct {
		versions []string
		hosts    map[string][]string
		unknown  string
		notOne   bool
	}
	ask := func(group, artifact string, enforcedOnly bool) said {
		s := said{hosts: map[string][]string{}}
		for _, p := range platforms {
			if enforcedOnly && !p.enforced {
				continue
			}
			if p.ctx == nil {
				s.unknown = oracleWorse(s.unknown, p.missing)
				continue
			}
			q := &oracleQuestion{searched: map[string]bool{}, gap: p.ctx.gap}
			v, found, via, stop := p.ctx.lookup(q, group, artifact, 0)
			s.unknown = oracleWorse(s.unknown, q.before)
			switch {
			case stop, found && refNotOneVersion(v):
				s.notOne = true
			case found:
				if _, seen := s.hosts[v]; !seen {
					s.versions = append(s.versions, v)
				}
				s.hosts[v] = oracleHosts(p.ctx.chain, append(append([]string{}, via...), s.hosts[v]...))
			}
		}
		return s
	}
	// verdict of one mechanism: "" nothing, "v" a version, "?" not known
	// (why), "-" managed at what is not one version.
	type verdict struct {
		kind, version, why string
		hosts              []string
	}
	settle := func(s said) verdict {
		switch {
		case s.unknown != "" && (len(s.versions) > 0 || s.notOne):
			return verdict{kind: "?", why: s.unknown}
		case s.notOne:
			return verdict{kind: "-"}
		case len(s.versions) == 0:
			return verdict{}
		}
		if v, ok := refHighest(s.versions); ok {
			return verdict{kind: "v", version: v, hosts: s.hosts[v]}
		}
		return verdict{kind: "?", why: "platforms_disagree"}
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
		defer func() { out = append(out, a) }()
		if a.Version != "" {
			return // G3: written, and it stands
		}
		if written {
			return // written with a version that could not be read
		}
		// G1, G2: the platforms.
		var byPlatforms verdict
		if enf := ask(group, artifact, true); enf.unknown != "" {
			byPlatforms = verdict{kind: "?", why: enf.unknown}
		} else if enf.notOne || len(enf.versions) > 0 {
			byPlatforms = settle(enf)
		} else {
			byPlatforms = settle(ask(group, artifact, false))
		}
		// G5: the version given outright, else the LAST BOM that manages it.
		var byPlugin verdict
		if v, given := explicit[group+":"+artifact]; given {
			byPlugin = verdict{kind: "v", version: v}
		} else {
			for k := len(springBoms) - 1; k >= 0 && byPlugin.kind == ""; k-- {
				p := springBoms[k]
				if p.ctx == nil {
					byPlugin = verdict{kind: "?", why: p.missing}
					break
				}
				q := &oracleQuestion{searched: map[string]bool{}, gap: p.ctx.gap}
				v, found, via, stop := p.ctx.lookup(q, group, artifact, 0)
				switch {
				case q.before != "":
					byPlugin = verdict{kind: "?", why: q.before}
				case stop, found && refNotOneVersion(v):
					byPlugin = verdict{kind: "-"}
				case found:
					byPlugin = verdict{kind: "v", version: v, hosts: oracleHosts(p.ctx.chain, via)}
				}
			}
		}
		// G8.
		final := byPlatforms
		switch {
		case byPlatforms.kind == "":
			final = byPlugin
		case byPlugin.kind == "", byPlatforms.kind == "?":
		case byPlugin.kind == "?":
			final = byPlugin
		case byPlatforms.kind == "v" && byPlugin.kind == "v" && byPlatforms.version == byPlugin.version:
			final.hosts = oracleHosts(nil, append(append([]string{}, byPlatforms.hosts...), byPlugin.hosts...))
		default:
			final = verdict{kind: "?", why: "platforms_disagree"}
		}
		switch final.kind {
		case "v":
			a.Version, a.Source, a.Hosts = final.version, "managed", append([]string{}, final.hosts...)
		case "?":
			a.Reason = final.why
			g.withheld++
		default:
			a.Reason = worst
		}
	}
	for _, line := range lines {
		m := refDep.FindStringSubmatch(line)
		if m == nil || refPlatform.MatchString(m[1]) || refEnforced.MatchString(line) {
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
		var assigns []string
		for n := range assigned {
			assigns = append(assigns, n)
		}
		sort.Strings(assigns)
		half := len(assigns) / 2
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
		// Round 9 (S1): where the first half of the assignments stands: in
		// front of the scripts the file applies, after the first of them,
		// or after them all.
		place, applies := rng.Intn(3), rng.Intn(3)
		if place == 0 {
			writeAssigns(assigns[:half])
		}
		for i := 0; i < applies; i++ {
			s := pick(scripts)
			switch rng.Intn(3) {
			case 0:
				sb.WriteString("apply from: \"$rootDir/gradle/" + s + ".gradle\"\n")
			case 1:
				sb.WriteString("apply from: '" + up + "gradle/" + s + ".gradle'\n")
			default:
				sb.WriteString("apply from: \"" + up + "gradle/" + s + ".gradle\"\n")
			}
			if place == 1 && i == 0 {
				writeAssigns(assigns[:half])
			}
		}
		if place == 2 || place == 1 && applies == 0 {
			writeAssigns(assigns[:half])
		}
		if withDeps && rng.Intn(2) == 0 {
			// Round 9 (G9): a property of the project that a BOM may set.
			sb.WriteString("ext['m.version'] = '" + pick([]string{"8.8", "5.5"}) + "'\n")
		}
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
			if rng.Intn(6) == 0 {
				// Round 8 (G2).
				sb.WriteString("  implementation enforcedPlatform('t:" + pick([]string{"bomA", "bomB", "gone1", "rb1", "bomR"}) + ":1')\n")
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
				case 4:
					// Round 8 (G3): written, and a platform may manage it.
					fmt.Fprintf(&sb, "  implementation 'g:m%d:%s'\n", i, pick([]string{"1.0", "5.0", "6.1", "6.1.Final", "7.5", "9.9"}))
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
		if withDeps {
			// Round 8 (G5, G6): the Spring plugin's statements, anywhere
			// among each other; the Boot plugin with or without the
			// dependency-management plugin.
			var spring []string
			for i, n := 0, rng.Intn(3)*rng.Intn(2); i < n; i++ {
				spring = append(spring, "dependencyManagement { imports { mavenBom 't:"+pick([]string{"bomA", "bomB", "gone1", "rb1", "bomR"})+":1' } }\n")
			}
			if rng.Intn(8) == 0 {
				spring = append(spring, fmt.Sprintf("dependencyManagement { dependencies { dependency 'g:m%d:3.3' } }\n", rng.Intn(len(names))))
			}
			if rng.Intn(5) == 0 {
				spring = append(spring, "plugins { id 'org.springframework.boot' version '3.3.0' }\n")
				if rng.Intn(4) != 0 {
					spring = append(spring, "plugins { id 'io.spring.dependency-management' version '1.1.4' }\n")
				}
			}
			rng.Shuffle(len(spring), func(i, j int) { spring[i], spring[j] = spring[j], spring[i] })
			sb.WriteString(strings.Join(spring, ""))
		}
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
	// Round 9 (G10): the root project sometimes has dependencies of its
	// own, which what its subprojects block declares is not for.
	rootDeps := rng.Intn(2) == 0
	rootBuild := body("", rootDeps)
	// G9: from the build file above. Not beside an assignment of the body's
	// own: Gradle takes the last of two in one file, and the reader does
	// not read a property a file sets twice (a known difference, held by
	// TestAProjectPropertyOverridesASpringBOMsProperty's "two values" case;
	// until the platforms of these trees were published ones the two met
	// too seldom for the test to see it).
	if rng.Intn(3) == 0 && !strings.Contains(rootBuild, "ext['m.version']") {
		rootBuild += "ext['m.version'] = '7.7'\n"
	}
	if rng.Intn(2) == 0 {
		rootBuild += "subprojects {\n  dependencies {\n    implementation platform(" + pick([]string{"'t:bomA:1'", "'t:bomB:1'", "libs.bom", "'t:gone1:1'", "'t:rb1:1'"}) + ")\n  }\n}\n"
	}
	maybe(0.8, "build.gradle", rootBuild)
	if _, has := files["build.gradle"]; has && rootDeps {
		builds = append(builds, "build.gradle")
	}
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

	// The platforms: what repositories have. The poms of the clone that
	// give themselves the coordinates of two of them (bomA, bomB) are what
	// a fixture is: Gradle reads no pom.xml of the checkout for a platform,
	// and their versions (5.0 to 7.0) are never the published ones (8.x).
	entry := func(i int, version string) string { return r4dep("g", fmt.Sprintf("m%d", i), version) }
	bom := func(artifact string, extra string) string {
		s := "<project><groupId>t</groupId><artifactId>" + artifact + "</artifactId><version>1</version>"
		byProperty := rng.Intn(2) == 0
		if byProperty {
			s += "<properties><m.version>6.0</m.version></properties>"
		}
		s += "<dependencyManagement><dependencies>"
		if byProperty {
			s += entry(0, "${m.version}") // G9: first, so it is the BOM's entry for m0
		}
		for i := range names {
			if rng.Intn(2) == 0 {
				s += entry(i, pick([]string{"5.0", "6.1", "[1,2)", "7.0", "6.1.Final", "6.1-rc1", "6-1", "6.1.1"}))
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
		if rng.Intn(2) == 0 {
			// G9: a version the BOM writes with a property.
			r.Properties = map[string]string{"m.version": "8.1"}
			r.Managed = append(r.Managed, RemoteManaged{Group: "g", Artifact: "m0", Version: "${m.version}"})
		}
		for i := range names {
			if rng.Intn(2) == 0 {
				r.Managed = append(r.Managed, RemoteManaged{Group: "g", Artifact: fmt.Sprintf("m%d", i), Version: pick([]string{"8.0", "8.5"})})
			}
		}
		return r
	}
	bootBOM := fetched("spring-boot-dependencies")
	bootBOM.Group, bootBOM.Version = "org.springframework.boot", "3.3.0"
	publishedB := fetched("bomB")
	if rng.Intn(2) == 0 {
		publishedB.Managed = append(publishedB.Managed, RemoteManaged{Group: "t", Artifact: pick([]string{"bomA", "gone1", "rb1"}), Version: "1", Import: true})
	}
	src = &oracleSource{poms: map[string]*RemotePom{"t:rb1:1": fetched("rb1"), "t:bomR:1": fetched("bomR"),
		"t:bomA:1": fetched("bomA"), "t:bomB:1": publishedB,
		"org.springframework.boot:spring-boot-dependencies:3.3.0": bootBOM}, outcomes: map[string]RemoteOutcome{}}
	for _, k := range []string{"t:gone1:1", "t:gone2:1", "t:rb1:1", "t:bomR:1", "org.springframework.boot:spring-boot-dependencies:3.3.0"} {
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

// refGradleTrees (race_off_test.go, race_on_test.go) is how many random
// clones the test compares, each in three read orders.

func TestTheGradleTreeAnswersAsTheIndependentReferenceDoes(t *testing.T) {
	rng := rand.New(rand.NewSource(20261122))
	root := t.TempDir()
	readings, versions, managed, withheld, fromPlatformsAbove, notOrdered, overridden := 0, 0, 0, 0, 0, 0, 0
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
		notOrdered += ref.notOrdered
		overridden += ref.overridden
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
	t.Logf("the reference took a script's value over the file's own for %d variables (S1), and a property of the project answered inside a BOM %d times (G9)", notOrdered, overridden)
	if notOrdered < refGradleTrees/20 || overridden < refGradleTrees/20 {
		t.Errorf("too little to compare: %d variables not ordered and %d property overrides over %d trees", notOrdered, overridden, refGradleTrees)
	}
	if versions < 5*refGradleTrees || managed < refGradleTrees/2 || fromPlatformsAbove < refGradleTrees/4 || withheld < refGradleTrees/20 {
		t.Errorf("too little to compare: %d versions, %d from platforms, %d withheld, %d trees with a platform from above, over %d trees", versions, managed, withheld, fromPlatformsAbove, refGradleTrees)
	}
}

// refHighest is the highest of several versions by G4. ok is false when
// two different texts stand equal highest.
func refHighest(versions []string) (string, bool) {
	best, tied := "", false
	for i, v := range versions {
		c := 1
		if i > 0 {
			c = refCompare(v, best)
		}
		switch {
		case c > 0:
			best, tied = v, false
		case c == 0 && v != best:
			tied = true
		}
	}
	return best, len(versions) > 0 && !tied
}

// refCompare is G4, written from the rule and not from the reader.
func refCompare(a, b string) int {
	special := map[string]int{"dev": -1, "rc": 1, "snapshot": 2, "final": 3, "ga": 4, "release": 5, "sp": 6}
	pa, pb := refPart.FindAllString(a, -1), refPart.FindAllString(b, -1)
	number := func(p string) (int, bool) {
		n, err := strconv.Atoi(p)
		return n, err == nil
	}
	for i := 0; i < len(pa) || i < len(pb); i++ {
		if i >= len(pa) || i >= len(pb) {
			// A further number makes a version the higher, a further word
			// the lower.
			longer, sign := pa, 1
			if i >= len(pa) {
				longer, sign = pb, -1
			}
			if _, isNumber := number(longer[i]); !isNumber {
				sign = -sign
			}
			return sign
		}
		x, xNumber := number(pa[i])
		y, yNumber := number(pb[i])
		switch {
		case xNumber && yNumber:
			if x != y {
				if x < y {
					return -1
				}
				return 1
			}
		case xNumber:
			return 1
		case yNumber:
			return -1
		default:
			rx, ry := special[strings.ToLower(pa[i])], special[strings.ToLower(pb[i])]
			if rx != ry {
				if rx < ry {
					return -1
				}
				return 1
			}
			if rx == 0 && pa[i] != pb[i] {
				return strings.Compare(pa[i], pb[i])
			}
		}
	}
	return 0
}
