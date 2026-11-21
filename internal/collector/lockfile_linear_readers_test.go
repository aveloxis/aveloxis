// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"gopkg.in/yaml.v3"
)

// The lockfile readers that were made linear in 0.29.213 (the YAML ones,
// which no longer decode into Go maps and structs, and package-lock.json
// v1, which no longer decodes each level again) read what the readers they
// replace read. The replaced readers are kept here, as they were, as the
// reference: they are the definition of "what was read", and they are
// quadratic, which is why they are here and not in the parsers.

func refPnpm(data []byte) (parsedLockfileData, error) {
	type pnpmPkg struct {
		Dependencies         map[string]string `yaml:"dependencies"`
		OptionalDependencies map[string]string `yaml:"optionalDependencies"`
	}
	var doc struct {
		Packages  map[string]pnpmPkg `yaml:"packages"`
		Snapshots map[string]pnpmPkg `yaml:"snapshots"`
		Importers map[string]struct {
			Dependencies    map[string]yaml.Node `yaml:"dependencies"`
			DevDependencies map[string]yaml.Node `yaml:"devDependencies"`
		} `yaml:"importers"`
		Dependencies    map[string]yaml.Node `yaml:"dependencies"`
		DevDependencies map[string]yaml.Node `yaml:"devDependencies"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	directNames := map[string]bool{}
	collectDirect := func(deps map[string]yaml.Node) {
		for name := range deps {
			directNames[name] = true
		}
	}
	for _, imp := range doc.Importers {
		collectDirect(imp.Dependencies)
		collectDirect(imp.DevDependencies)
	}
	collectDirect(doc.Dependencies)
	collectDirect(doc.DevDependencies)
	var out parsedLockfileData
	for key := range doc.Packages {
		name, version := splitPnpmPackageKey(key)
		if name == "" || version == "" {
			continue
		}
		out.Entries = append(out.Entries, LockfileEntry{Name: name, Version: version, Direct: directNames[name]})
	}
	collectEdges := func(m map[string]pnpmPkg) {
		for key, pkg := range m {
			pName, pVersion := splitPnpmPackageKey(key)
			if pName == "" {
				continue
			}
			for child, constraint := range pkg.Dependencies {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: pName, ParentVersion: pVersion, ChildName: child, Constraint: constraint})
			}
			for child, constraint := range pkg.OptionalDependencies {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: pName, ParentVersion: pVersion, ChildName: child, Constraint: constraint})
			}
		}
	}
	collectEdges(doc.Packages)
	collectEdges(doc.Snapshots)
	out.DirectKnown = true
	return out, nil
}

func refPubspec(data []byte) (parsedLockfileData, error) {
	var doc struct {
		Packages map[string]struct {
			Dependency string `yaml:"dependency"`
			Version    string `yaml:"version"`
		} `yaml:"packages"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	var entries []LockfileEntry
	for name, pkg := range doc.Packages {
		if pkg.Version == "" {
			continue
		}
		entries = append(entries, LockfileEntry{Name: name, Version: pkg.Version, Direct: strings.HasPrefix(pkg.Dependency, "direct")})
	}
	return parsedLockfileData{Entries: entries, DirectKnown: true}, nil
}

func refStack(data []byte) (parsedLockfileData, error) {
	var doc struct {
		Packages []struct {
			Completed struct {
				Hackage string `yaml:"hackage"`
			} `yaml:"completed"`
		} `yaml:"packages"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	var entries []LockfileEntry
	for _, p := range doc.Packages {
		spec := p.Completed.Hackage
		if at := strings.IndexByte(spec, '@'); at >= 0 {
			spec = spec[:at]
		}
		if m := stackHackageNameVersionRe.FindStringSubmatch(spec); m != nil {
			entries = append(entries, LockfileEntry{Name: m[1], Version: m[2]})
		}
	}
	return parsedLockfileData{Entries: entries}, nil
}

// refNpmV1 is the v1 half of parsePackageLockJSON as it was.
func refNpmV1(data []byte) (parsedLockfileData, error) {
	var doc struct {
		Dependencies map[string]json.RawMessage `json:"dependencies"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	seen := map[string]bool{}
	var out parsedLockfileData
	var walk func(parentName, parentVersion string, deps map[string]json.RawMessage)
	walk = func(parentName, parentVersion string, deps map[string]json.RawMessage) {
		for name, raw := range deps {
			var node struct {
				Version      string                     `json:"version"`
				Dependencies map[string]json.RawMessage `json:"dependencies"`
			}
			if err := json.Unmarshal(raw, &node); err != nil {
				continue
			}
			node.Version = npmLockVersion(name, node.Version)
			if node.Version != "" && !seen[name+"@"+node.Version] {
				seen[name+"@"+node.Version] = true
				out.Entries = append(out.Entries, LockfileEntry{Name: name, Version: node.Version})
			}
			if parentName != "" {
				out.Edges = append(out.Edges, LockfileEdge{ParentName: parentName, ParentVersion: parentVersion, ChildName: name})
			}
			if len(node.Dependencies) > 0 {
				walk(name, node.Version, node.Dependencies)
			}
		}
	}
	walk("", "", doc.Dependencies)
	return out, nil
}

// sameParse compares two readings without regard to order: the old readers
// went through Go maps.
func sameParse(a, b parsedLockfileData) string {
	key := func(d parsedLockfileData) (string, string) {
		var entries, edges []string
		for _, e := range d.Entries {
			entries = append(entries, fmt.Sprintf("%q", []any{e.Name, e.Version, e.Direct, e.Scope}))
		}
		for _, e := range d.Edges {
			edges = append(edges, fmt.Sprintf("%q", []any{e.ParentName, e.ParentVersion, e.ChildName, e.Constraint}))
		}
		sort.Strings(entries)
		sort.Strings(edges)
		return strings.Join(entries, "\n"), strings.Join(edges, "\n")
	}
	ae, ad := key(a)
	be, bd := key(b)
	switch {
	case ae != be:
		return fmt.Sprintf("entries:\n%s\n-- against --\n%s", ae, be)
	case ad != bd:
		return fmt.Sprintf("edges:\n%s\n-- against --\n%s", ad, bd)
	case a.DirectKnown != b.DirectKnown:
		return "DirectKnown"
	}
	return ""
}

// yamlValues are what a generated YAML document puts where a reader expects
// a mapping, a string or a list: the right thing, mostly, and every wrong
// kind.
func genYAMLScalar(r *rand.Rand) string {
	return []string{"1.0.0", "'2.0.0'", "\"3.1\"", "1.10", "0x1F", "true", "~", "null", "", "2024-01-02", "1e3", ".inf", "!!str 5", "!!binary aGVsbG8=", "a b", "'@s/p@1.0.0'", "p@1.2.3", "/p/1.2.3", "'/@s/p@4.5.6(peer@1.0.0)'", "name-1.2.3@sha256:abc", "direct main", "transitive"}[r.Intn(22)]
}

func genYAMLKey(r *rand.Rand, i int) string {
	return []string{
		fmt.Sprintf("p%d@1.%d.0", i, i), fmt.Sprintf("'/@s/q%d@2.%d.0(x@1.0.0)'", i, i), fmt.Sprintf("/r%d/3.%d.0", i, i), fmt.Sprintf("c%d", i),
		"p0@1.0.0", "c0", "~", "1", "1.0", "true", "'1'", "\"\"", "[a, b]", "{a: b}", "? x", "!!str 7",
	}[r.Intn(16)]
}

func genYAMLNode(r *rand.Rand, depth int, keys []string) string {
	pad := strings.Repeat("  ", depth)
	switch k := r.Intn(12); {
	case depth >= 4 || k < 3:
		return " " + genYAMLScalar(r) + "\n"
	case k == 3:
		return " [" + genYAMLScalar(r) + ", x]\n"
	case k == 4:
		return " {" + keys[r.Intn(len(keys))] + ": " + genYAMLScalar(r) + "}\n"
	case k == 5:
		return "\n" + pad + "- " + genYAMLScalar(r) + "\n" + pad + "- completed:\n" + pad + "    hackage: " + genYAMLScalar(r) + "\n"
	}
	var b strings.Builder
	b.WriteString("\n")
	for i, n := 0, 1+r.Intn(4); i < n; i++ {
		key := keys[r.Intn(len(keys))]
		if r.Intn(3) == 0 {
			key = genYAMLKey(r, r.Intn(6))
		}
		if strings.HasPrefix(key, "? ") {
			key = "x" // a complex key needs its own line; a plain one here
		}
		b.WriteString(pad + key + ":" + genYAMLNode(r, depth+1, keys))
	}
	return b.String()
}

// genLockYAML is a document of the format's own shape in which each place a
// reader looks holds what belongs there, most of the time, and now and then
// a scalar of any kind, a list, a mapping or a null.
func genLockYAML(r *rand.Rand, format string) string {
	odd := func(right string) string {
		switch r.Intn(25) {
		case 0:
			return " " + genYAMLScalar(r) + "\n"
		case 1:
			return " [1, {a: b}]\n"
		case 2:
			return " {x: y}\n"
		case 3:
			return " ~\n"
		}
		return right
	}
	str := func() string {
		if r.Intn(3) > 0 {
			return fmt.Sprintf("%d.%d.0", r.Intn(4), r.Intn(4))
		}
		return strings.TrimSpace(odd(" " + genYAMLScalar(r) + "\n"))
	}
	// A key: a package's own, nearly always one this document has not used.
	used := 0
	key := func() string {
		if r.Intn(8) == 0 {
			return genYAMLKey(r, r.Intn(5))
		}
		used++
		return fmt.Sprintf([]string{"p%d@1.2.3", "'/@s/q%d@2.0.0(x@1.0.0)'", "/r%d/3.1.0", "c%d", "name%d"}[r.Intn(5)], used)
	}
	deps := func(pad, name string) string {
		if r.Intn(3) == 0 {
			return ""
		}
		var b strings.Builder
		for i, n := 0, 1+r.Intn(3); i < n; i++ {
			fmt.Fprintf(&b, "%s  %s: %s\n", pad, key(), str())
		}
		return pad + name + ":" + odd("\n"+b.String())
	}
	var b strings.Builder
	switch format {
	case "pnpm":
		b.WriteString("lockfileVersion: '9.0'\n")
		if r.Intn(2) == 0 {
			b.WriteString("importers:" + odd("\n  .:"+odd("\n"+deps("    ", "dependencies")+deps("    ", "devDependencies")+"    x: 1\n")))
		} else {
			b.WriteString(deps("", "dependencies") + deps("", "devDependencies"))
		}
		for _, section := range []string{"packages", "snapshots"} {
			if r.Intn(4) == 0 {
				continue
			}
			var pkgs strings.Builder
			for i, n := 0, r.Intn(5); i < n; i++ {
				pkgs.WriteString("  " + key() + ":" + odd("\n    resolution: {integrity: x}\n"+deps("    ", "dependencies")+deps("    ", "optionalDependencies")))
			}
			b.WriteString(section + ":" + odd("\n"+pkgs.String()))
		}
	case "pubspec":
		var pkgs strings.Builder
		for i, n := 0, r.Intn(6); i < n; i++ {
			pkgs.WriteString("  " + key() + ":" + odd(fmt.Sprintf("\n    dependency: %s\n    source: hosted\n    version: %s\n", str(), str())))
		}
		b.WriteString("packages:" + odd("\n"+pkgs.String()) + "sdks: {dart: '>=3'}\n")
	case "stack":
		var pkgs strings.Builder
		for i, n := 0, r.Intn(6); i < n; i++ {
			pkgs.WriteString("-" + odd(" completed:"+odd(fmt.Sprintf("\n    hackage: pkg%d-%s@sha256:abc\n    pantry-tree: {size: 1}\n", i, str()))+"  original: {hackage: x}\n"))
		}
		b.WriteString("packages:" + odd("\n"+pkgs.String()) + "snapshots: []\n")
	}
	return b.String()
}

func TestTheLinearYAMLReadersReadWhatTheDecoderRead(t *testing.T) {
	keys := []string{"packages", "snapshots", "importers", "dependencies", "devDependencies", "optionalDependencies", "version", "dependency", "completed", "hackage", ".", "specifier", "p1@1.1.0", "'/@s/q2@2.2.0'", "c3"}
	r := rand.New(rand.NewSource(213))
	type reader struct {
		name     string
		now, was func([]byte) (parsedLockfileData, error)
	}
	readers := []reader{{"pnpm-lock.yaml", parsePnpmLock, refPnpm}, {"pubspec.lock", parsePubspecLock, refPubspec}, {"stack.yaml.lock", parseStackYamlLock, refStack}}
	docs := []string{"", "~\n", "packages:\n", "packages: ~\n", "- a\n", "5\n", "packages: {a@1.0.0: ~}\n", "---\npackages: {a@1.0.0: {}}\n---\npackages: 7\n",
		"packages:\n  a@1.0.0: {}\n  a@1.0.0: {}\n", "packages:\n  1: {}\n  '1': {}\n", "packages:\n  a: {version: 1.10, dependency: direct dev}\n", "packages:\n- completed: {hackage: x-1.0@sha256:a}\n- ~\n- completed: ~\n",
		"packages: {a: {version: [1]}}\n", "packages: [[1]]\n", "packages: {a: 5}\n", "packages:\n  a@1.0.0:\n    dependencies: {c: {d: 1}}\n", "importers: {.: {dependencies: {x: [1, 2]}}}\n", "importers: {.: 5}\n",
		"packages:\n  p@1.0.0:\n    dependencies:\n      c: 1\n      c: 2\n", "\tpackages: 1\n", "packages: {a: {version: !!binary aGVsbG8=}}\n", "packages:\n  ? [a, b]\n  : {}\n"}
	// Round 7: keys that are written differently and decode to one string
	// are one key; a scalar tagged !!null that is not a null is refused; a
	// key that is not a scalar beside one whose text is empty.
	docs = append(docs,
		"packages:\n  a@1.0.0: {version: 1}\n  !!binary YUAxLjAuMA==: {version: 2}\n", "packages:\n  !!binary YUAxLjAuMA==: {version: 2}\n  a@1.0.0: {version: 1}\n",
		"packages:\n  a: {version: 1, dependency: direct main}\n  !!binary YQ==: {version: 2, dependency: transitive}\n",
		"packages: {a: {version: !!null x}}\n", "packages: !!null x\n", "packages: {a: !!null x}\n", "packages: {a: {version: !!null ~}}\n", "packages: !!null null\n",
		"packages:\n  ? [a]\n  : {}\n  '': {}\n", "packages:\n  '': {}\n  ? [a]\n  : {}\n", "packages:\n  ? {a: 1}\n  : {}\n  '': {}\n",
		"packages:\n  1: {version: 1}\n  !!str 1: {version: 2}\n", "packages:\n  a: {version: 1}\n  'a': {version: 2}\n", "packages:\n  ~: {version: 1}\n  a: {version: 2}\n",
		"importers: {.: {dependencies: {x: {version: 1}, !!binary eA==: {version: 2}}}}\n")
	for i := 0; i < 1500; i++ {
		docs = append(docs, strings.TrimPrefix(genYAMLNode(r, 0, keys), "\n"), genLockYAML(r, "pnpm"), genLockYAML(r, "pubspec"), genLockYAML(r, "stack"))
	}
	read, failed, withEntries := 0, 0, 0
	for _, doc := range docs {
		if strings.ContainsAny(doc, "&*") || strings.Contains(doc, "<<") {
			continue // the one difference, and it has its own test
		}
		for _, rd := range readers {
			was, wasErr := rd.was([]byte(doc))
			now, nowErr := rd.now([]byte(doc))
			read++
			if (wasErr != nil) != (nowErr != nil) {
				t.Fatalf("%s: the decoder said %v and the reader says %v, of:\n%s", rd.name, wasErr, nowErr, doc)
			}
			if wasErr != nil {
				failed++
				continue
			}
			if diff := sameParse(was, now); diff != "" {
				t.Fatalf("%s reads another thing than the decoder did, of:\n%s\n%s", rd.name, doc, diff)
			}
			if len(now.Entries)+len(now.Edges) > 0 {
				withEntries++
			}
		}
	}
	if failed < read/20 || withEntries < read/10 || read-failed < read/4 {
		t.Errorf("the documents do not try the readers: %d readings, %d refused, %d with entries or edges", read, failed, withEntries)
	}
	t.Logf("%d readings, %d refused by both, %d with entries or edges", read, failed, withEntries)
}

// An alias or a merge key where a reader looks is a lockfile that is not
// read, and the reason says so. Elsewhere in the document it is not looked
// at, as nothing there is.
func TestAYAMLLockfileWithAnAliasIsNotRead(t *testing.T) {
	for name, doc := range map[string]string{
		"an aliased package":         "x: &x {dependencies: {c: 1.0.0}}\npackages:\n  a@1.0.0: *x\n",
		"aliased packages":           "x: &x {a@1.0.0: {}}\npackages: *x\n",
		"an aliased version":         "x: &v 1.0.0\npackages:\n  a@1.0.0:\n    dependencies:\n      c: *v\n",
		"a merge key":                "x: &x {c: 1.0.0}\npackages:\n  a@1.0.0:\n    dependencies:\n      <<: *x\n",
		"a merge key in the root":    "x: &x {packages: {}}\n<<: *x\n",
		"an aliased key":             "x: &k a@1.0.0\npackages:\n  *k : {}\n",
		"an alias that holds itself": "packages: &p\n  a@1.0.0: *p\n",
	} {
		_, err := parsePnpmLock([]byte(doc))
		if !errors.Is(err, errYAMLLockAlias) {
			t.Errorf("%s: %v; want the alias refusal", name, err)
			continue
		}
		// The reason reads as a decision, to whoever meets it in a log.
		for _, says := range []string{"uses a YAML alias or merge key, which no package manager writes in a lockfile", "on purpose"} {
			if !strings.Contains(err.Error(), says) {
				t.Errorf("%s: the refusal does not say %q: %v", name, says, err)
			}
		}
	}
	if _, err := parsePubspecLock([]byte("x: &x {version: '1'}\npackages:\n  a: *x\n")); !errors.Is(err, errYAMLLockAlias) {
		t.Errorf("pubspec.lock: %v", err)
	}
	if _, err := parseStackYamlLock([]byte("x: &x {hackage: a-1}\npackages:\n- completed: *x\n")); !errors.Is(err, errYAMLLockAlias) {
		t.Errorf("stack.yaml.lock: %v", err)
	}
	if _, err := parseStackYamlLock([]byte("x: &x [1]\npackages: *x\n")); !errors.Is(err, errYAMLLockAlias) {
		t.Errorf("stack.yaml.lock, an aliased list: %v", err)
	}
	got, err := parsePnpmLock([]byte("unread: &x {a: 1}\nalso: *x\nsettings: {<<: *x}\npackages:\n  a@1.0.0: {resolution: *x}\n"))
	if err != nil || len(got.Entries) != 1 {
		t.Errorf("an alias where no reader looks: %v, %+v", err, got.Entries)
	}
	// And the walk reports a lockfile it refused as a failed parse, whose
	// previous rows are kept.
	dir := writeClone(t, map[string]string{"pnpm-lock.yaml": "x: &x {a@1.0.0: {}}\npackages: *x\n"})
	var log recorded
	set := collectLockfiles(dir, log.logger())
	if len(set.Parsed) != 0 || len(set.Failed) != 1 || set.Failed[0].Reason != "parse" {
		t.Errorf("the walk: %+v, %+v", set.Parsed, set.Failed)
	}
	if len(log.lines) != 1 || !strings.Contains(log.lines[0], "level=WARN") || !strings.Contains(log.lines[0], "uses a YAML alias or merge key, which no package manager writes in a lockfile") {
		t.Errorf("the walk's log does not give the reason: %q", log.lines)
	}
}

func genNpmV1(r *rand.Rand, depth int) string {
	if depth > 5 {
		return `{"version":"1.0.0"}`
	}
	switch r.Intn(14) {
	case 0:
		return `"1.0.0"`
	case 1:
		return "null"
	case 2:
		return "7"
	case 3:
		return `["a"]`
	case 4:
		return `{"version":5,"dependencies":{"lost":{"version":"1.0.0"}}}`
	case 5:
		return `{"version":"2.0.0","dependencies":["x"]}`
	case 6:
		return `{"version":null,"dependencies":null}`
	}
	var b strings.Builder
	b.WriteString("{")
	fields := []string{`"version":"1.%d.0"`, `"VERSION":"9.%d.0"`, `"resolved":"https://r/p/-/p-1.%d.0.tgz"`, `"version":"https://registry.npmjs.org/c1/-/c1-3.%d.0.tgz"`, `"requires":{"a":"%d"}`, `"dev":[%d,{"dependencies":{}}]`}
	for i, n := 0, r.Intn(4); i < n; i++ {
		fmt.Fprintf(&b, fields[r.Intn(len(fields))]+",", r.Intn(3))
	}
	for sections := 1 + r.Intn(2); sections > 0; sections-- {
		name := []string{"dependencies", "dependencies", "Dependencies", "DEPENDENCIES"}[r.Intn(4)]
		fmt.Fprintf(&b, `"%s":{`, name)
		for i, n := 0, r.Intn(4); i < n; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `"c%d":%s`, r.Intn(4), genNpmV1(r, depth+1))
		}
		b.WriteString("},")
	}
	b.WriteString(`"x":1}`)
	return b.String()
}

func TestTheLinearPackageLockV1ReaderReadsWhatTheOldOneRead(t *testing.T) {
	r := rand.New(rand.NewSource(213))
	docs := []string{`{}`, `{"dependencies":null}`, `{"dependencies":{}}`, `{"lockfileVersion":1,"dependencies":{"a":{"version":"1.0.0","dependencies":{"b":{"version":"2.0.0"}}}}}`,
		`{"dependencies":{"a":null,"b":{"version":"1"},"b":{"version":"2"}}}`, `{"dependencies":{"a":{"version":"1"}},"Dependencies":{"b":{"version":"2"},"a":{"version":"3"}}}`,
		`{"dependencies":{"a":{"dependencies":{"b":null}}}}`, `{"dependencies":5}`, `[1]`, `{"dependencies":{"a":{"version":"1","dependencies":{"b":{"version":"2"}},"dependencies":{"c":{"version":"3"},"b":{"version":"4"}}}}}`,
		`{"dependencies":{"a":{"version":"1","dependencies":{"b":{"version":"2"}},"dependencies":7}}}`, `{"dependencies":{"":{"version":"1","dependencies":{"x":{"version":"2"}}}}}`}
	// Round 7: a "dependencies" that is null after one that is not empties
	// it, as encoding/json sets a map to nil for a null.
	docs = append(docs,
		`{"lockfileVersion":1,"dependencies":{"resolved":{"dependencies":{"@s/c":{}},"dependencies":null}}}`,
		`{"dependencies":{"a":{"version":"1"}},"dependencies":null}`,
		`{"dependencies":{"a":{"version":"1"}},"dependencies":null,"dependencies":{"b":{"version":"2"}}}`,
		`{"dependencies":{"a":{"version":"1","dependencies":{"b":{"version":"2"}},"dependencies":null,"dependencies":{"c":{"version":"3"}}}}}`,
		`{"dependencies":null,"dependencies":{"a":{"version":"1"}}}`, `{"dependencies":{"a":{"version":"1"}},"DEPENDENCIES":null}`)
	for i := 0; i < 3000; i++ {
		docs = append(docs, `{"lockfileVersion":1,"dependencies":{"top":`+genNpmV1(r, 0)+`,"c1":`+genNpmV1(r, 3)+`}}`)
		// The same with keys given twice, a null among them.
		node := func() string { return genNpmV1(r, 2) }
		docs = append(docs, `{"lockfileVersion":1,"dependencies":{"top":{"version":"1","dependencies":{"x":`+node()+`},"dependencies":`+
			[]string{"null", `{"y":` + node() + `}`, `{"x":null}`}[r.Intn(3)]+`,"dependencies":`+[]string{"null", `{"z":` + node() + `}`, `{}`}[r.Intn(3)]+`}}`+
			[]string{"", `,"dependencies":null`, `,"dependencies":{"w":` + node() + `}`}[r.Intn(3)]+`}`)
	}
	refused, entries, edges := 0, 0, 0
	for _, doc := range docs {
		was, wasErr := refNpmV1([]byte(doc))
		now, nowErr := parsePackageLockJSON([]byte(doc))
		if (wasErr != nil) != (nowErr != nil) {
			t.Fatalf("the old reader said %v and the new says %v, of:\n%s", wasErr, nowErr, doc)
		}
		if wasErr != nil {
			refused++
			continue
		}
		if diff := sameParse(was, now); diff != "" {
			t.Fatalf("the v1 reader reads another thing than it did, of:\n%s\n%s", doc, diff)
		}
		entries += len(now.Entries)
		edges += len(now.Edges)
	}
	if refused < 2 || entries < 3000 || edges < 3000 {
		t.Errorf("the documents do not try the reader: %d refused, %d entries, %d edges", refused, entries, edges)
	}
}

// yaml.Unmarshal is given nothing but a yaml.Node in the code a child runs
// on a repository's files: into a Go map or struct it compares every key of
// a mapping with every other.
func TestNoRepositoryYAMLIsDecodedIntoGoValues(t *testing.T) {
	examined := 0
	for _, pkg := range []string{"internal/collector", "internal/manifest"} {
		for file, src := range srctest.PackageFiles(t, pkg, 10) {
			code := srctest.StripGoComments(src)
			examined++
			own := strings.Contains(code, "func yamlLockRoot(") // the one file that parses YAML
			for _, banned := range []string{"yaml.NewDecoder(", "yaml.Unmarshal("} {
				n := strings.Count(code, banned)
				if own && banned == "yaml.Unmarshal(" {
					if n != 1 || !strings.Contains(code, "var doc yaml.Node\n\tif err := yaml.Unmarshal(data, &doc)") {
						t.Errorf("%s: yaml.Unmarshal is called %d times; want once, into a yaml.Node", file, n)
					}
					continue
				}
				if n > 0 {
					t.Errorf("%s calls %s: read the document through yamlLockRoot's walk, which is linear in a mapping's keys", file, banned)
				}
			}
			// A node's own Decode makes the same comparison when it is
			// given a mapping: in a file that has the YAML package at all,
			// nothing is decoded but a scalar into a string, in one place.
			if !strings.Contains(code, `"gopkg.in/yaml.v3"`) {
				continue
			}
			want := 0
			if own {
				want = strings.Count(code, ".Decode(&s)")
				if want != 1 {
					t.Errorf("%s: a scalar is decoded into a string in %d places; want one", file, want)
				}
			}
			if n := strings.Count(code, ".Decode("); n != want {
				t.Errorf("%s decodes %d times beside a YAML document; want %d: a node is walked (yamlPairs), never decoded into a Go value", file, n, want)
			}
		}
	}
	if examined < 40 {
		t.Fatalf("only %d files examined", examined)
	}
}
