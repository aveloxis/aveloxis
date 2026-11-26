// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"gopkg.in/yaml.v3"
)

// fullFormTests are the tests of this package that do less under the race
// detector (they read raceEnabled, or a constant of race_on_test.go and
// race_off_test.go), so their full form runs only where the detector is
// off: the weekly manifest-speed job. Named by the functions themselves, so
// a rename breaks this build and cannot leave the job's -run list matching
// one test fewer, or none, with the job still green (0.29.228, review round
// 17; the sbom-validators job has had this pin since round 12).
var fullFormTests = []func(*testing.T){
	TestAGemfileLineOfManyStatementsIsReadInLinearTime,
	TestNoRepeatedUnitIsReadInSuperLinearTime,
	TestAStatementThatDoesNotEndIsNotReadAgainForEveryLineOfIt,
	TestBuildFileReadersScaleLinearly,
	TestGemLogicalLinesAnswersAsThePatternsDo,
	TestThePomTreeAnswersAsTheNaiveReaderDoes,
	TestTheGradleTreeAnswersAsTheNaiveReaderDoes,
	TestTheGradleTreeAnswersAsTheIndependentReferenceDoes,
}

// TestTheWeeklyJobRunsEveryFullFormTest pins the manifest-speed job of
// network-canary.yml: no needs, if or continue-on-error on the job or the
// step, and one step whose whole command is go test over exactly the
// functions above, anchored, without -race: not in the command and not
// through an env block (GOFLAGS: -race at the step, the job or the workflow
// would put the detector back on with the command unchanged; the job needs
// no environment, so it has none).
func TestTheWeeklyJobRunsEveryFullFormTest(t *testing.T) {
	var wf struct {
		Env  any `yaml:"env"`
		Jobs map[string]struct {
			Env             any `yaml:"env"`
			Needs           any `yaml:"needs"`
			If              any `yaml:"if"`
			ContinueOnError any `yaml:"continue-on-error"`
			Defaults        any `yaml:"defaults"`
			Container       any `yaml:"container"`
			Strategy        any `yaml:"strategy"`
			Services        any `yaml:"services"`
			Steps           []struct {
				Name             string            `yaml:"name"`
				Uses             string            `yaml:"uses"`
				With             map[string]string `yaml:"with"`
				Env              any               `yaml:"env"`
				If               any               `yaml:"if"`
				ContinueOnError  any               `yaml:"continue-on-error"`
				Shell            any               `yaml:"shell"`
				WorkingDirectory any               `yaml:"working-directory"`
				Run              string            `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(srctest.Read(t, ".github/workflows/network-canary.yml")), &wf); err != nil {
		t.Fatalf("network-canary.yml does not parse: %v", err)
	}
	job, ok := wf.Jobs["manifest-speed"]
	if !ok {
		t.Fatal("network-canary.yml must keep the manifest-speed job")
	}
	if job.Needs != nil || job.If != nil || job.ContinueOnError != nil || job.Defaults != nil {
		t.Errorf("the manifest-speed job must have no needs, if, continue-on-error or defaults (needs=%v if=%v continue-on-error=%v defaults=%v)", job.Needs, job.If, job.ContinueOnError, job.Defaults)
	}
	if wf.Env != nil || job.Env != nil {
		t.Errorf("the manifest-speed job must run with no env block on the workflow or the job: GOFLAGS there changes what go test does (workflow env=%v job env=%v)", wf.Env, job.Env)
	}
	if job.Container != nil || job.Strategy != nil || job.Services != nil {
		t.Errorf("the manifest-speed job must have no container, strategy or services (container=%v strategy=%v services=%v)", job.Container, job.Strategy, job.Services)
	}
	// The whole step list, not the go test step alone: a step before it can
	// switch the detector on for the next one (go env -w GOFLAGS=-race, or a
	// line appended to $GITHUB_ENV) with the pinned command unchanged.
	type stepShape struct {
		uses string
		with map[string]string
		runs bool
	}
	wantSteps := []stepShape{
		{uses: "actions/checkout@v4"},
		{uses: "actions/setup-go@v5", with: map[string]string{"go-version-file": "go.mod"}},
		{runs: true},
	}
	var gotSteps []stepShape
	for _, st := range job.Steps {
		gotSteps = append(gotSteps, stepShape{uses: st.Uses, with: st.With, runs: st.Run != ""})
	}
	if !reflect.DeepEqual(gotSteps, wantSteps) {
		t.Errorf("the manifest-speed job must have exactly three steps (checkout, setup-go from go.mod, one run step):\n got %+v\nwant %+v", gotSteps, wantSteps)
	}
	names := fullFormTestNames()
	want := "go test ./internal/manifest/ -run '^(" + strings.Join(names, "|") + ")$' -count=1 -v -timeout 20m"
	ran := 0
	for _, st := range job.Steps {
		if st.If != nil || st.ContinueOnError != nil || st.Shell != nil || st.WorkingDirectory != nil || st.Env != nil {
			t.Errorf("step %q must have no if, continue-on-error, shell, working-directory or env", st.Name)
		}
		if !strings.Contains(st.Run, "go test") {
			continue
		}
		ran++
		if got := strings.TrimSpace(st.Run); got != want {
			t.Errorf("the manifest-speed step must run exactly the full-form tests, without the race detector:\n got %s\nwant %s", got, want)
		}
	}
	if ran != 1 {
		t.Errorf("the manifest-speed job must have one go test step, found %d", ran)
	}
}

// fullFormTestNames gives fullFormTests by name, in the list's order.
func fullFormTestNames() []string {
	names := make([]string, 0, len(fullFormTests))
	for _, f := range fullFormTests {
		fn := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		names = append(names, fn[strings.LastIndex(fn, ".")+1:])
	}
	return names
}

// raceDependentTests reads the test files of dir and gives, sorted, every
// Test function that names something race_off_test.go declares (a constant,
// a variable or a function), and one complaint for each other place that
// names one (another function, a package-level declaration) and for each
// other test file with a build line that mentions race: only a Test may
// name one and only the two race files may be built by the detector's tag,
// so that the list below cannot miss a test that does less under the
// detector through a helper, a package variable or a file of its own.
func raceDependentTests(t *testing.T, dir string) (tests, complaints []string) {
	t.Helper()
	fset := token.NewFileSet()
	parse := func(path string) *ast.File {
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution|parser.ParseComments)
		if err != nil {
			t.Fatalf("%s does not parse: %v", path, err)
		}
		return f
	}
	const declaring, itsTwin = "race_off_test.go", "race_on_test.go"
	consts := map[string]bool{}
	for _, d := range parse(filepath.Join(dir, declaring)).Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			consts[d.Name.Name] = true
		case *ast.GenDecl:
			for _, sp := range d.Specs {
				if vs, ok := sp.(*ast.ValueSpec); ok {
					for _, n := range vs.Names {
						consts[n.Name] = true
					}
				}
			}
		}
	}
	if len(consts) == 0 {
		t.Fatalf("%s declares nothing: this pin has nothing to look for", declaring)
	}
	firstNamed := func(n ast.Node) (named string) {
		ast.Inspect(n, func(c ast.Node) bool {
			if id, ok := c.(*ast.Ident); ok && consts[id.Name] {
				named = id.Name
			}
			return named == ""
		})
		return named
	}
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no test files found in %s (%v)", dir, err)
	}
	for _, path := range files {
		if base := filepath.Base(path); base == declaring || base == itsTwin {
			continue
		}
		f := parse(path)
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if strings.HasPrefix(c.Text, "//go:build") && strings.Contains(c.Text, "race") && c.Pos() < f.Package {
					complaints = append(complaints, filepath.Base(path)+": build line "+c.Text)
				}
			}
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok {
				if named := firstNamed(d); named != "" {
					complaints = append(complaints, filepath.Base(path)+": a package-level declaration names "+named)
				}
				continue
			}
			if fd.Body == nil {
				continue
			}
			named := firstNamed(fd.Body)
			if named == "" {
				continue
			}
			if fd.Recv == nil && strings.HasPrefix(fd.Name.Name, "Test") {
				tests = append(tests, fd.Name.Name)
				continue
			}
			complaints = append(complaints, filepath.Base(path)+": "+fd.Name.Name+" names "+named)
		}
	}
	sort.Strings(tests)
	return tests, complaints
}

// TestFullFormTestsAreTheTestsThatDoLessUnderTheDetector keeps the list the
// weekly job runs equal to the tests that read a race constant (review of
// 0.29.228: the list was kept by hand, so a ninth such test, or one dropped
// from both the list and the workflow, left the job green and that test's
// full form run nowhere).
func TestFullFormTestsAreTheTestsThatDoLessUnderTheDetector(t *testing.T) {
	got, complaints := raceDependentTests(t, ".")
	for _, c := range complaints {
		t.Errorf("%s: name what the race files declare only inside a Test function, and leave the detector's build tag to the two race files, so the weekly job's list can be checked against the tests", c)
	}
	want := fullFormTestNames()
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fullFormTests must be exactly the tests that name a race constant:\n in the sources %v\n in the list    %v", got, want)
	}
}

// TestRaceDependentTestsFindsThemAndRefusesAHelper drives the reader on a
// package written here: a Test naming a constant, a variable or a function
// of the race files is found, a Test naming none is not, and a helper or a
// package-level declaration naming one, and a third file under the
// detector's build tag, are complaints.
func TestRaceDependentTestsFindsThemAndRefusesAHelper(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte("package p\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("race_off_test.go", "const raceEnabled = false\nconst (\n\tcases = 5\n)\nvar slow = false\nfunc trees() int { return 9 }\n")
	write("race_on_test.go", "const raceEnabled = true\nconst (\n\tcases = 1\n)\nvar slow = true\nfunc trees() int { return 1 }\n")
	if err := os.WriteFile(filepath.Join(dir, "b_test.go"), []byte("//go:build !race\n\npackage p\nvar gate = raceEnabled\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	write("a_test.go", "import \"testing\"\n"+
		"func TestSkips(t *testing.T) { if raceEnabled { return } }\n"+
		"func TestCounts(t *testing.T) { for i := 0; i < cases; i++ {} }\n"+
		"func TestByVar(t *testing.T) { if slow { return } }\n"+
		"func TestByFunc(t *testing.T) { _ = trees() }\n"+
		"// raceEnabled in a comment is not a reference\n"+
		"func TestPlain(t *testing.T) { _ = \"raceEnabled\" }\n"+
		"func helper() int { return cases }\n")
	tests, complaints := raceDependentTests(t, dir)
	if want := []string{"TestByFunc", "TestByVar", "TestCounts", "TestSkips"}; !reflect.DeepEqual(tests, want) {
		t.Errorf("tests = %v, want %v", tests, want)
	}
	sort.Strings(complaints)
	want := []string{
		"a_test.go: helper names cases",
		"b_test.go: a package-level declaration names raceEnabled",
		"b_test.go: build line //go:build !race",
	}
	if !reflect.DeepEqual(complaints, want) {
		t.Errorf("complaints = %v, want %v", complaints, want)
	}
}
