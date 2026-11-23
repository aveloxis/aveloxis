// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"reflect"
	"runtime"
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
// functions above, anchored, without -race.
func TestTheWeeklyJobRunsEveryFullFormTest(t *testing.T) {
	var wf struct {
		Jobs map[string]struct {
			Needs           any `yaml:"needs"`
			If              any `yaml:"if"`
			ContinueOnError any `yaml:"continue-on-error"`
			Defaults        any `yaml:"defaults"`
			Steps           []struct {
				Name             string `yaml:"name"`
				If               any    `yaml:"if"`
				ContinueOnError  any    `yaml:"continue-on-error"`
				Shell            any    `yaml:"shell"`
				WorkingDirectory any    `yaml:"working-directory"`
				Run              string `yaml:"run"`
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
	names := make([]string, 0, len(fullFormTests))
	for _, f := range fullFormTests {
		fn := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		names = append(names, fn[strings.LastIndex(fn, ".")+1:])
	}
	want := "go test ./internal/manifest/ -run '^(" + strings.Join(names, "|") + ")$' -count=1 -v -timeout 20m"
	ran := 0
	for _, st := range job.Steps {
		if st.If != nil || st.ContinueOnError != nil || st.Shell != nil || st.WorkingDirectory != nil {
			t.Errorf("step %q must have no if, continue-on-error, shell or working-directory", st.Name)
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
