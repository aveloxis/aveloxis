package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestGitURLDoesNotDoubleGitSuffix verifies that the gitURL construction
// in runFacadeAndAnalysis does not append .git when the repo name already
// ends with .git. Many JOSS/Augur-imported URLs have the .git suffix,
// producing https://github.com/owner/repo.git.git which returns 404.
func TestGitURLDoesNotDoubleGitSuffix(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find where gitURL is constructed (near runFacadeAndAnalysis).
	idx := strings.Index(code, "func (s *Scheduler) runFacadeAndAnalysis")
	if idx < 0 {
		t.Fatal("cannot find runFacadeAndAnalysis")
	}
	fnBody := code[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	// Must not blindly append .git — must check/strip first.
	if strings.Contains(fnBody, `/%s.git"`) && !strings.Contains(fnBody, "TrimSuffix") {
		t.Error("gitURL construction must not blindly append .git — " +
			"repos imported from JOSS/Augur already have .git suffix, " +
			"producing double .git.git which returns 404")
	}
}
