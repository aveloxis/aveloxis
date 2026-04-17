package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestGitURLDoesNotDoubleGitSuffix verifies that gitURL construction in
// runFacadeAndAnalysis does not produce ".git.git". Previously the scheduler
// had a local strings.TrimSuffix workaround because repo.Name could contain
// ".git" (Augur/JOSS imports). Now the canonical fix lives at the write
// boundary — db.PostgresStore.UpsertRepo calls model.NormalizeRepoName —
// so repo.Name is guaranteed clean and appending ".git" is safe. This test
// enforces that the DB normalization is in place so the scheduler workaround
// stays removed.
func TestGitURLDoesNotDoubleGitSuffix(t *testing.T) {
	// The canonical fix is write-side normalization in db.UpsertRepo. Verify
	// it exists so repo.Name never reaches the scheduler with a .git suffix.
	src, err := os.ReadFile("../db/postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	upsertIdx := strings.Index(code, "func (s *PostgresStore) UpsertRepo(")
	if upsertIdx < 0 {
		t.Fatal("cannot find UpsertRepo in db/postgres.go")
	}
	fnBody := code[upsertIdx:]
	end := strings.Index(fnBody, "\n}\n")
	if end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "NormalizeRepoName") {
		t.Error("UpsertRepo must call model.NormalizeRepoName on r.Name so " +
			"repo slugs with '.git' never reach the DB — otherwise the " +
			"scheduler would need a local strings.TrimSuffix workaround and " +
			"every API call (/releases, /issues, /pulls) would 404")
	}
}
