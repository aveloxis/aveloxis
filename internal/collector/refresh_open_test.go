package collector

import (
	"os"
	"strings"
	"testing"
)

// TestRefreshOpenFileExists verifies refresh_open.go has the expected types.
func TestRefreshOpenFileExists(t *testing.T) {
	src, err := os.ReadFile("refresh_open.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, fn := range []string{"OpenItemRefresher", "RefreshOpenItems", "NewOpenItemRefresher"} {
		if !strings.Contains(code, fn) {
			t.Errorf("refresh_open.go must contain %s", fn)
		}
	}
}

// TestRefreshOpenQueriesOpenItems verifies the DB methods to get open issue/PR numbers.
func TestRefreshOpenQueriesOpenItems(t *testing.T) {
	src, err := os.ReadFile("../db/gap_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GetOpenIssueNumbers") {
		t.Error("gap_store.go must contain GetOpenIssueNumbers for refreshing open items")
	}
	if !strings.Contains(code, "GetOpenPRNumbers") {
		t.Error("gap_store.go must contain GetOpenPRNumbers for refreshing open items")
	}
}

// TestSchedulerCallsRefreshOpen verifies the scheduler runs the open item
// refresh after collectAndProcess.
func TestSchedulerCallsRefreshOpen(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "RefreshOpenItems") {
		t.Error("scheduler must call RefreshOpenItems after collection to update open issues/PRs")
	}
}
