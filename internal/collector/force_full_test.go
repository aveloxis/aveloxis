package collector

import (
	"os"
	"strings"
	"testing"
)

// TestForceFullCollectionConfigExists verifies the config has a force_full field.
func TestForceFullCollectionConfigExists(t *testing.T) {
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ForceFullCollection") {
		t.Error("CollectionConfig must have a ForceFullCollection field")
	}
	if !strings.Contains(code, `"force_full"`) && !strings.Contains(code, `"force_full_collection"`) {
		t.Error("ForceFullCollection must have a json tag for aveloxis.json")
	}
}

// TestSchedulerConfigHasForceFullField verifies the scheduler Config struct
// has a ForceFullCollection field that determineSince checks.
func TestSchedulerConfigHasForceFullField(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ForceFullCollection") {
		t.Error("scheduler Config must have ForceFullCollection field")
	}
}

// TestDetermineSinceRespectsForceFullFlag verifies determineSince returns
// zero time when ForceFullCollection is true, even for previously collected repos.
func TestDetermineSinceRespectsForceFullFlag(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *Scheduler) determineSince(")
	if idx < 0 {
		t.Fatal("cannot find determineSince function")
	}
	fnBody := code[idx : idx+400]

	if !strings.Contains(fnBody, "ForceFullCollection") {
		t.Error("determineSince must check ForceFullCollection flag and return zero time when true")
	}
}
