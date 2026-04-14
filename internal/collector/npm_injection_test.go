package collector

import (
	"os"
	"strings"
	"testing"
)

// TestNPMViewUsesArgSeparator verifies the npm view command uses "--" to
// separate flags from the package name argument. Without this, a malicious
// package.json with a key like "--registry=http://attacker.example" causes
// npm to treat it as a flag, redirecting resolution to the attacker's registry.
func TestNPMViewUsesArgSeparator(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the npm view exec.CommandContext call.
	idx := strings.Index(code, `"npm", "view"`)
	if idx < 0 {
		// Try alternate form.
		idx = strings.Index(code, `"npm","view"`)
	}
	if idx < 0 {
		t.Fatal("cannot find npm view command in analysis.go")
	}

	// The argv must contain "--" before the dep name to prevent argument injection.
	// Look in a window around the npm view call.
	window := code[idx:]
	if len(window) > 500 {
		window = window[:500]
	}

	if !strings.Contains(window, `"--"`) {
		t.Error("npm view command must include \"--\" separator before the package name " +
			"to prevent argument injection from malicious package.json keys")
	}
}

// TestNPMViewRejectsDashPrefix verifies that dep names starting with "-" are
// rejected or sanitized before being passed to npm.
func TestNPMViewRejectsDashPrefix(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the npm resolver function.
	idx := strings.Index(code, "func resolveNPMLibyear")
	if idx < 0 {
		idx = strings.Index(code, "func (a *AnalysisCollector) resolveNPMLibyear")
	}
	if idx < 0 {
		t.Skip("cannot find resolveNPMLibyear function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	// Must either use "--" separator OR reject names starting with "-".
	hasArgSep := strings.Contains(fnBody, `"--"`)
	hasDashCheck := strings.Contains(fnBody, `strings.HasPrefix(dep.Name, "-")`) ||
		strings.Contains(fnBody, `dep.Name[0] == '-'`)

	if !hasArgSep && !hasDashCheck {
		t.Error("resolveNPMLibyear must either use '--' argument separator or reject " +
			"dep names starting with '-' to prevent npm argument injection")
	}
}
