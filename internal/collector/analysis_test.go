package collector

import (
	"encoding/json"
	"testing"
)

func TestParseRequirementsTxt(t *testing.T) {
	content := `
# This is a comment
flask==2.3.0
requests>=2.28.0
numpy~=1.24
pandas
-e ./local_package
# another comment
sqlalchemy==2.0.0
`
	deps := parseRequirementsTxt(content)
	expected := map[string]bool{
		"flask":      true,
		"requests":   true,
		"numpy":      true,
		"pandas":     true,
		"sqlalchemy": true,
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
		delete(expected, dep)
	}
	for name := range expected {
		t.Errorf("missing dep: %q", name)
	}
}

func TestParseGoMod(t *testing.T) {
	content := `module github.com/example/project

go 1.23

require (
	github.com/jackc/pgx/v5 v5.9.1
	github.com/spf13/cobra v1.8.1
	// indirect
	golang.org/x/sync v0.17.0
)
`
	deps := parseGoMod(content)
	if len(deps) != 3 {
		t.Fatalf("expected 3 deps, got %d: %v", len(deps), deps)
	}
	expected := map[string]bool{
		"github.com/jackc/pgx/v5": true,
		"github.com/spf13/cobra":  true,
		"golang.org/x/sync":       true,
	}
	for _, dep := range deps {
		if !expected[dep] {
			t.Errorf("unexpected dep: %q", dep)
		}
	}
}

func TestParsePackageJSON(t *testing.T) {
	data := []byte(`{
		"name": "myapp",
		"dependencies": {
			"express": "^4.18.0",
			"lodash": "~4.17.0"
		},
		"devDependencies": {
			"jest": "^29.0.0"
		}
	}`)
	deps, err := parsePackageJSON(data)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(deps) != 3 {
		t.Fatalf("expected 3 deps, got %d", len(deps))
	}
}

func TestCleanVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"^4.18.0", "4.18.0"},
		{"~1.2.3", "1.2.3"},
		{">=2.0.0", "2.0.0"},
		{"1.0.0", "1.0.0"},
		{"", ""},
	}
	for _, tt := range tests {
		got := cleanVersion(tt.input)
		if got != tt.want {
			t.Errorf("cleanVersion(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCalcLibyear(t *testing.T) {
	// Exactly 1 year apart.
	ly := calcLibyear("2023-01-01T00:00:00Z", "2024-01-01T00:00:00Z")
	if ly < 0.99 || ly > 1.01 {
		t.Errorf("calcLibyear 1 year = %f, want ~1.0", ly)
	}

	// Same date.
	ly = calcLibyear("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z")
	if ly != 0 {
		t.Errorf("calcLibyear same date = %f, want 0", ly)
	}

	// Invalid dates.
	ly = calcLibyear("", "2024-01-01T00:00:00Z")
	if ly != 0 {
		t.Errorf("calcLibyear empty = %f, want 0", ly)
	}
}

func TestParseGemfile(t *testing.T) {
	content := `
source 'https://rubygems.org'
gem 'rails', '~> 7.0'
gem 'pg'
gem "puma", "~> 5.0"
`
	deps := parseGemfile(content)
	if len(deps) != 3 {
		t.Fatalf("expected 3 deps, got %d: %v", len(deps), deps)
	}
}

func TestManifestFiles(t *testing.T) {
	// Verify all expected manifest files are registered.
	expected := []string{"package.json", "requirements.txt", "go.mod", "Cargo.toml", "Gemfile", "pom.xml"}
	for _, name := range expected {
		if _, ok := manifestFiles[name]; !ok {
			t.Errorf("manifestFiles missing %q", name)
		}
	}
}

func TestSCCLanguageUnmarshal(t *testing.T) {
	data := []byte(`[{"Name":"Go","Files":[{"Location":"/tmp/main.go","Lines":100,"Code":80,"Comment":10,"Blank":10,"Complexity":5}]}]`)
	var langs []sccLanguage
	if err := json.Unmarshal(data, &langs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(langs) != 1 {
		t.Fatalf("expected 1 language, got %d", len(langs))
	}
	if langs[0].Name != "Go" {
		t.Errorf("Name = %q", langs[0].Name)
	}
	if len(langs[0].Files) != 1 {
		t.Fatalf("expected 1 file")
	}
	f := langs[0].Files[0]
	if f.Code != 80 || f.Complexity != 5 {
		t.Errorf("Code=%d Complexity=%d", f.Code, f.Complexity)
	}
}
