package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestConfigurationDocsCoverEveryJSONField pins the
// docs/getting-started/configuration.md file against the struct
// tags in this package. Every `json:"..."` field on Config and its
// nested structs must be mentioned in the configuration doc — by
// the JSON key name, not the Go field name.
//
// This exists because the docs drifted significantly through
// v0.18.x and v0.19.x: the original doc covered only 4 collection
// fields and had no web section, while the code grew 9 new
// collection options + the entire WebConfig + MailConfig. A
// future config addition that lands without updating the doc
// will now fail this test before merging.
//
// The check is one-way: every JSON tag in code must appear in
// the doc. The reverse (every doc field must exist in code) is
// not enforced here because we sometimes document operator-
// facing concepts (like "search_path") that aren't directly a
// config field.
func TestConfigurationDocsCoverEveryJSONField(t *testing.T) {
	tags, err := collectJSONTags("config.go")
	if err != nil {
		t.Fatalf("parsing config.go: %v", err)
	}

	docPath := filepath.Join("..", "..", "docs", "getting-started", "configuration.md")
	docBytes, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("reading %s: %v", docPath, err)
	}
	doc := string(docBytes)

	for _, tag := range tags {
		// Skip the empty tag (`json:"-"` doesn't appear in our
		// collector because we already filtered it; defensive).
		if tag == "" || tag == "-" {
			continue
		}
		// The doc may mention a field via its bare key
		// (`api_internal_url`) or via the dotted path
		// (`web.api_internal_url`). Either is acceptable; the goal
		// is that the operator can grep for the JSON key.
		if !strings.Contains(doc, tag) {
			t.Errorf("configuration.md does not mention JSON field %q — every json tag on Config and its nested structs must appear in the docs by its JSON key name. If you added a new config field, document it in docs/getting-started/configuration.md.", tag)
		}
	}
}

// collectJSONTags walks the AST of the given Go file and
// returns the list of `json:"..."` tag values on every struct
// field (recursively, via embedded type references — but we just
// scan all StructType nodes in the file, which catches everything
// we care about since all config structs live here).
func collectJSONTags(filename string) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var tags []string
	ast.Inspect(file, func(n ast.Node) bool {
		st, ok := n.(*ast.StructType)
		if !ok || st.Fields == nil {
			return true
		}
		for _, f := range st.Fields.List {
			if f.Tag == nil {
				continue
			}
			raw := strings.Trim(f.Tag.Value, "`")
			// Parse `json:"name,omitempty"`-style.
			const prefix = `json:"`
			idx := strings.Index(raw, prefix)
			if idx < 0 {
				continue
			}
			rest := raw[idx+len(prefix):]
			end := strings.Index(rest, `"`)
			if end < 0 {
				continue
			}
			val := rest[:end]
			// Strip trailing options after a comma
			// (",omitempty", ",string", etc.)
			if c := strings.Index(val, ","); c >= 0 {
				val = val[:c]
			}
			if val != "" && val != "-" {
				tags = append(tags, val)
			}
		}
		return true
	})
	return tags, nil
}
