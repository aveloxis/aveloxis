// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/json"
	"testing"
)

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
