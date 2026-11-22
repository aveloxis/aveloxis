// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The round-10 reviewer's probes (testdata/round10/a1.txt): a parameter of
// a function body reaching a registered column THROUGH a batch item of the
// write's FROM (unnest($1), VALUES ($1), SELECT $1) and not as a bare $n.
// 0.29.216 refused the bare form (the three controls, A02c, A03c, A06c) and
// accepted the nine wrapped ones: a body's parameter is not incoming in any
// form (0.29.217, review round 10, S10-1).
func TestABodysParameterIsNotIncomingThroughABatchItem(t *testing.T) {
	var lines []string
	for _, c := range probeCases(t, "round10", "a1.txt") {
		v := round9Verdict(t, c)
		if v == "pass" || strings.Contains(v, "unparsed") {
			t.Errorf("%s: judged %q; a write of a registered column from a body's parameter is refused, in every form the parameter takes", c.head, v)
		}
		lines = append(lines, fmt.Sprintf("a1.txt\t%s\t%s", c.head, v))
	}
	if len(lines) != 12 {
		t.Fatalf("%d cases read, the file holds 12 (nine wrapped forms and three bare controls)", len(lines))
	}
	got := strings.Join(lines, "\n") + "\n"
	path := filepath.Join("testdata", "round10", "verdicts.tsv")
	if os.Getenv("ROUND10_VERDICTS") == "write" {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	kept, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(kept) != got {
		t.Errorf("the probes' verdicts differ from testdata/round10/verdicts.tsv; if the change is meant, ROUND10_VERDICTS=write rewrites it:\n%s", got)
	}
}
