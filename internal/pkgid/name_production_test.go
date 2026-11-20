// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build productionnames

package pkgid

import (
	"bufio"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestGrammarAcceptsEveryProductionName runs the grammar over the operator's
// full export of distinct (package manager, name) pairs from production
// (summary/69-scratch/chunk1-prebuild.sql, query 1). The export is not in the
// repository, so this file is built only with the productionnames tag:
//
//	PRODUCTION_NAMES="chunk1-names.tsv chunk1-names-small.tsv" \
//	  go test -tags productionnames ./internal/pkgid/ -run TestGrammarAcceptsEveryProductionName -v
func TestGrammarAcceptsEveryProductionName(t *testing.T) {
	files := strings.Fields(os.Getenv("PRODUCTION_NAMES"))
	if len(files) == 0 {
		t.Fatal("set PRODUCTION_NAMES to the exported TSV files")
	}
	seen := map[string]bool{}
	var refused []string
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			manager, name, ok := strings.Cut(sc.Text(), "\t")
			if !ok || seen[sc.Text()] {
				continue
			}
			seen[sc.Text()] = true
			if !ValidName(manager, name) {
				refused = append(refused, manager+"\t"+name)
			}
		}
		if err := sc.Err(); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	sort.Strings(refused)
	t.Logf("%d distinct names, %d refused", len(seen), len(refused))
	for _, pair := range refused {
		if !productionNamesRefused[pair] {
			t.Errorf("the grammar refuses %q, which has an inventory row on production", pair)
		}
	}
	if len(refused) != len(productionNamesRefused) {
		t.Errorf("refused %d names, the list holds %d", len(refused), len(productionNamesRefused))
	}
}
