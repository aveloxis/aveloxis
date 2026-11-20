// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// The allowances the readers counted for themselves from 0.29.195 to
// 0.29.212 are gone (pom_tree.go, "What bounds the reading of a build
// tree"): work per byte read, memory per byte read, requests per byte read,
// a wall-clock backstop. They refused honest builds, gave a wrong version,
// and never covered every path. This is the tripwire against one coming
// back under its old name: the bound on a walk is the limit on the process
// the collector reads manifests in, and a new count in this package would be
// a second bound that someone must keep complete.
func TestTheReadersCountNoAllowanceOfTheirOwn(t *testing.T) {
	gone := map[string]bool{
		"pomWorkFactor": true, "gradleWorkFactor": true, "gradleWalkFactor": true, "gradleScanBytes": true, "gradleTokensPerUnit": true,
		"pomRequestBytes": true, "pomLookBytes": true, "pomCompactPerUnit": true, "pomRemoteEntryBytes": true,
		"walkMemFactor": true, "walkMemPerFile": true, "walkAccount": true, "newWalkAccount": true,
		"walkBackstopFloor": true, "walkBackstopMargin": true, "walkUnitNanos": true, "walkClockEvery": true,
		"pomBudget": true, "pomWork": true, "gradleWalkWork": true,
		"errPomWorkBound": true, "errGradleWorkBound": true, "errWalkBackstop": true, "errWalkMemory": true,
	}
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	idents := 0
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				idents++
				if gone[id.Name] {
					t.Errorf("%s: %s is back; the readers count no allowance of their own", fset.Position(id.Pos()), id.Name)
				}
			}
			return true
		})
	}
	if idents < 10000 {
		t.Errorf("only %d identifiers examined: the package was not parsed", idents)
	}
	if _, err := filepath.Glob("walk_account.go"); err != nil {
		t.Fatal(err)
	}
	if matches, _ := filepath.Glob("walk_account.go"); len(matches) != 0 {
		t.Error("walk_account.go is back")
	}
}
