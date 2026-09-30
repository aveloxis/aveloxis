// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// accountMailScope is the code that handles account holders' mail — users,
// the operator, the SMTP account: the web and API handlers, the mailer, the
// CLI, and the vulnerability digest's sender. Scoped by meaning, not by key
// alone: "to"/"from" are time windows and redirect targets in the forge
// clients, and "email" elsewhere (collector, scheduler, db) is a commit
// author's or a mailing-list sender's address from public git history and
// archives — a separate class the operator decided to leave unredacted
// (2026-09-30: they identify a failed alias when debugging; whole-branch
// review F3, recorded in the ledger — do not re-raise).
var accountMailScope = []string{"internal/web/", "internal/mailer/", "internal/api/", "cmd/aveloxis/", "internal/scheduler/vuln_digest.go"}

// isAccountAddressKey: inside accountMailScope, these keys carry an address.
func isAccountAddressKey(key string) bool {
	switch key {
	case "to", "from", "recipient", "email":
		return true
	}
	return strings.HasSuffix(key, "_email")
}

// TestEveryAccountAddressLogAttributeIsRedacted — v0.29.71 whole-branch
// review F3. Item 1 masked the mailer's startup line, and an SMTP outage
// still wrote every affected user's full address at WARN (a failed
// confirmation, a failed group-approved mail, mailer.Send), the vulnerability
// digest logged the operator's address at INFO, and test-mail both. The
// rule is mechanical: an account-address attribute's value is
// platform.RedactEmail(...) or a string literal. Denominator: the address
// attributes examined.
func TestEveryAccountAddressLogAttributeIsRedacted(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			for _, f := range unredactedAddressLogAttrs(t, rel, string(src), &examined) {
				t.Errorf("%s:%d: log attribute %q carries an account holder's address — wrap it in platform.RedactEmail", rel, f.line, f.key)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "account-address log attributes examined", examined, 10)
}

// TestAccountAddressScanCorpus proves the scan both ways.
func TestAccountAddressScanCorpus(t *testing.T) {
	src := `package p
func f() {
	l.Warn("m", "to", addr, "error", err)
	l.Info("m", "to", platform.RedactEmail(addr))
	l.Warn("m", "email", e)
	l.Debug("m", "operator_email", "")
	l.WarnContext(ctx, "m", "recipient", r)
	l.Info("to", "user_id", 3)
}
`
	n := 0
	var keys []string
	for _, f := range unredactedAddressLogAttrs(t, "internal/web/corpus.go", src, &n) {
		keys = append(keys, f.key)
	}
	if n != 5 || strings.Join(keys, ",") != "to,email,recipient" {
		t.Errorf("examined %d, flagged %v; want 5 examined, flagged to,email,recipient (the message \"to\" is not a key)", n, keys)
	}
	n = 0
	if got := unredactedAddressLogAttrs(t, "internal/collector/corpus.go", src, &n); len(got) != 0 || n != 0 {
		t.Errorf("outside the account-mail scope nothing is examined: examined %d, flagged %v", n, got)
	}
	n = 0
	if got := unredactedAddressLogAttrs(t, "internal/scheduler/vuln_digest.go", src, &n); len(got) != 3 {
		t.Errorf("a scoped FILE is examined: flagged %v", got)
	}
}

// unredactedAddressLogAttrs returns the account-address attributes in the
// file's log calls whose value is neither platform.RedactEmail(...) nor a
// string literal, counting every address attribute examined.
func unredactedAddressLogAttrs(t testing.TB, name, src string, examined *int) []urlLogAttr {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	inScope := false
	for _, p := range accountMailScope {
		if strings.HasPrefix(name, p) {
			inScope = true
		}
	}
	if !inScope {
		return nil
	}
	var out []urlLogAttr
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !logMethods[sel.Sel.Name] {
			return true
		}
		for i := firstKeyArg(sel.Sel.Name); i+1 < len(call.Args); i += 2 {
			lit, ok := call.Args[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			key := strings.Trim(lit.Value, "`\"")
			if !isAccountAddressKey(key) {
				continue
			}
			*examined++
			v := call.Args[i+1]
			if vl, ok := v.(*ast.BasicLit); ok && vl.Kind == token.STRING {
				continue
			}
			if c, ok := v.(*ast.CallExpr); ok {
				if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "RedactEmail" {
					continue
				}
			}
			out = append(out, urlLogAttr{fset.Position(v.Pos()).Line, key})
		}
		return true
	})
	return out
}
