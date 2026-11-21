// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"sort"
	"strings"
	"testing"
)

// Open builders that met the 0.29.214 rule's letter (each condition on a
// parameter exercised on both sides by the registered inputs, or no
// condition at all) and still returned, for an input nobody registered, a
// statement nobody judged (review round 8; the cases are the reviewer's).
// Each is refused now, and the two shapes the store's open builders have
// (one comparison of a number with a constant, taken on both sides; a
// count that repeats a fragment with a separator before all but the
// first) are accepted.
func TestAnOpenBuilderCannotPickItsTextByItsInput(t *testing.T) {
	run := func(a []string) (string, error) { return "SELECT 1", nil }
	const bad = `"UPDATE aveloxis_data.repos SET platform_repo_id = 0"`
	cases := map[string]struct {
		builder, args string
		site          Site
	}{
		"01 the parameter is clamped, then compared: inputs 0 and 5 'take both sides' of n == 0, 5000 never run": {`func b(n int) string { n = n / 1000; if n == 0 { return "SELECT 1" }; return ` + bad + ` }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"0"}, {"5"}}, Open: "the limit"}},
		"02 a clamp by if, then a second comparison": {`func b(n int) string { if n > 500 { n = 500 }; if n == 500 { return ` + bad + ` }; return fmt.Sprintf("SELECT 1 LIMIT %d", n) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}, {"500"}, {"501"}}, Open: "the limit"}},
		"03 the condition is kept in a local bool": {`func b(n int) string { big := n > 100; if big { return ` + bad + ` }; return fmt.Sprintf("SELECT 1 LIMIT %d", n) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}},
		"04 a for used as an if": {`func b(n int) string { for n > 100 { return ` + bad + ` }; return fmt.Sprintf("SELECT 1 LIMIT %d", n) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}},
		"05 a loop bound that picks the text": {`func b(n int) string { q := "SELECT 1"; for i := 0; i < n; i++ { if i == 3 { q = ` + bad + ` } }; return q }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"2"}}, Open: "the row count"}},
		"06 an index into a list of statements": {`func b(n int) string { return []string{"SELECT 1", ` + bad + `}[n] }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"0"}}, Open: "the kind"}},
		"07 a map of statements keyed by the string": {`var m = map[string]string{"a": "SELECT 1", "b": ` + bad + `}; func b(k string) string { return m[k] }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"a"}}, Open: "the kind"}},
		"08 two comparisons, each exercised, the conjunction never": {`func b(n, m int) string { x := "SELECT 1"; if n > 0 { x += " /*n*/" }; if m > 0 { if n > 0 { return ` + bad + ` } }; return x }`, `len(os.Args), len(os.Args)`,
			Site{Tuples: [][]string{{"0", "1"}, {"1", "0"}}, Open: "the limits"}},
		"09 a comparison whose true side is exercised only where an earlier return hides it": {`func b(n int) string { if n < 10 { return "SELECT 1" }; if n > 5 { return ` + bad + ` }; return "SELECT 2" }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"3"}, {"7"}}, Open: "the limit"}},
		"10 min() chooses": {`func b(n int) string { return []string{"SELECT 1", ` + bad + `}[min(n, 1)] }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"0"}}, Open: "the kind"}},
		"11 strings.Repeat by the number": {`func b(n int) string { return "SELECT 1" + strings.Repeat("; UPDATE aveloxis_data.repos SET platform_repo_id = 0", n) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"0"}}, Open: "the count"}},
		"12 the round-7 shape, one side": {`func b(n int) string { if n == 7 { return ` + bad + ` }; return fmt.Sprintf("SELECT 1 LIMIT %d", n) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}},
	}
	// Two more of the same kind, for the rules' own edges: a switch (whose
	// cases are one place) beside a second condition, and a way out of a
	// counted loop under the one condition such a loop may hold.
	cases["13 a switch, every case given, and a second condition"] = struct {
		builder, args string
		site          Site
	}{`func b(k string, n int) string { q := "SELECT 1"; switch k { case "a": q = "SELECT 2"; case "b": q = "SELECT 3" }; if n > 0 { if q == "SELECT 3" { return ` + bad + ` } }; return q }`, `os.Getenv("A"), len(os.Args)`,
		Site{Tuples: [][]string{{"a", "1"}, {"b", "0"}, {"c", "0"}}, Open: "the kind and the limit"}}
	cases["14 a return inside the loop a count bounds"] = struct {
		builder, args string
		site          Site
	}{`func b(n int) string { for i := 0; i < n; i++ { if i > 0 { return ` + bad + ` } }; return "SELECT 1" }`, `len(os.Args)`,
		Site{Tuples: [][]string{{"1"}}, Open: "the row count"}}
	accepted := map[string]struct {
		builder, args string
		site          Site
	}{
		"one comparison with a constant, both sides taken": {`func b(n int) string { q := "SELECT 1"; if n > 0 { q += fmt.Sprintf(" LIMIT %d", n) }; return q }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"0"}, {"25"}}, Open: "the limit"}},
		"a count that repeats a fragment, with a separator": {`func b(rows int) string { var sb strings.Builder; for i := 0; i < rows; i++ { if i > 0 { sb.WriteByte(',') }; for p := 1; p <= 3; p++ { if p > 1 { sb.WriteByte(',') }; fmt.Fprintf(&sb, "$%d", i*3+p) } }; return "SELECT " + sb.String() }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}, {"2"}, {"500"}}, Open: "the row count"}},
	}
	tests := map[string]bool{}
	probe := func(builder, args string, site Site) []string {
		site.Decl, site.Ordinal, site.Builder, site.Run, site.Domain, site.Reason = "f", 1, "b", run, "d", "r"
		return siteProbe(t, builder+"\nfunc f(ctx context.Context, tx pool) { tx.Exec(ctx, b("+args+")) }", []Site{site}, tests).Problems
	}
	var names []string
	for n := range cases {
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) != 14 {
		t.Fatalf("%d cases, want the reviewer's 12 and the 2 added", len(names))
	}
	for _, name := range names {
		c := cases[name]
		if problems := probe(c.builder, c.args, c.site); len(problems) == 0 {
			t.Errorf("%s: accepted as an open builder; it returns a statement nobody judged for an input nobody registered\n  %s", name, c.builder)
		}
	}
	for name, c := range accepted {
		if problems := probe(c.builder, c.args, c.site); len(problems) != 0 {
			t.Errorf("%s: refused (%s); this is a shape the store's open builders have\n  %s", name, strings.Join(problems, " | "), c.builder)
		}
	}
	// The rules that are new in 0.29.215 each refuse for their own reason
	// (cases 04 to 08 and 10 passed every rule before it).
	for name, want := range map[string]string{
		"04 a for used as an if":                                    "spelled as a loop",
		"05 a loop bound that picks the text":                       "other than a comparison with 0",
		"06 an index into a list of statements":                     "picked from a list",
		"07 a map of statements keyed by the string":                "picked from a list",
		"08 two comparisons, each exercised, the conjunction never": "at most one condition",
		"10 min() chooses":                                          "picked from a list",
		"13 a switch, every case given, and a second condition":     "at most one condition",
		"14 a return inside the loop a count bounds":                "a return inside the loop",
	} {
		c, ok := cases[name]
		if !ok {
			t.Errorf("no case named %q", name)
			continue
		}
		if all := strings.Join(probe(c.builder, c.args, c.site), " | "); !strings.Contains(all, want) {
			t.Errorf("%s: refused, but not for %q: %s", name, want, all)
		}
	}
}
