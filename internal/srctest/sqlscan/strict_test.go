// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"strings"
	"testing"
)

func strictOf(t *testing.T, src string, env Env) []StrictArg {
	t.Helper()
	args, err := StrictArguments(typedRoot, typedFiles(src), env)
	if err != nil {
		t.Fatalf("%v\n%s", err, src)
	}
	return args
}

// one is the single SQL argument of a probe.
func one(t *testing.T, src string, env Env) StrictArg {
	t.Helper()
	args := strictOf(t, src, env)
	if len(args) != 1 {
		t.Fatalf("%d SQL arguments, want 1:\n%s", len(args), src)
	}
	return args[0]
}

const forbidden = "UPDATE aveloxis_data.repos SET repo_git = 'x'"

// Rule 1: an argument is read when the compiler calls it a string constant
// of the basic type, and at no other time. Each row that is NOT read was a
// way an evaluator of Go read text wrongly in rounds 1 to 5.
func TestAnArgumentIsReadOnlyWhenTheCompilerCallsItAConstant(t *testing.T) {
	q := "`" + forbidden + "`"
	for name, c := range map[string]struct {
		src  string
		kind StrictKind
		text string
	}{
		"a literal":              {`func f(ctx context.Context, tx pool) { tx.Exec(ctx, ` + q + `) }`, StrictConstant, forbidden},
		"a constant":             {`const s = ` + q + `; func f(ctx context.Context, tx pool) { tx.Exec(ctx, s) }`, StrictConstant, forbidden},
		"constants joined":       {`const a = "UPDATE aveloxis_data.repos "; func f(ctx context.Context, tx pool) { tx.Exec(ctx, a+"SET repo_git = 'x'") }`, StrictConstant, forbidden},
		"a conversion to string": {`type T string; const s T = ` + q + `; func f(ctx context.Context, tx pool) { tx.Exec(ctx, string(s)) }`, StrictConstant, forbidden},
		"a constant of a declared type (it may print through a method)": {`type T string; func (T) String() string { return "x" }; const s T = ` + q + `; type R interface{ Exec(ctx context.Context, sql T) }; func f(ctx context.Context, tx pool, r R) { tx.Exec(ctx, fmt.Sprintf("%s", s)) }`, StrictHole, ""},
		"a constant of a declared type, as the argument":                {`type T string; type R interface{ Exec(ctx context.Context, sql T) }; const s T = ` + q + `; func f(ctx context.Context, r R) { r.Exec(ctx, s) }`, StrictHole, ""},
		"a variable given twice":                                        {`func f(ctx context.Context, tx pool, b bool) { s := "SELECT 1"; if b { s = ` + q + ` }; tx.Exec(ctx, s) }`, StrictHole, ""},
		"a variable changed after":                                      {`func f(ctx context.Context, tx pool) { s := "SELECT 1"; defer func() { s = ` + q + ` }(); tx.Exec(ctx, s) }`, StrictHole, ""},
		"a variable whose address is taken":                             {`func g(p *string) { *p = ` + q + ` }; func f(ctx context.Context, tx pool) { s := "SELECT 1"; g(&s); tx.Exec(ctx, s) }`, StrictHole, ""},
		"a named result":                                                {`func f(ctx context.Context, tx pool) (s string) { tx.Exec(ctx, s); return }`, StrictHole, ""},
		"a package variable assigned elsewhere":                         {`var s = "SELECT 1"; func init() { s = ` + q + ` }; func f(ctx context.Context, tx pool) { tx.Exec(ctx, s) }`, StrictHole, ""},
		"an exported package variable":                                  {`var S = "SELECT 1"; func f(ctx context.Context, tx pool) { tx.Exec(ctx, S) }`, StrictHole, ""},
		"a struct field":                                                {`func f(ctx context.Context, tx pool, j job) { tx.Exec(ctx, j.sql) }`, StrictHole, ""},
		"a map lookup":                                                  {`var m = map[string]string{"a": "SELECT 1"}; func f(ctx context.Context, tx pool) { tx.Exec(ctx, m["a"]) }`, StrictHole, ""},
		"a list element":                                                {`func f(ctx context.Context, tx pool) { l := []string{"SELECT 1"}; tx.Exec(ctx, l[0]) }`, StrictHole, ""},
		"a loop variable":                                               {`func f(ctx context.Context, tx pool) { for _, s := range []string{"SELECT 1"} { tx.Exec(ctx, s) } }`, StrictHole, ""},
		"a function's result":                                           {`func g() string { return "SELECT 1" }; func f(ctx context.Context, tx pool) { tx.Exec(ctx, g()) }`, StrictHole, ""},
		"strings.Join of a list held in a variable":                     {`func f(ctx context.Context, tx pool) { l := []string{"SELECT", "1"}; tx.Exec(ctx, strings.Join(l, " ")) }`, StrictHole, ""},
		"a builder":                                                     {`func f(ctx context.Context, tx pool) { var b strings.Builder; b.WriteString("SELECT 1"); tx.Exec(ctx, b.String()) }`, StrictHole, ""},
		"an environment value":                                          {`func f(ctx context.Context, tx pool) { tx.Exec(ctx, os.Getenv("Q")) }`, StrictHole, ""},
		"a variable given once":                                         {`func f(ctx context.Context, tx pool) { s := ` + q + `; tx.Exec(ctx, s) }`, StrictPure, forbidden},
		"a package variable given once":                                 {`var s = ` + q + `; func f(ctx context.Context, tx pool) { tx.Exec(ctx, s) }`, StrictPure, forbidden},
		"a variable given once from an environment value":               {`var s = os.Getenv("Q"); func f(ctx context.Context, tx pool) { tx.Exec(ctx, s) }`, StrictHole, ""},
	} {
		a := one(t, c.src, Env{})
		if a.Kind != c.kind || a.Text != c.text {
			t.Errorf("%s: %s %q, want %s %q (%s)", name, a.Kind, a.Text, c.kind, c.text, a.Why)
		}
	}
}

// Rule 2: the listed functions, on compiler constants, are CALLED. The
// expected text comes from calling the same function here, so a model of
// one (a verb counted, a flag skipped) would differ from it.
func TestAPureCallOnConstantsIsRun(t *testing.T) {
	for src, want := range map[string]string{
		`fmt.Sprintf("SELECT %s, %[1]s, %d|%[2]d%%", "a", 7)`:               "SELECT a, a, 7|7%",
		`fmt.Sprintf("LIMIT %d", uint8(200))`:                               "LIMIT 200",
		`strings.Repeat("?, ", 3)`:                                          strings.Repeat("?, ", 3),
		`strings.ReplaceAll("a.b.c", ".", "_")`:                             strings.ReplaceAll("a.b.c", ".", "_"),
		`strings.Replace("a.b.c", ".", "_", 2-1)`:                           strings.Replace("a.b.c", ".", "_", 1),
		`strings.Join([]string{"a", "b" + "c", "d"}, ", ")`:                 strings.Join([]string{"a", "bc", "d"}, ", "),
		`"SELECT " + strings.Join([]string{}, ", ") + "1"`:                  "SELECT 1",
		`strings.TrimSpace("  SELECT 1 ")`:                                  "SELECT 1",
		`strings.TrimPrefix("xSELECT 1", "x")`:                              "SELECT 1",
		`strings.TrimSuffix("SELECT 1;", ";")`:                              "SELECT 1",
		`"SELECT " + strings.Repeat("1", 2) + fmt.Sprintf(" FROM %s", "t")`: "SELECT 11 FROM t",
	} {
		a := one(t, `func f(ctx context.Context, tx pool) { tx.Exec(ctx, `+src+`) }`, Env{})
		if a.Kind != StrictPure || a.Text != want {
			t.Errorf("%s: %s %q, want pure %q (%s)", src, a.Kind, a.Text, want, a.Why)
		}
	}
	// One argument that is not a compiler constant of a basic type, and
	// the whole call is not read; so is a call fmt itself complains of.
	for _, src := range []string{
		// Round 6: only %s, %d and %% are read, each with a constant of its
		// own kind: every other verb's output depends on the Go type the
		// value has in the program, which the reading does not hand it.
		`fmt.Sprintf("UPDATE aveloxis_data.repos SET %.1Tpdated_at = NULL", uint(1))`, `fmt.Sprintf("SELECT %v", 1)`, `fmt.Sprintf("SELECT %q", "a")`, `fmt.Sprintf("SELECT %5d", 1)`,
		`fmt.Sprintf("SELECT %c", 59)`, `fmt.Sprintf("SELECT %x", 255)`, `fmt.Sprintf("SELECT %s", 1)`, `fmt.Sprintf("SELECT %d", 1.5)`, `fmt.Sprintf("SELECT %t", true)`,
		`fmt.Sprintf("SELECT %[3]s", "a")`, `fmt.Sprintf("SELECT %[x]s", "a")`, `fmt.Sprintf("SELECT %", "a")`, `fmt.Sprintf("SELECT %s %s", "a")`, `fmt.Sprintf("SELECT %s", "a", "b")`,
		`fmt.Sprintf("SELECT %s", os.Getenv("X"))`, `fmt.Sprintf(os.Getenv("F"), "a")`, `fmt.Sprintf("SELECT %d", "a")`, `fmt.Sprintf("SELECT %s")`,
		`fmt.Sprintf("SELECT %s", []any{"a"}...)`, `strings.Repeat("?", len(os.Args))`, `strings.Join([]string{"a", os.Getenv("X")}, ",")`, `strings.Join([]string{"a"}, os.Getenv("S"))`,
		`strings.Join(os.Args, ",")`, `strings.Join([]string{2: "c", 0: "a"}, ",")`, `strings.Join(append([]string{"a"}, "b"), ",")`, `strings.ToUpper("select 1")`, `fmt.Sprint("SELECT 1")`,
	} {
		if a := one(t, `func f(ctx context.Context, tx pool) { tx.Exec(ctx, `+src+`) }`, Env{}); a.Kind != StrictHole {
			t.Errorf("%s: read as %s %q", src, a.Kind, a.Text)
		}
	}
	a := one(t, `type T string; func (T) String() string { return "evil" }; const c T = "good"; func f(ctx context.Context, tx pool) { tx.Exec(ctx, fmt.Sprintf("SELECT %s", c)) }`, Env{})
	if a.Kind != StrictHole {
		t.Errorf("a constant of a type with a String method was handed to Sprintf as its text: %s %q", a.Kind, a.Text)
	}
}

// The store's own text is run, not described: a call of a package function
// on constants gives what the Env's function gives, with those constants.
func TestBuiltTextIsWhatTheEnvsFunctionGives(t *testing.T) {
	var got [][]string
	env := Env{
		Embedded: map[string]string{"schemaSQL": "CREATE TABLE s.t (a int)"},
		Funcs: map[string]func([]string) string{"bucket": func(a []string) string {
			got = append(got, a)
			return "date_trunc('week', " + a[0] + ")"
		}},
	}
	src := `var schemaSQL string
func bucket(e string) string { return "something else entirely" }
func f(ctx context.Context, tx pool) {
	tx.Exec(ctx, "SELECT " + bucket("ts") + " FROM t")
	tx.Exec(ctx, schemaSQL)
}`
	args := strictOf(t, src, env)
	if args[0].Kind != StrictBuilt || args[0].Text != "SELECT date_trunc('week', ts) FROM t" || len(got) != 1 || got[0][0] != "ts" {
		t.Errorf("a built argument: %s %q (called with %v)", args[0].Kind, args[0].Text, got)
	}
	if args[1].Kind != StrictBuilt || args[1].Text != "CREATE TABLE s.t (a int)" {
		t.Errorf("an embedded variable: %s %q", args[1].Kind, args[1].Text)
	}
	// With no Env entry, neither is read.
	for i, a := range strictOf(t, src, Env{}) {
		if a.Kind != StrictHole {
			t.Errorf("argument %d is read with no Env: %s", i+1, a.Kind)
		}
	}
	// Not read: an argument that is not a constant, a method, a variable
	// that is given a value somewhere.
	for name, s := range map[string]string{
		"a non-constant argument": `func bucket(e string) string { return e }; func f(ctx context.Context, tx pool, x string) { tx.Exec(ctx, "SELECT " + bucket(x)) }`,
		"an embedded variable that is assigned": `var schemaSQL string; func init() { schemaSQL = "x" }; func bucket(e string) string { return e }
func f(ctx context.Context, tx pool) { tx.Exec(ctx, schemaSQL + bucket("a")) }`,
	} {
		args, _ := StrictArguments(typedRoot, typedFiles(s), env)
		if len(args) != 1 || args[0].Kind != StrictHole {
			t.Errorf("%s: read", name)
		}
	}
	// An Env entry that nothing uses is refused.
	if _, err := StrictArguments(typedRoot, typedFiles(`func f(ctx context.Context, tx pool) { tx.Exec(ctx, "SELECT 1") }`), env); err == nil {
		t.Error("an Env naming what no argument is built from was accepted")
	}
}

// A parameter handed on untouched is its callers' argument; one that is
// changed, or that another package can supply, is not.
func TestAParameterIsReadAtItsCallersOnlyWhenNothingElseCanSetIt(t *testing.T) {
	args := strictOf(t, `func run(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool) { run(ctx, tx, `+"`"+forbidden+"`"+`) }`, Env{})
	kinds := ""
	for _, a := range args {
		kinds += a.Kind.String() + ":" + a.Text + ";"
	}
	if kinds != "parameter:;constant:"+forbidden+";" {
		t.Errorf("a helper's parameter and its caller: %s", kinds)
	}
	for name, src := range map[string]string{
		"changed before the call": `func run(ctx context.Context, tx pool, q string) { q = q + " RETURNING 1"; tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool) { run(ctx, tx, "SELECT 1") }`,
		"its address taken": `func g(*string) {}; func run(ctx context.Context, tx pool, q string) { g(&q); tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool) { run(ctx, tx, "SELECT 1") }`,
		"an exported function": `func Run(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool) { Run(ctx, tx, "SELECT 1") }`,
	} {
		args := strictOf(t, src, Env{})
		if args[0].Kind != StrictHole {
			t.Errorf("%s: the parameter is %s", name, args[0].Kind)
		}
	}
}

func siteProbe(t *testing.T, src string, sites []Site, tests map[string]bool) SiteReport {
	t.Helper()
	files := typedFiles(src)
	args, err := StrictArguments(typedRoot, files, Env{})
	if err != nil {
		t.Fatal(err)
	}
	pins, err := StrictPins(typedRoot, files, Env{})
	if err != nil {
		t.Fatal(err)
	}
	return CheckSites(args, sites, pins, tests)
}

// A hole is a failure until it is registered, and a registration is held
// to what it says.
func TestAHoleMustBeRegisteredAndTheRegistrationHeld(t *testing.T) {
	const src = `func table() string { return os.Getenv("T") }
func stmt(tbl string) string { return "UPDATE " + tbl + " SET repo_git = 'x'" }
var whole = stmt(table())
func f(ctx context.Context, tx pool) {
	tx.Exec(ctx, stmt(table()))
	tx.Exec(ctx, whole)
	tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE n = %d", table(), len(os.Args)))
}`
	pin := func() string {
		files := typedFiles(src)
		pins, _ := StrictPins(typedRoot, files, Env{})
		return pins["f"].Digest
	}()
	tests := map[string]bool{"TestStmtRefuses": true}
	good := []Site{
		{Decl: "f", Ordinal: 1, Builder: "stmt", Tuples: [][]string{{"aveloxis_data.repos"}, {"aveloxis_data.other"}},
			Run:    func(a []string) (string, error) { return "UPDATE " + a[0] + " SET repo_git = 'x'", nil },
			Domain: "the two tables", RefusalTest: "TestStmtRefuses", Reason: "r"},
		{Decl: "f", Ordinal: 2, Builder: "whole", Statements: []string{"SELECT 1"}, Domain: "no input", Pin: pin, Reason: "r"},
		{Decl: "f", Ordinal: 3, Frozen: pin, Fills: [][]string{{"s.t", "7"}}, Reason: "r"},
	}
	rep := siteProbe(t, src, good, tests)
	if len(rep.Problems) != 0 {
		t.Fatalf("a full registration has problems: %v", rep.Problems)
	}
	var texts []string
	for _, s := range rep.Statements {
		texts = append(texts, s.SQL)
	}
	if got := strings.Join(texts, " | "); got != "UPDATE aveloxis_data.repos SET repo_git = 'x' | UPDATE aveloxis_data.other SET repo_git = 'x' | SELECT 1 | DELETE FROM s.t WHERE n = 7" {
		t.Errorf("the statements handed on: %s", got)
	}
	if rep.Executed != 2 || rep.Frozen != 1 {
		t.Errorf("counts: %d executed, %d frozen", rep.Executed, rep.Frozen)
	}
	change := func(i int, f func(*Site)) []Site {
		out := append([]Site(nil), good...)
		f(&out[i])
		return out
	}
	for name, sites := range map[string][]Site{
		"no registration":                       good[1:],
		"a registration of nothing":             append(append([]Site(nil), good...), Site{Decl: "f", Ordinal: 9, Frozen: pin, Reason: "r"}),
		"registered twice":                      append(append([]Site(nil), good...), good[0]),
		"another builder named":                 change(0, func(s *Site) { s.Builder = "table" }),
		"no builder named":                      change(0, func(s *Site) { s.Builder = "" }),
		"no input":                              change(0, func(s *Site) { s.Tuples = nil }),
		"no builder to run":                     change(0, func(s *Site) { s.Run = nil }),
		"an input of the wrong size":            change(0, func(s *Site) { s.Tuples = [][]string{{"a", "b"}} }),
		"an input the builder refuses":          change(0, func(s *Site) { s.Run = func([]string) (string, error) { return "", fmt.Errorf("no") } }),
		"another result of the builder":         change(0, func(s *Site) { s.Result = 1 }),
		"a function with statements of its own": change(0, func(s *Site) { s.Statements = []string{"SELECT 1"} }),
		"a variable with no value":              change(1, func(s *Site) { s.Statements = nil }),
		"no domain":                             change(0, func(s *Site) { s.Domain = "" }),
		"no refusal test":                       change(0, func(s *Site) { s.RefusalTest = "" }),
		"a refusal test that does not exist":    change(0, func(s *Site) { s.RefusalTest = "TestNoSuch" }),
		"no reason":                             change(0, func(s *Site) { s.Reason = "" }),
		"an open registration with a test":      change(0, func(s *Site) { s.Open = "the table" }),
		"a variable with a refusal test":        change(1, func(s *Site) { s.RefusalTest = "TestStmtRefuses" }),
		"a variable builder with no pin":        change(1, func(s *Site) { s.Pin = "" }),
		"a variable builder with a stale pin":   change(1, func(s *Site) { s.Pin = "0000/1" }),
		"a stale digest":                        change(2, func(s *Site) { s.Frozen = "0000/1" }),
		"a frozen write with nothing to judge":  change(2, func(s *Site) { s.Fills = nil }),
		"a fill with too few values":            change(2, func(s *Site) { s.Fills = [][]string{{"s.t"}} }),
		"a fill that is not a number":           change(2, func(s *Site) { s.Fills = [][]string{{"s.t", "seven"}} }),
		"a frozen registration with a builder":  change(2, func(s *Site) { s.Builder = "stmt" }),
		"fills on an executed registration":     change(0, func(s *Site) { s.Fills = [][]string{{"x"}} }),
	} {
		if rep := siteProbe(t, src, sites, tests); len(rep.Problems) == 0 {
			t.Errorf("%s: no problem reported", name)
		}
	}
	// A builder that is a variable must be given its value once.
	rep = siteProbe(t, strings.Replace(src, "func f(", "func init() { whole = \"x\" }\nfunc f(", 1), good, tests)
	found := false
	for _, p := range rep.Problems {
		found = found || strings.Contains(p, "variable that something in the package gives a value")
	}
	if !found {
		t.Errorf("a builder variable assigned elsewhere was accepted: %v", rep.Problems)
	}
}

// A frozen registration's digest covers the source its text is made of: a
// change to a function it calls for text changes it; a change elsewhere,
// or to a comment, does not.
func TestAFrozenDigestFollowsTheSourceTheTextIsMadeOf(t *testing.T) {
	src := func(where, other, body string) string {
		return `func where() string { return "` + where + `" }
func other() int { return ` + other + ` }
func f(ctx context.Context, tx pool, q string) { // ` + body + `
	_ = other()
	tx.Exec(ctx, "DELETE FROM s.t WHERE " + where() + os.Getenv("X"))
}`
	}
	digest := func(s string) string {
		pins, err := StrictPins(typedRoot, typedFiles(s), Env{})
		if err != nil {
			t.Fatal(err)
		}
		return pins["f"].Digest
	}
	base := digest(src("a = 1", "1", "note"))
	if base == "" || digest(src("a = 1", "1", "another note")) != base {
		t.Error("a comment changed the digest")
	}
	if digest(src("a = 1", "2", "note")) != base {
		t.Error("a function that returns no text changed the digest")
	}
	if digest(src("TRUE", "1", "note")) == base {
		t.Error("the text of a function the statement is built from changed, and the digest did not")
	}
	if digest(strings.Replace(src("a = 1", "1", "note"), "DELETE FROM s.t", "DELETE FROM s.u", 1)) == base {
		t.Error("the statement's own fixed text changed, and the digest did not")
	}
}

// Harvest judges the fixed statements of the source, and a conflict's tail
// behind the INSERT written beside it.
func TestHarvestJudgesTheFixedStatementsOfTheSource(t *testing.T) {
	const src = `func f(ctx context.Context, tx pool, rows []int) {
	steps := []step{{"a", "UPDATE aveloxis_data.repos SET repo_git = 'x'"}, {"b", "SELECT 1"}}
	for _, st := range steps {
		tx.Exec(ctx, st.sql)
	}
	var b strings.Builder
	b.WriteString("INSERT INTO aveloxis_data.repos (repo_id, repo_git) VALUES ")
	for range rows {
		b.WriteString("($1, $2)")
	}
	b.WriteString(" ON CONFLICT (repo_id) DO UPDATE SET repo_git = EXCLUDED.repo_git")
	tx.Exec(ctx, b.String())
}`
	files := typedFiles(src)
	pins, _ := StrictPins(typedRoot, files, Env{})
	args, _ := StrictArguments(typedRoot, files, Env{})
	rep := CheckSites(args, []Site{{Decl: "f", Ordinal: 1, Frozen: pins["f"].Digest, Harvest: true, Reason: "r"}, {Decl: "f", Ordinal: 2, Frozen: pins["f"].Digest, Harvest: true, Reason: "r"}}, pins, nil)
	if len(rep.Problems) != 0 {
		t.Fatal(rep.Problems)
	}
	chk := Check(rep.Statements, repoGit, nil)
	exprs := map[string]bool{}
	for _, v := range chk.Violations {
		exprs[v.Expr] = true
	}
	// (A violation's Expr is the value written out from its tree: a name as
	// PostgreSQL folds it.)
	if !exprs["'x'"] || !exprs["excluded.repo_git"] {
		t.Errorf("the step's write and the conflict's update must both be judged; violations: %+v", chk.Violations)
	}
}

// S5s-4: SQL the library runs with no text at the call is closed off at
// the types that carry it, wherever a value of one stands.
func TestACarrierOfSQLIsReportedAtEveryDoor(t *testing.T) {
	for name, src := range map[string]string{
		"TxOptions with no field names":                                    `func f(ctx context.Context, p *pgxpool.Pool) { p.BeginTx(ctx, pgx.TxOptions{"", "", "", "BEGIN; ` + forbidden + `", ""}) }`,
		"a batch made with its list":                                       `func f(ctx context.Context, p *pgxpool.Pool) { p.SendBatch(ctx, &pgx.Batch{QueuedQueries: []*pgx.QueuedQuery{{SQL: "SELECT 1"}}}) }`,
		"a batch's list appended to":                                       `func f(ctx context.Context, p *pgxpool.Pool, q *pgx.QueuedQuery) { b := &pgx.Batch{}; b.QueuedQueries = append(b.QueuedQueries, q); p.SendBatch(ctx, b) }`,
		"a batch's list handed on":                                         `func g([]*pgx.QueuedQuery) {}; func f(b *pgx.Batch) { g(b.QueuedQueries) }`,
		"the address of an SQL field":                                      `func f(q *pgx.QueuedQuery) *string { return &q.SQL }`,
		"an SQL field given several at once":                               `func two() (string, error) { return "", nil }; func f(q *pgx.QueuedQuery) { var err error; q.SQL, err = two(); _ = err }`,
		"an SQL field added to":                                            `func f(q *pgx.QueuedQuery) { q.SQL += "; ` + forbidden + `" }`,
		"an exported function takes one":                                   `func Send(ctx context.Context, p *pgxpool.Pool, b *pgx.Batch) { p.SendBatch(ctx, b) }`,
		"an exported function takes a list of them":                        `func Send(ctx context.Context, p *pgxpool.Pool, bs []*pgx.Batch) { p.SendBatch(ctx, bs[0]) }`,
		"an exported function takes a maker":                               `func Send(ctx context.Context, p *pgxpool.Pool, mk func() *pgx.Batch) { p.SendBatch(ctx, mk()) }`,
		"an interface's exported method takes one":                         `type Sender interface{ Send(ctx context.Context, b *pgx.Batch) }; func f(ctx context.Context, s Sender) { s.Send(ctx, &pgx.Batch{}) }`,
		"an unexported function that takes one, handed to another package": `func send(b *pgx.Batch) {}; func f() { fmt.Println(send) }`,
		"an exported variable holds one":                                   `var Pending = &pgx.Batch{}; func f(ctx context.Context, p *pgxpool.Pool) { p.SendBatch(ctx, Pending) }`,
		"an exported variable holds a function that takes one":             `func run(ctx context.Context, p *pgxpool.Pool, bs []*pgx.Batch) { p.SendBatch(ctx, bs[0]) }; var Runner = run`,
		"an exported field holds one":                                      `type W struct{ Batch *pgx.Batch }; func f(ctx context.Context, p *pgxpool.Pool, w W) { p.SendBatch(ctx, w.Batch) }`,
		"a type assertion":                                                 `func f(ctx context.Context, p *pgxpool.Pool, v any) { p.SendBatch(ctx, v.(*pgx.Batch)) }`,
		"a type switch":                                                    `func f(ctx context.Context, p *pgxpool.Pool, v any) { switch b := v.(type) { case *pgx.Batch: p.SendBatch(ctx, b) } }`,
		"a channel of them":                                                `func f(ctx context.Context, p *pgxpool.Pool, c chan *pgx.Batch) { p.SendBatch(ctx, <-c) }`,
		"one from another package's call":                                  `func f(ctx context.Context, p *pgxpool.Pool) { p.SendBatch(ctx, reflect.ValueOf(1).Interface().(*pgx.Batch)) }`,
		"one handed to another package":                                    `func f(b *pgx.Batch) { fmt.Sscan("x", b) }`,
		"a prepared statement of database/sql":                             `func f(db *sql.DB) { st, _ := db.Prepare("SELECT 1"); st.Exec() }`,
		"a statement description":                                          `func f(ctx context.Context, c *pgx.Conn) { d, _ := c.Prepare(ctx, "n", "SELECT 1"); _ = d }`,
	} {
		refs, err := StrictReferences(typedRoot, typedFiles(src))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(refs) == 0 {
			t.Errorf("%s: nothing reported\n  %s", name, src)
		}
	}
	// The forms that are read are not reported: a batch made empty and
	// filled by Queue, a keyed literal whose SQL field is read as an
	// argument, a field out of other packages' reach.
	for name, src := range map[string]string{
		"an empty batch and Queue": `func f(ctx context.Context, p *pgxpool.Pool) { b := &pgx.Batch{}; b.Queue("SELECT 1"); _ = b.Len(); p.SendBatch(ctx, b) }`,
		"keyed TxOptions":          `func f(ctx context.Context, p *pgxpool.Pool) { p.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}) }`,
		"reading the list":         `func f(b *pgx.Batch) int { n := 0; for range b.QueuedQueries { n++ }; return n + len(b.QueuedQueries) }; func g() int { return f(&pgx.Batch{}) }`,
		"an unexported field":      `type w struct{ batch *pgx.Batch }; func New() *w { return &w{batch: &pgx.Batch{}} }`,
	} {
		refs, err := StrictReferences(typedRoot, typedFiles(src))
		if err != nil || len(refs) != 0 {
			t.Errorf("%s: %v %+v", name, err, refs)
		}
	}
	// A keyed literal's SQL field IS an SQL argument, read like any other.
	a := one(t, `func f(ctx context.Context, p *pgxpool.Pool) { p.BeginTx(ctx, pgx.TxOptions{BeginQuery: "BEGIN; `+forbidden+`"}) }`, Env{})
	if a.Kind != StrictConstant || !strings.Contains(a.Text, forbidden) {
		t.Errorf("TxOptions.BeginQuery is not read as an SQL argument: %s %q", a.Kind, a.Text)
	}
}

// The carriers are a closed list, taken from the library: a type that
// could carry SQL and is in neither list fails here (a library upgrade).
func TestEveryLibraryTypeThatCouldCarrySQLIsClassified(t *testing.T) {
	candidates, unclassified, stale, err := LibraryCarrierCandidates(typedRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) < len(libraryCarriers) {
		t.Fatalf("only %d candidate types were found in the library; the carriers alone are %d", len(candidates), len(libraryCarriers))
	}
	for _, c := range unclassified {
		t.Errorf("%s could carry SQL (it holds or reports SQL, holds a carrier, or is what a call that runs SQL returns) and is in neither libraryCarriers nor libraryNotCarriers: read it and say which", c)
	}
	for _, c := range stale {
		t.Errorf("%s is classified and is no candidate in this library version: delete the entry", c)
	}
	// The check can fail: a candidate taken out of the lists is named.
	const probe = "pgx.Rows"
	saved := libraryNotCarriers[probe]
	delete(libraryNotCarriers, probe)
	_, unclassified, _, err = LibraryCarrierCandidates(typedRoot)
	libraryNotCarriers[probe] = saved
	if err != nil || strings.Join(unclassified, ",") != probe {
		t.Errorf("with %s unclassified, the unclassified candidates are %v (%v)", probe, unclassified, err)
	}
}

// FragmentWords reads names out of a piece of SQL, whole or not.
func TestFragmentWordsFoldsNamesAndNeverFails(t *testing.T) {
	got := strings.Join(FragmentWords(`UPDATE Aveloxis_Data."Repos" SET x = 'UPDATE y' -- DELETE`), ",")
	if got != "update,aveloxis_data,Repos,set,x" {
		t.Errorf("a whole piece: %s", got)
	}
	if got := strings.Join(FragmentWords(`) UPDATE REPOS SET a = '`), ","); !strings.Contains(got, "update") || !strings.Contains(got, "repos") {
		t.Errorf("a piece that does not tokenise: %s", got)
	}
}

// Every reference to something that runs SQL which is not a call of it at
// a place the reading reads is reported where it stands (typed.go).
func TestAReferenceToWhatRunsSQLIsReported(t *testing.T) {
	for name, src := range map[string]string{
		"a helper taken as a value": `func run(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool) { h := run; h(ctx, tx, "SELECT 1") }`,
		"a helper literal held in an exported variable": `var Run = func(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool) { Run(ctx, tx, "SELECT 1") }`,
		"a runner taken as a value": `func f(ctx context.Context, p *pgxpool.Pool) { e := p.Exec; e(ctx, "SELECT 1") }`,
		"a method expression":       `func f(ctx context.Context, p *pgxpool.Pool) { (*pgxpool.Pool).Exec(p, ctx, "SELECT 1") }`,
		"a call by reflection":      `func f(p *pgxpool.Pool) { reflect.ValueOf(p).MethodByName("Exec") }`,
		"rows written with no text": `func f(ctx context.Context, p *pgxpool.Pool) { p.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"a"}, nil) }`,
		"a batch-taking literal whose variable has its address taken": `func f(ctx context.Context, p *pgxpool.Pool) { h := func(b *pgx.Batch) { p.SendBatch(ctx, b) }; g := &h; (*g)(&pgx.Batch{}) }`,
		// Round 6.
		"the several results of one call as a runner's arguments": `func two() (context.Context, string) { return context.Background(), "UPDATE aveloxis_data.repos SET repo_git = 'x'" }
func f(p *pgxpool.Pool) { p.Exec(two()) }`,
		"the several results of one call as a helper's arguments": `func run(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }
func three(tx pool) (context.Context, pool, string) { return context.Background(), tx, "UPDATE aveloxis_data.repos SET repo_git = 'x'" }
func f(ctx context.Context, tx pool) { run(ctx, tx, "SELECT 1"); run(three(tx)) }`,
		"a query rewriter among a runner's arguments": `type rw struct{}
func (rw) RewriteQuery(ctx context.Context, c *pgx.Conn, sql string, a []any) (string, []any, error) { return "UPDATE aveloxis_data.repos SET repo_git = 'x'", nil, nil }
func f(ctx context.Context, p *pgxpool.Pool) { p.Exec(ctx, "SELECT 1", rw{}) }`,
		"the library's own rewriter":   `func f(ctx context.Context, p *pgxpool.Pool) { p.Exec(ctx, "SELECT @a", pgx.NamedArgs{"a": 1}) }`,
		"a rewriter queued on a batch": `func f(b *pgx.Batch) { b.Queue("SELECT @a", pgx.NamedArgs{"a": 1}) }`,
		"a returned SQL-running function held": `func mk(tx pool) func(context.Context, string) { return func(ctx context.Context, q string) { tx.Exec(ctx, q) } }
func f(ctx context.Context, tx pool) { mk(tx)(ctx, "SELECT 1"); g := mk(tx); g(ctx, "UPDATE aveloxis_data.repos SET repo_git = 'x'") }`,
		"a returned SQL-running function passed on": `func mk(tx pool) func(context.Context, string) { return func(ctx context.Context, q string) { tx.Exec(ctx, q) } }
func use(g func(context.Context, string)) { g(context.Background(), "UPDATE aveloxis_data.repos SET repo_git = 'x'") }
func f(ctx context.Context, tx pool) { mk(tx)(ctx, "SELECT 1"); use(mk(tx)) }`,
		"a helper method of a generic type": `type G[T any] struct{ tx pool }
func (g G[T]) run(ctx context.Context, q string) { g.tx.Exec(ctx, q) }
type r interface{ run(context.Context, string) }
func ok(ctx context.Context, x r) { x.run(ctx, "SELECT 1") }
func f(ctx context.Context, tx pool) { G[int]{tx}.run(ctx, "UPDATE aveloxis_data.repos SET repo_git = 'x'") }`,
	} {
		refs, err := StrictReferences(typedRoot, typedFiles(src))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(refs) == 0 {
			t.Errorf("%s: nothing reported\n  %s", name, src)
		}
	}
	// A helper called by its name everywhere, and a batch-taker whose
	// every call is found, are not references.
	refs, err := StrictReferences(typedRoot, typedFiles(`func run(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }
func send(ctx context.Context, p *pgxpool.Pool, b *pgx.Batch) { p.SendBatch(ctx, b) }
func f(ctx context.Context, tx pool, p *pgxpool.Pool) { run(ctx, tx, "SELECT 1"); b := &pgx.Batch{}; b.Queue("SELECT 1"); send(ctx, p, b) }`))
	if err != nil || len(refs) != 0 {
		t.Errorf("ordinary helpers were reported: %v %+v", err, refs)
	}
	// Nor are a generic FUNCTION (its calls name the one object, and are
	// read), a returned function called where it is made, and arguments
	// that are no rewriter.
	refs, err = StrictReferences(typedRoot, typedFiles(`func read[T any](ctx context.Context, tx pool, q string, args ...any) { tx.Exec(ctx, q, args...) }
func mk(tx pool) func(context.Context, string) { return func(ctx context.Context, q string) { tx.Exec(ctx, q) } }
func f(ctx context.Context, tx pool, id int64) { read[int](ctx, tx, "SELECT 1", id, "x"); mk(tx)(ctx, "SELECT 2") }`))
	if err != nil || len(refs) != 0 {
		t.Errorf("forms the reading follows were reported: %v %+v", err, refs)
	}
	args := strictOf(t, `func read[T any](ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool) { read[int](ctx, tx, "`+forbidden+`") }`, Env{})
	seen := false
	for _, a := range args {
		seen = seen || (a.Kind == StrictConstant && a.Text == forbidden)
	}
	if !seen {
		t.Errorf("the SQL handed to a generic function was not read at its call: %+v", args)
	}
}

// A local taken once from a builder's call names that builder, with which
// result it is; given anything after, it names none.
func TestALocalTakenOnceFromABuilderNamesIt(t *testing.T) {
	const decls = `func three(k string) (a, b string, err error) { return os.Getenv(k), k, nil }
func single(k string) string { return os.Getenv(k) }
`
	for name, c := range map[string]struct {
		body    string
		builder string
		result  int
	}{
		"the second result":                 {`_, q, err := three("x"); _ = err; tx.Exec(ctx, q)`, "three", 1},
		"the first result":                  {`q, _, err := three("x"); _ = err; tx.Exec(ctx, q)`, "three", 0},
		"a single result":                   {`q := single("x"); tx.Exec(ctx, q)`, "single", 0},
		"given again":                       {`_, q, err := three("x"); _ = err; q = "SELECT 1"; tx.Exec(ctx, q)`, "", 0},
		"given again by a call":             {`_, q, err := three("x"); _, q, err = three("y"); _ = err; tx.Exec(ctx, q)`, "", 0},
		"its address taken":                 {`_, q, err := three("x"); _ = err; p := &q; _ = p; tx.Exec(ctx, q)`, "", 0},
		"changed in place":                  {`q := single("x"); q += "; SELECT 2"; tx.Exec(ctx, q)`, "", 0},
		"a call inside a larger expression": {`tx.Exec(ctx, single("x")+" LIMIT 1")`, "", 0},
	} {
		a := one(t, decls+`func f(ctx context.Context, tx pool) { `+c.body+` }`, Env{})
		if a.Kind != StrictHole || a.Builder != c.builder || a.BuilderResult != c.result {
			t.Errorf("%s: %s, builder %q result %d; want a hole of %q result %d", name, a.Kind, a.Builder, a.BuilderResult, c.builder, c.result)
		}
	}
}

// An open registration's builder may put what varies into the text only
// inside a pgx.Identifier, through a registered quoting helper with its
// test, or as a number; a builder of bools needs every combination.
func TestAnOpenBuilderMayOnlyQuoteWhatVaries(t *testing.T) {
	run := func(a []string) (string, error) { return "SELECT 1", nil }
	tests := map[string]bool{"TestQuote": true}
	for name, c := range map[string]struct {
		builder, args string
		site          Site
		ok            bool
	}{
		"inside an Identifier": {`func b(s, t string, n int) string { return fmt.Sprintf("SELECT %d FROM %s", n, pgx.Identifier{s, t}.Sanitize()) }`, `os.Getenv("A"), os.Getenv("B"), len(os.Args)`,
			Site{Tuples: [][]string{{"s", "t", "1"}}, Open: "the table"}, true},
		"a list converted to an Identifier": {`func b(parts []string) string { return "SELECT 1 FROM " + pgx.Identifier(parts).Sanitize() }`, `os.Args`,
			Site{Tuples: [][]string{{"s.t"}}, Open: "the table"}, true},
		"through a registered quoter": {`func quote(s string) string { return pgx.Identifier{s}.Sanitize() }; func b(t string) string { return "SELECT 1 FROM " + quote(t) }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table", Quoters: map[string]string{"quote": "TestQuote"}}, true},
		"through a helper that is not registered": {`func quote(s string) string { return s }; func b(t string) string { return "SELECT 1 FROM " + quote(t) }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table"}, false},
		"a quoter whose test does not exist": {`func quote(s string) string { return s }; func b(t string) string { return "SELECT 1 FROM " + quote(t) }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table", Quoters: map[string]string{"quote": "TestNoSuch"}}, false},
		"joined into the text": {`func b(t string) string { return "SELECT 1 FROM " + t }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table"}, false},
		"through Sprintf": {`func b(t string) string { return fmt.Sprintf("SELECT 1 FROM %s", t) }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table"}, false},
		"inside a literal that is no Identifier": {`func b(t string) string { return "SELECT 1 FROM " + strings.Join([]string{t}, "") }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table"}, false},
		"raw by design, with its sentence": {`func b(t, where string) string { return "SELECT 1 FROM " + pgx.Identifier{t}.Sanitize() + " WHERE " + where }`, `os.Getenv("A"), os.Getenv("B")`,
			Site{Tuples: [][]string{{"t", "TRUE"}}, Open: "the table", Raw: map[string]string{"where": "a read-only transaction"}}, true},
		"raw with no sentence": {`func b(t, where string) string { return "SELECT 1 FROM " + pgx.Identifier{t}.Sanitize() + " WHERE " + where }`, `os.Getenv("A"), os.Getenv("B")`,
			Site{Tuples: [][]string{{"t", "TRUE"}}, Open: "the table", Raw: map[string]string{"where": " "}}, false},
		"raw registered for a parameter that is quoted": {`func b(t string) string { return "SELECT 1 FROM " + pgx.Identifier{t}.Sanitize() }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table", Raw: map[string]string{"t": "x"}}, false},
		"another parameter than the raw one is loose": {`func b(t, where string) string { return "SELECT 1 FROM " + t + " WHERE " + where }`, `os.Getenv("A"), os.Getenv("B")`,
			Site{Tuples: [][]string{{"t", "TRUE"}}, Open: "the table", Raw: map[string]string{"where": "a read-only transaction"}}, false},
		"a chooser: the parameter only picks among fixed texts": {`func pick(k string) string { switch k { case "a": return "x" }; return "y" }; func b(k string) string { if k == "z" { return "SELECT 0" }; return "SELECT " + pick(k) }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"a"}}, Open: "the kind"}, true},
		"a list of names, each quoted": {`func b(cols []string) string { out := "SELECT 1"; for i := range cols { out += ", " + pgx.Identifier{cols[i]}.Sanitize() }; for _, c := range cols { out += ", " + pgx.Identifier{c}.Sanitize() }; return out + fmt.Sprint(len(cols)) }`, `os.Args`,
			Site{Tuples: [][]string{{"a"}}, Open: "the columns"}, true},
		"a list of names, one joined raw": {`func b(cols []string) string { out := "SELECT 1"; for _, c := range cols { out += ", " + c }; return out }`, `os.Args`,
			Site{Tuples: [][]string{{"a"}}, Open: "the columns"}, false},
		"a number written with %d": {`func b(n int) string { return fmt.Sprintf("SELECT 1 LIMIT %d", n+1) + strconv.Itoa(n) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, true},
		"a number written as a character": {`func b(n int) string { return fmt.Sprintf("SELECT 1 %c", n) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, false},
		"a number converted to a string": {`func b(n int) string { return "SELECT 1 " + string(rune(n)) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, false},
		"a number handed to other code": {`func b(n int) string { return "SELECT 1 " + strings.Repeat(";", n) + os.Getenv(fmt.Sprint(reflect.ValueOf(n))) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, false},
		"copied to a local first": {`func b(t string) string { u := t; return "SELECT 1 FROM " + pgx.Identifier{u}.Sanitize() }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table"}, false},
		// Round 6: the ways what varies reached the text unquoted.
		"an Identifier indexed, not sanitized": {`func b(t string) string { return "SELECT 1 FROM " + pgx.Identifier{t}[0] }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table"}, false},
		"an Identifier joined, not sanitized": {`func b(t string) string { return "SELECT 1 FROM " + strings.Join(pgx.Identifier{t}, ".") }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table"}, false},
		"a converted list indexed, not sanitized": {`func b(parts []string) string { return "SELECT 1 FROM " + pgx.Identifier(parts)[0] }`, `os.Args`,
			Site{Tuples: [][]string{{"s.t"}}, Open: "the table"}, false},
		"a string ranged over, character by character": {`func b(t string) string { var sb strings.Builder; for _, r := range t { sb.WriteRune(r) }; return "SELECT 1 FROM " + sb.String() }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table"}, false},
		"a list ranged into a variable declared elsewhere": {`func b(cols []string) string { out := "SELECT 1"; var c string; for _, c = range cols { out += ", " + c }; return out }`, `os.Args`,
			Site{Tuples: [][]string{{"a"}}, Open: "the columns"}, false},
		"a method named like a registered quoter": {`type T struct{}; func (T) quote(s string) string { return s }; func b(t string) string { return "SELECT 1 FROM " + T{}.quote(t) }`, `os.Getenv("A")`,
			Site{Tuples: [][]string{{"t"}}, Open: "the table", Quoters: map[string]string{"quote": "TestQuote"}}, false},
		"a number copied, then written as a character": {`func b(n int) string { m := n; return "SELECT 1 " + string(rune(m)) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, false},
		"a number in a list of bytes": {`func b(n int) string { return "SELECT 1 " + string([]byte{byte(n)}) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, false},
		"a number written in base 36": {`func b(n int) string { return "SELECT 1 " + strconv.FormatInt(int64(n), 36) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, false},
		"a number written as a quoted character": {`func b(n int) string { return "SELECT 1 " + strconv.QuoteRune(rune(n)) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, false},
		"a number written in base ten": {`func b(n int) string { return "SELECT 1 LIMIT " + strconv.FormatInt(int64(n), 10) }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, true},
		"a number that bounds a loop of placeholders": {`func b(n int) string { out := "SELECT 1"; for i := 0; i < n; i++ { out += fmt.Sprintf(", $%d", i+1) }; return out }`, `len(os.Args)`,
			Site{Tuples: [][]string{{"2"}}, Open: "the row count"}, true},
		"a number of a declared type": {`type level uint; func (level) String() string { return "; DROP TABLE x" }; func b(n level) string { return fmt.Sprintf("SELECT 1 LIMIT %v", n) }`, `level(len(os.Args))`,
			Site{Tuples: [][]string{{"1"}}, Open: "the limit"}, false},
		"bools, the same combination twice": {`func b(x bool) string { if x { return "UPDATE aveloxis_data.repos SET repo_git = 'x'" }; return "SELECT 2" }`, `len(os.Args) > 1`,
			Site{Tuples: [][]string{{"false"}, {"false"}}}, false},
		"bools, an input that is no bool": {`func b(x bool) string { if x { return "SELECT 1" }; return "SELECT 2" }`, `len(os.Args) > 1`,
			Site{Tuples: [][]string{{"false"}, {"yes"}}}, false},
		"bools, every combination": {`func b(x, y bool) string { if x && y { return "SELECT 1" }; return "SELECT 2" }`, `len(os.Args) > 1, len(os.Args) > 2`,
			Site{Tuples: [][]string{{"false", "false"}, {"false", "true"}, {"true", "false"}, {"true", "true"}}}, true},
		"bools, one combination missing": {`func b(x, y bool) string { if x && y { return "SELECT 1" }; return "SELECT 2" }`, `len(os.Args) > 1, len(os.Args) > 2`,
			Site{Tuples: [][]string{{"false", "false"}, {"false", "true"}, {"true", "false"}}}, false},
	} {
		site := c.site
		site.Decl, site.Ordinal, site.Builder, site.Run, site.Domain, site.Reason = "f", 1, "b", run, "d", "r"
		rep := siteProbe(t, c.builder+"\nfunc f(ctx context.Context, tx pool) { tx.Exec(ctx, b("+c.args+")) }", []Site{site}, tests)
		if (len(rep.Problems) == 0) != c.ok {
			t.Errorf("%s: problems %v, want none=%v", name, rep.Problems, c.ok)
		}
	}
}

// A builder's inputs may be the constants its callers pass, found by the
// reading through parameters nothing changes: then they are the whole
// domain. One caller that passes anything else, or that cannot be found,
// and there are none.
func TestCallerTuplesAreTheConstantsEveryCallerPasses(t *testing.T) {
	const base = `func stmt(tbl, col string, n int) string { return fmt.Sprintf("ALTER TABLE %s ADD %s int DEFAULT %d", tbl, col, n) }
func helper(ctx context.Context, tx pool, tbl, col string) { q := stmt(tbl, col, 7); tx.Exec(ctx, q) }
func outer(ctx context.Context, tx pool, col string) { helper(ctx, tx, "s.t", col) }
`
	a := one(t, base+`func f(ctx context.Context, tx pool) { outer(ctx, tx, "a"); outer(ctx, tx, "b"); helper(ctx, tx, "s.u", "c") }`, Env{})
	got := fmt.Sprint(a.CallerTuples)
	if a.Builder != "stmt" || (got != "[[s.t a 7] [s.t b 7] [s.u c 7]]" && got != "[[s.u c 7] [s.t a 7] [s.t b 7]]") {
		t.Errorf("builder %q, caller tuples %s (%s)", a.Builder, got, a.CallerWhy)
	}
	for name, src := range map[string]string{
		"a caller passes a variable": base + `func f(ctx context.Context, tx pool, x string) { outer(ctx, tx, "a"); outer(ctx, tx, x) }`,
		"a caller is exported":       strings.Replace(base, "func outer(", "func Outer(", 1) + `func f(ctx context.Context, tx pool) { Outer(ctx, tx, "a") }`,
		"the helper is used as a value": base + `var h = helper
func f(ctx context.Context, tx pool) { outer(ctx, tx, "a") }`,
		"the parameter is changed": strings.Replace(base, `{ q := stmt(tbl, col, 7)`, `{ col += "x"; q := stmt(tbl, col, 7)`, 1) + `func f(ctx context.Context, tx pool) { outer(ctx, tx, "a") }`,
		"the helper is a method an interface of the package can call": `type runs interface{ helper(ctx context.Context, tx pool, tbl, col string) }
type T struct{}
func stmt(tbl, col string, n int) string { return tbl + col }
func (T) helper(ctx context.Context, tx pool, tbl, col string) { q := stmt(tbl, col, 7); tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool, r runs) { T{}.helper(ctx, tx, "s.t", "a"); r.helper(ctx, tx, os.Getenv("T"), "b") }`,
		"nothing calls it": base,
		// Round 6: a variadic parameter holds every further argument of a
		// call; its first is not the builder's input.
		"the parameter is variadic": `func stmt(cols []string) string { return fmt.Sprint(cols) }
func helper(ctx context.Context, tx pool, cols ...string) { tx.Exec(ctx, stmt(cols)) }
func f(ctx context.Context, tx pool) { helper(ctx, tx, "a", "b") }`,
		"the parameter is variadic and spread": `func stmt(cols ...string) string { return fmt.Sprint(cols) }
func helper(ctx context.Context, tx pool, cols ...string) { tx.Exec(ctx, stmt(cols...)) }
func f(ctx context.Context, tx pool) { helper(ctx, tx, "a", "b") }`,
		"a variadic parameter handed on as one value": `func stmt(c string) string { return "SELECT " + c }
func pick(cols ...string) string { return cols[len(cols)-1] }
func helper(ctx context.Context, tx pool, cols ...string) { c := pick(cols...); tx.Exec(ctx, stmt(c)) }
func f(ctx context.Context, tx pool) { helper(ctx, tx, "a", "b") }`,
	} {
		args, err := StrictArguments(typedRoot, typedFiles(src), Env{})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, a := range args {
			if a.Builder == "stmt" && a.CallerTuples != nil {
				t.Errorf("%s: caller tuples %v", name, a.CallerTuples)
			}
		}
	}
	// len(p) of a parameter is the number of elements of the list each
	// caller writes out; a list that is not written out there gives none.
	const counted = `func pick(where string, n int) string { return fmt.Sprintf("SELECT 1 WHERE %s AND x = $%d", where, n+1) }
func helper(ctx context.Context, tx pool, where string, args []any) { tx.Exec(ctx, pick(where, len(args)), args...) }
`
	a = one(t, counted+`func f(ctx context.Context, tx pool, x int) { helper(ctx, tx, "a = $1 AND b = $2", []any{x, x}); helper(ctx, tx, "TRUE", []any{}) }`, Env{})
	if got := fmt.Sprint(a.CallerTuples); got != "[[a = $1 AND b = $2 2] [TRUE 0]]" && got != "[[TRUE 0] [a = $1 AND b = $2 2]]" {
		t.Errorf("len of a caller's list: %s (%s)", got, a.CallerWhy)
	}
	for name, call := range map[string]string{
		"a list held in a variable": `l := []any{x}; helper(ctx, tx, "a = $1", l)`,
		"a keyed list":              `helper(ctx, tx, "a = $1", []any{3: x})`,
	} {
		if a := one(t, counted+`func f(ctx context.Context, tx pool, x int) { `+call+` }`, Env{}); a.CallerTuples != nil {
			t.Errorf("%s: caller tuples %v", name, a.CallerTuples)
		}
	}
	// Some inputs from the callers and the others the registration's:
	// every combination is run, and the registration's part needs its
	// refusal test like any finite domain.
	const mixed = `type page struct{ sort string }
func list(member, sort string) string { return "SELECT 1 FROM (" + member + ") m ORDER BY " + sort }
func helper(ctx context.Context, tx pool, member string, p page) { tx.Exec(ctx, list(member, p.sort)) }
func f(ctx context.Context, tx pool, p page) { helper(ctx, tx, "SELECT a FROM s.t", p); helper(ctx, tx, "SELECT b FROM s.u", p) }`
	a = one(t, mixed, Env{})
	if fmt.Sprint(a.CallerFree) != "[1]" || len(a.CallerTuples) != 2 {
		t.Fatalf("free %v, caller tuples %v (%s)", a.CallerFree, a.CallerTuples, a.CallerWhy)
	}
	runList := func(in []string) (string, error) { return "SELECT 1 FROM (" + in[0] + ") m ORDER BY " + in[1], nil }
	mixedSite := Site{Decl: "helper", Ordinal: 1, Builder: "list", FromCallers: true, Tuples: [][]string{{"a"}, {"b"}}, Run: runList, Domain: "d", RefusalTest: "TestListRefuses", Reason: "r"}
	if rep := siteProbe(t, mixed, []Site{mixedSite}, map[string]bool{"TestListRefuses": true}); len(rep.Problems) != 0 || len(rep.Statements) != 4 {
		t.Errorf("a mixed registration: problems %v, %d statements, want the four combinations", rep.Problems, len(rep.Statements))
	}
	noTest := mixedSite
	noTest.RefusalTest = ""
	if rep := siteProbe(t, mixed, []Site{noTest}, nil); len(rep.Problems) == 0 {
		t.Error("a mixed registration with no refusal test for its own part was accepted")
	}
	// The site check runs the builder for each of them, and refuses a
	// registration that asks for them where there are none.
	run := func(in []string) (string, error) { return "ALTER TABLE " + in[0] + " ADD " + in[1] + " int", nil }
	site := []Site{{Decl: "helper", Ordinal: 1, Builder: "stmt", FromCallers: true, Run: run, Domain: "the callers' constants", Reason: "r"}}
	rep := siteProbe(t, base+`func f(ctx context.Context, tx pool) { outer(ctx, tx, "a"); helper(ctx, tx, "s.u", "c") }`, site, nil)
	if len(rep.Problems) != 0 || len(rep.Statements) != 2 {
		t.Errorf("problems %v, %d statements, want the builder's two", rep.Problems, len(rep.Statements))
	}
	if rep := siteProbe(t, base+`func f(ctx context.Context, tx pool, x string) { outer(ctx, tx, x) }`, site, nil); len(rep.Problems) == 0 {
		t.Error("a registration from callers was accepted where a caller passes a variable")
	}
}

// Round 6: text with a gap in it is NOT read, whatever fills the gap: a
// number (the reading that made the statement for sample numbers passed a
// %c in a comment and a %T anywhere), or a parameter (the reading that
// filled it with what the callers pass took a variadic parameter's first
// argument for all of them, and an `any` for its text). Each is a hole,
// and a hole is registered: a builder of the store, run.
func TestTextWithAGapInItIsAHole(t *testing.T) {
	for name, body := range map[string]string{
		"a savepoint named by a counter":        `sp := fmt.Sprintf("cntrb_sp_%d", *n); tx.Exec(ctx, "SAVEPOINT "+sp)`,
		"a limit":                               `tx.Exec(ctx, fmt.Sprintf("SELECT a FROM s.t LIMIT %d OFFSET %d", *n, 10))`,
		"a number in a string":                  `tx.Exec(ctx, fmt.Sprintf("SELECT 'v%d' FROM s.t", *n))`,
		"a number written as its type":          `tx.Exec(ctx, fmt.Sprintf("UPDATE aveloxis_data.repos SET %.1Tpdated_at = NULL", *n))`,
		"a number as a character, in a comment": `tx.Exec(ctx, fmt.Sprintf("UPDATE s.t SET a = a --%c, repo_git = 'x'", *n))`,
		"a parameter":                           `tx.Exec(ctx, "DELETE FROM "+table+" WHERE id = $1")`,
		"a parameter under a verb":              `tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", table))`,
		"every further argument of a call":      `tx.Exec(ctx, fmt.Sprintf("SELECT 1 -- %s", rest))`,
		"anything at all":                       `tx.Exec(ctx, fmt.Sprintf("SELECT %q", v))`,
	} {
		for _, a := range strictOf(t, `func helper(ctx context.Context, tx pool, n *uint, table string, v any, rest ...string) { `+body+` }
func f(ctx context.Context, tx pool, n *uint) { helper(ctx, tx, n, "s.t", 39, "safe", "\n, repo_git = 'x' --") }`, Env{}) {
			if a.Kind != StrictHole {
				t.Errorf("%s: read as %s %q", name, a.Kind, a.Text)
			}
		}
	}
}

// The SQL of a statement queued on a batch, read back to copy the batch, is
// not a new statement: it was judged where it was queued. Only that exact
// form is taken so.
func TestTheSQLOfAQueuedStatementReadBackIsNotANewStatement(t *testing.T) {
	copyOf := func(body string) string {
		return `func fresh(b *pgx.Batch, other []*pgx.QueuedQuery, x *pgx.QueuedQuery) *pgx.Batch {
	out := &pgx.Batch{}
	` + body + `
	return out
}
func f(ctx context.Context, p *pgxpool.Pool) { b := &pgx.Batch{}; b.Queue("SELECT 1"); p.SendBatch(ctx, fresh(b, nil, nil)) }`
	}
	kindOf := func(src string) StrictKind {
		t.Helper()
		for _, a := range strictOf(t, src, Env{}) {
			if strings.Contains(a.Expr, ".SQL") {
				return a.Kind
			}
		}
		t.Fatalf("no .SQL argument found in:\n%s", src)
		return StrictHole
	}
	if k := kindOf(copyOf(`for _, q := range b.QueuedQueries { out.QueuedQueries = append(out.QueuedQueries, &pgx.QueuedQuery{SQL: q.SQL, Arguments: q.Arguments}) }`)); k != StrictQueued {
		t.Errorf("a copy of a batch's queued statements: %s", k)
	}
	for name, body := range map[string]string{
		"a list that is no batch's":       `for _, q := range other { out.QueuedQueries = append(out.QueuedQueries, &pgx.QueuedQuery{SQL: q.SQL}) }`,
		"a query handed in":               `out.QueuedQueries = append(out.QueuedQueries, &pgx.QueuedQuery{SQL: x.SQL})`,
		"the loop variable given another": `for _, q := range b.QueuedQueries { q = x; out.QueuedQueries = append(out.QueuedQueries, &pgx.QueuedQuery{SQL: q.SQL}) }`,
		"the text changed on the way":     `for _, q := range b.QueuedQueries { out.QueuedQueries = append(out.QueuedQueries, &pgx.QueuedQuery{SQL: q.SQL + "; SELECT 2"}) }`,
	} {
		if k := kindOf(copyOf(body)); k != StrictHole {
			t.Errorf("%s: %s", name, k)
		}
	}
}
