// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"testing"
)

// Review round 6 ran mutants over the reading and twelve survived for want
// of a test. Each rule they changed has its case here.

func kindsOf(t *testing.T, src string) (holes, parameters int, texts []string) {
	t.Helper()
	for _, a := range strictOf(t, src, Env{}) {
		switch a.Kind {
		case StrictHole:
			holes++
		case StrictParameter:
			parameters++
		default:
			texts = append(texts, a.Text)
		}
	}
	return holes, parameters, texts
}

// A function literal that hands its parameter to a call that runs SQL, and
// that nothing calls by a name (it is handed straight to other code), is
// not "read at its callers": there are none to read. Its argument is a hole.
func TestALiteralNothingCallsByNameIsNotReadAtItsCallers(t *testing.T) {
	holes, parameters, _ := kindsOf(t, `func each(f func(string)) { f(os.Getenv("Q")) }
func f(ctx context.Context, tx pool) { each(func(q string) { tx.Exec(ctx, q) }) }`)
	if holes != 1 || parameters != 0 {
		t.Errorf("%d holes, %d parameters; want the literal's argument a hole", holes, parameters)
	}
}

// A parameter whose address is taken is not what the callers passed.
func TestAParameterWhoseAddressIsTakenIsNotReadAtItsCallers(t *testing.T) {
	holes, parameters, _ := kindsOf(t, `func helper(ctx context.Context, tx pool, q string) { p := &q; *p = os.Getenv("Q"); tx.Exec(ctx, q) }
func f(ctx context.Context, tx pool) { helper(ctx, tx, "SELECT 1") }`)
	if holes != 1 || parameters != 0 {
		t.Errorf("%d holes, %d parameters; want the helper's argument a hole", holes, parameters)
	}
}

// A local that a range clause assigns into is given more than once.
func TestALocalARangeAssignsIntoIsNotGivenOnce(t *testing.T) {
	holes, _, texts := kindsOf(t, `func f(ctx context.Context, tx pool, l []string) { s := "SELECT 1"; for _, s = range l { _ = s }; tx.Exec(ctx, s) }`)
	if holes != 1 || len(texts) != 0 {
		t.Errorf("%d holes, read %q; want a hole", holes, texts)
	}
}

// A value given to a library field that holds SQL is an SQL argument, by
// assignment as well as in a literal.
func TestAssigningALibraryFieldThatHoldsSQLIsAnArgument(t *testing.T) {
	_, _, texts := kindsOf(t, `func f(q *pgx.QueuedQuery) { q.SQL = "`+forbidden+`" }`)
	if len(texts) != 1 || texts[0] != forbidden {
		t.Errorf("read %q; want the assigned statement", texts)
	}
}

// The SQL of a queued statement read back is "queued" only for the
// library's own list of a batch: a list of queued queries held anywhere
// else is not the batch's queue.
func TestOnlyTheLibrarysQueueIsReadBack(t *testing.T) {
	for name, src := range map[string]string{
		"a struct of the package with a field of that name": `type mine struct{ QueuedQueries []*pgx.QueuedQuery }
func f(m mine, b *pgx.Batch) { for _, q := range m.QueuedQueries { b.Queue(q.SQL) } }`,
		"a list held in a variable": `func f(list []*pgx.QueuedQuery, b *pgx.Batch) { for _, q := range list { b.Queue(q.SQL) } }`,
		// A struct that holds a batch in an exported field is itself a
		// carrier of one; another list of it is still not the batch's queue.
		"another list of a struct that also holds a batch": `type mine struct {
	B    pgx.Batch
	List []*pgx.QueuedQuery
}
func f(m mine, b *pgx.Batch) { for _, q := range m.List { b.Queue(q.SQL) } }`,
	} {
		queued := 0
		for _, a := range strictOf(t, src, Env{}) {
			if a.Kind == StrictQueued {
				queued++
			}
		}
		if queued != 0 {
			t.Errorf("%s: read as a queued statement read back", name)
		}
	}
}

// A builder's inputs are not "what the callers pass" when the function
// that calls it is a literal whose variable has its address taken: it can
// then be called through the pointer, by a call no search by object finds.
// (A variable merely given another literal is still called only by its
// name: every call is found, and the callers are a superset.)
func TestALiteralWhoseVariableIsAddressedHasNoClosedCallers(t *testing.T) {
	for _, a := range strictOf(t, `func stmt(tbl string) string { return "SELECT 1 FROM " + tbl }
func f(ctx context.Context, tx pool) {
	h := func(tbl string) { tx.Exec(ctx, stmt(tbl)) }
	h("s.t")
	g := &h
	(*g)(os.Getenv("T"))
}`, Env{}) {
		if a.Builder == "stmt" && a.CallerTuples != nil {
			t.Errorf("caller tuples %v for a literal that is also called through a pointer to its variable", a.CallerTuples)
		}
	}
}

// References the carriers and the runner finder report, each with one case.
func TestRound6ReferencesAreReported(t *testing.T) {
	for name, src := range map[string]string{
		"an interface's CopyFrom (rows written with no text)": `type cp interface {
	CopyFrom(ctx context.Context, t pgx.Identifier, c []string, r pgx.CopyFromSource) (int64, error)
}
func f(ctx context.Context, x cp) { x.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"a"}, nil) }`,
		"text handed to an interface of the package that nothing in it implements": `type runner interface{ Run(ctx context.Context, sql string) }
func f(ctx context.Context, r runner) { r.Run(ctx, "` + forbidden + `") }`,
		"a conversion to a type that carries SQL": `type opts struct {
	IsoLevel       pgx.TxIsoLevel
	AccessMode     pgx.TxAccessMode
	DeferrableMode pgx.TxDeferrableMode
	BeginQuery     string
	CommitQuery    string
}
func f(o opts) pgx.TxOptions { return pgx.TxOptions(o) }`,
		"a carrier of SQL returned by a call of something that is no function of the package": `func f() int { return func() *pgx.Batch { return &pgx.Batch{} }().Len() }`,
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
}

// A literal whose variable is given another function has no closed set of
// callers, even when every call through the variable passes a constant:
// the calls reach whichever function the variable held, and the reading
// does not say which.
func TestALiteralWhoseVariableIsGivenAgainHasNoClosedCallers(t *testing.T) {
	for _, a := range strictOf(t, `func stmt(tbl string) string { return "SELECT 1 FROM " + tbl }
func f(ctx context.Context, tx pool) {
	run := func(tbl string) { tx.Exec(ctx, stmt(tbl)) }
	run("s.t")
	run = func(tbl string) { tx.Exec(ctx, stmt(tbl)) }
	run("s.u")
}`, Env{}) {
		if a.Builder == "stmt" && a.CallerTuples != nil {
			t.Errorf("caller tuples %v for a literal whose variable is given twice", a.CallerTuples)
		}
	}
}

// A caller that spreads a list is refused, whatever it spreads into: the
// rule is "every caller's arguments are written out", not "the followed
// parameter happens to be fixed".
func TestACallerThatSpreadsAListGivesNoCallerTuples(t *testing.T) {
	for _, a := range strictOf(t, `func stmt(tbl string) string { return "SELECT 1 FROM " + tbl }
func helper(ctx context.Context, tx pool, tbl string, args ...any) { tx.Exec(ctx, stmt(tbl), args...) }
func f(ctx context.Context, tx pool, more []any) { helper(ctx, tx, "s.t", more...) }`, Env{}) {
		if a.Builder == "stmt" && a.CallerTuples != nil {
			t.Errorf("caller tuples %v from a caller that spreads a list", a.CallerTuples)
		}
	}
}

// A variable of a DECLARED string type, given once, is never read as its
// declaration's text: every place Go lets it reach an SQL argument is a
// conversion or a formatting of it, and each is a hole. (The once-given
// rule itself is for the basic type string.)
func TestAOnceGivenVariableOfADeclaredStringTypeIsNotRead(t *testing.T) {
	const decl = `type S string
func (S) String() string { return "` + forbidden + `" }
`
	for name, body := range map[string]string{
		"converted":                 `x := S("SELECT 1"); tx.Exec(ctx, string(x))`,
		"formatted":                 `x := S("SELECT 1"); tx.Exec(ctx, fmt.Sprintf("%s", x))`,
		"joined after a conversion": `x := S("SELECT 1"); tx.Exec(ctx, "SELECT 2; "+string(x))`,
		"a package variable":        `tx.Exec(ctx, string(pkgS))`,
	} {
		holes, _, texts := kindsOf(t, decl+"var pkgS S = \"SELECT 1\"\nfunc f(ctx context.Context, tx pool) { "+body+" }")
		if holes != 1 || len(texts) != 0 {
			t.Errorf("%s: %d holes, read %q; want a hole", name, holes, texts)
		}
	}
}
