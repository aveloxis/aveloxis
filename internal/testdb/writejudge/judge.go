// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package writejudge is the run-time judge of the column write policies
// (0.29.213, operator decision 2026-11-20 after review round 6), and the
// one registry of those policies.
//
// It is TEST SUPPORT: only test code imports it (internal/testdb installs
// it for every DB-tier package; scripts/statement_trace_seam_test.go fails
// on any other importer), so the PostgreSQL parser it links is in no
// binary.
//
// The static check (internal/db/column_write_policy_test.go) reads the
// store's Go and decides what text each SQL argument holds. Six review
// rounds found that reading Go cannot be made complete. This judge reads
// no Go. It is a pgx tracer on every pool the store builds while a DB-tier
// package's tests run, and sees the text of every statement the library is
// handed. Each statement that PRODUCTION code sends is parsed by
// PostgreSQL's parser and judged by sqlscan.CheckIn with Policies and
// Exceptions; the verdict is given once, after the package's tests
// (testdb.Main).
//
// Whose statement it is (Origin, 0.29.214): production's unless a test
// wrote it. The frames of the stack that are in this module, outside this
// package, are read from the library outwards. A _test.go frame that only
// FORWARDS (a method named as a library function that takes SQL is: Exec,
// Query, QueryRow, SendBatch, CopyFrom, Prepare, on a test's stand-in for
// a transaction or pool) is passed over: the text it was handed is its
// caller's. The first frame left decides. In a _test.go file: the
// statement is a fixture's (a test may write a protected column any way it
// likes) and is not judged. Otherwise it is production's, and that frame's
// line is the call that sent it: what the coverage requirement compares
// with the static list of such calls in internal/db (Sites). A stack with
// NO such frame (a goroutine or a callback that starts at a library
// function: `go pool.Exec(ctx, sql)`) is production's too, with its origin
// unknown: the judge does not take what it cannot place for a fixture.
//
// What it sees, from pgx v5.11's source: the text of Query / QueryRow /
// Exec as the caller gave it (the trace is taken BEFORE a
// pgx.QueryRewriter runs; see Rewriters for what is done about that);
// every statement queued on a batch, when the batch is sent; the table and
// columns of a CopyFrom; the text of an explicit Prepare. It is on every
// pool and connection opened through internal/db's openers, which is every
// one this module opens outside the test harness itself
// (scripts/pool_openers_test.go). It does not see what is written past pgx
// to the wire: pgconn's own Exec / ExecParams / Prepare on a connection
// taken out of a pool (conn.Conn().PgConn()), or a raw write.
package writejudge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/aveloxis/aveloxis/internal/db/stmttrace"
	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
	"github.com/jackc/pgx/v5"
)

// Frame is one production frame of a statement's stack.
type Frame struct {
	File string // "internal/db/repo_gone.go"
	Line int
	Decl string // "PostgresStore.SidelineRepo", as sqlscan names a declaration
}

// Sent is one distinct statement production code sent, from one distinct
// stack.
type Sent struct {
	SQL     string
	Frames  []Frame // production frames, innermost first
	Batched bool
	// Seq is the statement's place among everything recorded (the first
	// time it was seen): what "production made that table EARLIER" is
	// decided by.
	Seq int
}

// UnknownOrigin is the one frame of a statement sent from a stack that
// holds no frame of this module.
var UnknownOrigin = Frame{File: "(no frame of this module)", Decl: "origin unknown (no module frame: a goroutine or callback started outside this module's frames)"}

// Judge collects what the tracer sees.
type Judge struct {
	mu   sync.Mutex
	sent map[string]*Sent
	// fixtureDDL are the fixtures' own statements that define a relation
	// (see Verdict: a scratch table a test made and dropped is known to
	// the catalog by these).
	fixtureDDL map[string]bool
	// rewritten are the findings against production statements sent with a
	// query rewriter other than the library's own named arguments.
	rewritten map[string]bool
	fixtures  int
	sends     int
}

// New is a judge that is not installed (a test of the judge builds one).
func New() *Judge {
	return &Judge{sent: map[string]*Sent{}, fixtureDDL: map[string]bool{}, rewritten: map[string]bool{}}
}

var installed *Judge

// Install puts a judge on every pool the store builds from here on, and
// returns it. internal/testdb calls it once, before a DB-tier package's
// tests.
func Install() *Judge {
	j := New()
	installed = j
	stmttrace.Wrap = func(inner pgx.QueryTracer) pgx.QueryTracer { return Tracer{Inner: inner, Judge: j} }
	return j
}

// Installed is the judge Install put in place, or nil.
func Installed() *Judge { return installed }

const module = "github.com/aveloxis/aveloxis/"

// self is this package's functions: its frames are the judge's, never an
// origin.
const self = module + "internal/testdb/writejudge."

// DeclOf turns a frame's function name into the name sqlscan gives the
// declaration: "…/internal/db.(*PostgresStore).SidelineRepo.func1" is
// "PostgresStore.SidelineRepo".
func DeclOf(function string) string {
	name := function
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if i := strings.Index(name, "."); i >= 0 {
		name = name[i+1:] // the package
	}
	var parts []string
	for _, p := range strings.Split(name, ".") {
		p = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(p, "("), "*"), ")")
		if i := strings.Index(p, "["); i >= 0 {
			p = p[:i] // a generic's type arguments
		}
		if p == "" || strings.HasPrefix(p, "func") || strings.HasPrefix(p, "gowrap") || strings.Trim(p, "0123456789") == "" {
			break // a closure of the declaration, or a deferred or go wrapper
		}
		parts = append(parts, p)
		if len(parts) == 2 {
			break
		}
	}
	return strings.Join(parts, ".")
}

// rel is a frame's file as the source tools name it: relative to the
// module root, slash separated.
func rel(file string) string {
	file = filepath.ToSlash(file)
	for _, mark := range []string{"/internal/", "/cmd/", "/scripts/"} {
		if i := strings.LastIndex(file, mark); i >= 0 {
			return file[i+1:]
		}
	}
	return file
}

// StackFrame is one frame of a stack, as the runtime gives it.
type StackFrame struct {
	Function, File string
	Line           int
}

// forwards are the names of the library's functions that take SQL (or
// rows for a table): a method of one of these names in a test file, on a
// test's stand-in for a transaction, a connection or a pool, hands on the
// text it was given.
var forwards = map[string]bool{"Exec": true, "Query": true, "QueryRow": true, "SendBatch": true, "CopyFrom": true, "Prepare": true}

// forwardingTestFrame: a METHOD named as a library function that takes
// SQL, declared in a _test.go file.
func forwardingTestFrame(f StackFrame) bool {
	if !strings.HasSuffix(f.File, "_test.go") {
		return false
	}
	typ, method, isMethod := strings.Cut(DeclOf(f.Function), ".")
	// The declaration's own frame, not a closure inside it.
	return isMethod && typ != "" && forwards[method] && strings.HasSuffix(f.Function, "."+method)
}

// Origin says whose statement a stack sent (the frames from the library
// outwards): a fixture's, or production's with every production frame of
// the stack, innermost first. The rule is the package comment's.
func Origin(stack []StackFrame) (frames []Frame, fixture bool) {
	decided := false
	for _, f := range stack {
		if !strings.HasPrefix(f.Function, module) || strings.HasPrefix(f.Function, self) {
			continue
		}
		test := strings.HasSuffix(f.File, "_test.go")
		if !decided {
			if forwardingTestFrame(f) {
				continue
			}
			if test {
				return nil, true
			}
			decided = true
		}
		if !test {
			frames = append(frames, Frame{File: rel(f.File), Line: f.Line, Decl: DeclOf(f.Function)})
		}
	}
	if !decided {
		return []Frame{UnknownOrigin}, false
	}
	return frames, false
}

// originHere is Origin of the calling goroutine's stack.
func originHere(skip int) ([]Frame, bool) {
	pcs := make([]uintptr, 96)
	n := runtime.Callers(skip, pcs)
	it := runtime.CallersFrames(pcs[:n])
	var stack []StackFrame
	for {
		f, more := it.Next()
		stack = append(stack, StackFrame{Function: f.Function, File: f.File, Line: f.Line})
		if !more {
			break
		}
	}
	return Origin(stack)
}

func (j *Judge) record(sql string, batched bool, args []any) {
	frames, fixture := originHere(4)
	if fixture {
		frames = nil
	}
	j.RecordFrom(frames, sql, batched, args)
}

// RecordFrom records one statement sent from the given production frames
// (none: a fixture's). The tracer calls it with the stack's; a test of the
// judge calls it with frames of its own.
func (j *Judge) RecordFrom(frames []Frame, sql string, batched bool, args []any) {
	production := len(frames) > 0
	j.mu.Lock()
	defer j.mu.Unlock()
	j.sends++
	if production {
		for _, a := range args {
			if why := rewriterFinding(a); why != "" {
				at := frames[0]
				j.rewritten[fmt.Sprintf("%s:%d (%s) sent a statement with %s among its arguments: the library ran what it returned, which was not judged:\n  %.300s",
					at.File, at.Line, at.Decl, why, strings.Join(strings.Fields(sql), " "))] = true
			}
		}
	}
	if !production {
		j.fixtures++
		if up := strings.ToUpper(sql); strings.Contains(up, "CREATE ") || strings.Contains(up, "ALTER ") {
			j.fixtureDDL[sql] = true
		}
		return
	}
	var key strings.Builder
	key.WriteString(sql)
	for _, f := range frames {
		fmt.Fprintf(&key, "\x00%s:%d", f.File, f.Line)
	}
	if _, seen := j.sent[key.String()]; !seen {
		j.sent[key.String()] = &Sent{SQL: sql, Frames: frames, Batched: batched, Seq: j.sends}
	}
}

// rewriterFinding says what is wrong with an argument that rewrites the
// statement, or "" for one that does not.
//
// pgx hands the statement to a QueryRewriter among a call's arguments and
// runs what it returns; the tracer is shown the text BEFORE that, so the
// judge cannot judge what ran. Every rewriter is therefore held against
// the statement, the library's own pgx.NamedArgs and pgx.StrictNamedArgs
// included: no code in this module uses named arguments (0.29.213), and a
// statement written with @name is not text PostgreSQL's parser reads. The
// first use of them is the time to teach the judge to read @name as the
// placeholder the library puts in its place.
func rewriterFinding(arg any) string {
	if _, rewrites := arg.(pgx.QueryRewriter); rewrites {
		return fmt.Sprintf("a query rewriter (%T)", arg)
	}
	return ""
}

// Statements is what production sent, in a stable order.
func (j *Judge) Statements() []*Sent {
	j.mu.Lock()
	defer j.mu.Unlock()
	keys := make([]string, 0, len(j.sent))
	for k := range j.sent {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*Sent, len(keys))
	for i, k := range keys {
		out[i] = j.sent[k]
	}
	return out
}

// Add records a statement as sent (a test of the verdict gives the judge
// what it would have seen).
func (j *Judge) Add(s *Sent) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.sends++
	if s.Seq == 0 {
		s.Seq = j.sends
	}
	j.sent[fmt.Sprintf("%s\x00%v", s.SQL, s.Frames)] = s
}

// Fixtures is how many statements the judge took for fixtures'.
func (j *Judge) Fixtures() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.fixtures
}

// Rewriters are the findings against statements sent with a rewriter.
func (j *Judge) Rewriters() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]string, 0, len(j.rewritten))
	for msg := range j.rewritten {
		out = append(out, msg)
	}
	sort.Strings(out)
	return out
}

// Tracer is the tracer: it records, then hands on to the store's own.
type Tracer struct {
	Inner pgx.QueryTracer
	Judge *Judge
}

func (t Tracer) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	t.Judge.record(d.SQL, false, d.Args)
	if t.Inner != nil {
		return t.Inner.TraceQueryStart(ctx, c, d)
	}
	return ctx
}

func (t Tracer) TraceQueryEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryEndData) {
	if t.Inner != nil {
		t.Inner.TraceQueryEnd(ctx, c, d)
	}
}

func (t Tracer) TraceBatchStart(ctx context.Context, c *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	if d.Batch != nil {
		for _, q := range d.Batch.QueuedQueries {
			if q != nil {
				t.Judge.record(q.SQL, true, q.Arguments)
			}
		}
	}
	if b, ok := t.Inner.(pgx.BatchTracer); ok {
		return b.TraceBatchStart(ctx, c, d)
	}
	return ctx
}

func (t Tracer) TraceBatchQuery(ctx context.Context, c *pgx.Conn, d pgx.TraceBatchQueryData) {
	if b, ok := t.Inner.(pgx.BatchTracer); ok {
		b.TraceBatchQuery(ctx, c, d)
	}
}

func (t Tracer) TraceBatchEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceBatchEndData) {
	if b, ok := t.Inner.(pgx.BatchTracer); ok {
		b.TraceBatchEnd(ctx, c, d)
	}
}

// A CopyFrom carries no text: it is judged as the COPY it is.
func (t Tracer) TraceCopyFromStart(ctx context.Context, c *pgx.Conn, d pgx.TraceCopyFromStartData) context.Context {
	cols := make([]string, len(d.ColumnNames))
	for i, n := range d.ColumnNames {
		cols[i] = pgx.Identifier{n}.Sanitize()
	}
	t.Judge.record("COPY "+d.TableName.Sanitize()+" ("+strings.Join(cols, ", ")+") FROM STDIN", false, nil)
	if b, ok := t.Inner.(pgx.CopyFromTracer); ok {
		return b.TraceCopyFromStart(ctx, c, d)
	}
	return ctx
}

func (t Tracer) TraceCopyFromEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceCopyFromEndData) {
	if b, ok := t.Inner.(pgx.CopyFromTracer); ok {
		b.TraceCopyFromEnd(ctx, c, d)
	}
}

func (t Tracer) TracePrepareStart(ctx context.Context, c *pgx.Conn, d pgx.TracePrepareStartData) context.Context {
	t.Judge.record(d.SQL, false, nil)
	if b, ok := t.Inner.(pgx.PrepareTracer); ok {
		return b.TracePrepareStart(ctx, c, d)
	}
	return ctx
}

func (t Tracer) TracePrepareEnd(ctx context.Context, c *pgx.Conn, d pgx.TracePrepareEndData) {
	if b, ok := t.Inner.(pgx.PrepareTracer); ok {
		b.TracePrepareEnd(ctx, c, d)
	}
}

// The tracer is every tracer of the library that carries SQL or rows.
var (
	_ pgx.QueryTracer    = Tracer{}
	_ pgx.BatchTracer    = Tracer{}
	_ pgx.CopyFromTracer = Tracer{}
	_ pgx.PrepareTracer  = Tracer{}
)

// Violations is what the policies hold against the statements sent. A
// statement is held against only if it violates under EVERY production
// frame of its stack: an Exception names the function a statement stands
// in, and a statement handed to a helper is sent from the helper's frame.
func Violations(cat *sqlscan.Catalog, sent []*Sent, policies []sqlscan.Registered, exceptions []sqlscan.Exception) []string {
	// One judging of every distinct text with no exception finds the few
	// that need a second look.
	texts := map[string][]*Sent{}
	var stmts []sqlscan.Stmt
	for _, s := range sent {
		text := s.SQL
		if texts[text] == nil {
			stmts = append(stmts, sqlscan.Stmt{File: s.Frames[0].File, Decl: s.Frames[0].Decl, SQL: text})
		}
		texts[text] = append(texts[text], s)
	}
	rep := sqlscan.CheckIn(cat, stmts, policies, nil)
	suspect := map[string]bool{}
	var out []string
	for _, u := range rep.Unparsed {
		at := texts[u.SQL][0].Frames[0]
		out = append(out, fmt.Sprintf("%s:%d (%s) sent a statement the policy check does not read to its end (%s), so nothing was checked on it:\n  %.300s",
			at.File, at.Line, at.Decl, u.Detail, strings.Join(strings.Fields(u.SQL), " ")))
	}
	// Whether the database holds the schema at all: a database with NONE of
	// the registered tables (the harness's own tests run on an empty one)
	// has nothing a statement could write against a policy.
	schemaThere := false
	for _, p := range policies {
		schemaThere = schemaThere || cat.Kind(p.Table) == "table"
	}
	for _, v := range rep.Violations {
		if v.File == "" {
			// A finding against no statement: a registered table the
			// catalog does not know (unknown-table), in a database that
			// holds others of them, so it was renamed, dropped or made
			// something else while the tests ran. It was dropped until
			// 0.29.214, having no statement to be suspect of.
			if schemaThere {
				out = append(out, fmt.Sprintf("the database's catalog, as the judge read it, has a finding of its own (%s) on %s: %s", v.Kind, v.Table, v.Detail))
			}
			continue
		}
		suspect[v.File+"\x00"+v.Decl] = true
	}
	if len(rep.Violations) == 0 {
		sort.Strings(out)
		return out
	}
	for _, st := range stmts {
		if !suspect[st.File+"\x00"+st.Decl] {
			continue
		}
		for _, s := range texts[st.SQL] {
			var held []sqlscan.Violation
			for i, f := range s.Frames {
				r := sqlscan.CheckIn(cat, []sqlscan.Stmt{{File: f.File, Decl: f.Decl, SQL: st.SQL}}, policies, exceptions)
				if len(r.Violations) == 0 {
					held = nil
					break
				}
				if i == 0 {
					held = r.Violations
				}
			}
			for _, v := range held {
				at := s.Frames[0]
				what := v.Table + "." + v.Column
				if v.Column == "" {
					what = v.Table
				}
				out = append(out, fmt.Sprintf("%s:%d (%s) SENT a statement that writes %s against its policy %s (%s):\n  expr: %s\n  %s\n  statement: %.300s\n  TRIAGE (never weaken a matcher): fix the SQL | correct the registered policy | add an Exception WITH a reason, naming this file and declaration",
					at.File, at.Line, at.Decl, what, v.Want, v.Kind, v.Expr, v.Detail, strings.Join(strings.Fields(s.SQL), " ")))
			}
		}
	}
	sort.Strings(out)
	var uniq []string
	for i, s := range out {
		if i == 0 || out[i-1] != s {
			uniq = append(uniq, s)
		}
	}
	return uniq
}

// withWhatTheRunDeclared adds to the database's catalog what is not read
// from the database at the end: what production created while the tests
// ran (learnWhatProductionMade), and the session tables one text makes and
// another writes, by the one declared list the static check uses too
// (TempTables): written as pg_temp.name, each is that session's own table.
func withWhatTheRunDeclared(cat *sqlscan.Catalog, sent []*Sent) {
	learnWhatProductionMade(cat, sent)
	cat.Temp(TempTables...)
}

// SitesDirEnv names the directory each DB-tier package writes the store's
// library calls it reached into (one file per package), when it is set:
// the closing suite sets it, and afterwards requires the UNION of the
// files to cover every such call of internal/db that is not on the
// reviewed baseline (internal/db: TestTheSuiteReachesEveryStatementSite).
const SitesDirEnv = "AVELOXIS_STATEMENT_SITES_DIR"

// Sites is what a run reached, of the store's library calls that take SQL.
type Sites struct {
	// Lines are the "file:line" of the innermost production frame of every
	// statement sent from internal/db.
	Lines map[string]bool
	// BatchTexts are the statements sent on a batch, their white space
	// folded; BatchDecls the "file\x00declaration" of every internal/db
	// frame a batch was sent under.
	BatchTexts map[string]bool
	BatchDecls map[string]bool
	// BatchTextDecls are "text\x00file\x00declaration": each statement
	// sent on a batch with each internal/db frame it was sent under. A
	// Queue call whose text another Queue call also queues is reached only
	// by its text sent under its OWN declaration (0.29.214: three calls
	// queue the same stamp, and a batch from any one marked all three).
	BatchTextDecls map[string]bool
}

// NewSites is an empty record.
func NewSites() *Sites {
	return &Sites{Lines: map[string]bool{}, BatchTexts: map[string]bool{}, BatchDecls: map[string]bool{}, BatchTextDecls: map[string]bool{}}
}

// Fold is a statement with its white space folded: how batch texts are
// compared.
func Fold(sql string) string { return strings.Join(strings.Fields(sql), " ") }

const storeDir = "internal/db/"

// SitesOf is what the statements reached.
func SitesOf(sent []*Sent) *Sites {
	s := NewSites()
	for _, st := range sent {
		if at := st.Frames[0]; strings.HasPrefix(at.File, storeDir) {
			s.Lines[fmt.Sprintf("%s:%d", at.File, at.Line)] = true
		}
		if st.Batched {
			s.BatchTexts[Fold(st.SQL)] = true
			for _, f := range st.Frames {
				if strings.HasPrefix(f.File, storeDir) {
					s.BatchDecls[f.File+"\x00"+f.Decl] = true
					s.BatchTextDecls[Fold(st.SQL)+"\x00"+f.File+"\x00"+f.Decl] = true
				}
			}
		}
	}
	return s
}

// Write puts the record in dir under name, atomically (a reader never
// sees half a file: the closing suite reads these after every package).
func (s *Sites) Write(dir, name string) error {
	var b strings.Builder
	put := func(kind string, set map[string]bool) {
		keys := make([]string, 0, len(set))
		for k := range set {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString(kind + "\t" + strings.ReplaceAll(k, "\x00", "\t") + "\n")
		}
	}
	put("L", s.Lines)
	put("T", s.BatchTexts)
	put("D", s.BatchDecls)
	put("X", s.BatchTextDecls)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".sites-*")
	if err != nil {
		return err
	}
	if _, err := tmp.WriteString(b.String()); err != nil {
		tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, name+".sites"))
}

// ReadSites is the union of every package's record in dir, and how many
// records there were.
func ReadSites(dir string) (*Sites, int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	union, n := NewSites(), 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sites") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, 0, err
		}
		n++
		for _, line := range strings.Split(string(b), "\n") {
			kind, rest, ok := strings.Cut(line, "\t")
			if !ok {
				continue
			}
			switch kind {
			case "L":
				union.Lines[rest] = true
			case "T":
				union.BatchTexts[rest] = true
			case "D":
				union.BatchDecls[strings.ReplaceAll(rest, "\t", "\x00")] = true
			case "X":
				union.BatchTextDecls[strings.ReplaceAll(rest, "\t", "\x00")] = true
			}
		}
	}
	return union, n, nil
}

// packageName is this test binary's package as a file name: its directory
// under the module root ("internal_db", "cmd_aveloxis").
func packageName() string {
	wd, err := os.Getwd()
	if err != nil {
		return "unknown"
	}
	dir := strings.TrimSuffix(rel(filepath.ToSlash(wd)+"/x"), "/x")
	return strings.NewReplacer("/", "_", ".", "_").Replace(dir)
}

// Verdict is the judge's finding after a package's tests: every violation
// of a statement production code sent. The catalog it judges in is the
// database's own (dsn: the package's test database, still there), with
// what the fixtures defined while the tests ran. When SitesDirEnv is set
// it also writes what the run reached.
func (j *Judge) Verdict(ctx context.Context, dsn string) error {
	sent := j.Statements()
	if len(sent) == 0 && j.Fixtures() == 0 {
		// The package's tests sent nothing through the store: there is
		// nothing to judge, and nothing was missed.
		return j.writeSites(sent)
	}
	cat, err := DatabaseCatalog(ctx, dsn)
	if err != nil {
		return fmt.Errorf("the write-policy judge could not read the test database's catalog: %w", err)
	}
	j.mu.Lock()
	for ddl := range j.fixtureDDL {
		_ = cat.Add(ddl) // a fixture's text that is not read defines nothing
	}
	j.mu.Unlock()
	withWhatTheRunDeclared(cat, sent)
	failures := Violations(cat, sent, Policies, Exceptions)
	failures = append(failures, j.Rewriters()...)
	if err := j.writeSites(sent); err != nil {
		failures = append(failures, "the record of the store's library calls this run reached could not be written: "+err.Error())
	}
	if len(failures) > 0 {
		return fmt.Errorf("the write-policy judge (internal/testdb/writejudge) holds %d thing(s) against what production code sent during the tests:\n%s", len(failures), strings.Join(failures, "\n"))
	}
	return nil
}

// learnWhatProductionMade puts in the catalog the relations PRODUCTION
// code made while the tests ran (0.29.214): the catalog is otherwise the
// database as it stands at the end, so a scratch table production made,
// filled and dropped again was unknown, and the fill was held against it
// as an unknown target.
//
// Only statements that do nothing but CREATE a table, a view, a
// materialized view or a foreign table are learned, each as what it is: a
// view production made over a registered table is in the catalog as a
// VIEW, so a write through it is still a violation. A rename or a move to
// another schema is not learned (each is a finding of the policy check's
// own), and a temporary table belongs to its session: one that another
// text writes is declared by name, for the static check and the judge
// alike (TempTables, Catalog.Temp), and written as pg_temp.name.
func learnWhatProductionMade(cat *sqlscan.Catalog, sent []*Sent) {
	ordered := append([]*Sent{}, sent...)
	sort.Slice(ordered, func(i, k int) bool { return ordered[i].Seq < ordered[k].Seq })
	for _, s := range ordered {
		defs, err := sqlscan.Defs(s.SQL)
		if err != nil || len(defs) == 0 {
			continue
		}
		creates := true
		for _, d := range defs {
			switch d.Kind {
			case "table", "view", "matview", "foreign":
				creates = creates && !d.Temp
			default:
				creates = false
			}
		}
		if creates {
			_ = cat.Add(s.SQL)
		}
	}
}

func (j *Judge) writeSites(sent []*Sent) error {
	dir := os.Getenv(SitesDirEnv)
	// Only the INSTALLED judge's record is the package's: a judge a test
	// built and asked for a verdict (the tests of the judge do) saw a few
	// statements of that test, and its record under the package's name
	// would replace the real one (0.29.214: it did, whenever the package's
	// own verdict was not reached afterwards).
	if dir == "" || j != installed {
		return nil
	}
	return SitesOf(sent).Write(dir, packageName())
}

// DatabaseCatalog is the catalog of the database itself: every base table,
// view, materialized view and foreign table outside the system schemas,
// each view with the relations behind it, each partition and inheritance
// child with its parent. It is what PostgreSQL will act on, so a write the
// judge accepts as "a base table" is one.
func DatabaseCatalog(ctx context.Context, dsn string) (*sqlscan.Catalog, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	cat := sqlscan.NewCatalog()
	const notSystem = `n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_toast%' AND n.nspname NOT LIKE 'pg\_temp%'`
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname, c.relkind::text,
		       COALESCE((SELECT string_agg(DISTINCT quote_ident(sn.nspname) || '.' || quote_ident(sc.relname), ', ')
		                   FROM pg_rewrite r
		                   JOIN pg_depend d ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid AND d.refclassid = 'pg_class'::regclass
		                   JOIN pg_class sc ON sc.oid = d.refobjid AND sc.oid <> c.oid AND sc.relkind IN ('r', 'p', 'v', 'm', 'f')
		                   JOIN pg_namespace sn ON sn.oid = sc.relnamespace
		                  WHERE r.ev_class = c.oid), '') AS sources,
		       COALESCE((SELECT string_agg(quote_ident(pn.nspname) || '.' || quote_ident(pc.relname), ', ')
		                   FROM pg_inherits i
		                   JOIN pg_class pc ON pc.oid = i.inhparent
		                   JOIN pg_namespace pn ON pn.oid = pc.relnamespace
		                  WHERE i.inhrelid = c.oid), '') AS parents
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') AND `+notSystem+`
		 ORDER BY 1, 2`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ddl, tables []string
	for rows.Next() {
		var schema, name, kind, sources, parents string
		if err := rows.Scan(&schema, &name, &kind, &sources, &parents); err != nil {
			return nil, err
		}
		q := pgx.Identifier{schema, name}.Sanitize()
		if kind == "r" || kind == "p" {
			tables = append(tables, schema+"."+name)
		}
		from := ""
		if sources != "" {
			from = " FROM " + sources
		}
		switch {
		case kind == "v":
			ddl = append(ddl, "CREATE VIEW "+q+" AS SELECT 1"+from)
		case kind == "m":
			ddl = append(ddl, "CREATE MATERIALIZED VIEW "+q+" AS SELECT 1"+from)
		case kind == "f":
			ddl = append(ddl, "CREATE FOREIGN TABLE "+q+" () SERVER s")
		case parents != "":
			ddl = append(ddl, "CREATE TABLE "+q+" () INHERITS ("+parents+")")
		default:
			cat.Table(schema + "." + name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Tables first (above), then what stands on them, in an order that
	// does not matter to the catalog.
	var bad []string
	for _, text := range ddl {
		if err := cat.Add(text); err != nil {
			bad = append(bad, err.Error())
		}
	}
	if len(bad) > 0 {
		return nil, errors.New("a relation of the database could not be put in the catalog: " + strings.Join(bad, "; "))
	}
	// The foreign keys, which this catalog reads from the database and not
	// from DDL: with them TRUNCATE x CASCADE is held against the registered
	// tables that reference x (and those that reference them), and without
	// them against every registered table.
	fks, err := conn.Query(ctx, `
		SELECT fn.nspname, fc.relname, tn.nspname, tc.relname
		  FROM pg_constraint k
		  JOIN pg_class fc ON fc.oid = k.conrelid
		  JOIN pg_namespace fn ON fn.oid = fc.relnamespace
		  JOIN pg_class tc ON tc.oid = k.confrelid
		  JOIN pg_namespace tn ON tn.oid = tc.relnamespace
		 WHERE k.contype = 'f'`)
	if err != nil {
		return nil, err
	}
	defer fks.Close()
	for fks.Next() {
		var fromSchema, from, toSchema, to string
		if err := fks.Scan(&fromSchema, &from, &toSchema, &to); err != nil {
			return nil, err
		}
		cat.Reference(fromSchema+"."+from, toSchema+"."+to)
	}
	if err := fks.Err(); err != nil {
		return nil, err
	}
	// Every table's foreign keys are now declared: all of them were read.
	cat.Defined(tables...)
	return cat, nil
}
