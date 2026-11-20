// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Statements reads one backtick literal at a time, so a statement assembled
// in Go (a shared constant joined to a WHERE clause, a format string, a
// builder function) reached the write-policy check in pieces or not at all:
// the blind spot its documentation named. Assemble closes it. It reads every
// assembled string expression of a package and puts it together when every
// part is FIXED TEXT, and reports it when a part is not (v0.29.203;
// summary/71, the 0.29.166 item).
//
// Fixed text is: a string literal; a package constant; a package variable
// that nothing assigns after its declaration; a local variable every one of
// whose values is fixed (one statement per value); fmt.Sprintf whose verbs
// are all %s or %[n]s and whose arguments are fixed; strings.Join over a
// fixed list; a call to a one-line builder (a function whose body is a
// single return) with fixed arguments; and the variable of a range loop over
// a fixed list, a field of it included (one statement per element). Since
// 0.29.208 also: fmt.Sprint; strings.ReplaceAll over fixed text; a local
// built up with += (each append in order; one under a condition gives the
// text with and without it; one in a loop is a hole); and a strings.Builder
// or bytes.Buffer written with WriteString, fmt.Fprintf or fmt.Fprint and
// read with String(). There is no type information: the reading is by
// syntax, and where it cannot be sure it reports instead of guessing.
//
// WHAT IS LOOKED AT (the roots): every string sum, every call named above,
// every x += y, every String() of a written builder, and the SQL argument
// of a call that runs SQL, whatever its form: Exec, Query, QueryRow, Queue,
// Prepare and the ...Context forms, called as a method, a function or a
// generic instantiation, together with the same argument of any function
// of the package (a declaration, or a literal under the name it is given)
// that hands a parameter of its own to one of those, found to any depth and
// at every position functions of that name have it. An argument of a call
// the reader does not follow is looked at for roots of its own.
//
// THE RULE, in one sentence (0.29.210): AN SQL ARGUMENT IS READ WHOLE,
// OR IT IS REGISTERED, OR THE BUILD FAILS.
//
// It replaces a rule of 0.29.209 ("what the reader cannot prove it has read
// whole is reported") that still left a judgement in place: a part that
// stood where it "could only choose rows" was passed. Three review rounds
// each found another place where such a part could change what is written
// (a replacement, a wrapper around an unread statement, the tail of an
// INSERT ... SELECT). Nothing is judged harmless now:
//
//   - Every SQL argument of a call that runs SQL (Runners: a read included,
//     because `UPDATE ... RETURNING` runs through Query) is READ WHOLE, or
//     reported with SQLArg set. CheckUnreadable passes a reported one only
//     when a Fill gives its values, a Shape lists its declaration and
//     matches its whole text, or an UnreadableException names it.
//   - An expression that is not such an argument is reported when what it
//     shows writes a registered table, or a table that is itself unread,
//     and then needs a Fill or an exception.
//   - An EXPORTED function that hands a parameter of its own to a call
//     that runs SQL is reported where it is declared: its callers in other
//     packages are not read.
//
// What READ WHOLE takes, where the reader used to guess:
//
//   - A name's binding is its value only while nothing else can change it.
//     A local given several values takes each of them; one appended to is
//     folded in order; one changed in place is a hole. `var q string` is
//     the empty text. A declaration under a condition (`if x { q := ... }`)
//     is another variable and replaces nothing. A range loop's variable is
//     its element only if the loop's body leaves it alone and no parameter
//     of the declaration shares its name. A parameter that is given a
//     value keeps "what came in" as one of its values.
//   - A builder (strings.Builder, bytes.Buffer) is read only if EVERY use
//     of its name is one this reader follows: its declaration with no
//     content, a write as a statement of its own (WriteString, fmt.Fprintf,
//     fmt.Fprint, io.WriteString), String() and Len(). Any other use (a
//     write inside another call, a deferred write, a method value, an
//     argument, another name) makes the whole builder unread, and so does
//     content given where it is declared (bytes.NewBufferString).
//   - A package constant or variable is fixed text only if it is declared
//     without a type or as a string (a value of another type may print
//     through a String method), and a variable only if it is unexported,
//     never assigned, and (a list) never handed out.
//   - A function that writes SQL taken as a VALUE (ex := tx.Exec; a helper
//     passed as a callback) is reported where it is taken; a function with
//     a goto that writes SQL is reported whole.
//   - A table written with quotes it does not need ("aveloxis_data"."repos")
//     is read as the plain name (plainWriteHead), here and in Statements.
//
// THE GUARANTEE (0.29.211), for a package read with AssembleTyped, which
// is how the store's package is read. It is stated in terms of the type
// checker's objects, not of spellings:
//
//	Every argument at the SQL position of a call whose callee is (a) a
//	function or method of pgx, pgxpool, pgconn or database/sql that takes
//	SQL text, (b) a method of an interface with the name and SQL position
//	of one of those, (c) a function, method or function literal of the
//	package that hands a parameter on to such a call, to any depth, or
//	(d) a method of an interface of the package whose namesake method is
//	one of (c); and every value given to a library field that holds SQL
//	(pgx.QueuedQuery.SQL, pgx.TxOptions.BeginQuery and CommitQuery); is
//	either read WHOLE, with every name in it resolved to its object, or
//	REPORTED. Every reference to one of those callees that is not a call
//	(a method value, a method expression, a callback), every use of a
//	library call that writes rows with no SQL text (CopyFrom), every
//	call by reflection, every exported function that takes a parameter
//	it hands on, every batch that did not come from this package (an
//	exported function's, or any function's whose calls cannot all be
//	found, or another package's result), and every call with text
//	through an interface of the package that no type of the package
//	implements, is REPORTED. And every string
//	literal of the package that holds a statement that writes, wherever
//	it stands, is read as a statement, unless it is an argument of a
//	call that only says text (a logger, fmt.Errorf, panic).
//
// A package that does not type-check is not read at all: that is an error.
//
// WHAT "READ WHOLE" FOLLOWS (0.29.212), all of it by object:
//
//   - constants (from the type checker), concatenation, fmt.Sprintf with
//     %s and with integers, fmt.Sprint, strings.Join, ReplaceAll, Replace
//     with a constant count, TrimPrefix and TrimSuffix of fixed text;
//   - a call of a function of the package: every return statement's value,
//     with the parameters bound to the call's arguments; one of several
//     results (a, b, err := f(x));
//   - a PARAMETER of a function every one of whose calls can be found (it
//     is not exported, is never used as a value, and no interface of the
//     package could call it): the statement is read once per call, with
//     what that call gives; a parameter of function type is the function
//     the call gives; a range loop's element handed on whole keeps its
//     fields;
//   - a lookup in a package map of strings that nothing changes (by
//     object: never assigned, never handed to anything but a query's
//     arguments): every value of it, and the empty string unless the miss
//     leaves (v, ok := m[k]; if !ok { return }); a constant index of such
//     a list. An EXPORTED one is relied on and reported (ExportedRelied),
//     for the caller to check the other packages;
//   - pgx.Identifier{...}.Sanitize() whose parts are fixed: the quoted
//     identifier itself;
//   - a list of placeholders a loop builds: one round of it.
//
// WHAT A REGISTRATION CAN REST ON. An argument that is still not read whole
// has unread parts, and the reader says what KIND each is, from the
// expression that produces it (kindsOf):
//
//	identifier   the result of pgx.Identifier's Sanitize;
//	literal      a value with every quote doubled, between two quotes;
//	repeated     the mark after text a loop adds, the text itself read;
//	queued       the SQL of a query already on a batch (not reported);
//	unknown      anything else.
//
// A Shape covers an argument only when every part is identifier, literal
// or repeated, as its entry names them, and never an identifier in a
// statement that writes rows. A part of kind unknown is covered by an
// UnreadableException alone, which must say where the text comes from
// (Source). A Fill gives the values themselves, and each resulting
// statement is checked whole.
//
// WHAT IS OUTSIDE THE GUARANTEE, each with why it is argued and not
// checked, so that nobody takes this for a proof:
//
//   - The library's classification is a person's reading of 91 functions,
//     methods and fields (typed.go). A parameter named sql or query is
//     found from the signature; the others are listed by hand with what
//     each takes. A library version that adds one fails
//     TestEveryLibraryMemberThatTakesTextIsClassified until it is read.
//     A database library other than those four is not known here.
//   - An interface of the package that a type of the package DOES
//     implement, and a type of another package implements too: the call
//     is read against this package's method; the other is not looked at.
//     (One that no type here implements, called with text, is reported.)
//   - What SQL does once it runs: a trigger, a function body created
//     elsewhere and called here, a write through an updatable view, a
//     format() inside EXECUTE. This reader takes Go source.
//   - unsafe, cgo, and a function value that arrives from another package
//     in a variable of a plain func type and is called with SQL here.
//   - A value read from the database and put into a statement: it is of
//     kind unknown unless Sanitize quotes it, whatever the database did
//     to it (quote_ident). Such a part needs an exception.
//   - What a Fill SAYS: its values are the test's, computed from the
//     package's own values, and that the argument takes exactly those is
//     pinned per fill (a name given its value once, by object), not
//     derived by this reader.
//   - The reader's own evaluation of Go. Anything it does not follow is a
//     hole, and a hole is reported; a wrong EVALUATION of something it
//     does follow (reading a text the program does not produce) would be
//     a miss, and the cases in the test files are what stands against it.
//     Two approximations are deliberate: an integer is read as 0, and a
//     loop's text is read as one round.
//
// Assemble (untyped) remains for text that is not a compilable package; it
// finds calls by name and carries none of the guarantee above.

// Unknown stands in an Unreadable's Text for a part that is not fixed.
const Unknown = "‹?›"

// Unreadable is one assembled expression with a part that is not fixed
// text. Text is its fixed parts with Unknown at each hole; Why names the
// first thing in the way.
type Unreadable struct {
	File string
	// Decl is the top-level declaration it stands in: F, T.M for a method,
	// or a variable's or constant's name. Fills and exceptions name it,
	// never the file, so that moving a declaration breaks neither.
	Decl string
	Line int
	Text string
	Why  string
	// Arg is the expression as written, for an SQL argument with no fixed
	// text at all (Text is Unknown): what a fill for it must name.
	Arg string
	// WriteArg: the expression is the SQL argument of a call that writes
	// (Exec, Queue, Prepare, or a helper that reaches one).
	WriteArg bool
	// SQLArg (0.29.210): the expression is the SQL argument of ANY call
	// that runs SQL, a read (Query, QueryRow) included. Such an argument
	// is never judged by what its fixed text looks like: it is read whole,
	// filled, or excepted (CheckUnreadable).
	SQLArg bool
	// Call and Of (0.29.211): for an SQL argument, its place among the SQL
	// arguments of its declaration in source order (1 is the first), and
	// how many the declaration has. A Shape or an exception names both: a
	// statement added to a registered declaration changes Of, and every
	// registration of that declaration is then stale until someone reads
	// the declaration again.
	// Kinds (0.29.212) is the kind of each Unknown in Text, in order
	// (KindIdentifier, KindPlaceholders, KindUnknown).
	Kinds    []string
	Call, Of int
}

// UnreadableException is the last resort: it records that a person read one
// unreadable write and judged it. Match is a whitespace-normalized needle
// found in the statement's Text. It does not make the statement checked;
// CheckUnreadable offers two ways that do, and they come first.
type UnreadableException struct {
	Decl   string
	Match  string
	Reason string
	// Arg, when set, is the SQL argument as written: the exception is for
	// that argument only. It is REQUIRED for an argument with no fixed
	// text (Match is Unknown), which would otherwise cover every unread
	// argument of the declaration.
	Arg string
	// Source (0.29.212) is REQUIRED for an SQL argument: where the text
	// this reader cannot read comes from (a rules file, a configuration
	// value, the catalog). An exception is the only registration that can
	// cover a part of no verified kind.
	Source string
	// Call and Of (0.29.211) are REQUIRED for an SQL argument: its place
	// among the declaration's SQL arguments and how many there are
	// (Unreadable.Call, Unreadable.Of). A statement added to the
	// declaration makes the exception stale.
	Call, Of int
}

// Fill gives the real values an unreadable statement's holes take: one row
// per instance, one value per hole in the order of its Text. The test that
// registers it takes the rows from the variable the code itself reads, so a
// value added there is checked without anyone remembering to. Reason says
// where the rows come from and what keeps the code to them.
type Fill struct {
	Decl   string
	Match  string
	Values [][]string
	Reason string
	// Arg is required when Match is Unknown itself, a fill for an SQL
	// argument with no fixed text: the argument as written (`j.sql`). The
	// fill then matches that argument and no other of the declaration.
	Arg string
}

// UnreadReport is CheckUnreadable's result.
type UnreadReport struct {
	// Statements go to Check with the rest: the filled instances, and the
	// arguments of a registered shape (holes left in: Check fails a
	// registered column whose value holds one).
	Statements []Stmt
	// Violations: a write to a registered table (or to a table that is
	// itself unread) that nothing above covers.
	Violations      []Unreadable
	BadFills        []string
	StaleFills      []Fill
	StaleExceptions []UnreadableException
	// StaleShapes: a declaration listed in a Shape with no argument of
	// that shape, or listed twice.
	StaleShapes []string
}

// kinds (0.29.212) has one letter per Unknown in text, in order: what KIND
// of thing each unread part is, as the reader verified it from the
// expression that produces it. '?' is "not verified"; see PartKinds.
type assembled struct{ text, why, kinds string }

func hole(why string) assembled { return assembled{Unknown, why, "?"} }

// holeOf is an unread part of a verified kind.
func holeOf(kind byte, why string) assembled { return assembled{Unknown, why, string(kind)} }

// The kinds of unread part a registration can rest on. A part of any other
// origin is KindUnknown, and only a named exception that says where the
// text comes from covers an argument that has one.
const (
	// KindIdentifier: the result of pgx.Identifier's Sanitize (found by
	// object), which is one quoted SQL identifier or a dotted path of
	// them, whatever its operands hold.
	KindIdentifier = "identifier"
	// KindRepeated: the mark stands after text a loop adds (to a builder,
	// to a string, to a list that is then joined). The text before the
	// mark is one round of it, READ like any other text and visible to
	// every check; the mark says only that the loop may add it again, or
	// not at all. A list of `$n` placeholders built in a loop is this.
	KindRepeated = "repeated"
	// KindLiteral: a value inside a quoted SQL string, every single quote
	// in it doubled (strings.ReplaceAll(v, "'", "''") between two quotes,
	// which is how the package's sqlLiteral writes one). Whatever the
	// value holds, it is the content of one string literal.
	KindLiteral = "literal"
	// KindQueued: the SQL of a query already queued on a batch (the
	// library's QueuedQuery.SQL, read). It was read where it was queued.
	KindQueued = "queued"
	// KindUnknown: anything else.
	KindUnknown = "unknown"
)

var kindNames = map[byte]string{'i': KindIdentifier, 'r': KindRepeated, 'e': KindLiteral, 'q': KindQueued, '?': KindUnknown}

// kindsOf gives the kind of each Unknown in v's text. A text whose count of
// holes is not the count of recorded kinds (a transformation that did not
// carry them) has no verified part.
func kindsOf(v assembled) []string {
	n := strings.Count(v.text, Unknown)
	out := make([]string, n)
	at := 0
	for i := range out {
		out[i] = KindUnknown
		k := strings.Index(v.text[at:], Unknown) + at
		at = k + len(Unknown)
		if len(v.kinds) != n {
			continue
		}
		out[i] = kindNames[v.kinds[i]]
		// A quote-doubled value is a literal's content only between the
		// literal's two quotes.
		if v.kinds[i] == 'e' && !(strings.HasSuffix(v.text[:k], "'") && strings.HasPrefix(v.text[at:], "'")) {
			out[i] = KindUnknown
		}
	}
	return out
}

// alts is every value an expression can take. assembleAlternatives bounds
// it: the store's widest case is two values of one local, and a bound keeps
// a pathological product from running away (past it the expression is a
// hole, which is reported, never dropped).
type alts []assembled

const assembleAlternatives = assembleInstances

func cross(l, r alts) alts {
	if len(l)*len(r) > assembleAlternatives {
		return alts{hole("more alternative values than this reader follows")}
	}
	out := make(alts, 0, len(l)*len(r))
	for _, a := range l {
		for _, b := range r {
			why := a.why
			if why == "" {
				why = b.why
			}
			out = append(out, assembled{a.text + b.text, why, a.kinds + b.kinds})
		}
	}
	return out
}

// binding is what a name stands for while one expression is read: a
// builder's parameter (vals), or a range loop's variable (elem, with the
// field order of its type and the scope its expressions are read in).
type binding struct {
	// fn (0.29.212): the name is a parameter of function type, bound to
	// the function a call gives it (a literal, read in scope sc, or a
	// declaration of the package).
	fn ast.Node
	// as: a range loop's variable handed to a function as an argument is
	// bound under the parameter's name; as is the name it has in its loop.
	as     string
	vals   alts
	elem   ast.Expr
	fields []string
	sc     *localScope
	// body bounds the loop whose variable this is (elem bindings): the
	// variable is its element only while the loop's body leaves it alone.
	bodyPos, bodyEnd token.Pos
}

// localScope is what one top-level declaration defines: its parameters
// (those of its function literals too), every value each local is given,
// and which names are changed in place.
type localScope struct {
	params  map[string]bool
	defs    map[string][]localDef
	mutated map[string]bool
	// appends are the x += y statements of each plain name, in source
	// order; writes are what is written to each builder (b.WriteString,
	// fmt.Fprintf(&b, ...)), in source order.
	appends map[string][]localDef
	writes  map[string][]localDef
	// escaped: the name is used as a VALUE somewhere other code could keep
	// or change what it refers to: passed to a call this reader does not
	// know to be harmless, given to another name, put in a composite. For
	// a string that changes nothing; for a list or a builder it means the
	// name's binding no longer says what it holds.
	escaped map[string]bool
}

// localDef is one value a local is given, and where: enough to tell, for
// some uses, that it cannot be the value in use.
type localDef struct {
	val ast.Expr // nil: a value that is not one expression
	pos token.Pos
	// id is the name the value is given to, where it is written.
	id *ast.Ident
	// from and idx (0.29.212): the name is one of several given by ONE
	// expression (v, ok := m[k]; a, b, err := f(x)): that expression, and
	// which of its results this name takes. val is then nil.
	from ast.Expr
	idx  int
	// plain: the statement stands directly in a block that is a function's,
	// a loop's or a bare block's body (block), or directly in the THEN body
	// of an if with no init, which stands in block (guard is that if's
	// condition). Anything else (an else arm, a switch, a select) is not
	// plain, and nothing is concluded from it.
	plain bool
	guard ast.Expr
	block *ast.BlockStmt
	lit   *ast.FuncLit // the function literal it stands in, nil for none
	// loopPos and loopEnd bound the innermost loop the statement stands
	// in (NoPos for none): an append or a write in a loop happens a number
	// of times this reader does not know.
	loopPos, loopEnd token.Pos
	// reset: a builder's Reset or Truncate.
	reset bool
	// declares: the statement declares the name (:= or var). One under a
	// condition declares a NEW variable, visible in that body only: it
	// replaces nothing outside it (0.29.210).
	declares bool
}

// useSite is where the expression being read stands: its position, the
// blocks around it and its function literal.
type useSite struct {
	pos    token.Pos
	blocks map[*ast.BlockStmt]bool
	lit    *ast.FuncLit
}

type scopeName struct {
	sc   *localScope
	name string
}

type assembler struct {
	consts, vars map[string]ast.Expr
	types        map[string]*ast.StructType
	assigned     map[string]bool // package names assigned somewhere in the package
	builders     map[string]*ast.FuncDecl
	calledOnly   map[string]bool // builders judged at their calls
	scopes       map[ast.Node]*localScope
	busy         map[scopeName]bool
	// sqlParams: the functions of the package that hand one of their own
	// parameters to a call that runs SQL (or to another such function), by
	// NAME, with every position such a parameter has under that name (two
	// methods of one name need not agree) and whether the call it reaches
	// writes. A function literal is one under the name it is given.
	sqlParams map[string]map[int]bool
	// runs: for each function (declaration or literal) the parameters it
	// hands on so, and named: the name its callers are found by ("" for a
	// literal that has none: nobody can be shown to call it).
	runs  map[ast.Node]map[string]bool
	named map[ast.Node]string
	// enclosing is the function a literal stands in; declares the
	// parameters each function declares.
	enclosing map[ast.Node]ast.Node
	declares  map[ast.Node]map[string]bool
	// valueUses are the places a function that runs SQL is taken as a
	// VALUE (ex := tx.Exec): its later calls cannot be found.
	valueUses []ast.Expr
	// argRoots are the SQL arguments visited as roots because of where
	// they stand, with whether the call writes.
	argRoots map[ast.Expr]bool
	// cur is the scope of the declaration whose roots are being found.
	cur *localScope
	// useID is the name being read by ident (typed mode): its object says
	// which of several locals of that name is meant.
	useID *ast.Ident
	// readLits are the string literals a reading went through.
	readLits map[*ast.BasicLit]bool
	// following are the functions a call is being followed into.
	following map[ast.Node]bool
	// typed is the package as the type checker sees it (AssembleTyped);
	// nil for Assemble, which reads by name alone.
	typed *typedPkg
	// at is the use being read, nil while a builder's body or a local's own
	// value is read (those are read at another place than the use, and
	// nothing is concluded from order there).
	at *useSite
}

// Assemble returns the statements of every assembled expression that is
// fixed text, and the expressions that are not. files is the shape
// srctest.PackageFiles returns: the non-test files of ONE package.
func Assemble(files map[string]string) ([]Stmt, []Unreadable, error) {
	return assemble("", files)
}

// AssembleTyped is Assemble for a package that compiles, with the type
// checker deciding what each thing IS (typed.go): which calls run SQL,
// which functions hand SQL on, which name is which object. root is the
// module's directory, where the package's imports are found. It is what the
// store's check uses; Assemble stays for text that is not a whole package.
func AssembleTyped(root string, files map[string]string) ([]Stmt, []Unreadable, error) {
	if root == "" {
		return nil, nil, fmt.Errorf("sqlscan.AssembleTyped: no module root")
	}
	return assemble(root, files)
}

func assemble(root string, files map[string]string) ([]Stmt, []Unreadable, error) {
	src, err := parsedOf(root, files)
	if err != nil {
		return nil, nil, err
	}
	names, fset, parsed := src.names, src.fset, src.parsed
	a := &assembler{following: map[ast.Node]bool{}, consts: map[string]ast.Expr{}, vars: map[string]ast.Expr{}, types: map[string]*ast.StructType{},
		assigned: map[string]bool{}, builders: map[string]*ast.FuncDecl{}, calledOnly: map[string]bool{},
		scopes: map[ast.Node]*localScope{}, busy: map[scopeName]bool{}, readLits: map[*ast.BasicLit]bool{}, sqlParams: map[string]map[int]bool{}, argRoots: map[ast.Expr]bool{},
		runs: map[ast.Node]map[string]bool{}, named: map[ast.Node]string{},
		enclosing: map[ast.Node]ast.Node{}, declares: map[ast.Node]map[string]bool{}}
	a.typed = src.typed
	for _, af := range parsed {
		a.declarations(af)
	}
	a.uses(parsed)
	if a.typed == nil {
		a.helpers(parsed)
	}
	// What Statements already reads: an argument that is one of those
	// literals (or a name for one) is not handed on a second time.
	literal := map[string]bool{}
	for _, st := range Statements(files) {
		literal[st.SQL] = true
	}

	var whole []Stmt
	var unread []Unreadable
	for i, af := range parsed {
		for _, d := range af.Decls {
			// A builder that is only ever called is read at its calls: the
			// text its ONE return puts together is judged there, with the
			// arguments each call gives it. Everything else in it is read
			// here like any other code (0.29.211: the whole declaration
			// used to be passed over, and a builder that returned a
			// closure which ran SQL was never read at all).
			var builderReturn ast.Expr
			if fd, ok := d.(*ast.FuncDecl); ok && a.calledOnly[fd.Name.Name] {
				builderReturn = fd.Body.List[0].(*ast.ReturnStmt).Results[0]
			}
			sc := a.scopeOf(d)
			a.cur = sc
			// The SQL arguments of the declaration, in source order.
			var sqlArgList []ast.Expr
			ast.Inspect(d, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					for at := range a.sqlArgs(x) {
						if at >= 0 && at < len(x.Args) {
							sqlArgList = append(sqlArgList, x.Args[at])
						}
					}
				case ast.Expr:
					if a.typed != nil && a.typed.fieldArgs[x] {
						sqlArgList = append(sqlArgList, x)
					}
				}
				return true
			})
			sort.Slice(sqlArgList, func(i, j int) bool { return sqlArgList[i].Pos() < sqlArgList[j].Pos() })
			ordinal := map[ast.Expr]int{}
			for k, e := range sqlArgList {
				ordinal[e] = k + 1
			}
			// SQL that leaves the package before it runs. An EXPORTED
			// function that hands a parameter of its own to a call that
			// runs SQL can be called from anywhere, and this reader takes
			// one package's files: what it would be called with is not
			// read. Reported where it is declared (0.29.210; there was
			// none, and the comment here said another package's check
			// would see it, when no other package has one).
			if fd, ok := d.(*ast.FuncDecl); ok && ast.IsExported(fd.Name.Name) && (len(a.runs[fd]) > 0 || (a.typed != nil && a.typed.exported[fd])) {
				unread = append(unread, Unreadable{File: names[i], Decl: declName(d, fd.Pos()), Line: fset.Position(fd.Pos()).Line, Text: Unknown,
					Why: "an exported function hands a parameter of its own to a call that runs SQL: its callers in other packages are not read",
					Arg: "exported " + fd.Name.Name, WriteArg: true, SQLArg: true})
			}
			a.roots(d, nil, func(e ast.Expr, around []ast.Node) {
				writes, isArg := a.argRoots[e]
				if builderReturn != nil && !isArg && e.Pos() >= builderReturn.Pos() && e.End() <= builderReturn.End() {
					inLit := false
					for _, n := range around {
						if _, ok := n.(*ast.FuncLit); ok {
							inLit = true
						}
					}
					if !inLit {
						return // the builder's own text: read at its calls
					}
				}
				bare := false // the argument was reported once as having no fixed text
				seen := map[string]bool{}
				site := &useSite{pos: e.Pos(), blocks: map[*ast.BlockStmt]bool{}}
				var loops []*ast.RangeStmt
				for _, n := range around {
					switch x := n.(type) {
					case *ast.RangeStmt:
						loops = append(loops, x)
					case *ast.BlockStmt:
						site.blocks[x] = true
					case *ast.FuncLit:
						site.lit = x
					}
				}
				envs := a.instances(loops, sc)
				if a.typed != nil {
					// The functions e stands in, innermost first: each
					// whose every call can be found is read once per
					// call, its parameters bound to what that call
					// gives them (callEnvs).
					chain := []ast.Node{}
					for k := len(around) - 1; k >= 0; k-- {
						if lit, ok := around[k].(*ast.FuncLit); ok {
							chain = append(chain, lit)
						}
					}
					if fd, ok := d.(*ast.FuncDecl); ok {
						chain = append(chain, fd)
					}
					for _, fn := range chain {
						if per := a.callEnvs(fn, 0); per != nil {
							envs = envProduct(envs, per)
						}
					}
				}
				for _, env := range envs {
					a.at = site
					vals := a.resolve(e, sc, env, 0)
					a.at = nil
					for _, r := range vals {
						if seen[r.text+"\x00"+r.kinds] {
							continue
						}
						seen[r.text+"\x00"+r.kinds] = true
						if r.why == "" {
							for _, piece := range splitStatements(srctest.StripSQLComments(r.text)) {
								if piece = strings.TrimSpace(piece); piece != "" && !(isArg && literal[piece]) {
									whole = append(whole, Stmt{File: names[i], SQL: piece})
								}
							}
							continue
						}
						// No fixed text at all: a sum, or a message made of
						// values. Not so for the SQL argument of a call
						// that writes: that is a statement nobody can read,
						// and it is reported as one. A parameter passed on
						// is read where the function is called (helpers).
						if strings.TrimSpace(strings.ReplaceAll(r.text, Unknown, "")) == "" {
							if !isArg || a.readAtItsCalls(e, d, site, sc) || r.kinds == "q" {
								continue // (a queued query's SQL was read where it was queued)
							}
							if !bare {
								bare = true
								unread = append(unread, Unreadable{File: names[i], Decl: declName(d, e.Pos()), Line: fset.Position(e.Pos()).Line, Text: Unknown, Why: r.why, Arg: types.ExprString(e), WriteArg: writes, SQLArg: true, Call: ordinal[e], Of: len(sqlArgList)})
							}
							continue
						}
						unread = append(unread, Unreadable{File: names[i], Decl: declName(d, e.Pos()), Line: fset.Position(e.Pos()).Line, Text: r.text, Why: r.why, Arg: argText(e, isArg), WriteArg: isArg && writes, SQLArg: isArg, Call: ordinal[e], Of: len(sqlArgList), Kinds: kindsOf(r)})
					}
				}
			})
			// goto: this reader concludes things from the ORDER of a
			// function's statements (a value given before the use replaces
			// the earlier ones; an append after the use has not happened).
			// A goto can run them in another order, so a declaration that
			// has one and writes SQL is not read at all.
			if pos := gotoIn(d); pos.IsValid() {
				for arg, writes := range a.argRoots {
					if writes && arg.Pos() >= d.Pos() && arg.End() <= d.End() {
						unread = append(unread, Unreadable{File: names[i], Decl: declName(d, pos), Line: fset.Position(pos).Line, Text: Unknown,
							Why: "a goto in a function that writes SQL: the order its statements run in is not the order they are written in", Arg: "goto", WriteArg: true, SQLArg: true})
						break
					}
				}
			}
			// A function that writes SQL, taken as a value in this
			// declaration: what it is later called with is not found.
			for _, e := range a.valueUses {
				if e.Pos() >= d.Pos() && e.End() <= d.End() {
					unread = append(unread, Unreadable{File: names[i], Decl: declName(d, e.Pos()), Line: fset.Position(e.Pos()).Line, Text: Unknown,
						Why: types.ExprString(e) + " is taken as a value: the SQL it is later called with cannot be found by this reader", Arg: types.ExprString(e)})
				}
			}
			// The same, by OBJECT (typed.go): every reference to something
			// that runs SQL which is not a call of it, a reader included.
			if a.typed != nil {
				for _, ref := range a.typed.taken {
					if ref.expr.Pos() >= d.Pos() && ref.expr.End() <= d.End() {
						arg := types.ExprString(ref.expr)
						if _, lit := ref.expr.(*ast.FuncLit); lit {
							arg = "func literal"
						}
						unread = append(unread, Unreadable{File: names[i], Decl: declName(d, ref.expr.Pos()), Line: fset.Position(ref.expr.Pos()).Line, Text: Unknown,
							Why: arg + ": " + ref.why, Arg: arg, WriteArg: true, SQLArg: true})
					}
				}
			}
		}
	}
	// EVERY quoted string that holds a statement which writes is read,
	// wherever it stands (0.29.211): a value returned, a map's value, a
	// struct's field, an element of a list, an argument of a function this
	// reader does not follow. Statements reads every backtick literal the
	// same way. One that the reading above went THROUGH (as an argument, or
	// as part of something assembled) has been judged there, as part of
	// its whole statement; every other one is judged here, alone. Until
	// this, a quoted statement was read only as the argument of a call and
	// only when the write was its first word: a data-modifying CTE handed
	// to a registered read ("TRUE), w AS (UPDATE ...") was never seen.
	// What is said to a log or put in an error is not run, and is not read.
	for i, af := range parsed {
		said := map[*ast.BasicLit]bool{}
		ast.Inspect(af, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok && a.messageSink(c) {
				for _, arg := range c.Args {
					ast.Inspect(arg, func(m ast.Node) bool {
						switch x := m.(type) {
						case *ast.FuncLit:
							return false
						case *ast.BasicLit:
							said[x] = true
						}
						return true
					})
				}
			}
			return true
		})
		ast.Inspect(af, func(n ast.Node) bool {
			if _, imp := n.(*ast.ImportSpec); imp {
				return false
			}
			if f, ok := n.(*ast.Field); ok && f.Tag != nil {
				said[f.Tag] = true
			}
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING || !strings.HasPrefix(lit.Value, `"`) || a.readLits[lit] || said[lit] {
				return true
			}
			if text, err := strconv.Unquote(lit.Value); err == nil && quotedWrite.MatchString(text) {
				for _, piece := range splitStatements(srctest.StripSQLComments(text)) {
					if piece = strings.TrimSpace(piece); piece != "" && !literal[piece] {
						whole = append(whole, Stmt{File: names[i], SQL: piece})
					}
				}
			}
			return true
		})
	}
	for i := range whole {
		whole[i].SQL = plainWriteHead(whole[i].SQL)
	}
	for i := range unread {
		unread[i].Text = plainWriteHead(unread[i].Text)
	}
	return whole, unread, nil
}

// gotoIn is the position of the first goto under n, NoPos for none.
func gotoIn(n ast.Node) token.Pos {
	at := token.NoPos
	ast.Inspect(n, func(m ast.Node) bool {
		if b, ok := m.(*ast.BranchStmt); ok && b.Tok == token.GOTO && !at.IsValid() {
			at = b.Pos()
		}
		return !at.IsValid()
	})
	return at
}

// readAtItsCalls reports whether e, an SQL argument with no fixed text, is
// a parameter that this reader reads where the function is CALLED: a plain
// name that is a parameter of the nearest function around the use, which
// that function hands on untouched, and which has a name its callers are
// found by (a declaration, or a literal assigned to a name). Anything else
// with no fixed text is reported where it is run. Until 0.29.209 any plain
// name that was a parameter of ANY function in the declaration was taken
// for one, by spelling.
func (a *assembler) readAtItsCalls(e ast.Expr, d ast.Decl, site *useSite, sc *localScope) bool {
	id, plain := e.(*ast.Ident)
	if !plain || len(sc.defs[id.Name]) > 0 || len(sc.appends[id.Name]) > 0 || sc.mutated[id.Name] {
		return false
	}
	if a.typed != nil {
		// By object: the parameter of a function that is called, through
		// a name its calls are found by.
		return a.typed.readAt(e) && a.typed.ambiguous[id] == ""
	}
	var f ast.Node = d
	if site.lit != nil {
		f = site.lit
	}
	for ; f != nil; f = a.enclosing[f] {
		if a.runs[f][id.Name] {
			return a.named[f] != ""
		}
		if a.declares[f][id.Name] {
			return false // the nearest function that declares the name does not hand it on
		}
	}
	return false
}

// declName names the top-level declaration d for the expression at pos.
func declName(d ast.Decl, pos token.Pos) string {
	switch x := d.(type) {
	case *ast.FuncDecl:
		if x.Recv != nil && len(x.Recv.List) == 1 {
			t := x.Recv.List[0].Type
			if s, ok := t.(*ast.StarExpr); ok {
				t = s.X
			}
			if id, ok := t.(*ast.Ident); ok {
				return id.Name + "." + x.Name.Name
			}
		}
		return x.Name.Name
	case *ast.GenDecl:
		for _, s := range x.Specs {
			if s.Pos() <= pos && pos <= s.End() {
				switch sp := s.(type) {
				case *ast.ValueSpec:
					return sp.Names[0].Name
				case *ast.TypeSpec:
					return sp.Name.Name
				}
			}
		}
	}
	return ""
}

// declarations records a file's package constants, variables, struct types
// and one-line builders.
func (a *assembler) declarations(af *ast.File) {
	for _, d := range af.Decls {
		switch x := d.(type) {
		case *ast.GenDecl:
			for _, s := range x.Specs {
				switch sp := s.(type) {
				case *ast.TypeSpec:
					if st, ok := sp.Type.(*ast.StructType); ok {
						a.types[sp.Name.Name] = st
					}
				case *ast.ValueSpec:
					into := a.consts
					if x.Tok == token.VAR {
						into = a.vars
					}
					// A value of a declared type other than string may be
					// printed by a String method, not as its text; and an
					// exported variable can be given a value by another
					// package. Neither is fixed text (0.29.210).
					if t, typed := sp.Type.(*ast.Ident); sp.Type != nil && !(typed && t.Name == "string") {
						continue
					}
					for i, n := range sp.Names {
						if x.Tok == token.VAR && ast.IsExported(n.Name) {
							continue
						}
						if len(sp.Values) == len(sp.Names) {
							into[n.Name] = sp.Values[i]
						}
					}
				}
			}
		case *ast.FuncDecl:
			if isBuilder(x) {
				a.builders[x.Name.Name] = x
			}
		}
	}
}

// isBuilder: a plain function whose parameters are all strings and whose
// body is one return of one value.
func isBuilder(fd *ast.FuncDecl) bool {
	if fd.Recv != nil || fd.Body == nil || len(fd.Body.List) != 1 || fd.Type.TypeParams != nil {
		return false
	}
	ret, ok := fd.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return false
	}
	for _, p := range fd.Type.Params.List {
		if id, ok := p.Type.(*ast.Ident); !ok || id.Name != "string" || len(p.Names) == 0 {
			return false
		}
	}
	return true
}

func builderParams(fd *ast.FuncDecl) []string {
	var out []string
	for _, p := range fd.Type.Params.List {
		for _, n := range p.Names {
			out = append(out, n.Name)
		}
	}
	return out
}

// uses records which package names are assigned after their declaration,
// and which builders are used only by being called (so that every use can
// be seen and the body judged there).
func (a *assembler) uses(files []*ast.File) {
	calls, refs := map[string]int{}, map[string]int{}
	for _, af := range files {
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				if x.Tok != token.DEFINE {
					for _, l := range x.Lhs {
						if name := rootIdent(l); name != "" {
							a.assigned[name] = true
						}
					}
				}
			case *ast.IncDecStmt:
				if name := rootIdent(x.X); name != "" {
					a.assigned[name] = true
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND {
					if name := rootIdent(x.X); name != "" {
						a.assigned[name] = true
					}
				}
			case *ast.ReturnStmt:
				// A package list handed out by a function is no longer
				// known to hold what its declaration gave it.
				for _, r := range x.Results {
					if id, ok := r.(*ast.Ident); ok {
						if v, isVar := a.vars[id.Name]; isVar && !isStringExpr(v) {
							a.assigned[id.Name] = true
						}
					}
				}
			case *ast.CallExpr:
				if id, ok := x.Fun.(*ast.Ident); ok && a.builders[id.Name] != nil {
					calls[id.Name]++
				}
			case *ast.FuncDecl:
				if x.Recv == nil && a.builders[x.Name.Name] == x {
					refs[x.Name.Name]-- // its own name is not a use
				}
			case *ast.Ident:
				if a.builders[x.Name] != nil {
					refs[x.Name]++
				}
			}
			return true
		})
	}
	for name, fd := range a.builders {
		if len(builderParams(fd)) > 0 && calls[name] > 0 && refs[name] == calls[name] {
			a.calledOnly[name] = true
		}
	}
	// A package variable handed, as a value, to something that could keep
	// or change it (a list passed to a function, given to another name) is
	// no longer known to hold what its declaration gave it: the same rule
	// as for a local (escapes), with the same consequence as an
	// assignment.
	for _, af := range files {
		for _, d := range af.Decls {
			sc := a.scopeOf(d)
			for name := range sc.escaped {
				// A string cannot be changed by whoever is handed it; a
				// list can.
				if v, isVar := a.vars[name]; isVar && !sc.params[name] && len(sc.defs[name]) == 0 && !isStringExpr(v) {
					a.assigned[name] = true
				}
			}
		}
	}
}

// rootIdent is the name an assignment target or an operand rests on: x for
// x, x[i], x.f, *x and (x).
func rootIdent(e ast.Expr) string {
	for {
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name
		case *ast.IndexExpr:
			e = x.X
		case *ast.SelectorExpr:
			e = x.X
		case *ast.ParenExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		default:
			return ""
		}
	}
}

func (a *assembler) scopeOf(d ast.Node) *localScope {
	if sc := a.scopes[d]; sc != nil {
		return sc
	}
	sc := &localScope{params: map[string]bool{}, defs: map[string][]localDef{}, mutated: map[string]bool{},
		appends: map[string][]localDef{}, writes: map[string][]localDef{}, escaped: map[string]bool{}}
	// written are the &b operands that are a write's target: taking that
	// address does not make b "changed in place" by something unread.
	written := map[ast.Expr]bool{}
	a.scopes[d] = sc
	fields := func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, f := range fl.List {
			for _, n := range f.Names {
				sc.params[n.Name] = true
			}
		}
	}
	var open []ast.Node
	// where describes the statement being visited (the top of open is its
	// parent).
	where := func(pos token.Pos) localDef {
		def := localDef{pos: pos}
		for _, n := range open {
			switch x := n.(type) {
			case *ast.FuncLit:
				def.lit = x
				def.loopPos, def.loopEnd = token.NoPos, token.NoPos // a literal's body runs when it is called
			case *ast.ForStmt, *ast.RangeStmt:
				def.loopPos, def.loopEnd = x.Pos(), x.End()
			}
		}
		if len(open) < 2 {
			return def
		}
		body, ok := open[len(open)-1].(*ast.BlockStmt)
		if !ok {
			return def
		}
		switch p := open[len(open)-2].(type) {
		case *ast.FuncDecl, *ast.FuncLit, *ast.ForStmt, *ast.RangeStmt, *ast.BlockStmt:
			def.plain, def.block = true, body
		case *ast.IfStmt:
			if outer, ok := open[max(len(open)-3, 0)].(*ast.BlockStmt); ok && len(open) >= 3 && p.Body == body && p.Init == nil {
				def.plain, def.guard, def.block = true, p.Cond, outer
			}
		}
		return def
	}
	// give records one value per plain name on the left; a target that is
	// not a plain name (x[i], x.f) changes x in place.
	give := func(at localDef, lhs []ast.Expr, rhs []ast.Expr) {
		for i, l := range lhs {
			id, ok := l.(*ast.Ident)
			if !ok {
				if name := rootIdent(l); name != "" {
					sc.mutated[name] = true
				}
				continue
			}
			def := at
			def.id = id
			if len(lhs) == len(rhs) {
				def.val = rhs[i]
			} else if len(rhs) == 1 {
				def.from, def.idx = rhs[0], i
			}
			sc.defs[id.Name] = append(sc.defs[id.Name], def)
		}
	}
	mutate := func(e ast.Expr) {
		if name := rootIdent(e); name != "" {
			sc.mutated[name] = true
		}
	}
	// write records a call that writes to a builder: b.WriteString(s),
	// fmt.Fprintf(&b, format, ...), fmt.Fprint(&b, ...), io.WriteString(&b, s).
	// Any other write to it is a part that cannot be read.
	// builderUse are the places a builder's name stands where this reader
	// follows it: the target of a write recorded below, and b.String(),
	// b.Len(). Its name anywhere else (an argument, a defer, a method
	// value, another name) means something this reader does not see can
	// write to it, and the whole builder is unread (0.29.210: a write
	// wrapped in a call, deferred, or made through a method value was
	// simply not counted, and the text read as less than it is).
	builderUse := map[*ast.Ident]bool{}
	write := func(at localDef, c *ast.CallExpr) {
		target := func(e ast.Expr) string {
			if u, ok := e.(*ast.UnaryExpr); ok && u.Op == token.AND {
				if id, ok := u.X.(*ast.Ident); ok {
					builderUse[id] = true
					written[u] = true
					return id.Name
				}
				return ""
			}
			if id, ok := e.(*ast.Ident); ok {
				written[id] = true // a builder held by pointer: a write's target, not an escape
				builderUse[id] = true
				return id.Name
			}
			return ""
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		pkg, _ := sel.X.(*ast.Ident)
		name := ""
		switch {
		case pkg != nil && pkg.Name == "fmt" && (sel.Sel.Name == "Fprintf" || sel.Sel.Name == "Fprint" || sel.Sel.Name == "Fprintln") && len(c.Args) >= 1:
			if name = target(c.Args[0]); name != "" && sel.Sel.Name != "Fprintln" {
				as := map[string]string{"Fprintf": "Sprintf", "Fprint": "Sprint"}[sel.Sel.Name]
				at.val = &ast.CallExpr{Fun: &ast.SelectorExpr{X: ast.NewIdent("fmt"), Sel: ast.NewIdent(as)}, Args: c.Args[1:]}
			}
		case pkg != nil && pkg.Name == "io" && sel.Sel.Name == "WriteString" && len(c.Args) == 2:
			if name = target(c.Args[0]); name != "" {
				at.val = c.Args[1]
			}
		case pkg != nil:
			switch sel.Sel.Name {
			case "WriteString":
				if len(c.Args) == 1 {
					name, at.val = pkg.Name, c.Args[0]
				}
			case "WriteByte", "WriteRune":
				name = pkg.Name
				if len(c.Args) == 1 {
					if lit, ok := c.Args[0].(*ast.BasicLit); ok && lit.Kind == token.CHAR {
						if text, err := strconv.Unquote(lit.Value); err == nil {
							at.val = &ast.BasicLit{ValuePos: lit.Pos(), Kind: token.STRING, Value: strconv.Quote(text)}
						}
					}
				}
			case "Write", "ReadFrom":
				name = pkg.Name // val stays nil: a part that is not read
			case "Reset", "Truncate":
				name, at.reset = pkg.Name, true
			}
		}
		if name != "" {
			sc.writes[name] = append(sc.writes[name], at)
			if pkg != nil && pkg.Name == name {
				builderUse[pkg] = true
			}
		}
	}
	ast.Inspect(d, func(n ast.Node) bool {
		if n == nil {
			open = open[:len(open)-1]
			return true
		}
		switch x := n.(type) {
		case *ast.ExprStmt:
			if c, ok := x.X.(*ast.CallExpr); ok {
				write(where(x.Pos()), c)
			}
		case *ast.FuncDecl:
			fields(x.Recv)
			fields(x.Type.Params)
			fields(x.Type.Results)
		case *ast.FuncLit:
			fields(x.Type.Params)
			fields(x.Type.Results)
		case *ast.AssignStmt:
			if len(x.Rhs) == 1 {
				if c, ok := x.Rhs[0].(*ast.CallExpr); ok { // _, err := fmt.Fprintf(&b, ...)
					write(where(x.Pos()), c)
				}
			}
			if x.Tok == token.DEFINE || x.Tok == token.ASSIGN {
				at := where(x.Pos())
				at.declares = x.Tok == token.DEFINE
				give(at, x.Lhs, x.Rhs)
				break
			}
			if id, ok := x.Lhs[0].(*ast.Ident); ok && x.Tok == token.ADD_ASSIGN && len(x.Lhs) == 1 && len(x.Rhs) == 1 {
				at := where(x.Pos())
				at.val = x.Rhs[0]
				sc.appends[id.Name] = append(sc.appends[id.Name], at)
				break
			}
			for _, l := range x.Lhs { // -=, x[i] += and the like
				mutate(l)
			}
		case *ast.IncDecStmt:
			mutate(x.X)
		case *ast.UnaryExpr:
			if x.Op == token.AND && !written[x] {
				mutate(x.X)
			}
		case *ast.RangeStmt:
			for _, e := range []ast.Expr{x.Key, x.Value} {
				if e != nil {
					give(localDef{pos: x.Pos()}, []ast.Expr{e}, nil)
				}
			}
		case *ast.DeclStmt:
			if g, ok := x.Decl.(*ast.GenDecl); ok && (g.Tok == token.VAR || g.Tok == token.CONST) {
				at := where(x.Pos())
				at.declares = true
				for _, s := range g.Specs {
					vs := s.(*ast.ValueSpec)
					lhs := make([]ast.Expr, len(vs.Names))
					for i, n := range vs.Names {
						lhs[i] = n
					}
					values := vs.Values
					// `var q string` is the empty text: a value like any
					// other, so that a statement chosen in the arms of an
					// if or a switch below it is read as its values.
					if t, ok := vs.Type.(*ast.Ident); ok && t.Name == "string" && len(values) == 0 {
						for range vs.Names {
							values = append(values, &ast.BasicLit{Kind: token.STRING, Value: `""`})
						}
					}
					give(at, lhs, values)
				}
			}
		}
		open = append(open, n)
		return true
	})
	a.escapes(d, sc, written)
	// Every other use of a written builder's name.
	var stack []ast.Node
	ast.Inspect(d, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		id, ok := n.(*ast.Ident)
		if ok && len(sc.writes[id.Name]) > 0 && !builderUse[id] && len(stack) > 0 {
			followed := false
			switch p := stack[len(stack)-1].(type) {
			case *ast.SelectorExpr:
				// b.String() and b.Len() as calls; x.b is a field of that name.
				if p.Sel == id {
					followed = true
				} else if len(stack) > 1 {
					if c, isCall := stack[len(stack)-2].(*ast.CallExpr); isCall && c.Fun == ast.Expr(p) && (p.Sel.Name == "String" || p.Sel.Name == "Len") {
						followed = true
					}
				}
			case *ast.ValueSpec:
				for _, name := range p.Names {
					followed = followed || name == id
				}
			case *ast.AssignStmt:
				for _, l := range p.Lhs {
					followed = followed || (l == ast.Expr(id) && p.Tok == token.DEFINE)
				}
			case *ast.KeyValueExpr:
				followed = p.Key == ast.Expr(id)
			case *ast.Field:
				followed = true // a parameter's declaration; builderShared says what that means
			}
			if !followed {
				sc.escaped[id.Name] = true
			}
		}
		stack = append(stack, n)
		return true
	})
	return sc
}

// isStringExpr: the expression is plainly a string (a literal, or a sum or
// a formatting of things). Anything else (a composite, a call) may be a
// list.
func isStringExpr(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.BasicLit:
		return x.Kind == token.STRING
	case *ast.ParenExpr:
		return isStringExpr(x.X)
	case *ast.BinaryExpr:
		return x.Op == token.ADD
	case *ast.CallExpr:
		return isPkgCall(x, "fmt", "Sprintf") || isPkgCall(x, "fmt", "Sprint") || isPkgCall(x, "strings", "Join") ||
			isPkgCall(x, "strings", "ReplaceAll") || isPkgCall(x, "strings", "TrimSpace")
	}
	return false
}

// harmlessCalls take a list or a builder and can neither keep it nor
// change what it holds.
var harmlessCalls = map[string]bool{"len": true, "cap": true, "strings.Join": true, "slices.Contains": true,
	"slices.Index": true, "fmt.Sprint": true, "fmt.Sprintf": true, "fmt.Sprintln": true}

// escapes fills sc.escaped: every use of a name as a value that is not one
// of the uses this reader knows to leave it alone. Those are: being
// assigned to, ranged over, indexed, selected from or called on (a
// builder's methods are read as writes where they are; a field write is
// "changed in place"); compared; returned; and passed to len, cap, append
// (as the list appended to), strings.Join, slices.Contains or a fmt
// formatter. Anything else — an argument of any other call, the right side
// of an assignment, an element of a composite, a value sent or captured by
// go and defer — may keep the value or change it.
func (a *assembler) escapes(d ast.Node, sc *localScope, written map[ast.Expr]bool) {
	var open []ast.Node
	ast.Inspect(d, func(n ast.Node) bool {
		if n == nil {
			open = open[:len(open)-1]
			return true
		}
		id, ok := n.(*ast.Ident)
		if !ok || len(open) == 0 {
			open = append(open, n)
			return true
		}
		open = append(open, n)
		if written[id] {
			return true
		}
		safe := false
		switch p := open[len(open)-2].(type) {
		case *ast.AssignStmt:
			for _, l := range p.Lhs {
				safe = safe || l == ast.Expr(id)
			}
		case *ast.RangeStmt:
			safe = true // its key, its value or what it ranges over
		case *ast.ValueSpec:
			for _, name := range p.Names {
				safe = safe || name == id
			}
		case *ast.SelectorExpr, *ast.IndexExpr, *ast.BinaryExpr, *ast.ReturnStmt, *ast.Field, *ast.FuncDecl,
			*ast.IfStmt, *ast.SwitchStmt, *ast.CaseClause, *ast.ParenExpr, *ast.StarExpr, *ast.IncDecStmt,
			*ast.LabeledStmt, *ast.BranchStmt, *ast.TypeAssertExpr, *ast.SliceExpr, *ast.ForStmt, *ast.ExprStmt,
			*ast.ArrayType, *ast.MapType, *ast.ChanType, *ast.Ellipsis, *ast.TypeSpec, *ast.ImportSpec,
			*ast.FuncType, *ast.StructType, *ast.InterfaceType, *ast.IndexListExpr:
			// A slice of it (x[i:j]) shares its elements and is treated as
			// the name itself wherever that slice goes: the parent of the
			// SliceExpr is what decides, and it is not looked at here, so
			// say so.
			if _, sliced := p.(*ast.SliceExpr); sliced {
				safe = false
			} else {
				safe = true
			}
		case *ast.KeyValueExpr:
			safe = p.Key == ast.Expr(id) // a field's name, not a value
		case *ast.UnaryExpr:
			safe = p.Op != token.AND || written[p] // any other &x is "changed in place" already
		case *ast.CallExpr:
			switch {
			case p.Fun == ast.Expr(id):
				safe = true // the function called
			case harmlessCalls[calleeName(p)]:
				safe = true
			case calleeName(p) == "append" && len(p.Args) > 0 && p.Args[0] == ast.Expr(id):
				safe = true
			}
		}
		if !safe {
			sc.escaped[id.Name] = true
		}
		return true
	})
}

// calleeName is f for f(...) and pkg.f for pkg.f(...); "" for anything
// else. A generic instantiation (f[T](...)) is f.
func calleeName(c *ast.CallExpr) string {
	fun := c.Fun
	for {
		switch x := fun.(type) {
		case *ast.ParenExpr:
			fun = x.X
			continue
		case *ast.IndexExpr:
			fun = x.X
			continue
		case *ast.IndexListExpr:
			fun = x.X
			continue
		}
		break
	}
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		if id, ok := f.X.(*ast.Ident); ok {
			return id.Name + "." + f.Sel.Name
		}
	}
	return ""
}

// live returns the values of a local that can be the one in use. Two
// things are concluded, and only when they are certain:
//
//   - A value given under `if c { ... }` is never given when c is read as
//     false (c is a field of a loop's element, written true or false in
//     the list; the pre-1970 repair's per-row flag).
//   - A value given for certain (plainly, or under a c read as true),
//     before the use and in a block the use stands in, replaces every
//     value given before it in the same function body.
//
// Everything else stays a possible value: one given after the use, in an
// inner block, in another arm, in a function literal other than the use's,
// or while a local's own value is being read.
func (a *assembler) live(defs []localDef, env map[string]binding) ([]localDef, token.Pos) {
	var out []localDef
	killer := token.NoPos
	for _, d := range defs {
		certain := d.plain
		if d.plain && d.guard != nil {
			v, known := a.truth(d.guard, env)
			if known && !v {
				continue // never given
			}
			// `if true { q := "x" }` declares another q: the outer one
			// keeps its values. Nothing is concluded from it.
			certain = known && !d.declares
		}
		out = append(out, d)
		if at := a.at; at != nil && certain && d.pos < at.pos && at.blocks[d.block] && d.lit == at.lit && d.pos > killer {
			killer = d.pos
		}
	}
	if killer == token.NoPos {
		return out, killer
	}
	kept := out[:0]
	for _, d := range out {
		if d.pos >= killer || d.lit != a.at.lit {
			kept = append(kept, d)
		}
	}
	return kept, killer
}

// fold applies a name's appends, or a builder's writes, to the values it
// starts from, in source order. Only what is certain is concluded:
//
//   - A step in a loop that does not also give the name a fresh value
//     happens a number of times this reader does not know: a hole.
//   - A step after the use (in the same function body, outside any such
//     loop) has not happened, and one before a value that replaced
//     everything (killer) is gone with what it was appended to.
//   - A step given for certain, in a block the use stands in, is applied.
//     Any other (under a condition, in another arm, read from somewhere
//     the use is not known) gives the text with it and without it.
func (a *assembler) fold(start alts, defs, steps []localDef, killer token.Pos, at *useSite, what string, sc *localScope, env map[string]binding, depth int) alts {
	cur := start
	for _, st := range steps {
		if st.pos < killer {
			continue
		}
		carried := st.loopPos != token.NoPos
		for _, d := range defs {
			if carried && d.pos >= st.loopPos && d.pos < st.loopEnd {
				carried = false // a fresh value each time round
			}
		}
		if !carried && at != nil && st.pos > at.pos && st.lit == at.lit {
			continue
		}
		certain := st.plain
		if st.plain && st.guard != nil {
			v, known := a.truth(st.guard, env)
			if known && !v {
				continue
			}
			certain = known
		}
		var vals alts
		switch {
		case carried:
			vals = alts{hole(what + ", which is added to in a loop")}
			// What the loop adds each time round is read: when every
			// value of it is fixed text that can only be placeholders,
			// the part is of that kind, however many times it is added.
			if st.val != nil && !st.reset && a.typed != nil {
				vals = nil
				for _, v := range a.resolve(st.val, sc, env, depth+1) {
					vals = append(vals, repeated(v, what+", which is added to in a loop"))
				}
			}
		case st.reset:
			vals = alts{hole(what + ", which is reset")}
		case st.val == nil:
			vals = alts{hole(what + ", part of which is not written as text")}
		default:
			vals = a.resolve(st.val, sc, env, depth+1)
		}
		next := cross(cur, vals)
		if !carried && !(certain && at != nil && at.blocks[st.block] && st.lit == at.lit) {
			seen := map[assembled]bool{}
			for _, v := range cur {
				seen[v] = true
			}
			merged := append(alts(nil), cur...)
			for _, v := range next {
				if !seen[v] {
					seen[v] = true
					merged = append(merged, v)
				}
			}
			next = merged
		}
		if len(next) > assembleAlternatives {
			return alts{hole(what + ", which takes more values than this reader follows")}
		}
		cur = next
	}
	return cur
}

// truth reads a condition that is a literal true or false: the name
// itself, a field of a loop's element that the list writes so, or the
// negation of one.
func (a *assembler) truth(e ast.Expr, env map[string]binding) (value, known bool) {
	if a.typed != nil && a.typed.unsure(e, true) != "" {
		return false, false // `false := true`: a name is what the checker says it is
	}
	if a.typed != nil {
		if id, ok := e.(*ast.Ident); ok && (id.Name == "true" || id.Name == "false") {
			if o := a.typed.info.Uses[id]; o == nil || o.Parent() != types.Universe {
				return false, false
			}
		}
	}
	switch x := e.(type) {
	case *ast.ParenExpr:
		return a.truth(x.X, env)
	case *ast.UnaryExpr:
		if x.Op == token.NOT {
			v, ok := a.truth(x.X, env)
			return !v, ok
		}
	case *ast.Ident:
		if _, bound := env[x.Name]; !bound && (x.Name == "true" || x.Name == "false") {
			return x.Name == "true", true
		}
	case *ast.SelectorExpr:
		if id, ok := x.X.(*ast.Ident); ok {
			if b, ok := env[id.Name]; ok && b.elem != nil && !loopVarChanged(b, id.Name) {
				if v, ok := fieldOf(b, x.Sel.Name).(*ast.Ident); ok && (v.Name == "true" || v.Name == "false") {
					return v.Name == "true", true
				}
			}
		}
	}
	return false, false
}

// quotedWrite: a quoted string that is, on its face, a statement that
// writes.
//
// 0.29.211: ANYWHERE in the string, not only as its first word. A fragment
// that begins by closing someone else's parenthesis and then writes
// ("TRUE), w AS (UPDATE t SET ...") is a statement that writes.
var quotedWrite = regexp.MustCompile(`(?is)\b(INSERT\s+INTO|UPDATE\s+(?:ONLY\s+)?[\w."]+(?:\s+(?:AS\s+)?\w+)?\s+SET|DELETE\s+FROM|MERGE\s+INTO|TRUNCATE\s|COPY\s+[\w."]+\s*(?:\(|FROM\b))`)

// argText is an SQL argument as written, for a registration to name.
func argText(e ast.Expr, isArg bool) string {
	if !isArg {
		return ""
	}
	return types.ExprString(e)
}

// roots calls visit for every outermost assembled expression under n (a
// string sum, a fmt.Sprintf call, a call to a builder that takes
// arguments), with the nodes it stands in, outermost first. A function
// literal inside one is walked for its own.
func (a *assembler) roots(n ast.Node, outer []ast.Node, visit func(ast.Expr, []ast.Node)) {
	open := append([]ast.Node(nil), outer...)
	ast.Inspect(n, func(m ast.Node) bool {
		if m == nil {
			open = open[:len(open)-1]
			return true
		}
		switch x := m.(type) {
		case *ast.CallExpr:
			// The SQL argument of a call that runs SQL, whatever its form.
			// A backtick literal is Statements' to read; a root is found
			// below like any other.
			for i, writes := range a.sqlArgs(x) {
				if i < 0 || i >= len(x.Args) {
					continue
				}
				arg := x.Args[i]
				lit, isLit := arg.(*ast.BasicLit)
				if isLit && strings.HasPrefix(lit.Value, "`") {
					continue
				}
				a.argRoots[arg] = a.argRoots[arg] || writes
				if !a.isRoot(arg) {
					visit(arg, append(append([]ast.Node(nil), open...), x))
				}
			}
			// A function that runs SQL, handed on as a value.
			for _, arg := range x.Args {
				a.valueUse(arg)
			}
			if a.messageSink(x) {
				// What is said to a log or put in an error is consumed
				// there: it is not judged as SQL. Its function literals
				// are still walked.
				around := append(append([]ast.Node(nil), open...), x)
				var said func(e ast.Expr)
				said = func(e ast.Expr) {
					switch y := e.(type) {
					case *ast.BasicLit, *ast.Ident, *ast.SelectorExpr:
					case *ast.ParenExpr:
						said(y.X)
					case *ast.BinaryExpr:
						said(y.X)
						said(y.Y)
					case *ast.CallExpr:
						// Text put together for the message is part of it;
						// any other call is code like any other, and is
						// walked (it may run SQL of its own).
						if isPkgCall(y, "fmt", "Sprintf") || isPkgCall(y, "fmt", "Sprint") || strings.HasPrefix(calleeName(y), "strconv.") {
							for _, arg := range y.Args {
								said(arg)
							}
							return
						}
						a.roots(y, around, visit)
					default:
						a.roots(e, around, visit)
					}
				}
				for _, arg := range x.Args {
					said(arg)
				}
				return false
			}
		case *ast.AssignStmt:
			// x += y: x, as it stands after the statement.
			if id, ok := x.Lhs[0].(*ast.Ident); ok && x.Tok == token.ADD_ASSIGN && len(x.Lhs) == 1 && len(x.Rhs) == 1 {
				visit(id, append(append([]ast.Node(nil), open...), x))
			}
			for _, r := range x.Rhs {
				a.valueUse(r)
				if a.typed != nil && a.typed.fieldArgs[r] {
					a.argRoots[r] = true
					if !a.isRoot(r) {
						visit(r, append(append([]ast.Node(nil), open...), x))
					}
				}
			}
		case *ast.ValueSpec:
			for _, r := range x.Values {
				a.valueUse(r)
			}
		case *ast.ReturnStmt:
			for _, r := range x.Results {
				a.valueUse(r)
			}
		case *ast.KeyValueExpr:
			a.valueUse(x.Value)
			// A value given to a library field that holds SQL the library
			// will run (pgx.QueuedQuery.SQL): an SQL argument.
			if a.typed != nil && a.typed.fieldArgs[x.Value] {
				a.argRoots[x.Value] = true
				if !a.isRoot(x.Value) {
					visit(x.Value, append(append([]ast.Node(nil), open...), x))
				}
			}
		case *ast.CompositeLit:
			for _, el := range x.Elts {
				a.valueUse(el)
			}
		}
		e, ok := m.(ast.Expr)
		if !ok || !a.isRoot(e) {
			open = append(open, m)
			return true
		}
		around := append([]ast.Node(nil), open...)
		visit(e, around)
		ast.Inspect(e, func(in ast.Node) bool {
			switch x := in.(type) {
			case *ast.FuncLit:
				a.roots(x.Body, append(around, x), visit)
				return false
			case *ast.CallExpr:
				// A call the reader does not follow reads as a hole, and
				// what stands in its arguments would go unseen: they are
				// looked at for roots of their own.
				if in != ast.Node(e) && !a.followed(x) {
					for _, arg := range x.Args {
						a.roots(arg, around, visit)
					}
					return false
				}
			}
			return true
		})
		return false
	})
}

// valueUse records e when it is a function that WRITES SQL taken as a
// value: `tx.Exec`, `b.Queue`, or a helper of the package that hands its
// parameter to one. What it is later called with cannot be found by name,
// so the taking itself is reported. (A reader, Query or QueryRow, taken so
// is not: an argument of a read with no fixed text is not reported
// either.)
func (a *assembler) valueUse(e ast.Expr) {
	if a.typed != nil {
		return // found by object, for every reference (typedPkg.taken)
	}
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			break
		}
		e = p.X
	}
	name := ""
	switch x := e.(type) {
	case *ast.Ident:
		if _, std := sqlRunners[x.Name]; std {
			return // a plain name like a runner's is a local or a parameter, not the method
		}
		name = x.Name
	case *ast.SelectorExpr:
		name = x.Sel.Name
	default:
		return
	}
	writes := false
	if r, ok := sqlRunners[name]; ok {
		writes = r.writes
	}
	for _, w := range a.sqlParams[name] {
		writes = writes || w
	}
	if !writes {
		return
	}
	for _, seen := range a.valueUses {
		if seen == e {
			return
		}
	}
	a.valueUses = append(a.valueUses, e)
}

// messageSinks are the calls whose arguments are said, not run: a logger's
// level methods, the fmt printers that write to the process's own output,
// and the two ways an error is made.
var messageSinks = map[string]bool{"Debug": true, "Info": true, "Warn": true, "Error": true,
	"DebugContext": true, "InfoContext": true, "WarnContext": true, "ErrorContext": true,
	"Errorf": true, "Printf": true, "Println": true, "Print": true, "New": true, "panic": true}

// messageSink reports whether c is one of those calls. A function of this
// package that hands a parameter on to a call that runs SQL is never one,
// whatever it is named; `New` is errors.New alone.
func (a *assembler) messageSink(c *ast.CallExpr) bool {
	if a.typed != nil {
		// By object: a call that only says its text is one of the
		// standard library's (fmt, errors, log, log/slog) or the builtin
		// panic. A method of that NAME on any other type is not one.
		return a.typed.says(c)
	}
	name := calledName(c)
	if !messageSinks[name] || a.sqlParams[name] != nil {
		return false
	}
	if _, std := sqlRunners[name]; std {
		return false
	}
	if name == "New" {
		return isPkgCall(c, "errors", "New")
	}
	if name == "panic" {
		_, plain := c.Fun.(*ast.Ident)
		return plain
	}
	_, method := c.Fun.(*ast.SelectorExpr)
	return method
}

func (a *assembler) isRoot(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.BinaryExpr:
		return x.Op == token.ADD
	case *ast.CallExpr:
		if isPkgCall(x, "fmt", "Sprintf") || isPkgCall(x, "fmt", "Sprint") || isPkgCall(x, "strings", "ReplaceAll") ||
			isPkgCall(x, "strings", "Replace") || isPkgCall(x, "strings", "Repeat") || a.builderString(x, a.cur) != "" {
			return true
		}
		if id, ok := x.Fun.(*ast.Ident); ok {
			if fd := a.builders[id.Name]; fd != nil && len(builderParams(fd)) > 0 {
				return true
			}
		}
	}
	return false
}

// followed reports whether resolve reads into a call's arguments.
func (a *assembler) followed(c *ast.CallExpr) bool {
	if a.isRoot(c) || isPkgCall(c, "strings", "Join") {
		return true
	}
	id, ok := c.Fun.(*ast.Ident)
	return ok && a.builders[id.Name] != nil
}

// builderString is b for the call b.String() when the scope writes to b.
func (a *assembler) builderString(c *ast.CallExpr, sc *localScope) string {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "String" || len(c.Args) != 0 || sc == nil {
		return ""
	}
	if id, ok := sel.X.(*ast.Ident); ok && len(sc.writes[id.Name]) > 0 {
		return id.Name
	}
	return ""
}

// builderShared says why a builder's writes, as this scope shows them, are
// not all of them; "" when they are.
func builderShared(sc *localScope, name string) string {
	switch {
	case sc.mutated[name] || sc.escaped[name]:
		return "which is also written where this reader does not follow (where in the text is not known)"
	case sc.params[name]:
		return "which is a parameter: what was written to it before is not seen"
	case len(sc.defs[name]) != 1:
		return "which is not declared once in this function"
	case !emptyBuilder(sc.defs[name][0].val):
		return "which is given content where it is declared (bytes.NewBufferString and the like): that content is not read"
	}
	return ""
}

// emptyBuilder: the value a builder is declared with holds no text: none
// (var b strings.Builder), a composite literal, its address, or new(T).
func emptyBuilder(v ast.Expr) bool {
	switch x := v.(type) {
	case nil:
		return true
	case *ast.CompositeLit:
		return len(x.Elts) == 0
	case *ast.UnaryExpr:
		return x.Op == token.AND && emptyBuilder(x.X)
	case *ast.CallExpr:
		id, ok := x.Fun.(*ast.Ident)
		return ok && id.Name == "new"
	}
	return false
}

// sqlRunners are the calls that run SQL, by name, with the position of the
// SQL argument and whether the call writes. pgx: Exec(ctx, sql, ...), Query
// and QueryRow the same, Queue(sql, ...), Prepare(ctx, name, sql);
// database/sql's ...Context forms take the context first too. Query and
// QueryRow read, as far as a name says.
var sqlRunners = map[string]struct {
	index  int
	writes bool
}{
	"Exec": {1, true}, "Query": {1, false}, "QueryRow": {1, false}, "Queue": {0, true}, "Prepare": {2, true},
	"ExecContext": {1, true}, "QueryContext": {1, false}, "QueryRowContext": {1, false}, "PrepareContext": {1, true},
}

// Runner is one call that runs SQL, as this reader knows it by name.
type Runner struct {
	Name   string
	Index  int // the position of the SQL argument
	Writes bool
}

// Runners lists the calls that run SQL, sorted by name: what the tests that
// must cover every one of them iterate, so that an entry added here and
// tested nowhere, or removed and still relied on, is seen.
func Runners() []Runner {
	out := make([]Runner, 0, len(sqlRunners))
	for name, r := range sqlRunners {
		out = append(out, Runner{name, r.index, r.writes})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// calledName is the bare name a call goes by: f for f(...), x.f(...) and
// the generic f[T](...).
func calledName(c *ast.CallExpr) string {
	fun := c.Fun
	for {
		switch x := fun.(type) {
		case *ast.ParenExpr:
			fun = x.X
			continue
		case *ast.IndexExpr:
			fun = x.X
			continue
		case *ast.IndexListExpr:
			fun = x.X
			continue
		}
		break
	}
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

// sqlArgs are the positions of the SQL arguments of a call that runs SQL,
// by the call's name, each with whether the call writes; nil for any other
// call. A name that two functions share with the SQL at different places
// gives both places.
func (a *assembler) sqlArgs(c *ast.CallExpr) map[int]bool {
	if a.typed != nil {
		return a.typed.args[c] // by the callee's object, never its name
	}
	name := calledName(c)
	if r, ok := sqlRunners[name]; ok {
		return map[int]bool{r.index: r.writes}
	}
	return a.sqlParams[name]
}

// helpers finds the functions that hand a parameter of their own to a call
// that runs SQL, to any depth: their callers hold the text. A function is a
// declaration, or a literal (found under the name it is assigned to). The
// parameter is the nearest enclosing function's of that name: a closure
// inside a helper that runs the helper's parameter makes the helper one.
func (a *assembler) helpers(files []*ast.File) {
	type fn struct {
		node   ast.Node
		params map[string]int
	}
	paramsOf := func(ft *ast.FuncType) map[string]int {
		at, n := map[string]int{}, 0
		for _, f := range ft.Params.List {
			for _, name := range f.Names {
				at[name.Name] = n
				n++
			}
			if len(f.Names) == 0 {
				n++
			}
		}
		return at
	}
	// The names literals go by.
	for _, af := range files {
		ast.Inspect(af, func(m ast.Node) bool {
			switch x := m.(type) {
			case *ast.FuncDecl:
				a.named[x] = x.Name.Name
			case *ast.AssignStmt:
				for i, r := range x.Rhs {
					if lit, ok := r.(*ast.FuncLit); ok && len(x.Lhs) == len(x.Rhs) {
						if id, ok := x.Lhs[i].(*ast.Ident); ok {
							a.named[lit] = id.Name
						}
					}
				}
			case *ast.ValueSpec:
				for i, r := range x.Values {
					if lit, ok := r.(*ast.FuncLit); ok && len(x.Names) == len(x.Values) {
						a.named[lit] = x.Names[i].Name
					}
				}
			}
			return true
		})
	}
	for changed := true; changed; {
		changed = false
		for _, af := range files {
			var stack []fn
			var open []ast.Node
			ast.Inspect(af, func(m ast.Node) bool {
				if m == nil {
					top := open[len(open)-1]
					open = open[:len(open)-1]
					if len(stack) > 0 && stack[len(stack)-1].node == top {
						stack = stack[:len(stack)-1]
					}
					return true
				}
				open = append(open, m)
				note := func(n ast.Node, params map[string]int) {
					if len(stack) > 0 {
						a.enclosing[n] = stack[len(stack)-1].node
					}
					a.declares[n] = map[string]bool{}
					for name := range params {
						a.declares[n][name] = true
					}
					stack = append(stack, fn{n, params})
				}
				switch x := m.(type) {
				case *ast.FuncDecl:
					note(x, paramsOf(x.Type))
				case *ast.FuncLit:
					note(x, paramsOf(x.Type))
				case *ast.CallExpr:
					for i, writes := range a.sqlArgs(x) {
						if i < 0 || i >= len(x.Args) {
							continue
						}
						id, ok := x.Args[i].(*ast.Ident)
						if !ok {
							continue
						}
						for k := len(stack) - 1; k >= 0; k-- {
							idx, isParam := stack[k].params[id.Name]
							if !isParam {
								continue
							}
							f := stack[k].node
							if a.runs[f] == nil {
								a.runs[f] = map[string]bool{}
							}
							a.runs[f][id.Name] = true
							name := a.named[f]
							if r, std := sqlRunners[name]; std && r.index != idx {
								// A function named like a runner with its SQL
								// elsewhere: its callers are read by the
								// runner's rule, so its parameter is not one
								// that can be passed on unread.
								delete(a.runs[f], id.Name)
								break
							}
							if _, std := sqlRunners[name]; std || name == "" {
								break
							}
							if a.sqlParams[name] == nil {
								a.sqlParams[name] = map[int]bool{}
							}
							if old, had := a.sqlParams[name][idx]; !had || (writes && !old) {
								a.sqlParams[name][idx] = writes || old
								changed = true
							}
							break
						}
					}
				}
				return true
			})
		}
	}
}

func isPkgCall(c *ast.CallExpr, pkg, fn string) bool {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != fn {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg
}

// assembleInstances bounds one expression's loop instances (the product of
// the fixed lists it is nested in). The store's longest list is a few dozen
// rows; past the bound a loop is left unexpanded, so its variable is a
// hole, which is reported.
const assembleInstances = 4096

// instances returns one set of bindings per combination of elements of the
// fixed lists the loops range over. A loop whose list is not fixed binds
// nothing, and its variable reads as a hole.
func (a *assembler) instances(loops []*ast.RangeStmt, sc *localScope) []map[string]binding {
	envs := []map[string]binding{nil}
	for _, r := range loops {
		v, ok := r.Value.(*ast.Ident)
		if !ok || v.Name == "_" || r.Tok != token.DEFINE {
			continue
		}
		elems, fields, ok := a.elements(r.X, sc, 0)
		if !ok || len(elems) == 0 || len(envs)*len(elems) > assembleInstances {
			continue
		}
		next := make([]map[string]binding, 0, len(envs)*len(elems))
		for _, env := range envs {
			for _, el := range elems {
				e := map[string]binding{v.Name: {elem: el, fields: fields, sc: sc, bodyPos: r.Body.Pos(), bodyEnd: r.Body.End()}}
				for k, b := range env {
					if k != v.Name {
						e[k] = b
					}
				}
				next = append(next, e)
			}
		}
		envs = next
	}
	return envs
}

// elements reads a fixed list: its element expressions and, for a list of
// structs, the field names in order.
func (a *assembler) elements(e ast.Expr, sc *localScope, depth int) ([]ast.Expr, []string, bool) {
	if depth > assembleDepth {
		return nil, nil, false
	}
	switch x := e.(type) {
	case *ast.CompositeLit:
		at, ok := x.Type.(*ast.ArrayType)
		if !ok {
			return nil, nil, false
		}
		var st *ast.StructType
		switch el := at.Elt.(type) {
		case *ast.StructType:
			st = el
		case *ast.Ident:
			st = a.types[el.Name]
		}
		var fields []string
		if st != nil {
			for _, f := range st.Fields.List {
				for _, n := range f.Names {
					fields = append(fields, n.Name)
				}
			}
		}
		for _, el := range x.Elts {
			if _, keyed := el.(*ast.KeyValueExpr); keyed { // an indexed element
				return nil, nil, false
			}
		}
		return x.Elts, fields, true
	case *ast.CallExpr:
		// A function of the package (by object) that is one return of a
		// list written out.
		if a.typed != nil && len(x.Args) == 0 {
			if fd, _ := a.typed.calleeDecl(x); fd != nil && fd.Body != nil && len(fd.Body.List) == 1 {
				if r, ok := fd.Body.List[0].(*ast.ReturnStmt); ok && len(r.Results) == 1 {
					return a.elements(r.Results[0], a.scopeOf(fd), depth+1)
				}
			}
		}
	case *ast.Ident:
		if sc != nil {
			if sc.params[x.Name] {
				return nil, nil, false
			}
			if defs, ok := sc.defs[x.Name]; ok {
				if len(defs) != 1 || defs[0].val == nil || sc.mutated[x.Name] || sc.escaped[x.Name] {
					return nil, nil, false
				}
				return a.elements(defs[0].val, sc, depth+1)
			}
		}
		if v, ok := a.vars[x.Name]; ok && !a.assigned[x.Name] {
			return a.elements(v, nil, depth+1)
		}
		if a.typed != nil {
			if lit := a.typed.stableList(x); lit != nil {
				return a.elements(lit, nil, depth+1)
			}
		}
	}
	return nil, nil, false
}

// assembleDepth bounds the reading of constants made of constants and
// builders that call builders; nothing in the corpus nests a tenth as deep,
// and a cycle cannot hang the test.
const assembleDepth = 64

func (a *assembler) resolve(e ast.Expr, sc *localScope, env map[string]binding, depth int) alts {
	if depth > assembleDepth {
		return alts{hole("an expression nested too deeply to read")}
	}
	if a.typed != nil {
		// A name that could be taken for another object is not read; and
		// what the type checker folds to a string constant is that text.
		// A sum, a parenthesis and a name are read one name at a time
		// (each comes back through here); anything else may look a name
		// up in a way that is not by object.
		soft := true
		switch e.(type) {
		case *ast.BinaryExpr, *ast.ParenExpr, *ast.Ident, *ast.BasicLit:
			soft = false
		}
		if c, ok := e.(*ast.CallExpr); ok {
			// A call's arguments come back through here too; its callee
			// and receiver are looked up by name.
			if why := a.typed.unsure(c.Fun, true); why != "" {
				return alts{hole(why)}
			}
			soft = false
		}
		if why := a.typed.unsure(e, soft); why != "" {
			return alts{hole(why)}
		}
		if text, ok := a.typed.constant(e); ok {
			a.typed.literalsOf(e, a.readLits, map[ast.Node]bool{})
			return alts{{text: text}}
		}
	}
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return alts{hole("the literal " + x.Value + ", which is not a string")}
		}
		s, err := strconv.Unquote(x.Value)
		if err != nil {
			return alts{hole("a string literal that does not unquote")}
		}
		a.readLits[x] = true
		return alts{{text: s}}
	case *ast.ParenExpr:
		return a.resolve(x.X, sc, env, depth)
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return alts{hole("an operator other than +")}
		}
		return cross(a.resolve(x.X, sc, env, depth+1), a.resolve(x.Y, sc, env, depth+1))
	case *ast.Ident:
		defer func(id *ast.Ident) { a.useID = id }(a.useID)
		a.useID = x
		return a.ident(x.Name, sc, env, depth)
	case *ast.CallExpr:
		return a.call(x, sc, env, depth)
	case *ast.IndexExpr:
		if v, ok := a.lookup(x, false, depth); ok {
			return v
		}
		if a.typed != nil {
			// An element of a package list nothing changes, at a constant
			// index.
			if n, ok := a.constInt(x.Index); ok {
				if lit := a.typed.stableList(x.X); lit != nil && n >= 0 && n < len(lit.Elts) {
					if _, keyed := lit.Elts[n].(*ast.KeyValueExpr); !keyed {
						return a.resolve(lit.Elts[n], nil, nil, depth+1)
					}
				}
			}
		}
	case *ast.SelectorExpr:
		if id, ok := x.X.(*ast.Ident); ok {
			if b, ok := env[id.Name]; ok && b.elem != nil {
				if loopVarChanged(b, id.Name) {
					return alts{hole("the loop variable " + id.Name + ", which its loop changes")}
				}
				if v := fieldOf(b, x.Sel.Name); v != nil {
					return a.resolve(v, b.sc, elemEnv(b, id.Name, env), depth+1)
				}
				return alts{hole("the field " + id.Name + "." + x.Sel.Name + ", which the list's elements do not give")}
			}
		}
		if a.typed != nil {
			if f, ok := a.typed.info.Uses[x.Sel].(*types.Var); ok && a.typed.lib.fields[f] && f.Name() == "SQL" && f.Pkg() != nil && f.Pkg().Path() == "github.com/jackc/pgx/v5" {
				return alts{holeOf('q', "the SQL of a query already queued on a batch")}
			}
		}
		return alts{hole("the value " + rootIdent(x) + "." + x.Sel.Name + ", which is not declared in this package")}
	}
	return alts{hole(fmt.Sprintf("an expression this reader does not follow (%T)", e))}
}

// loopVarChanged reports whether a range loop's variable is anything but
// its element where the loop's body reads it: the body gives it another
// value, appends to it, assigns one of its fields or takes its address. The
// binding to the element says what the variable STARTS as, each time round.
func loopVarChanged(b binding, name string) bool {
	if b.sc == nil {
		return false
	}
	if b.as != "" {
		name = b.as
	}
	if b.sc.mutated[name] {
		return true
	}
	in := func(defs []localDef) bool {
		for _, d := range defs {
			if d.pos >= b.bodyPos && d.pos < b.bodyEnd {
				return true
			}
		}
		return false
	}
	return in(b.sc.defs[name]) || in(b.sc.appends[name])
}

// fieldOf is the expression a struct element gives one field: by key, or by
// position in the type's field order.
func fieldOf(b binding, field string) ast.Expr {
	lit, ok := b.elem.(*ast.CompositeLit)
	if !ok {
		return nil
	}
	for i, el := range lit.Elts {
		if kv, ok := el.(*ast.KeyValueExpr); ok {
			if k, ok := kv.Key.(*ast.Ident); ok && k.Name == field {
				return kv.Value
			}
			continue
		}
		if i < len(b.fields) && b.fields[i] == field && len(lit.Elts) == len(b.fields) {
			return el
		}
	}
	return nil
}

func (a *assembler) ident(name string, sc *localScope, env map[string]binding, depth int) alts {
	// Several locals of this declaration share the name (typed mode): only
	// the values given to THE object this use names are its values. What
	// the scope keeps by name alone (appends, writes to a builder, changes
	// in place) cannot be told apart, so such a name is then unread.
	if a.typed != nil && a.useID != nil && a.typed.multi[a.useID] && sc != nil {
		obj := a.typed.objectOf(a.useID)
		if len(sc.appends[name]) > 0 || len(sc.writes[name]) > 0 || sc.mutated[name] {
			return alts{hole("the name " + name + ", which is more than one variable in this declaration, one of them appended to or changed in place")}
		}
		if !a.typed.rangeVars[obj] {
			var mine []localDef
			for _, d := range sc.defs[name] {
				if d.id == nil || a.typed.objectOf(d.id) == obj {
					mine = append(mine, d)
				}
			}
			only := &localScope{params: map[string]bool{}, defs: map[string][]localDef{}, mutated: sc.mutated,
				appends: sc.appends, writes: sc.writes, escaped: sc.escaped}
			for k, v := range sc.params {
				only.params[k] = v
			}
			for k, v := range sc.defs {
				only.defs[k] = v
			}
			only.params[name] = a.typed.params[obj]
			if len(mine) > 0 {
				only.defs[name] = mine
			} else {
				delete(only.defs, name)
			}
			id := a.useID
			a.useID = nil
			defer func() { a.useID = id }()
			envOnly := map[string]binding{}
			for k, v := range env {
				if k != name {
					envOnly[k] = v
				}
			}
			return a.identIn(name, only, sc, envOnly, env, depth)
		}
	}
	return a.identIn(name, sc, sc, env, env, depth)
}

// identIn is ident with the scope the NAME is looked up in (sc) apart from
// the scope its values are then read in (in): they differ only where a
// name is more than one variable.
func (a *assembler) identIn(name string, sc, in *localScope, env, envV map[string]binding, depth int) alts {
	if b, ok := env[name]; ok && b.fn != nil {
		return alts{hole("the function " + name + ", used as a value")}
	}
	if b, ok := env[name]; ok {
		if b.elem != nil {
			if loopVarChanged(b, name) {
				return alts{hole("the loop variable " + name + ", which its loop changes")}
			}
			if sc != nil && sc.params[name] && b.as == "" {
				return alts{hole("the name " + name + ", which is a loop variable and a parameter in one declaration: which is meant is not known")}
			}
			return a.resolve(b.elem, b.sc, elemEnv(b, name, env), depth+1)
		}
		return b.vals
	}
	if sc != nil {
		if sc.params[name] && len(sc.defs[name]) == 0 {
			if len(sc.appends[name]) == 0 || sc.mutated[name] || a.busy[scopeName{in, name}] {
				return alts{hole("the parameter " + name)}
			}
			key := scopeName{in, name}
			a.busy[key] = true
			defer delete(a.busy, key)
			at := a.at
			defer func(at *useSite) { a.at = at }(a.at)
			a.at = nil
			return a.fold(alts{hole("the parameter " + name)}, nil, sc.appends[name], token.NoPos, at, "the parameter "+name, in, envV, depth)
		}
		if defs, ok := sc.defs[name]; ok {
			key := scopeName{in, name}
			switch {
			case sc.mutated[name]:
				return alts{hole("the local variable " + name + ", which is changed in place")}
			case a.busy[key]:
				return alts{hole("the local variable " + name + ", which is defined in terms of itself")}
			}
			a.busy[key] = true
			defer delete(a.busy, key)
			var out alts
			seen := map[assembled]bool{}
			live, killer := a.live(defs, envV)
			// A local's own value is read where it was given, not at the use.
			at := a.at
			defer func(at *useSite) { a.at = at }(a.at)
			a.at = nil
			for _, d := range live {
				vals := alts{hole("the local variable " + name + ", one of whose values is not a single expression")}
				if d.val != nil {
					vals = a.resolve(d.val, in, envV, depth+1)
				} else if d.from != nil && a.typed != nil {
					if got, ok := a.several(d, in, envV, depth+1); ok {
						vals = got
					}
				}
				for _, v := range vals {
					if !seen[v] {
						seen[v] = true
						out = append(out, v)
					}
				}
			}
			if sc.params[name] {
				// A parameter that is also given a value here (or a local
				// that shares its name with one of another function of the
				// declaration): what came in is one of its values.
				if v := hole("the parameter " + name); !seen[v] {
					out = append(out, v)
				}
			}
			if len(out) > assembleAlternatives {
				return alts{hole("the local variable " + name + ", which takes more values than this reader follows")}
			}
			if len(sc.appends[name]) > 0 {
				return a.fold(out, defs, sc.appends[name], killer, at, "the local variable "+name, in, envV, depth)
			}
			return out
		}
	}
	if c, ok := a.consts[name]; ok {
		return a.resolve(c, nil, nil, depth+1)
	}
	if v, ok := a.vars[name]; ok {
		if a.assigned[name] {
			return alts{hole("the package variable " + name + ", which is assigned after its declaration")}
		}
		return a.resolve(v, nil, nil, depth+1)
	}
	if a.typed != nil && a.useID != nil {
		// A package variable nothing ever gives another value (by object).
		if v := a.typed.stableVar(a.useID); v != nil {
			return a.resolve(v, nil, nil, depth+1)
		}
	}
	return alts{hole("the name " + name + ", which is not a constant or variable this reader can follow")}
}

// constInt is e's value when the type checker folds it to an integer
// constant.
func (a *assembler) constInt(e ast.Expr) (int, bool) {
	if a.typed == nil {
		return 0, false
	}
	tv, ok := a.typed.info.Types[e]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.Int {
		return 0, false
	}
	n, exact := constant.Int64Val(tv.Value)
	return int(n), exact
}

// several reads one name of several that one expression gives: the value
// of a map lookup (v, ok := m[k]), or one result of a function of the
// package (a, b, err := f(x)).
func (a *assembler) several(d localDef, sc *localScope, env map[string]binding, depth int) (alts, bool) {
	switch x := unparen(d.from).(type) {
	case *ast.IndexExpr:
		if d.idx != 0 {
			return nil, false
		}
		guarded := false
		if a.typed != nil && d.id != nil {
			if as, ok := a.typed.ix.parent[d.id].(*ast.AssignStmt); ok {
				guarded = a.typed.ix.okGuarded[as]
			}
		}
		return a.lookup(x, guarded, depth)
	case *ast.CallExpr:
		return a.callResult(x, d.idx, sc, env, depth)
	}
	return nil, false
}

// lookup is every value an index of a stable package map of strings can
// give: each value the map is written with, and the empty string for a key
// that is not in it unless the lookup is guarded (present).
func (a *assembler) lookup(x *ast.IndexExpr, present bool, depth int) (alts, bool) {
	if a.typed == nil {
		return nil, false
	}
	lit := a.typed.stableMap(x.X)
	if lit == nil {
		return nil, false
	}
	var out alts
	seen := map[assembled]bool{}
	add := func(v assembled) {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	if !present {
		add(assembled{})
	}
	for _, el := range lit.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			return nil, false
		}
		for _, v := range a.resolve(kv.Value, nil, nil, depth+1) {
			add(v)
		}
	}
	if len(out) > assembleAlternatives {
		return alts{hole("a map with more values than this reader follows")}, true
	}
	return out, true
}

// callResult follows a call to a function of the package, found by object:
// its idx-th result is every value one of its return statements gives it,
// each read where it stands, with the function's parameters bound to the
// call's arguments. A literal is followed when it is one return statement
// (what it captures is read as of the call).
func (a *assembler) callResult(c *ast.CallExpr, idx int, sc *localScope, env map[string]binding, depth int) (alts, bool) {
	if a.typed == nil || depth > assembleDepth {
		return nil, false
	}
	fd, lit := a.typed.calleeDecl(c)
	var fn ast.Node
	var body *ast.BlockStmt
	var scF *localScope
	var top ast.Node
	bound := map[string]binding{}
	handed := false
	if id, ok := unwrapFun(c.Fun).(*ast.Ident); ok {
		// A parameter of function type, bound to what a call gave it.
		if b, ok := env[id.Name]; ok && b.fn != nil {
			fd, lit, handed = nil, nil, true
			switch f := b.fn.(type) {
			case *ast.FuncDecl:
				fd = f
			case *ast.FuncLit:
				t, _ := a.typed.around(f)
				if t == nil || len(f.Body.List) != 1 {
					return nil, false
				}
				if _, one := f.Body.List[0].(*ast.ReturnStmt); !one {
					return nil, false
				}
				fn, body, scF, top = f, f.Body, b.sc, t
			}
		}
	}
	switch {
	case fn != nil:
	case fd != nil && fd.Body != nil:
		fn, body, scF, top = fd, fd.Body, a.scopeOf(fd), fd
	case lit != nil:
		t, _ := a.typed.around(lit)
		if t == nil || a.scopeOf(t) != sc || len(lit.Body.List) != 1 {
			return nil, false
		}
		if _, one := lit.Body.List[0].(*ast.ReturnStmt); !one {
			return nil, false
		}
		fn, body, scF, top = lit, lit.Body, sc, t
		for k, v := range env {
			bound[k] = v
		}
	default:
		return nil, false
	}
	if a.following[fn] {
		return alts{hole("a function that calls itself")}, true
	}
	a.following[fn] = true
	defer delete(a.following, fn)
	names, variadic := paramNames(fn)
	if variadic {
		names = names[:len(names)-1] // the fixed parameters; the rest is not bound
	}
	if !c.Ellipsis.IsValid() && (len(names) == len(c.Args) || (variadic && len(names) <= len(c.Args))) {
		for i, p := range names {
			if a.bindable(p, scF, top) || (handed && fd == nil && a.bindableIn(p, scF)) {
				bound[p] = binding{vals: a.resolve(c.Args[i], sc, env, depth+1)}
			}
		}
	}
	var out alts
	seen := map[assembled]bool{}
	defer func(at *useSite) { a.at = at }(a.at)
	var open []ast.Node
	ast.Inspect(body, func(m ast.Node) bool {
		if m == nil {
			open = open[:len(open)-1]
			return true
		}
		if _, inner := m.(*ast.FuncLit); inner {
			return false
		}
		open = append(open, m)
		r, ok := m.(*ast.ReturnStmt)
		if !ok {
			return true
		}
		vals := alts{hole("a return that does not write this result as one expression")}
		if idx < len(r.Results) && (len(r.Results) > 1 || idx == 0) {
			if handed && fd == nil {
				a.at = nil
			} else if fd != nil {
				site := &useSite{pos: r.Pos(), blocks: map[*ast.BlockStmt]bool{}}
				for _, n := range open {
					if b, ok := n.(*ast.BlockStmt); ok {
						site.blocks[b] = true
					}
				}
				a.at = site
			}
			vals = a.resolve(r.Results[idx], scF, bound, depth+1)
		}
		for _, v := range vals {
			if !seen[v] {
				seen[v] = true
				out = append(out, v)
			}
		}
		return true
	})
	if len(out) == 0 {
		return nil, false
	}
	if len(out) > assembleAlternatives {
		return alts{hole("a function with more results than this reader follows")}, true
	}
	return out, true
}

// bindable reports whether the parameter p of a function whose scope is sc
// (in the top-level declaration top) can be bound to what a call gives it:
// it is one name in its declaration, and nothing there gives it another
// value, appends to it or changes it in place.
func (a *assembler) bindable(p string, sc *localScope, top ast.Node) bool {
	return a.bindableIn(p, sc) && a.typed.soleName(top, p)
}

// bindableIn is bindable without the one-name rule: enough for a literal
// that is one return statement, read with bindings that hold only its own
// parameters, where no other variable of the name can be meant.
func (a *assembler) bindableIn(p string, sc *localScope) bool {
	return p != "" && sc != nil && a.typed != nil && sc.params[p] && len(sc.defs[p]) == 0 && len(sc.appends[p]) == 0 && !sc.mutated[p] &&
		len(sc.writes[p]) == 0
}

// sanitized reads pgx.Identifier{...}.Sanitize(): the quoted identifier
// when every part is fixed text, else an unread part of KindIdentifier.
func (a *assembler) sanitized(c *ast.CallExpr, sc *localScope, env map[string]binding, depth int) (alts, bool) {
	if a.typed == nil {
		return nil, false
	}
	parts, literal, ok := a.typed.isSanitize(c)
	if !ok {
		return nil, false
	}
	unread := alts{holeOf('i', "an identifier quoted by pgx.Identifier.Sanitize, whose parts are not fixed text")}
	if !literal || len(parts) == 0 {
		return unread, true
	}
	out := alts{{}}
	for i, part := range parts {
		var quoted alts
		for _, v := range a.resolve(part, sc, env, depth+1) {
			if v.why != "" {
				return unread, true
			}
			quoted = append(quoted, assembled{text: `"` + strings.ReplaceAll(v.text, `"`, `""`) + `"`})
		}
		if i > 0 {
			out = cross(out, alts{{text: "."}})
		}
		if out = cross(out, quoted); len(out) == 1 && out[0].why != "" {
			return unread, true
		}
	}
	return out, true
}

func (a *assembler) call(c *ast.CallExpr, sc *localScope, env map[string]binding, depth int) alts {
	switch {
	case isPkgCall(c, "fmt", "Sprintf") && len(c.Args) > 0:
		args := make([]alts, len(c.Args)-1)
		ints := make([]bool, len(c.Args)-1)
		for i, arg := range c.Args[1:] {
			// An integer, by its type: whatever its value, its text is
			// digits and a sign, and cannot carry SQL. It is read as 0.
			if a.typed != nil && a.typed.integer(arg) {
				args[i], ints[i] = alts{{text: "0"}}, true
				continue
			}
			args[i] = a.resolve(arg, sc, env, depth+1)
		}
		var out alts
		for _, f := range a.resolve(c.Args[0], sc, env, depth+1) {
			out = append(out, sprintf(f, args, ints)...)
		}
		if len(out) > assembleAlternatives {
			return alts{hole("a format with more alternative values than this reader follows")}
		}
		return out
	case isPkgCall(c, "fmt", "Sprint"):
		// Operands that are text are joined as they are (fmt puts a space
		// only between two operands neither of which is a string).
		out := alts{{}}
		for _, arg := range c.Args {
			if out = cross(out, a.resolve(arg, sc, env, depth+1)); len(out) > assembleAlternatives {
				return alts{hole("fmt.Sprint with more alternative values than this reader follows")}
			}
		}
		return out
	case (isPkgCall(c, "strings", "ReplaceAll") && len(c.Args) == 3) || (isPkgCall(c, "strings", "Replace") && len(c.Args) == 4):
		in, old, with := a.resolve(c.Args[0], sc, env, depth+1), a.resolve(c.Args[1], sc, env, depth+1), a.resolve(c.Args[2], sc, env, depth+1)
		fixed := len(old) == 1 && old[0].why == "" && len(with) == 1 && with[0].why == ""
		// strings.Replace's count: read when the type checker folds it to
		// a constant (0.29.212); any other count is not read.
		count := -1
		if len(c.Args) == 4 {
			n, ok := a.constInt(c.Args[3])
			fixed, count = fixed && ok, n
		}
		var out alts
		if count < 0 && len(old) == 1 && old[0].why == "" && len(with) > 1 {
			// Several fixed replacements: each is applied.
			all := true
			for _, w := range with {
				all = all && w.why == ""
			}
			for _, s := range in {
				all = all && s.why == ""
			}
			if all {
				for _, s := range in {
					for _, w := range with {
						out = append(out, assembled{text: strings.ReplaceAll(s.text, old[0].text, w.text)})
					}
				}
				return out
			}
		}
		for _, s := range in {
			switch {
			case fixed && count < 0 && s.why != "" && old[0].text == "'" && with[0].text == "''":
				// Every quote doubled in a value that is not read: the
				// content of a string literal, if quotes stand round it.
				out = append(out, holeOf('e', "a value with every quote doubled ("+s.why+")"))
			case fixed && s.why == "":
				out = append(out, assembled{text: strings.Replace(s.text, old[0].text, with[0].text, count)})
			case s.why != "":
				out = append(out, assembled{s.text + Unknown, s.why, s.kinds + "?"})
			default:
				// The text before the replacement, so that what it writes
				// can still be seen, and a hole for what is replaced.
				out = append(out, assembled{s.text + Unknown, "a strings replacement one part of which is not fixed text", s.kinds + "?"})
			}
		}
		return out
	case len(c.Args) == 2 && (isPkgCall(c, "strings", "TrimPrefix") || isPkgCall(c, "strings", "TrimSuffix")):
		// Pure functions of fixed text are computed; of anything else, not.
		in, cut := a.resolve(c.Args[0], sc, env, depth+1), a.resolve(c.Args[1], sc, env, depth+1)
		if len(cut) != 1 || cut[0].why != "" {
			return alts{hole("a strings trim whose prefix or suffix is not fixed text")}
		}
		var out alts
		for _, s := range in {
			switch {
			case s.why != "":
				out = append(out, hole("a strings trim of text that is not fixed ("+s.why+")"))
			case isPkgCall(c, "strings", "TrimPrefix"):
				out = append(out, assembled{text: strings.TrimPrefix(s.text, cut[0].text)})
			default:
				out = append(out, assembled{text: strings.TrimSuffix(s.text, cut[0].text)})
			}
		}
		return out
	case isPkgCall(c, "strings", "Repeat") && len(c.Args) == 2:
		var out alts
		for _, s := range a.resolve(c.Args[0], sc, env, depth+1) {
			why := s.why
			if why == "" {
				why = "text repeated a number of times this reader does not read"
			}
			out = append(out, assembled{s.text + Unknown, why, s.kinds + "?"})
		}
		return out
	case a.builderString(c, sc) != "":
		name := a.builderString(c, sc)
		at := a.at
		defer func(at *useSite) { a.at = at }(a.at)
		a.at = nil
		out := a.fold(alts{{}}, sc.defs[name], sc.writes[name], token.NoPos, at, "the builder "+name, sc, env, depth)
		// A builder that something else can write to: its address or the
		// builder itself goes to a call or another name, it came in as a
		// parameter, it is not declared here, or it is given a second
		// value. WHERE that other writing lands in the text is not known,
		// so the hole is put at the START: a statement that begins with a
		// part that cannot be read is judged as a write to an unknown
		// table (writesUnknownTable). It used to be put at the end, where
		// it stood in the WHERE clause and was judged harmless.
		if why := builderShared(sc, name); why != "" {
			out = cross(alts{hole("the builder " + name + ", " + why)}, out)
		}
		return out
	case isPkgCall(c, "strings", "Join") && len(c.Args) == 2:
		list, why := a.list(c.Args[0], sc, env, depth+1)
		if why != "" {
			if v, ok := a.loopList(c, sc, env, depth); ok {
				return v
			}
			return alts{hole(why)}
		}
		var out alts
		for _, sep := range a.resolve(c.Args[1], sc, env, depth+1) {
			if sep.why != "" {
				out = append(out, hole(sep.why))
				continue
			}
			out = append(out, assembled{text: strings.Join(list, sep.text)})
		}
		return out
	}
	if v, ok := a.sanitized(c, sc, env, depth); ok {
		return v
	}
	if v, ok := a.callResult(c, 0, sc, env, depth); ok {
		return v
	}
	if id, ok := c.Fun.(*ast.Ident); ok {
		shadowed := sc != nil && (sc.params[id.Name] || len(sc.defs[id.Name]) > 0)
		if fd := a.builders[id.Name]; fd != nil && !shadowed {
			params := builderParams(fd)
			if len(params) == len(c.Args) && !c.Ellipsis.IsValid() {
				bound := map[string]binding{}
				for i, p := range params {
					bound[p] = binding{vals: a.resolve(c.Args[i], sc, env, depth+1)}
				}
				defer func(at *useSite) { a.at = at }(a.at)
				a.at = nil
				return a.resolve(fd.Body.List[0].(*ast.ReturnStmt).Results[0], a.scopeOf(fd), bound, depth+1)
			}
		}
		return alts{hole("the call " + id.Name + "(...), which is not a one-line builder")}
	}
	if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
		return alts{hole("the call " + rootIdent(sel.X) + "." + sel.Sel.Name + "(...)")}
	}
	return alts{hole("a call this reader does not follow")}
}

// sprintf applies one format whose verbs are %s and %[n]s (fmt's rule: a
// plain %s after %[n]s takes argument n+1). Any other verb is a hole: its
// output is not its argument's text.
func sprintf(format assembled, args []alts, ints []bool) alts {
	out := alts{{why: format.why}}
	lit := func(s string) { out = cross(out, alts{{text: s}}) }
	next := 0
	used := make([]bool, len(args))
	f := format.text
	for i := 0; i < len(f); i++ {
		if f[i] != '%' || i+1 >= len(f) {
			lit(f[i : i+1])
			continue
		}
		i++
		if f[i] == '%' {
			lit("%")
			continue
		}
		verb := "%" + f[i:i+1]
		if f[i] == '[' {
			end := strings.IndexByte(f[i:], ']')
			n, err := strconv.Atoi(f[i+1 : i+max(end, 1)])
			if end < 0 || err != nil || n < 1 || i+end+1 >= len(f) {
				out = cross(out, alts{hole("a numbered format verb this reader cannot parse")})
				continue
			}
			next = n - 1
			i += end + 1
			verb = "%[" + strconv.Itoa(n) + "]" + f[i:i+1]
		}
		switch {
		case f[i] == 's' && next < len(args):
			out = cross(out, args[next])
			used[next] = true
			next++
		case f[i] == 's':
			out = cross(out, alts{hole("a %s with no argument")})
		case (f[i] == 'd' || f[i] == 'v') && next < len(args) && next < len(ints) && ints[next]:
			out = cross(out, args[next])
			used[next] = true
			next++
		default:
			out = cross(out, alts{hole("the format verb " + verb + ", whose output is not its argument's text")})
			if next < len(args) {
				used[next] = true
				next++
			}
		}
	}
	for _, u := range used {
		if !u {
			// fmt appends %!(EXTRA type=value) for each: text that is in
			// the statement and not in the format.
			out = cross(out, alts{hole("an argument the format has no verb for, which fmt appends to the text")})
			break
		}
	}
	return out
}

// list reads a fixed list of strings: a composite literal of fixed
// elements, or a name that is one.
func (a *assembler) list(e ast.Expr, sc *localScope, env map[string]binding, depth int) ([]string, string) {
	if depth > assembleDepth {
		return nil, "a list nested too deeply to read"
	}
	switch x := e.(type) {
	case *ast.CompositeLit:
		var out []string
		for _, el := range x.Elts {
			r := a.resolve(el, sc, env, depth+1)
			if len(r) != 1 || r[0].why != "" {
				return nil, "a list one of whose elements is not one fixed text (" + r[0].why + ")"
			}
			out = append(out, r[0].text)
		}
		return out, ""
	case *ast.Ident:
		if sc != nil {
			if sc.params[x.Name] {
				return nil, "the parameter " + x.Name
			}
			if defs, ok := sc.defs[x.Name]; ok {
				if len(defs) != 1 || defs[0].val == nil || sc.mutated[x.Name] || sc.escaped[x.Name] {
					return nil, "the local list " + x.Name + ", which is not defined once from one value and left alone (it is also given to something that could change it)"
				}
				return a.list(defs[0].val, sc, env, depth+1)
			}
		}
		if v, ok := a.vars[x.Name]; ok {
			if a.assigned[x.Name] {
				return nil, "the package variable " + x.Name + ", which is assigned after its declaration"
			}
			return a.list(v, nil, nil, depth+1)
		}
		return nil, "the list " + x.Name + ", which this reader cannot follow"
	}
	return nil, "a list that is not written out where this reader can follow it"
}

// unknownTableHead finds a write's head followed by a table name with a
// part that could not be read: the bare hole, a schema and a hole
// (`aveloxis_data.‹?›`), a quoted hole, a name ending in one. In any case.
var unknownTableHead = regexp.MustCompile(`(?i)\b(INSERT\s+INTO|UPDATE(?:\s+ONLY)?|MERGE\s+INTO)\s+[\w."]*` + regexp.QuoteMeta(Unknown) + `[\w."‹?›]*`)

// What must follow the table for the text to be a statement and not a
// sentence ("could not update ‹?›: ‹?›" is a message): an UPDATE has a SET,
// an INSERT a column list, VALUES, a query or DEFAULT VALUES, a MERGE a
// USING.
var (
	updateContinues = regexp.MustCompile(`(?i)\bSET\b`)
	insertContinues = regexp.MustCompile(`(?i)^\s*(?:AS\s+\w+\s*)?(?:\(|VALUES\b|SELECT\b|WITH\b|TABLE\b|DEFAULT\s+VALUES\b|OVERRIDING\b)`)
	mergeContinues  = regexp.MustCompile(`(?i)\bUSING\b`)
)

// plainHead finds a write's head whose table is written with double quotes
// around names that need none (lower case, no special character): to
// PostgreSQL `"aveloxis_data"."repos"` is aveloxis_data.repos, and every
// matcher here spells a table the plain way.
var plainHead = regexp.MustCompile(`((?i:INSERT\s+INTO|UPDATE(?:\s+ONLY)?|MERGE\s+INTO)\s+)("?[a-z_][a-z0-9_]*"?(?:\."?[a-z_][a-z0-9_]*"?)?)`)

// plainWriteHead rewrites such heads without the quotes. A quoted name
// with a capital or any other character is a different name, and is left
// as written.
func plainWriteHead(sql string) string {
	if !strings.Contains(sql, `"`) {
		return sql
	}
	return plainHead.ReplaceAllStringFunc(sql, func(m string) string {
		sub := plainHead.FindStringSubmatch(m)
		name := sub[2]
		for _, part := range strings.Split(name, ".") {
			if strings.HasPrefix(part, `"`) != strings.HasSuffix(part, `"`) {
				return m // a quote that does not close here: not a plain name
			}
		}
		return sub[1] + strings.ReplaceAll(name, `"`, "")
	})
}

// What shows that text WRITES when its head cannot be read: an UPDATE's or
// an upsert's SET with a column to its right, an INSERT's column list
// before VALUES, or a write head further on.
var writeShows = regexp.MustCompile(`(?is)\bSET\s+\(?\s*"?[a-z_][\w.]*"?\s*[=,]|\)\s*VALUES\s*\(|\bDO\s+UPDATE\s+SET\b|` +
	`\bUPDATE(?:\s+ONLY)?\s+\S+(?:\s+(?:AS\s+)?\w+)?\s+SET\b|` +
	`\bINSERT\s+INTO\s+\S+\s*(?:AS\s+\w+\s*)?(?:\(|VALUES\b|SELECT\b|WITH\b|TABLE\b|DEFAULT\s+VALUES\b|OVERRIDING\b)|` +
	`\bMERGE\s+INTO\b.*\bUSING\b`)

// beginsUnread: one of the text's statements begins with a part that could
// not be read, and what can be seen of it writes. Its head, and so its
// table, is then not known: the unread part may BE the head (`head + " SET
// repo_git = $1"`), or may be text whose place in the statement this
// reader could not tell (a builder that something else also writes to) and
// was therefore put first.
func beginsUnread(sql string) bool {
	for _, piece := range splitStatements(sql) {
		piece = strings.TrimLeft(piece, " \t\r\n(")
		if rest, ok := strings.CutPrefix(piece, Unknown); ok && writeShows.MatchString(rest) {
			return true
		}
	}
	return false
}

// writesUnknownTable: the write's table is itself, in whole or in part, a
// part that could not be read. sql has its comments stripped.
func writesUnknownTable(sql string) bool {
	if strings.TrimSpace(sql) == Unknown {
		return true // no part of it can be read at all
	}
	if beginsUnread(sql) {
		return true
	}
	for _, m := range unknownTableHead.FindAllStringSubmatchIndex(sql, -1) {
		head, rest := strings.ToUpper(sql[m[2]:m[3]]), sql[m[1]:]
		switch {
		case strings.HasPrefix(head, "UPDATE"):
			if updateContinues.MatchString(rest) {
				return true
			}
		case strings.HasPrefix(head, "INSERT"):
			if insertContinues.MatchString(rest) {
				return true
			}
		default:
			if mergeContinues.MatchString(rest) {
				return true
			}
		}
	}
	return false
}

// Shape is a registered KIND of SQL argument with a part that is not fixed
// text, for the ones that are not filled value by value: a savepoint named
// with a counter, DDL on a table drawn from a list, a read with a filter
// built from validated input. It is an exception given to a named list of
// declarations at once, and it is checked: the argument's whole text, with
// Unknown at each hole, must match Text and must not match Forbid. An
// argument of a listed declaration that does not is a violation, and a
// declaration listed that has no such argument is stale.
//
// A Shape judges nothing about what a hole could hold. It records that a
// person looked at where each listed declaration's holes come from; Reason
// says what they found, for the whole list.
//
// 0.29.211: a shape lists ARGUMENTS, not declarations. Listed by
// declaration, any statement later added to a listed function that matched
// the pattern was registered the moment it was written, read by nobody (an
// injectable read added to a listed function passed). Each entry now names
// the declaration, the argument's place among the declaration's SQL
// arguments, and how many the declaration has: a statement added to it
// changes that count, and every entry of the declaration is stale until
// someone reads the declaration again and corrects them.
type Shape struct {
	Name   string
	Args   []ShapeArg
	Text   *regexp.Regexp
	Forbid *regexp.Regexp
	Reason string
}

// ShapeArg is one SQL argument a Shape covers: Unreadable.Call of
// Unreadable.Of in Decl.
type ShapeArg struct {
	Decl     string
	Call, Of int
	// Kinds (0.29.212) names the kinds of this argument's unread parts,
	// separated by commas: KindIdentifier, KindRepeated, KindLiteral. It is not a
	// description: the reader verifies the kind of every unread part from
	// the expression that produces it (kindsOf), and the entry covers the
	// argument only when every part is of a kind named here. A part of no
	// verified kind (KindUnknown) is covered by no shape, whatever the
	// pattern: only a named exception that says where the text comes from
	// covers it. And an identifier in a statement that writes rows is
	// covered by none either: which table or column it names is not
	// known, so the statement is read whole or it is a violation.
	Kinds string
}

func (s ShapeArg) String() string {
	return fmt.Sprintf("%s (SQL argument %d of %d)", s.Decl, s.Call, s.Of)
}

// CheckUnreadable decides what becomes of each expression Assemble could
// not read whole. DENY BY DEFAULT (0.29.210, after three review rounds each
// found another place where a part judged "harmless" was not): nothing here
// decides that an unread part cannot matter.
//
// The SQL argument of a call that runs SQL (a read included) is in one of
// three states, or it is a violation:
//
//  1. FILLED: a Fill gives the real values of its holes; every instance is
//     a whole statement, handed on to Check.
//  2. SHAPED: its declaration is listed in a Shape whose pattern its whole
//     text matches. It is handed on to Check with its holes in, which
//     judges every registered column it can see written and fails a value
//     that holds a hole.
//  3. EXCEPTED: an UnreadableException with a reason names it.
//
// An expression that is NOT such an argument (a sum, a format, a builder's
// result on its way somewhere) is looked at only for what it shows: if it
// writes a registered table, or a table that is itself unread, it must be
// filled or excepted too. One that shows neither is a fragment; where it
// reaches a call in this package, the argument is judged above.
func CheckUnreadable(unread []Unreadable, registry []Registered, fills []Fill, exceptions []UnreadableException, shapes []Shape) UnreadReport {
	var rep UnreadReport
	var res []*regexp.Regexp
	done := map[string]bool{}
	for _, r := range registry {
		if !done[r.Table] {
			done[r.Table] = true
			res = append(res, regexp.MustCompile(`(?i)(INSERT\s+INTO|UPDATE(?:\s+ONLY)?|MERGE\s+INTO)\s+`+regexp.QuoteMeta(r.Table)+`\b`))
		}
	}
	norm := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	hand := func(file, sql string) {
		for _, piece := range splitStatements(srctest.StripSQLComments(sql)) {
			if piece = strings.TrimSpace(piece); piece != "" {
				rep.Statements = append(rep.Statements, Stmt{File: file, SQL: plainWriteHead(piece)})
			}
		}
	}
	fillUsed, exUsed := make([]bool, len(fills)), make([]bool, len(exceptions))
	fillBad := make([]bool, len(fills))
	for i, f := range fills {
		switch {
		case strings.TrimSpace(f.Reason) == "":
			rep.BadFills, fillBad[i] = append(rep.BadFills, fmt.Sprintf("%s (%q): a fill REQUIRES a reason: where its values come from and what keeps the code to them", f.Decl, f.Match)), true
		case strings.Join(strings.Fields(f.Match), " ") == Unknown && f.Arg == "":
			rep.BadFills, fillBad[i] = append(rep.BadFills, fmt.Sprintf("%s: a fill for a whole argument (Match is %s) names the argument it is for, as written; without that it would cover any", f.Decl, Unknown)), true
		case len(f.Values) == 0:
			rep.BadFills, fillBad[i] = append(rep.BadFills, fmt.Sprintf("%s (%q): a fill with no values checks nothing", f.Decl, f.Match)), true
		}
	}
	shapeUsed := make([]map[ShapeArg]bool, len(shapes))
	for i, sh := range shapes {
		shapeUsed[i] = map[ShapeArg]bool{}
		if strings.TrimSpace(sh.Reason) == "" || sh.Text == nil || sh.Name == "" {
			rep.BadFills = append(rep.BadFills, fmt.Sprintf("shape %q: a shape REQUIRES a name, a pattern for the whole text, and a reason", sh.Name))
		}
		for _, d := range sh.Args {
			bad := strings.TrimSpace(d.Kinds) == "" || d.Call < 1 || d.Of < d.Call
			for _, k := range strings.Split(d.Kinds, ",") {
				bad = bad || (k != KindIdentifier && k != KindRepeated && k != KindLiteral)
			}
			if bad {
				rep.BadFills = append(rep.BadFills, fmt.Sprintf("shape %q: %s REQUIRES its place (Call of Of) and the kinds of its unread parts (Kinds: %s, %s, %s; no other kind can stand under a shape)", sh.Name, d.String(), KindIdentifier, KindRepeated, KindLiteral))
			}
		}
	}
	kindSeen := map[ShapeArg]map[string]bool{}
	for i, ex := range exceptions {
		if (ex.Call > 0 || ex.Of > 0) && strings.TrimSpace(ex.Source) == "" {
			rep.BadFills = append(rep.BadFills, fmt.Sprintf("exception %d (%s): an exception for an SQL argument REQUIRES Source: where the text this reader cannot read comes from", i, ex.Decl))
		}
	}
	for _, u := range unread {
		text := norm(u.Text)
		filled := false
		for i, f := range fills {
			if f.Decl == "" || f.Decl != u.Decl || f.Match == "" || !strings.Contains(text, norm(f.Match)) {
				continue
			}
			// A fill whose Match is the hole itself is for an argument
			// with no fixed text, and for the one it names.
			if norm(f.Match) == Unknown && (text != Unknown || f.Arg == "" || f.Arg != u.Arg) {
				continue
			}
			fillUsed[i] = true
			if fillBad[i] {
				continue
			}
			holes := strings.Count(u.Text, Unknown)
			ok := true
			for _, row := range f.Values {
				if len(row) != holes {
					ok = false
					rep.BadFills = append(rep.BadFills, fmt.Sprintf("%s:%d (%q): the statement has %d parts that are not fixed text and a row of the fill gives %d values (%q) — the statement or the list changed shape", u.File, u.Line, f.Match, holes, len(row), row))
				}
			}
			if !ok {
				continue
			}
			filled = true
			for _, row := range f.Values {
				sql := u.Text
				for _, v := range row {
					sql = strings.Replace(sql, Unknown, v, 1)
				}
				hand(u.File, sql)
			}
		}
		if filled {
			continue
		}
		bare := plainWriteHead(srctest.StripSQLComments(u.Text))
		if !u.SQLArg {
			// Not an argument: judged only by what it shows.
			registered := false
			for _, re := range res {
				registered = registered || re.MatchString(bare)
			}
			if !registered && !writesUnknownTable(bare) {
				continue
			}
		}
		shaped := false
		if u.SQLArg {
			flat := norm(bare)
			for i, sh := range shapes {
				for _, d := range sh.Args {
					if d.Decl != u.Decl || d.Call != u.Call || d.Of != u.Of || u.Call == 0 ||
						sh.Text == nil || !sh.Text.MatchString(flat) || (sh.Forbid != nil && sh.Forbid.MatchString(flat)) {
						continue
					}
					if !partsVerified(u, d.Kinds, flat) {
						continue
					}
					if kindSeen[d] == nil {
						kindSeen[d] = map[string]bool{}
					}
					for _, k := range u.Kinds {
						kindSeen[d][k] = true
					}
					shaped, shapeUsed[i][d] = true, true
				}
			}
		}
		if shaped {
			hand(u.File, u.Text)
			continue
		}
		excused := false
		for i, ex := range exceptions {
			if ex.Decl == "" || ex.Decl != u.Decl || ex.Match == "" || !strings.Contains(text, norm(ex.Match)) {
				continue
			}
			if ex.Arg != "" && ex.Arg != u.Arg {
				continue
			}
			if norm(ex.Match) == Unknown && (ex.Arg == "" || text != Unknown) {
				continue // a whole-argument exception names its argument, and covers only that
			}
			if u.Call > 0 && (ex.Call != u.Call || ex.Of != u.Of) {
				continue // an SQL argument is excepted by its place, in a declaration of that many
			}
			exUsed[i] = true
			excused = excused || (strings.TrimSpace(ex.Reason) != "" && (u.Call == 0 || strings.TrimSpace(ex.Source) != ""))
		}
		if !excused {
			switch {
			case strings.TrimSpace(bare) == Unknown:
				u.Why += "; and it is the whole SQL argument of a call that runs SQL: no part of it can be read"
			case u.SQLArg:
				u.Why += "; and it is the SQL argument of a call that runs SQL: an argument is read whole, filled, of a registered shape, or excepted, never passed on what its fixed text looks like"
			default:
				u.Why += "; and what can be seen of it writes a registered table, or a table that is itself unread"
			}
			rep.Violations = append(rep.Violations, u)
		}
	}
	for i, f := range fills {
		if !fillUsed[i] {
			rep.StaleFills = append(rep.StaleFills, f)
		}
	}
	for i, ex := range exceptions {
		if !exUsed[i] {
			rep.StaleExceptions = append(rep.StaleExceptions, ex)
		}
	}
	for i, sh := range shapes {
		seen := map[ShapeArg]bool{}
		for _, d := range sh.Args {
			if seen[d] {
				rep.StaleShapes = append(rep.StaleShapes, sh.Name+": "+d.String()+" is listed twice")
			}
			seen[d] = true
			for _, k := range strings.Split(d.Kinds, ",") {
				if shapeUsed[i][d] && !kindSeen[d][k] {
					rep.StaleShapes = append(rep.StaleShapes, sh.Name+": "+d.String()+" names the kind "+k+", and the argument has no unread part of that kind")
				}
			}
			if !shapeUsed[i][d] {
				rep.StaleShapes = append(rep.StaleShapes, sh.Name+": "+d.String()+" is not an unread SQL argument of this shape (the declaration changed, or the argument is read whole now): read the declaration again and correct or delete the entry")
			}
		}
	}
	return rep
}

// rowWrite is text that shows a statement which writes rows.
var rowWrite = regexp.MustCompile(`(?is)\b(INSERT\s+INTO|UPDATE\s+(?:ONLY\s+)?\S+(?:\s+(?:AS\s+)?\w+)?\s+SET|DELETE\s+FROM|MERGE\s+INTO|TRUNCATE\s|COPY\s)`)

// partsVerified reports whether every unread part of u is of a kind named
// in kinds (comma-separated), none is of no verified kind, and no
// identifier stands in a statement that writes rows.
func partsVerified(u Unreadable, kinds, flat string) bool {
	if len(u.Kinds) == 0 {
		return false
	}
	allowed := map[string]bool{}
	for _, k := range strings.Split(kinds, ",") {
		allowed[k] = true
	}
	for _, k := range u.Kinds {
		if k == KindUnknown || !allowed[k] {
			return false
		}
		if k == KindIdentifier && rowWrite.MatchString(flat) {
			return false
		}
	}
	return true
}

// envProduct pairs every binding set of l with every one of r.
func envProduct(l, r []map[string]binding) []map[string]binding {
	if len(l)*len(r) > assembleInstances {
		return l
	}
	out := make([]map[string]binding, 0, len(l)*len(r))
	for _, x := range l {
		for _, y := range r {
			m := make(map[string]binding, len(x)+len(y))
			for k, v := range x {
				m[k] = v
			}
			for k, v := range y {
				m[k] = v
			}
			out = append(out, m)
		}
	}
	return out
}

// callEnvs is, for a function every one of whose calls can be found
// (typedPkg.closedCallers), one binding set per call: each parameter that
// can be bound (bindable) to what that call gives it, read where the call
// stands, in the caller's own scope; the caller's parameters are bound the
// same way when its calls can be found too. nil when the calls cannot all
// be found, or there are too many: the parameters are then unread parts,
// as before.
func (a *assembler) callEnvs(fn ast.Node, depth int) []map[string]binding {
	if a.typed == nil || depth > 3 || a.following[fn] {
		return nil
	}
	calls, ok := a.typed.closedCallers(fn)
	if !ok {
		return nil
	}
	top, _ := a.typed.around(fn)
	if fd, isDecl := fn.(*ast.FuncDecl); isDecl {
		top = fd
	}
	if top == nil {
		return nil
	}
	scF := a.scopeOf(top)
	names, variadic := paramNames(fn)
	some := false
	for _, p := range names {
		some = some || a.bindable(p, scF, top)
	}
	if variadic {
		names = names[:len(names)-1]
	}
	funcParam := map[string]bool{}
	if a.typed != nil {
		var ft *ast.FuncType
		switch f := fn.(type) {
		case *ast.FuncDecl:
			ft = f.Type
		case *ast.FuncLit:
			ft = f.Type
		}
		for _, f := range ft.Params.List {
			if _, isFunc := f.Type.(*ast.FuncType); isFunc {
				for _, n := range f.Names {
					funcParam[n.Name] = true
					some = some || a.bindable(n.Name, scF, top)
				}
			}
		}
	}
	if !some {
		return nil
	}
	a.following[fn] = true
	defer delete(a.following, fn)
	defer func(at *useSite) { a.at = at }(a.at)
	var out []map[string]binding
	for _, c := range calls {
		if c.Ellipsis.IsValid() || len(c.Args) < len(names) || (!variadic && len(c.Args) != len(names)) {
			return nil
		}
		ctop, around := a.typed.around(c)
		if ctop == nil {
			return nil
		}
		scC := a.scopeOf(ctop)
		site := &useSite{pos: c.Pos(), blocks: map[*ast.BlockStmt]bool{}}
		var loops []*ast.RangeStmt
		var chain []ast.Node
		for _, n := range around {
			switch x := n.(type) {
			case *ast.RangeStmt:
				loops = append(loops, x)
			case *ast.BlockStmt:
				site.blocks[x] = true
			case *ast.FuncLit:
				site.lit = x
				chain = append([]ast.Node{x}, chain...)
			case *ast.FuncDecl:
				chain = append(chain, x)
			}
		}
		envs := a.instances(loops, scC)
		for _, caller := range chain {
			if per := a.callEnvs(caller, depth+1); per != nil {
				envs = envProduct(envs, per)
			}
		}
		for _, env := range envs {
			bound := map[string]binding{}
			for i, p := range names {
				if !a.bindable(p, scF, top) {
					continue
				}
				if funcParam[p] {
					switch f := unparen(c.Args[i]).(type) {
					case *ast.FuncLit:
						bound[p] = binding{fn: f, sc: scC}
					case *ast.Ident:
						if fd := a.typed.ix.declOf[a.typed.info.Uses[f]]; fd != nil {
							bound[p] = binding{fn: fd}
						}
					}
					continue
				}
				if id, ok := unparen(c.Args[i]).(*ast.Ident); ok {
					// A range loop's variable, handed on whole: the
					// parameter is that element, fields and all.
					if b, isLoop := env[id.Name]; isLoop && b.elem != nil && b.as == "" && !loopVarChanged(b, id.Name) {
						b.as = id.Name
						bound[p] = b
						continue
					}
				}
				a.at = site
				bound[p] = binding{vals: a.resolve(c.Args[i], scC, env, 0)}
			}
			out = append(out, bound)
			if len(out) > assembleInstances {
				return nil
			}
		}
	}
	return out
}

// repeated is one round of what a loop adds, with the mark that says the
// loop may add it again: the text is kept, so that every check sees it.
func repeated(v assembled, why string) assembled {
	if v.why == "" && placeholderText.MatchString(v.text) {
		// Placeholders and their punctuation only: one round of it is the
		// statement for every count of rounds, as far as any check here
		// reads (a list of $n is a list of $n). No mark: it is read whole.
		return v
	}
	if v.why != "" {
		why = v.why
	}
	return assembled{v.text + Unknown, why, v.kinds + "r"}
}

// loopList reads strings.Join(xs, sep) for a local list xs that is built
// by a loop: by OBJECT, every use of xs in its declaration is its own
// declaration (empty, made, or a literal), xs = append(xs, E...), xs[i] =
// E, len(xs), a range over it, or this Join. The result is one round of
// each E, each followed by the KindRepeated mark, joined by sep.
func (a *assembler) loopList(c *ast.CallExpr, sc *localScope, env map[string]binding, depth int) (alts, bool) {
	if a.typed == nil {
		return nil, false
	}
	id, ok := unparen(c.Args[0]).(*ast.Ident)
	if !ok {
		return nil, false
	}
	obj, ok := a.typed.info.Uses[id].(*types.Var)
	if !ok || obj.Parent() == a.typed.pkg.Scope() || isField(obj) || a.typed.params[obj] {
		return nil, false
	}
	if sl, isSlice := obj.Type().Underlying().(*types.Slice); !isSlice || !isString(sl.Elem()) {
		return nil, false
	}
	ix := a.typed.ix
	var pieces []ast.Expr
	for _, ref := range ix.refs[obj] {
		switch p := ix.parent[ref].(type) {
		case *ast.CallExpr:
			if p == c {
				continue
			}
			f, isID := unwrapFun(p.Fun).(*ast.Ident)
			if !isID {
				return nil, false
			}
			b, builtin := a.typed.info.Uses[f].(*types.Builtin)
			switch {
			case builtin && (b.Name() == "len" || b.Name() == "cap"):
			case builtin && b.Name() == "append" && len(p.Args) > 0 && p.Args[0] == ast.Expr(ref) && !p.Ellipsis.IsValid():
				as, isAssign := ix.parent[p].(*ast.AssignStmt)
				if !isAssign || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
					return nil, false
				}
				if l, isID := as.Lhs[0].(*ast.Ident); !isID || a.typed.objectOf(l) != obj {
					return nil, false
				}
				pieces = append(pieces, p.Args[1:]...)
			default:
				return nil, false
			}
		case *ast.AssignStmt:
			// xs = append(xs, ...): the left side, seen with its call.
			if len(p.Lhs) != 1 || p.Lhs[0] != ast.Expr(ref) || len(p.Rhs) != 1 {
				return nil, false
			}
			call, isCall := unparen(p.Rhs[0]).(*ast.CallExpr)
			if !isCall || len(call.Args) == 0 {
				return nil, false
			}
			if first, isID := call.Args[0].(*ast.Ident); !isID || a.typed.objectOf(first) != obj {
				return nil, false
			}
		case *ast.IndexExpr:
			as, isAssign := ix.parent[p].(*ast.AssignStmt)
			if p.X != ast.Expr(ref) || !isAssign || len(as.Lhs) != 1 || as.Lhs[0] != ast.Expr(p) || len(as.Rhs) != 1 || as.Tok != token.ASSIGN {
				return nil, false
			}
			pieces = append(pieces, as.Rhs[0])
		case *ast.RangeStmt:
			if p.X != ast.Expr(ref) {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	// What it starts as.
	for _, d := range sc.defs[id.Name] {
		if d.id == nil || a.typed.objectOf(d.id) != obj || d.val == nil {
			continue
		}
		switch v := unparen(d.val).(type) {
		case *ast.CompositeLit:
			pieces = append(append([]ast.Expr{}, v.Elts...), pieces...)
		case *ast.CallExpr:
			f, isID := unwrapFun(v.Fun).(*ast.Ident)
			if isID {
				if b, builtin := a.typed.info.Uses[f].(*types.Builtin); builtin && (b.Name() == "make" || b.Name() == "append") {
					continue
				}
			}
			return nil, false
		case *ast.Ident:
			if v.Name != "nil" {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	defer func(at *useSite) { a.at = at }(a.at)
	a.at = nil
	var out alts
	for _, sep := range a.resolve(c.Args[1], sc, env, depth+1) {
		cur := alts{{}}
		for i, piece := range pieces {
			if i > 0 {
				cur = cross(cur, alts{sep})
			}
			var round alts
			for _, v := range a.resolve(piece, sc, env, depth+1) {
				round = append(round, repeated(v, "a list built in a loop"))
			}
			cur = cross(cur, round)
		}
		if len(pieces) == 0 {
			cur = alts{{}}
		}
		out = append(out, cur...)
	}
	if len(out) > assembleAlternatives {
		return alts{hole("a list with more values than this reader follows")}, true
	}
	return out, true
}

// elemEnv is the bindings a range loop's element is read with. One written
// in the declaration being read (the usual case) can name that
// declaration's other bound names: the enclosing loops' variables, and its
// parameters when its calls are followed. One handed in from another
// function (b.as) was written there, and is read with none.
func elemEnv(b binding, name string, env map[string]binding) map[string]binding {
	if b.as != "" || len(env) == 0 {
		return nil
	}
	out := make(map[string]binding, len(env))
	for k, v := range env {
		if k != name {
			out[k] = v
		}
	}
	return out
}

// placeholderText is text that can only be placeholders and the
// punctuation of a list of them.
var placeholderText = regexp.MustCompile(`^[\s$0-9,()]*$`)
