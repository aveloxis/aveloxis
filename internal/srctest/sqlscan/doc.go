// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

// WHAT THE WRITE-POLICY CHECKS ARE FOR (0.29.213), and what they are not.
//
// There are two checks, and they stand on different ground.
//
// THE RUN-TIME JUDGE (internal/testdb/writejudge) DECIDES. It reads no Go.
// It is a tracer that the DB-tier test harness (testdb.Main) puts on every
// pool the store builds while ANY DB-tier package's tests run, and it is
// handed the text of every statement the library is given: Query, QueryRow
// and Exec, each statement of a batch when the batch is sent, the table
// and columns of a CopyFrom, the text of a Prepare. A statement whose first
// frame in this module is a non-test file is PRODUCTION's: it is parsed and
// judged (step 3 below: the same CheckIn, with the one registry of
// policies and Exceptions, in the test database's own catalog), and a
// violation fails the package after its tests. A statement sent from a
// _test file is a fixture's and is not judged. What the judge sees is
// exactly what ran, so no way of building a string in Go can mislead it;
// what it does not see is a statement no test runs. So there is a COVERAGE
// REQUIREMENT beside it: every call, in internal/db, of a library function
// that takes SQL (RunnerCalls, sites.go) must have had a statement sent
// through it by some DB-tier package when the whole suite has run (the
// closing suite, scripts/skip-census.sh, checks the union), or stand on a
// reviewed list that only shrinks
// (internal/db/testdata/unexecuted_statement_sites.txt).
//
// THE STATIC READER (steps 1 and 2 below) gives a maintainer fast feedback
// on an HONEST MISTAKE, without a database: a write of a protected column
// written as a constant, built by a registered builder, or handed to a
// helper, in the forms listed. It is deliberately small. Six review rounds
// showed that a reader of Go cannot be made complete against Go written to
// get around it, and by the operator's decision of 2026-11-20 that is not
// what it is for (OUT OF SCOPE, at the end).
//
// The static check is three steps, each of which fails closed.
//
// 1. FIND (typed.go, carriers.go). The package is type-checked. An SQL
//    argument is the argument at the SQL position of a call whose callee is,
//    BY OBJECT, a function of the database library classified as running
//    SQL (every member of pgx, pgxpool, pgconn and database/sql that takes
//    text is classified, and a test fails on one that is not), a method of
//    an interface with such a name and position, or a function of the
//    package that hands its own parameter to one; or a value given to a
//    library field that holds SQL. Reported as a REFERENCE, which fails
//    until registered with its reason:
//      - something that runs SQL taken as a value, written as a method
//        expression, held in an exported variable, or reached by reflect;
//      - a call whose arguments are the several results of ONE call
//        (run(two())): the SQL stands at no argument;
//      - an argument of such a call whose type has a RewriteQuery method
//        (pgx.QueryRewriter, pgx.NamedArgs): the library runs what it
//        returns;
//      - the result of a function that returns an SQL-running function
//        literal, wherever it is not called at once (F(...)(...));
//      - a method of a generic type that hands its parameter to a call that
//        runs SQL (a generic FUNCTION is followed: its calls name one
//        object);
//      - the doors carriers.go lists, by which a library type that carries
//        SQL with no text at the call (a batch, a queued query, transaction
//        options, a prepared statement) arrives from or leaves to code the
//        reading does not follow.
//
// 2. READ (strict.go). There is no evaluator of Go. An argument is read
//    only as one of:
//      - CONSTANT: the compiler's string constant, of the basic type string;
//      - PURE: one of the listed standard-library functions, or +, on such
//        constants, evaluated by calling that function: strings.Join of a
//        list of constants written in place, Repeat, Replace, ReplaceAll,
//        TrimSpace, TrimPrefix, TrimSuffix; and fmt.Sprintf with the verbs
//        %s (a string constant), %d (an integer constant) and %%, plain or
//        indexed (%[2]s), and NO other verb, flag or width: those three
//        write the same text for every Go type the value may have, which
//        matters because the constant is handed to fmt as a plain string
//        or int and not as the type it has in the program. Also a string
//        variable whose declaration is the only thing in the package that
//        gives it a value, read as that declaration's value;
//      - BUILT: such text joined with a call of one of the package's own
//        functions on string constants, or with an embedded file's
//        variable, evaluated by RUNNING the real function, which the
//        package's test supplies (Env);
//      - QUEUED: the SQL of a statement read back from a batch's own
//        queue to copy it (q.SQL for q ranging over a pgx.Batch's
//        QueuedQueries), which was read where it was queued;
//      - PARAMETER: the parameter of an unexported function, never
//        assigned and never addressed, handed on untouched: read at each
//        call of the function found by object.
//    Text with a gap in it (a number, a parameter, anything) is NOT read:
//    the two readings that filled gaps in 0.29.212 are removed. Everything
//    else is a HOLE, and a hole fails until it is registered (Site), which
//    means EXECUTED: the argument is exactly a call of the store's builder
//    (or a local taken once from one), and the check RUNS the real builder
//      - over the whole of a finite domain: the index of each member of a
//        list (which the store's test shows nothing changes), every key of
//        a map, the SET of every combination of its bools, or the constants
//        every caller passes (found by object, through unexported
//        functions that nothing uses as a value, never through a variadic
//        parameter, to four calls), with a test showing an input outside
//        the domain gives no statement;
//      - or, OPEN, over representative inputs, where what varies may reach
//        the text only as the operand of pgx.Identifier{...}.Sanitize()
//        called on it where it is written, through a registered quoting
//        function of the package (a function, by its own name; a method of
//        that name is not it), or, for a number of a BASIC integer type,
//        through %d, %s or %v of fmt, strconv.Itoa, or FormatInt /
//        FormatUint in base ten. A parameter ranged over as a string or
//        into variables declared elsewhere, a number copied to another
//        variable or put into a list, and a parameter of a declared number
//        or bool type each fail.
//    A registration with nothing run (FROZEN) fails the store's test.
//
// 3. JUDGE (pgread.go, policy.go). Each statement is parsed by POSTGRESQL'S
//    OWN PARSER (libpg_query: the server's parser, compiled to WebAssembly
//    and run in the test process; no cgo, and no production binary links
//    it), and its writes are read from the parse tree: UPDATE, INSERT with
//    its ON CONFLICT DO UPDATE, DELETE, MERGE with each of its actions,
//    TRUNCATE, COPY FROM, ALTER TABLE's actions that rewrite or destroy a
//    column (TYPE, SET EXPRESSION, DROP, RENAME), DROP TABLE; at any depth
//    (a data-modifying WITH, a subquery, EXPLAIN, PREPARE, a rule's
//    actions, CREATE TABLE AS); and in the body of a DO block or a
//    function, which is parsed in its own language (an SQL body by the
//    same parser, a PL/pgSQL body by the server's PL/pgSQL parser, every
//    statement and expression of it then read in turn; a trigger's
//    assignment to NEW.col is a write of col). A value is judged on its
//    tree, and so is the WHERE that guards a fill. Violations:
//      - a statement the parser refuses (a text with a NUL byte included),
//        or that holds a statement kind or an ALTER TABLE action this
//        package has not classified (the lists are the parser's whole sets
//        for its grammar version; a test fails on a parser of another
//        major version until they are redone);
//      - a body in a language that is not read, EXECUTE of text made at
//        run time;
//      - a write whose target is not a base table the CATALOG knows (below);
//      - a statement that changes what a registered table is or holds
//        without assigning a column (below);
//      - a value of a registered column that does not match its policy.
//    The grammar is PostgreSQL 17's. A statement that needs a later
//    grammar is refused by the parser, and so is a violation, never a pass.
//
//    Two questions are each answered ONCE, for every rule (0.29.213; before,
//    each matcher looked at one node and guessed):
//
//    WHOSE COLUMN A NAME IS (policy.go, Write.classify). The reader records,
//    for each write, what a qualifier can name where it stands: the
//    target and its alias, the write's own FROM / USING items and a MERGE's
//    source, EXCLUDED (in ON CONFLICT DO UPDATE and nowhere else), and the
//    variables, records, parameters and function name of the body it
//    stands in, with that body's #variable_conflict. A reference whose last
//    part is the column's name is then exactly one of:
//      - STORED: bare; or qualified by the target's alias; or, when the
//        target has no alias, by its name or schema.name. (PostgreSQL stops
//        a statement in which a bare name is also another relation's
//        column; a parameter of an SQL function yields to a column.)
//      - INCOMING: EXCLUDED.col. (A $N placeholder is incoming too; either
//        may carry one cast.)
//      - ANOTHER RELATION'S: qualified by a FROM item, the MERGE source, or
//        a relation of the sub-select the reference stands in.
//      - A VARIABLE: a record's field, a parameter qualified by the
//        function's name, or a bare name the body's #variable_conflict
//        use_variable gives to a variable.
//      - UNRESOLVED: a qualifier that names nothing in scope, or both the
//        target and something else; a bare name that is also a variable of
//        a PL/pgSQL body that does not say which wins; a name of more
//        parts than schema.table.column.
//    A policy's "stored" operand is a STORED reference to the SAME column,
//    and nothing else; its "incoming" operand is a placeholder or
//    EXCLUDED's SAME column. "The value does not read the stored value"
//    (AlwaysRefresh; the incoming side of a fill) fails on a STORED or an
//    UNRESOLVED reference at any depth, on the target's whole row (r, r.*,
//    to_jsonb(r), (r).col), and on the target TABLE named again in a
//    sub-select under any alias. The guard of a bare fill is one of four
//    forms on a STORED reference (Write.GuardsEmpty). A part of a column
//    as a target (col[1], col.f) is a violation.
//
//    WHAT IS A WRITE OF A TABLE (policy.go, Catalog; CheckIn). The catalog
//    is built from DDL by the same reader (bodies included): base tables;
//    views, materialized views and foreign tables with the relations
//    behind each, transitively (a rule ON SELECT makes a view; a rename
//    carries what the relation was); partitions and inheritance children.
//    The store's test builds it from every statement the store runs, and
//    the test of the other packages judges with that same catalog.
//      - A write of rows (INSERT, UPDATE, DELETE, MERGE, COPY FROM,
//        TRUNCATE) is accepted only on a schema-qualified name that is a
//        BASE TABLE of the catalog. A view, a materialized view, a foreign
//        table, a relation the catalog does not know, and any unqualified
//        name (but a temporary table the same text creates) are violations;
//        so is a write of a partition or child of a registered table.
//      - Violations by kind, each of which an Exception must name, when the
//        table is registered: DROP TABLE, TRUNCATE, DROP SCHEMA of its
//        schema; RENAME TO and SET SCHEMA (and another relation taking its
//        name); ATTACH / DETACH PARTITION, INHERIT, CREATE TABLE ...
//        PARTITION OF / INHERITS it; CREATE RULE on it; CREATE TRIGGER on
//        it, WHATEVER function it runs; a foreign key on a registered
//        column whose action writes it (ON UPDATE CASCADE / SET NULL / SET
//        DEFAULT, ON DELETE SET NULL / SET DEFAULT, the column list form
//        included); ALTER of the column's type, expression or name, DROP
//        of it, another column renamed TO its name; COPY FROM of it.
//      - A trigger FUNCTION's body: NEW.col := v is a write of col (also
//        SELECT INTO, GET DIAGNOSTICS, FOR and FOREACH targets). Held
//        against it besides: any value given to OLD or to the whole NEW;
//        NEW or OLD used whole as a value (r := NEW, f(NEW), NEW.*); NEW or
//        OLD in a CALL; a RETURN of anything but NEW, OLD or NULL; a
//        variable named like a field of them.
//
//    AN EXCEPTION excuses one violation: its table, column, kind, file and
//    DECLARATION (the function the statement stands in; for the run-time
//    judge, any production function on the stack that sent it), and the
//    assigned expression whole (Fragment) or the statement whole
//    (Statement), compared as parse trees. Within one argument's text it is
//    spent by the first violation it matches.
//
// WHAT IS NOT GUARANTEED:
//   - An Env function, and a registered builder, is trusted to depend on
//     nothing but its arguments: the reading runs it once, in the test
//     process (a builder that read the environment or the clock would give
//     the judge at run time, not this reading, the statement it really
//     makes).
//   - The callers of an unexported function are taken to be the package's
//     non-test files: a _test file's call is not read, and is not a caller.
//   - The policy shapes of policy.go are a closed list: a value of another
//     shape fails, whatever it computes (a value taken from a FROM item
//     under a correct guard is such a value). A bare name in a sub-select
//     counts as the stored value, though the sub-select's own relation may
//     be what has the column.
//   - The catalog knows relations, not columns or their types: that a view
//     shows the column under another name, that DROP TYPE ... CASCADE
//     drops a column of the type, that a function called by SELECT f() or
//     CALL p() writes, are not in any text it reads. What a trigger or a
//     rule created OUTSIDE the statements read does is not either.
//   - The parser is PostgreSQL 17's and the fleet runs 18: what 18 reads
//     and 17 does not is refused, and what both read is read as 17 does.
//   - A trigger function's assignment to NEW.col is reported for every
//     registered column of that name, since the function does not say
//     which table its trigger is on.
//   - Text made in the database (EXECUTE of a computed string in a body) is
//     reported unread, never judged.
//   - The other source tripwires of sqlscan.go: Statements splits backtick
//     literals with patterns, and FindWrites and WritesColumn ask the
//     parse tree only of a piece that is a whole statement (a fragment, SQL
//     in a double-quoted Go string or split over two literals is a
//     pattern's, or unseen). The policy check does not use them.
//
// OUT OF SCOPE, by the operator's decision of 2026-11-20: Go written to get
// around the checks. The static reader does not defend against any of
// these, and a gap of this kind in it is not a defect of it. For each,
// whether the RUN-TIME JUDGE still holds a violation against it when a test
// of the store executes it:
//   - a statement routed through reflection, a method value passed about,
//     or a function value stored in a struct: JUDGED (the tracer is handed
//     the text whatever called the library);
//   - a carrier of SQL (a batch, a queued query) smuggled through a type
//     parameter, an `any`, or encoding/json: JUDGED when the batch is sent;
//   - an interface of the package whose implementation hides the call of
//     the library, or names it like a runner with the SQL at another
//     position: JUDGED;
//   - a registered builder's list changed at run time, a builder that
//     answers differently on a second call, a domain smaller than what
//     callers pass: JUDGED for what the tests run (the statement actually
//     sent is the one judged; the store's test also fails on a change to an
//     indexed builder's list written in the package);
//   - a statement sent by ANOTHER PACKAGE'S code handed the store's pool:
//     JUDGED when a DB-tier test of any package runs it (the judge is on
//     every such package's pools); the static test of the other packages
//     also reads their constants and fails on an unread argument that is
//     not on its reviewed list;
//   - a pgx.QueryRewriter behind an `any`: NOT JUDGED AS TEXT (the tracer
//     is shown the text before the rewriter runs), but the judge HOLDS
//     AGAINST any production statement sent with a rewriter among its
//     arguments, whatever its static type (the library's own
//     pgx.NamedArgs included: nothing in the module uses named arguments);
//   - a library entry point outside pgx, pgxpool, pgconn and database/sql
//     (pgproto3's frontend, a raw net.Conn write, sql.Conn.Raw, a second
//     connection opened with pgx.Connect): NOT JUDGED, the tracer is not on
//     that path. ServerConnectionBudget is the store's one such connection,
//     and runs SHOW;
//   - unsafe, go:linkname, cgo, assembly, a file behind a build tag the
//     test process is not built with: NOT JUDGED.
