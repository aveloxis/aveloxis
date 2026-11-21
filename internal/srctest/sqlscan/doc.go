// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

// WHAT THE WRITE-POLICY CHECKS ARE FOR (0.29.213; the judge's reach and its
// rule for whose statement it is, 0.29.214), and what they are not.
//
// There are two checks, and they stand on different ground.
//
// THE RUN-TIME JUDGE (internal/testdb/writejudge) DECIDES. It reads no Go.
// It is a tracer that the DB-tier test harness (testdb.Main) puts, while
// ANY DB-tier package's tests run, on every pool and every connection the
// module opens: all of them are opened in ONE file (internal/db/open.go,
// which hands each configuration to the seam; scripts/pool_openers_test.go
// fails on a library constructor anywhere else in non-test code, and in a
// DB-tier package's test files outside a reviewed list of one). It is
// handed the text of every statement the library is given: Query, QueryRow
// and Exec, each statement of a batch when the batch is sent, the table
// and columns of a CopyFrom, the text of a Prepare.
//
// Whose statement it is (writejudge.Origin): PRODUCTION's unless a test
// wrote it. The module's frames of the stack are read from the library
// outwards; a _test.go METHOD named as a library function that takes SQL
// (a test's stand-in for a transaction that hands on what it was given) is
// passed over; the first frame left decides. In a _test.go file: a
// fixture's, not judged (also a test's own closure run by a production
// retry or transaction helper: the text is the test's). Otherwise, and
// when the stack holds NO frame of this module (`go pool.Exec(ctx, sql)`),
// it is production's: parsed and judged (step 3 below: the same CheckIn,
// with the one registry of policies and Exceptions, in the test database's
// own catalog, to which what fixtures AND production created while the
// tests ran is added as what it is), and a violation fails the package
// after its tests.
//
// What the judge sees is exactly what ran, so no way of building a string
// in Go can mislead it; what it does not see is a statement no test runs.
// So there is a COVERAGE REQUIREMENT beside it: every statement site of
// internal/db (RunnerCalls, sites.go: a call of a library function that
// takes SQL, and a call of a method of one of the store's own interfaces
// that is such a function in name and signature) must have had a statement
// sent through it by some DB-tier package when the whole suite has run
// (the closing suite, scripts/skip-census.sh, checks the union). The list
// of sites no test reaches (internal/db/testdata/unexecuted_statement_sites.txt)
// is EMPTY since 0.29.214 and only shrinks; the list of sites itself is
// held complete by a second count of the store's calls by name alone.
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
//        or bool type each fail. And what an open builder RETURNS may
//        depend on a condition on its input (an if, a switch) only where
//        the condition compares a parameter with a constant AND the
//        registered inputs take both sides of it: a builder that chose
//        another statement for an input nobody registered returned text
//        nobody judged (0.29.214). A condition in a function the builder
//        CALLS on a value it was handed is not followed: that remains the
//        registration's word.
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
//      - INCOMING: EXCLUDED.col, in ON CONFLICT DO UPDATE only. (A $N
//        placeholder is incoming too, and so is ANY column of an INCOMING
//        FROM ITEM, qualified by the item's name: unnest of placeholders, a
//        VALUES list of placeholders, a sub-select whose every output is
//        one and that names no relation at all: the batch form, UPDATE t
//        SET c = COALESCE(v.c, t.c) FROM unnest($1::..[], $2::..[]) AS
//        v(id, c). An incoming value may carry one cast, and may stand in
//        TRIM / btrim of one argument.)
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
//    and nothing else (TRIM of it, a cast of it, another row's value of it
//    are not it); its "incoming" operand is one of the incoming values
//    above. "The value does not read the stored value" (the incoming side
//    of a fill) fails on a STORED or an UNRESOLVED reference at any depth,
//    on the target's whole row (r, r.*, to_jsonb(r), (r).col), and on the
//    target TABLE named again in a sub-select under any alias.
//
//    WHAT A FROM ITEM READS (policy.go, Write.read; 0.29.214). AlwaysRefresh
//    asks more than the row's own value: NOTHING of the column's stored
//    value, of any row, by any route. Each FROM item (a MERGE's source
//    too) is followed to what it reads: the relation itself; for a view or
//    a materialized view, everything behind it by the catalog,
//    transitively; for the name of a WITH query of the statement, what that
//    query reads; for a sub-select, every relation named in it. An item
//    that reads the TARGET is the stored value under another name:
//      - the target table again (a self-join): its column of the name;
//      - a view over it, a join's alias, an item whose alias renames its
//        columns, a query that is not one plain SELECT (a set operation, a
//        WITH of its own, a data-modifying statement), a query that reads
//        the target other than as itself written in place, a query with
//        `*` or with an unnamed output that may hold the value: EVERY
//        column of the item;
//      - one plain SELECT over the target written in place: its outputs are
//        enumerated, and the columns that may hold the value are those
//        whose expression names a column of the name, a whole row, or a
//        query of its own. (SELECT repo_id, count(*) AS n FROM t GROUP BY
//        repo_id hands over nothing of it; SELECT repo_id, ar AS x FROM t
//        hands it over as x.)
//    A column of such an item in the value (qualified, or bare where no
//    sub-select shadows it), the item's whole row, and a sub-select of the
//    value that reads the target through a view or a WITH query, fail
//    AlwaysRefresh. Another COLUMN of the table is not this column's stored
//    value (SET ar = r2.fe is as SET ar = fe). For the other policies an
//    item that reads the target is what any FROM item is: not the row's
//    own stored value (so no shape's "stored"), and not incoming; a fill
//    may take what it fills an EMPTY column with from one, since an
//    established value is still never overwritten.
//
//    THE SHAPES, as they may be written (policy.go, matchPolicy). The fill:
//    COALESCE(NULLIF(stored, ''), v), COALESCE(stored, v), CASE WHEN <the
//    column is empty> THEN v ELSE stored END, or a bare incoming value
//    under a guard. The guard of a bare fill is one of four forms on a
//    STORED reference, as one of the conditions ANDed at the top of the
//    write's WHERE (Write.GuardsEmpty): col IS NULL; COALESCE(col, '') =
//    ''; NULLIF(col, '') IS NULL; (col IS NULL OR col = ''); an equality
//    either way round. GreatestNonNull: GREATEST(stored, incoming) in
//    either order (PostgreSQL's GREATEST ignores a NULL argument), or the
//    older pair of COALESCEs. KeepCarryOrNow's "now" is the transaction's
//    start in its three spellings, NOW(), CURRENT_TIMESTAMP,
//    transaction_timestamp(), and COALESCE(incoming, that);
//    clock_timestamp() and statement_timestamp() are other values. A part
//    of a column as a target (col[1], col.f) is a violation.
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
//        name are violations; so is a write of a partition or child of a
//        registered table. A TEMPORARY table is the text's own where the
//        text made it EARLIER, at the same level (the same body, or the top
//        of the text), and has not dropped it since: then its bare name, or
//        pg_temp.name, is it. A session table made by ANOTHER text (two
//        Exec calls) is declared by name (writejudge.TempTables,
//        Catalog.Temp) and written as pg_temp.name; an unqualified name
//        there is whatever the search path makes it, and is refused.
//      - Violations by kind, each of which an Exception must name, when the
//        table is registered: DROP TABLE, TRUNCATE, DROP SCHEMA of its
//        schema, ALTER SCHEMA ... RENAME of its schema or of another ONTO
//        its schema's name; RENAME TO and SET SCHEMA (and another relation
//        taking its name); ATTACH / DETACH PARTITION, INHERIT, CREATE TABLE
//        ... PARTITION OF / INHERITS it (named with or without its
//        schema); CREATE RULE on it; CREATE TRIGGER on it, WHATEVER
//        function it runs; a foreign key on a registered column whose
//        action writes it (ON UPDATE CASCADE / SET NULL / SET DEFAULT, ON
//        DELETE SET NULL / SET DEFAULT, the column list form included),
//        declared as a table constraint or on the column, in CREATE TABLE
//        or by ALTER TABLE ... ADD CONSTRAINT / ADD COLUMN; ALTER of the
//        column's type, expression or name, DROP of it, another column
//        renamed TO its name; COPY FROM of it.
//      - Statements that reach a table WITHOUT NAMING IT. TRUNCATE x
//        CASCADE is held against every registered table that references x
//        through a foreign key, or references one that does (the catalog
//        reads the foreign keys from DDL); where the catalog was only told
//        of the tables (Catalog.Table: a catalog made from a live database)
//        and not of their foreign keys (Catalog.Reference, Defined), it is
//        held against AnyRegisteredTable. DROP OWNED BY, REASSIGN OWNED BY
//        and DROP TYPE / DOMAIN ... CASCADE are held against
//        AnyRegisteredTable always. (DROP TABLE x CASCADE and ALTER TABLE x
//        DROP COLUMN c CASCADE drop dependent views and constraints, and no
//        row of another table: they are x's own violation, or none.)
//      - A trigger FUNCTION's body: NEW.col := v is a write of col (also
//        SELECT INTO, GET DIAGNOSTICS, FOR and FOREACH targets). Held
//        against it besides: any value given to OLD or to the whole NEW;
//        NEW or OLD used whole as a value (r := NEW, f(NEW), NEW.*); NEW or
//        OLD in a CALL; a RETURN of anything but NEW, OLD or NULL; a
//        variable named like a field of them.
//
//    AN EXCEPTION PINS ONE STATEMENT, WHOLE (0.29.214): its table, column,
//    kind, file and DECLARATION (the function the statement stands in; for
//    the run-time judge, any production function on the stack that sent
//    it), and Statement: the whole statement the violation stands in (the
//    top-level statement of the text, a WITH and everything under it
//    included; for a write in a body, the statement of the body), compared
//    as PostgreSQL's parse tree with the places removed. Its target, SET
//    list, FROM, WHERE, ON CONFLICT and RETURNING are all part of what is
//    compared: a statement whose WHERE was changed or dropped is another
//    statement, reported, and the Exception is then stale. For an
//    assignment, Fragment (the assigned expression, whole) says which
//    assignment of the statement is excused, and must be one of it. One
//    Exception excuses one statement: two statements need two, in one text
//    or across a declaration's texts; the same statement met again (a
//    builder that gives it for two inputs, a text judged each time it is
//    sent) is that statement. A builder whose outputs differ needs one
//    Exception for each output that needs excusing (the gone-clear
//    statements of writejudge's registry). Exception.Pins answers "does
//    this text hold the pinned statement" for a caller with only text.
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
//     shape fails, whatever it computes. Refused though correct, each with
//     its way out: a cast of the WHOLE value (cast the incoming operand:
//     COALESCE(col, $1::t)); a guard spelled another way, length(col) = 0
//     among them (write one of the four); a bare name in a sub-select,
//     which counts as the stored value though the sub-select's own
//     relation may be what has the column (qualify it); a value that reads
//     the target table again for ANOTHER column in a sub-select (use a
//     FROM item whose outputs are named, or take an Exception).
//   - The catalog knows relations and foreign keys, not columns or their
//     types: which columns a view shows and under what names (so every
//     column of a view over the target counts as the stored value), which
//     tables DROP TYPE ... CASCADE reaches (so it is held against them
//     all), what a function called by SELECT f() or CALL p() writes, what
//     an extension's script or ALTER TYPE ... ALTER ATTRIBUTE ... CASCADE
//     rewrites. What a trigger or a rule created OUTSIDE the statements
//     read does is not in any text it reads either.
//   - A relation in FROM that the catalog does not know (a temporary table
//     of another text) is taken to read no registered table: the catalog
//     holds every view of the schema, and a relation outside it cannot be
//     one of them.
//   - What a PL/pgSQL variable HOLDS is not followed: a record filled from
//     the target table by a loop's query is a variable, and its field is
//     not the stored value to this reader.
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
//   - a statement sent by ANOTHER PACKAGE'S code, on the store's pool or on
//     a pool of its own: JUDGED when a DB-tier test of any package runs it
//     (the judge is on every pool and connection opened through
//     internal/db's openers, and non-test code opens them nowhere else);
//     the static test of the other packages also reads their constants and
//     fails on an unread argument that is not on its reviewed list;
//   - a pgx.QueryRewriter behind an `any`: NOT JUDGED AS TEXT (the tracer
//     is shown the text before the rewriter runs), but the judge HOLDS
//     AGAINST any production statement sent with a rewriter among its
//     arguments, whatever its static type (the library's own
//     pgx.NamedArgs included: nothing in the module uses named arguments);
//   - what is written PAST pgx's own Query, Exec, SendBatch, CopyFrom and
//     Prepare: pgconn's Exec, ExecParams and Prepare on a connection taken
//     out of a pool (conn.Conn().PgConn(): the tracer is pgx's, and pgconn
//     calls none), pgproto3's frontend, a raw net.Conn write, sql.Conn.Raw:
//     NOT JUDGED, the tracer is not on that path. (The static reader does
//     read the arguments of pgconn's functions, and such a call in the
//     store would be a statement site that no test could reach.) A second
//     connection or pool is no longer such a path (0.29.214): each is
//     opened through internal/db's openers and carries the judge, the
//     connection-budget probe's included;
//   - unsafe, go:linkname, cgo, assembly, a file behind a build tag the
//     test process is not built with: NOT JUDGED.
