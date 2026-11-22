// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

// WHAT THE WRITE-POLICY CHECKS ARE FOR (0.29.213; the judge's reach,
// 0.29.214; its rule for whose statement it is and re-executed test
// binaries, 0.29.215), and what they are not.
//
// There are two checks, and they stand on different ground.
//
// THE RUN-TIME JUDGE (internal/testdb/writejudge) DECIDES. It reads no Go.
// It is a tracer that the DB-tier test harness (testdb.Main) puts, while
// ANY DB-tier package's tests run, on every pool and every connection the
// module opens: all of them are opened in ONE file (internal/db/open.go,
// which hands each configuration to the seam; scripts/pool_openers_test.go
// fails on a library constructor anywhere else in non-test code, and in a
// DB-tier package's test files outside a reviewed list of one; the same
// test fails on any non-test code that gives a Tracer field a value
// outside that file and the store's own logging tracer, and the opener
// puts the seam back after a caller's BeforeConnect hook has run). It is
// handed the text of every statement the library is given: Query, QueryRow
// and Exec, each statement of a batch when the batch is sent, the table
// and columns of a CopyFrom, the text of a Prepare.
//
// A test binary that a test RE-EXECUTES (os.Args[0] run again as a child,
// for a subprocess under test) is judged too (0.29.215): the child's
// harness installs a judge that appends each thing it records, as it
// records it, to a file in a directory its ancestor named in the
// environment, and the ancestor's verdict reads those files with its own.
// A child that ends by os.Exit or is killed has handed over everything it
// recorded before that: there is one write per statement and no buffer.
//
// Whose statement it is (writejudge.Origin, 0.29.215): PRODUCTION's
// whenever a frame of production code stands anywhere on the stack. The
// rule reads the stack's SHAPE, not who called the library: only test
// frames of this module, a test sent it (a fixture's, not judged);
// production code nearest the library, production sent it; test code
// nearest the library with production code above it (a test's closure run
// by a production retry or transaction helper, a test's stand-in for a
// transaction that hands on what production gave it, in any spelling: a
// method, a closure, a free function, a generic type), production's as
// well, judged at the production frames, and the finding names the test
// code it came through. A test that really does send its OWN text through
// a production helper is listed, by file and declaration and with its
// reason, in writejudge.FixtureSenders; the list is empty, and a test
// holds it so (an entry needs its reviewed reason there, and must name a
// test that exists). A stack with NO frame of this module
// (`go pool.Exec(ctx, sql)`) is production's. A production statement is
// parsed and judged (step 3 below: the same CheckIn, with the one registry
// of policies and Exceptions, in the test database's own catalog, to which
// what fixtures AND production created while the tests ran is added as
// what it is, each in the order it was sent), and a violation fails the
// package after its tests.
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
// held complete by a second count of the store's calls by name alone, and
// no package the store imports sends SQL of its own (a forwarding runner
// there would be a site this list cannot see; a test fails on the first).
//
// That requirement is PER CALL SITE, and a site is reached by ANY one
// statement sent through it. A site that a builder, a list of steps or a
// loop feeds can send many statements, and "no site unreached" does not
// say each of them ran: the others were judged by the static reader from
// their text alone. (0.29.215 kept a list of the texts never sent; it is
// gone by the operator's decision of 2026-11-21. It excused a wrong sort
// key with "delete its line". The three listings whose variants made up
// most of it are tested variant by variant instead:
// internal/db/listing_orders_db_test.go.)
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
//        nobody judged (0.29.214). Since 0.29.215 it may hold only ONE
//        such condition (the cases of one switch count as one): both
//        sides of each of two say nothing of the two together. And it
//        may not pick its text without a condition: a `for cond {}` on a
//        parameter, an index or map lookup by a parameter or by a call
//        handed one, and, inside a loop a parameter bounds (how a count
//        repeats a fragment), a return, break or goto, or a condition on
//        the loop's variable other than a comparison with the constant 0
//        (the separator before all but the first), each fail. A builder
//        that needs one of these is given a finite domain, where every
//        input is run. A condition in a function the builder CALLS on a
//        value it was handed is not followed: that remains the
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
//      - INCOMING: EXCLUDED.col, in ON CONFLICT DO UPDATE only, and only
//        when the INSERT's own source is made of incoming values (0.29.215:
//        INSERT ... SELECT FROM the target hands one row's stored values
//        to another as EXCLUDED). (A $N
//        placeholder is incoming too, and so is ANY column of an INCOMING
//        FROM ITEM, qualified by the item's name: unnest of placeholders, a
//        VALUES list of placeholders, a sub-select whose every output is
//        one and that names no relation at all: the batch form, UPDATE t
//        SET c = COALESCE(v.c, t.c) FROM unnest($1::..[], $2::..[]) AS
//        v(id, c). WITH ORDINALITY's counter is one of its columns: the
//        position of a value handed in, which reads no stored value. A
//        LATERAL sub-select is such an item only if it QUALIFIES every
//        column it hands on: a bare name in it could be a column of the
//        item to its left. A placeholder may carry one cast, may stand in
//        TRIM / btrim of one argument, and that TRIM may carry one cast
//        more: NULLIF(TRIM($1::text)::varchar(3), '').)
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
//    above.
//
//    WHAT A VALUE MAY BE MADE OF (policy.go, Write.notIncoming; 0.29.215).
//    AlwaysRefresh says the value is never derived from the column's stored
//    value, of any row. Rounds 6 to 8 each found a route by which a stored
//    value arrived that the rule did not list (a FROM item; a LATERAL
//    sibling, a WITH query's column list, INSERT ... SELECT from the target
//    read back as EXCLUDED, a bare name in a sub-select). The rule no
//    longer lists routes. It says what the value may be made of, and
//    anything else fails. Every leaf of the value is one of:
//      - a placeholder (outside a body), a constant, DEFAULT, or a function
//        with no schema (or pg_catalog's) and operators over such leaves;
//      - EXCLUDED.<any column>, and EXCLUDED's whole row ((excluded).col,
//        row_to_json(excluded)), when the INSERT's source is incoming: one
//        question, asked of both (0.29.216; the whole row was not asked);
//      - a column QUALIFIED by a CLEAN item of the FROM list it stands in;
//      - ANOTHER column of the row being written (SET ar = lower(fe)): a
//        stored value, but not this column's. Bare, only when every item
//        of the write's FROM list is clean, since the name could be an
//        item's column.
//    A CLEAN item reads no stored value of the target's:
//      - an incoming batch (above);
//      - a schema-qualified BASE TABLE the catalog knows that is not the
//        target, nor a partition, child or parent of it, nor a table made
//        from a query (CREATE TABLE ... AS, SELECT ... INTO, under whatever
//        name it has since been given: what it holds is what the query
//        read, and that is not followed), nor ANY table the write's own
//        text makes, temporary or not (the text may have filled it from
//        anything; the pass for a session table a text makes itself is for
//        WRITES of it); a view or materialized view over such tables only;
//      - a WITH query of the statement or a sub-select written in place,
//        with an alias or with none, every item of which is clean and every leaf of which is incoming
//        by this same list (so a column list, AS old(a, b), renames
//        nothing into an incoming value: a WITH that reads the target is
//        not clean, whatever it calls its columns);
//      - pg_temp.<name> for a declared session table.
//    NOT clean: the target under another alias; a relation the catalog
//    does not know; a name with no schema that is no WITH query (a view
//    named without its schema, a session table: which relation it is, or
//    what it was filled from, the text does not say); a function in FROM
//    that is not unnest of placeholders; a data-modifying WITH; and a FROM
//    item of ANY kind the reading does not name: JSON_TABLE, XMLTABLE,
//    TABLESAMPLE, whatever a later grammar adds (their arguments are not
//    walked: the item is not clean, whatever it is given). The kinds of
//    FROM item are one list whose last arm refuses (0.29.216: the last arm
//    took an unknown kind for a join's alias, which reads nothing itself,
//    and a sub-select with no alias was no item at all). And when
//    ANY item of the write's own FROM list is not clean, no column of that
//    list is taken, because a LATERAL item or a bare name can carry a
//    sibling's value.
//    Refused as leaves: the column's own name, bare or qualified by the
//    target; the target's whole row; a variable or parameter of a body, in
//    every form it takes (a PL/pgSQL variable, an SQL function's parameter
//    by name, and a placeholder: $1 in a body is the function's parameter
//    or what EXECUTE ... USING gives it; 0.29.216, the last two were
//    taken; and a FROM item made of such placeholders, unnest($1) or
//    VALUES ($1) in a body, 0.29.217, which was taken as a batch); a
//    function of another schema (what it reads is not known); a
//    bare name where no item can give it; and an expression of any KIND
//    that is not on the list of kinds whose parts are walked
//    (policy.go, valueKinds: the SQL/JSON constructors and queries,
//    XML expressions, GROUPING, whatever a later grammar adds).
//    What a query SELECTS is its value; its conditions (WHERE, ON, GROUP
//    BY, HAVING, ORDER BY, LIMIT) pick rows and are not walked: a condition
//    may read anything, the stored value included, as the write's own
//    WHERE may. A relation a JOIN names is an item, whatever its ON says.
//    The other policies' shapes name two kinds of leaf, the row's own
//    STORED value and an INCOMING one; what a fill fills an empty column
//    WITH is incoming by this list too (0.29.214 let it come from anywhere:
//    another row of the table, read through a self-join, is not incoming).
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
//        and DROP ... CASCADE of anything a column can depend on (a type, a
//        domain, a function, procedure or aggregate, an extension, a
//        collation, an operator, a cast, a sequence, a language) are held
//        against AnyRegisteredTable always: which columns they drop, the
//        text does not say.
//      - A registered table's FAMILY (0.29.215). A partition or inheritance
//        child of a registered table holds rows that are that table's, and
//        a parent of it is read and written through to it. A write of such
//        a table's rows, DROP or TRUNCATE of it (DROP of a parent CASCADE
//        drops the child; TRUNCATE of a parent empties it), a trigger or a
//        rule on it, an ALTER of it (a column's type, NO INHERIT), a rename
//        and a foreign key's action on it are held against the registered
//        table (child-target). (DROP TABLE x CASCADE of a table that is no
//        relative drops dependent views and constraints, and no row of
//        another table.)
//      - A SESSION TABLE the text made itself, earlier and at the same
//        level, is passed over for writes of its ROWS only, and only while
//        it is a plain table that still stands: not one made a child of
//        another table (INHERITS, ALTER ... INHERIT), not after the text's
//        COMMIT when it was made ON COMMIT DROP, not after a ROLLBACK, not
//        once renamed. A rename, a move or an ALTER of it is judged as any
//        other.
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
//    What an Exception does NOT pin (0.29.215, said plainly): the ARGUMENTS
//    the statement is sent with (its text is the same whatever is bound to
//    $1, and neither check sees them), and where INSIDE a body the
//    statement stands. It does say whether it stands in a body at all
//    (InBody): an Exception for a statement at the top of a text does not
//    excuse the same statement in a DO block, where an IF or a loop decides
//    when it runs, nor the reverse.
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
//     shape fails, whatever it computes. Every refusal of a value ends
//     with what to write instead (policy.go, WaysOut: one table, by
//     policy; the tests hold every refused correct statement of the
//     reviewers' files to it). Refused though correct, each with
//     its way out: a cast of the WHOLE value (cast the incoming operand:
//     COALESCE(col, $1::t)); a guard spelled another way, length(col) = 0
//     among them (write one of the four); the column's own name written
//     bare in a sub-select of an AlwaysRefresh value, though the
//     sub-select's relation may be what has a column of that name (qualify
//     it: MAX(o.ar)); an AlwaysRefresh value that reads the target table
//     again, for another column or a count (an item that reads the target
//     hands on nothing: take an Exception); a derived table that JOINs the
//     target only to filter (filter with EXISTS or IN in its WHERE, which
//     is a condition); a batch handed in as JSON (jsonb_to_recordset: use
//     unnest of arrays); a value read from a session table named with no
//     schema (use the batch itself as the FROM item, or pg_temp.<name>
//     for a table an earlier text of the session made, declared in
//     writejudge.TempTables); a variable, a parameter or a placeholder of
//     a body where an incoming value is wanted, the guarded fill of a
//     function's parameter among them (send the statement from Go with
//     placeholders, or UPDATE ... FROM the relation the loop reads); a
//     value read from a table made from a query, or from a table the same
//     text makes (hand the rows in as a batch).
//   - The catalog knows relations and foreign keys, not columns or their
//     types: which columns a view shows and under what names (so no
//     column of a view over the target is taken), which tables DROP ...
//     CASCADE reaches (so it is held against them all), what a function
//     READS (one with no schema, called in a value, is taken to read no
//     table: lower(), COALESCE; a function of another schema is refused;
//     so a USER'S function found through the search path is taken as
//     pg_catalog's would be, and so are an operator written
//     OPERATOR(schema.op), a cast to a user's type (which may run a
//     user's cast function), a view whose body calls a function of a
//     schema, and a GENERATED column computed from the registered column:
//     each can hand a stored value on, and none is seen),
//     what a function called by SELECT f() or CALL p() writes, what
//     an extension's script or ALTER TYPE ... ALTER ATTRIBUTE ... CASCADE
//     rewrites. What a trigger or a rule created OUTSIDE the statements
//     read does is not in any text it reads either.
//   - What a declared session table (pg_temp.<name>, writejudge.TempTables)
//     HOLDS is not followed: declaring one says a person read what fills
//     it. So is what a table held BEFORE the texts the catalog read: a base
//     table the schema creates is taken not to be a copy of another.
//   - A value's CONDITIONS are not judged (which rows a sub-select picks
//     may depend on the stored value), and neither are the arguments a
//     statement is sent with.
//   - The parser is PostgreSQL 17's and the fleet runs 18: what 18 reads
//     and 17 does not is refused, and what both read is read as 17 does.
//     It is WebAssembly, and a statement nested deeply enough (thousands
//     of concatenated terms or nested sub-selects) overflows its stack:
//     that statement is reported unread, and so is EVERY statement after
//     it in the process, since the parser does not work again (pgread.go,
//     parserCall). For some shapes the parser does not come back at all
//     instead: nothing is reported and the run ends at the test timeout
//     (observed with arms of UNION ALL: 6,000 are read; 12,000 come back
//     from the parser and are reported unread, the tree being too deep,
//     with the parser still usable; 20,000 do not come back). Either way
//     the check fails; in neither does a statement pass.
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
