// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

// WHAT THE WRITE-POLICY CHECK GUARANTEES (0.29.212), and what it does not.
//
// The check is three steps, each of which fails closed.
//
// 1. FIND (typed.go, carriers.go). The package is type-checked. An SQL
//    argument is the argument at the SQL position of a call whose callee is,
//    BY OBJECT, a function of the database library classified as running
//    SQL (every member of pgx, pgxpool, pgconn and database/sql that takes
//    text is classified, and a test fails on one that is not), a method of
//    an interface with such a name and position, or a function of the
//    package that hands its own parameter to one; or a value given to a
//    library field that holds SQL. Every other reference to something that
//    runs SQL, and every door by which a library type that carries SQL
//    without text at the call can arrive or leave, is a Reference.
//
// 2. READ (strict.go). There is no evaluator of Go. An argument is read
//    only as one of:
//      - CONSTANT: the compiler's string constant, of the basic type string;
//      - PURE: one of the listed standard-library functions (fmt.Sprintf,
//        strings.Join of a list of constants written in place, Repeat,
//        Replace, ReplaceAll, TrimSpace, TrimPrefix, TrimSuffix), or +, on
//        such constants, evaluated by calling that function; or a string
//        variable whose declaration is the only thing in the package that
//        gives it a value, read as that declaration's value;
//      - BUILT: such text joined with a call of one of the package's own
//        functions on string constants, or with an embedded file's
//        variable, evaluated by RUNNING the real function, which the
//        package's test supplies (Env);
//      - NUMBERED: fixed text with integers in it (see below);
//      - FROM CALLERS: fixed text whose gaps are a function's parameters,
//        filled with what every caller passes (see below);
//      - QUEUED: the SQL of a statement read back from a batch's own
//        queue to copy it, which was judged where it was queued;
//      - PARAMETER: the function's own parameter, unexported, never
//        assigned and never addressed, read at each of its callers.
//    Everything else is a HOLE, and a hole fails until it is registered
//    (Site). EXECUTED: the argument is exactly a call of the store's
//    builder (or a local taken once from one), and the check RUNS the real
//    builder for every input: the whole of a finite domain (the index of
//    each member of a fixed list, every key of a map, every combination of
//    bools, the constants every caller passes), with a test showing an
//    input outside it gives no statement; or, OPEN, representative inputs,
//    where the builder may put what varies into the text only through
//    pgx.Identifier or a registered quoting helper (checked by object).
//    FROZEN: nothing was run; a digest of the source the text is made of
//    makes any change a failure, and an argument that can write must give
//    the policy check something to judge.
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
//      - a statement the parser refuses, or that holds a statement kind or
//        an ALTER TABLE action this package has not classified (the lists
//        are the parser's whole sets for its grammar version; a test fails
//        on a parser of another major version until they are redone);
//      - a body in a language that is not read, EXECUTE of text made at
//        run time, an assignment of the whole NEW row;
//      - a write whose target names no schema and assigns a registered
//        column's name; a write through a view over a registered table;
//      - a value of a registered column that does not match its policy.
//    The grammar is PostgreSQL 17's. A statement that needs a later
//    grammar is refused by the parser, and so is a violation, never a pass.
//
// THE TWO READINGS THAT FILL A GAP, exactly.
//
// NUMBERED (strict.go, numberedTexts). The argument is fmt.Sprintf of a
// readable format, or a + of readable text, and every operand that is not
// readable is of a BASIC integer type (a local given once by such an
// expression is followed into it). The statement is made by the real
// fmt.Sprintf for ten numbers, and read only if PostgreSQL's parser reads
// each, and to the same parse tree but for the digits (treeShape: a number
// is a number of the same sign, a name or a string the same but for its
// runs of digits); for a SIGNED type, for negative numbers too.
//   PROVES: whatever number the gap holds at run time, the statement is the
//   one that was judged: the number is a number, or the digits of a name or
//   of a string, and nothing else.
//   ASSUMES: that fmt.Sprintf writes a basic integer the same way for
//   every value of one sign, the verb deciding and not the value; and that
//   ten probes (with the code points of a quote, a semicolon, a backslash,
//   a minus and a slash among them) show every shape a verb gives.
//   REFUSES: a declared integer type (it may print through String or
//   Format); a verb that writes the number as a character or in letters
//   (%c, %x); text, in any gap; a signed number wherever a minus sign
//   changes the tree (LIMIT -1 is another constant, a name's digits are cut
//   from the name, a second minus begins a comment); a statement the
//   parser does not read whole.
//
// FROM CALLERS (strict.go, callerTuples). The argument is fmt.Sprintf of a
// readable format, or a + of readable text, and every other operand is a
// parameter of the function it stands in (a declaration, or a literal held
// in one variable that is given nothing else). The statement is made by
// the real fmt.Sprintf with what each caller passes. The same search gives
// a registered builder its inputs (Site.FromCallers).
//   PROVES: the statements judged are every statement the argument can be,
//   because every value the parameter can hold is a constant written at a
//   call the reading found.
//   ASSUMES: that the package's non-test files are all the code that can
//   call an unexported function of it (a _test file's call is not read,
//   and is not a caller); no reflection, unsafe or linkname.
//   REFUSES: a parameter that is assigned or has its address taken
//   anywhere, a closure that captures it included; an exported function,
//   one used as a value (a function value, a method value), a method an
//   interface of the package can call; a caller that passes anything the
//   reading does not read (a variable, a field, a call), at any depth; a
//   function with no caller in the files read; a function that is its own
//   caller, whose constants are never reached (the search stops four calls
//   deep). len(p) of a parameter is the number of elements of the list
//   literal each caller writes out, and nothing else.
//
// WHAT IS NOT GUARANTEED:
//   - A FROZEN statement is a person's reading plus fixed text judged with
//     representative values; its run-time parts are not checked.
//   - An Env function is trusted to depend on nothing but its arguments.
//   - The policy shapes of policy.go are a closed list: a value of another
//     shape fails, whatever it computes. "Stored" is any column reference
//     ending in the column's name that EXCLUDED does not qualify, so a
//     same-named column of another table in a subquery counts as stored.
//   - The parser is PostgreSQL 17's and the fleet runs 18: what 18 reads
//     and 17 does not is refused, and what both read is read as 17 does.
//   - A trigger function's assignment to NEW.col is reported for every
//     registered column of that name, since the function does not say
//     which table its trigger is on.
//   - Text made in the database (EXECUTE of a computed string in a body) is
//     reported unread, never judged.
//   - The other source tripwires of sqlscan.go (Statements, FindWrites,
//     WritesColumn, SetExprs) still match text with patterns. The policy
//     check does not use them.
//   - Packages other than the one read, reflection, unsafe and linkname.
