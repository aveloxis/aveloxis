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
//      - NUMBERED: fixed text with integers in it, made by the real
//        fmt.Sprintf for several numbers and accepted only if the number
//        never changes the statement's tokens;
//      - FROM CALLERS: fixed text whose gaps are parameters nothing
//        changes, made by the real fmt.Sprintf with what every caller
//        passes, when every caller (found by object, to any depth) passes
//        text the reading reads;
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
// 3. JUDGE (lex.go, parse.go, policy.go). Each statement is tokenised by
//    PostgreSQL's lexical rules and its writes are read from the tokens. A
//    statement not read to its end, a write whose target names no schema
//    and assigns a registered column's name, a write through a view over a
//    registered table, and a value of a registered column that does not
//    match its policy are violations.
//
// WHAT IS NOT GUARANTEED:
//   - A FROZEN statement is a person's reading plus fixed text judged with
//     representative values; its run-time parts are not checked.
//   - An Env function is trusted to depend on nothing but its arguments.
//   - The value matchers of policy.go read ONE extracted value with
//     patterns, as before.
//   - parse.go is not a grammar: it reads each write's target, SET list and
//     WHERE, and steps over the rest.
//   - Text made in the database (EXECUTE of a computed string in a body) is
//     reported unread, never judged.
//   - Packages other than the one read, reflection, unsafe and linkname.
