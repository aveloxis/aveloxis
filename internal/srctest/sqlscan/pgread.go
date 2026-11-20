// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	pgparser "github.com/wasilibs/go-pgquery/parser"
)

// The statement reader of the write-policy check (0.29.212). It does not
// read SQL: PostgreSQL's own parser does (libpg_query, the server's parser
// compiled to WebAssembly, run in the test process; no cgo), and this file
// reads the parse tree it returns. What a name is, where a string or a
// comment ends, what a statement's target and assignments are: all of that
// is the server's reading.
//
// It denies by default:
//
//   - text the parser refuses is not read, and ParseWrites fails;
//   - every statement node in the tree, at any depth, must be of a kind
//     this file has classified (stmtKinds), or ParseWrites fails; so must
//     every action of an ALTER TABLE (alterKinds);
//   - the body of a DO block or of CREATE FUNCTION / PROCEDURE is read too:
//     an SQL body by the same parser, a PL/pgSQL body by the server's
//     PL/pgSQL parser, each statement and expression of which is then read
//     in turn. A body in another language fails. EXECUTE of one string
//     constant is read as that statement, and of anything else fails (the
//     text is made at run time).
//
// The grammar is PostgreSQL 17's (the parser's own version; ParserVersion
// says which). A statement that needs a later grammar fails, as any text
// the parser refuses does.

// jnode is one object of the parse tree as the parser's JSON gives it.
type jnode = map[string]any

// Write is one place a statement changes rows or a table's stored values.
type Write struct {
	// Kind: "update", "conflict-update" (INSERT ... ON CONFLICT DO UPDATE),
	// "merge-update", "insert", "delete", "merge", "truncate", "copy",
	// "alter", "drop" (DROP TABLE), "trigger-assign" (NEW.col := v in a
	// trigger's body, whose table nobody can name: Table is "new").
	Kind string
	// Schema and Table name the target. Schema is "" when the statement
	// does not say one: which table that is depends on the session's
	// search_path, which no reader of text can know.
	Schema, Table string
	// Alias is the target's alias, "" when it has none.
	Alias string
	// Sets are the assignments of an update's SET list.
	Sets []SetItem
	// Alters are the actions of ALTER TABLE that change or destroy a
	// column's stored values.
	Alters []AlterAction
	// CopyColumns are COPY ... FROM's columns; CopyAll when it names none.
	CopyColumns []string
	CopyAll     bool
	// InsertColumns are the columns an INSERT (or a MERGE's INSERT arm)
	// lists; InsertAll when it lists none, and so writes every column.
	InsertColumns []string
	InsertAll     bool
	// where is the write's own WHERE, as the parser read it.
	where any
	loc   int
}

// SetItem is one assignment.
type SetItem struct {
	// Column is the column assigned. For SET (a, b) = ..., one item per
	// column, Multi set, and Expr the whole right side.
	Column string
	// Expr is the value, written out from the tree for a person to read
	// (render): it is not the source text, and nothing is judged on it.
	Expr  string
	Multi bool
	// val is the value's tree, which the policy is judged on.
	val any
}

// unreadValue is the Expr of an assignment whose value the body gives in a
// way that is not an expression to read (SELECT ... INTO NEW.col, a loop's
// variable, GET DIAGNOSTICS): val is nil, and no policy passes it.
const unreadValue = "(a value the body gives in a way that is not read)"

// AlterAction is one value-changing action on a column.
type AlterAction struct {
	Column string
	// Action: "type" (ALTER [COLUMN] c [SET DATA] TYPE, with or without
	// USING: every stored value is rewritten), "expression" (ALTER COLUMN c
	// SET EXPRESSION: a generated column recomputed), "drop" (DROP [COLUMN]
	// c), "rename" (RENAME [COLUMN] c TO).
	Action string
}

// Qualified is "schema.table", or "" when the target names no schema.
func (w Write) Qualified() string {
	if w.Schema == "" {
		return ""
	}
	return w.Schema + "." + w.Table
}

// Statement kinds. Every node of the parse tree whose kind ends in "Stmt"
// is looked up here, at any depth; one that is not listed fails the
// reading. The list is the parser's whole set for the grammar version
// parserGrammar names (TestEveryStatementKindOfTheParserIsClassified holds
// the version and the list together: a parser of another version fails it
// until each kind has been looked at again).
const (
	// kindReads: this file reads the kind's writes (visit).
	kindReads = iota + 1
	// kindPasses: the kind changes no stored value of an existing table's
	// column, or holds its statements as nodes this file reads where they
	// stand (EXPLAIN, PREPARE, a rule's actions, CREATE TABLE AS).
	kindPasses
	// kindRefused: the parser never gives this kind for statement text; a
	// tree that holds one is not what this file takes it for.
	kindRefused
)

// parserGrammar is the PostgreSQL major version whose grammar stmtKinds and
// alterKinds were classified for.
const parserGrammar = 17

var stmtKinds = map[string]int{
	"UpdateStmt": kindReads, "InsertStmt": kindReads, "DeleteStmt": kindReads, "MergeStmt": kindReads,
	"TruncateStmt": kindReads, "CopyStmt": kindReads, "AlterTableStmt": kindReads, "RenameStmt": kindReads,
	"DropStmt": kindReads, "DoStmt": kindReads, "CreateFunctionStmt": kindReads, "ViewStmt": kindReads,

	"RawStmt": kindRefused, "PLAssignStmt": kindRefused, "SetOperationStmt": kindRefused,

	"AlterCollationStmt": kindPasses, "AlterDatabaseRefreshCollStmt": kindPasses, "AlterDatabaseSetStmt": kindPasses,
	"AlterDatabaseStmt": kindPasses, "AlterDefaultPrivilegesStmt": kindPasses, "AlterDomainStmt": kindPasses,
	"AlterEnumStmt": kindPasses, "AlterEventTrigStmt": kindPasses, "AlterExtensionContentsStmt": kindPasses,
	"AlterExtensionStmt": kindPasses, "AlterFdwStmt": kindPasses, "AlterForeignServerStmt": kindPasses,
	"AlterFunctionStmt": kindPasses, "AlterObjectDependsStmt": kindPasses, "AlterObjectSchemaStmt": kindPasses,
	"AlterOpFamilyStmt": kindPasses, "AlterOperatorStmt": kindPasses, "AlterOwnerStmt": kindPasses,
	"AlterPolicyStmt": kindPasses, "AlterPublicationStmt": kindPasses, "AlterRoleSetStmt": kindPasses,
	"AlterRoleStmt": kindPasses, "AlterSeqStmt": kindPasses, "AlterStatsStmt": kindPasses,
	"AlterSubscriptionStmt": kindPasses, "AlterSystemStmt": kindPasses, "AlterTableMoveAllStmt": kindPasses,
	"AlterTableSpaceOptionsStmt": kindPasses, "AlterTSConfigurationStmt": kindPasses, "AlterTSDictionaryStmt": kindPasses,
	"AlterTypeStmt": kindPasses, "AlterUserMappingStmt": kindPasses, "CallStmt": kindPasses, "CheckPointStmt": kindPasses,
	"ClosePortalStmt": kindPasses, "ClusterStmt": kindPasses, "CommentStmt": kindPasses, "CompositeTypeStmt": kindPasses,
	"ConstraintsSetStmt": kindPasses, "CreateAmStmt": kindPasses, "CreateCastStmt": kindPasses,
	"CreateConversionStmt": kindPasses, "CreateDomainStmt": kindPasses, "CreateEnumStmt": kindPasses,
	"CreateEventTrigStmt": kindPasses, "CreateExtensionStmt": kindPasses, "CreateFdwStmt": kindPasses,
	"CreateForeignServerStmt": kindPasses, "CreateForeignTableStmt": kindPasses, "CreateOpClassStmt": kindPasses,
	"CreateOpFamilyStmt": kindPasses, "CreatePLangStmt": kindPasses, "CreatePolicyStmt": kindPasses,
	"CreatePublicationStmt": kindPasses, "CreateRangeStmt": kindPasses, "CreateRoleStmt": kindPasses,
	"CreateSchemaStmt": kindPasses, "CreateSeqStmt": kindPasses, "CreateStatsStmt": kindPasses, "CreateStmt": kindPasses,
	"CreateSubscriptionStmt": kindPasses, "CreateTableAsStmt": kindPasses, "CreateTableSpaceStmt": kindPasses,
	"CreateTransformStmt": kindPasses, "CreateTrigStmt": kindPasses, "CreateUserMappingStmt": kindPasses,
	"CreatedbStmt": kindPasses, "DeallocateStmt": kindPasses, "DeclareCursorStmt": kindPasses, "DefineStmt": kindPasses,
	"DiscardStmt": kindPasses, "DropOwnedStmt": kindPasses, "DropRoleStmt": kindPasses, "DropSubscriptionStmt": kindPasses,
	"DropTableSpaceStmt": kindPasses, "DropUserMappingStmt": kindPasses, "DropdbStmt": kindPasses, "ExecuteStmt": kindPasses,
	"ExplainStmt": kindPasses, "FetchStmt": kindPasses, "GrantRoleStmt": kindPasses, "GrantStmt": kindPasses,
	"ImportForeignSchemaStmt": kindPasses, "IndexStmt": kindPasses, "ListenStmt": kindPasses, "LoadStmt": kindPasses,
	"LockStmt": kindPasses, "NotifyStmt": kindPasses, "PrepareStmt": kindPasses, "ReassignOwnedStmt": kindPasses,
	"RefreshMatViewStmt": kindPasses, "ReindexStmt": kindPasses, "ReplicaIdentityStmt": kindPasses, "ReturnStmt": kindPasses,
	"RuleStmt": kindPasses, "SecLabelStmt": kindPasses, "SelectStmt": kindPasses, "TransactionStmt": kindPasses,
	"UnlistenStmt": kindPasses, "VacuumStmt": kindPasses, "VariableSetStmt": kindPasses, "VariableShowStmt": kindPasses,
}

// alterKinds: every action of ALTER TABLE. "" passes (the action changes
// no stored value of a column); "refuse" fails the reading (the action
// makes another table's rows this table's: no policy was written for
// that); anything else is the AlterAction recorded for the column named.
var alterKinds = map[string]string{
	"AT_AlterColumnType": "type", "AT_DropColumn": "drop", "AT_SetExpression": "expression",
	"AT_AttachPartition": "refuse", "AT_AddInherit": "refuse",

	"AT_AddColumn": "", "AT_AddColumnToView": "", "AT_AddConstraint": "", "AT_AddIdentity": "", "AT_AddIndex": "",
	"AT_AddIndexConstraint": "", "AT_AddOf": "", "AT_AlterColumnGenericOptions": "", "AT_AlterConstraint": "",
	"AT_ChangeOwner": "", "AT_CheckNotNull": "", "AT_ClusterOn": "", "AT_ColumnDefault": "", "AT_CookedColumnDefault": "",
	"AT_DetachPartition": "", "AT_DetachPartitionFinalize": "", "AT_DisableRowSecurity": "", "AT_DisableRule": "",
	"AT_DisableTrig": "", "AT_DisableTrigAll": "", "AT_DisableTrigUser": "", "AT_DropCluster": "", "AT_DropConstraint": "",
	"AT_DropExpression": "", "AT_DropIdentity": "", "AT_DropInherit": "", "AT_DropNotNull": "", "AT_DropOf": "",
	"AT_DropOids": "", "AT_EnableAlwaysRule": "", "AT_EnableAlwaysTrig": "", "AT_EnableReplicaRule": "",
	"AT_EnableReplicaTrig": "", "AT_EnableRowSecurity": "", "AT_EnableRule": "", "AT_EnableTrig": "", "AT_EnableTrigAll": "",
	"AT_EnableTrigUser": "", "AT_ForceRowSecurity": "", "AT_GenericOptions": "", "AT_NoForceRowSecurity": "",
	"AT_ReAddComment": "", "AT_ReAddConstraint": "", "AT_ReAddDomainConstraint": "", "AT_ReAddIndex": "",
	"AT_ReAddStatistics": "", "AT_ReplaceRelOptions": "", "AT_ReplicaIdentity": "", "AT_ResetOptions": "",
	"AT_ResetRelOptions": "", "AT_SetAccessMethod": "", "AT_SetCompression": "", "AT_SetIdentity": "", "AT_SetLogged": "",
	"AT_SetNotNull": "", "AT_SetOptions": "", "AT_SetRelOptions": "", "AT_SetStatistics": "", "AT_SetStorage": "",
	"AT_SetTableSpace": "", "AT_SetUnLogged": "", "AT_ValidateConstraint": "",
}

// maxBodyNesting: bodies inside bodies (a DO block that EXECUTEs a string
// that creates a function ...) are read this deep and no deeper.
const maxBodyNesting = 8

// parsed is one text as the parser read it.
type parsed struct {
	stmts   []any
	version int
	err     error
}

var (
	parseMu    sync.Mutex
	parseCache = map[string]parsed{}
)

// parseSQL is sql's parse tree: the "stmts" of the parser's JSON, each
// {"stmt": {Kind: {...}}, "stmt_location": n, "stmt_len": m}. The result
// is shared and must not be changed.
func parseSQL(sql string) parsed {
	parseMu.Lock()
	defer parseMu.Unlock()
	if p, ok := parseCache[sql]; ok {
		return p
	}
	var p parsed
	out, err := pgparser.ParseToJSON(sql)
	if err != nil {
		p.err = fmt.Errorf("PostgreSQL's parser does not read it: %w", err)
	} else {
		var tree struct {
			Version int   `json:"version"`
			Stmts   []any `json:"stmts"`
		}
		dec := json.NewDecoder(strings.NewReader(out))
		dec.UseNumber()
		if err := dec.Decode(&tree); err != nil {
			p.err = fmt.Errorf("the parser's tree is not read: %w", err)
		}
		p.stmts, p.version = tree.Stmts, tree.Version
	}
	parseCache[sql] = p
	return p
}

// ParserVersion is the PostgreSQL version whose grammar the parser reads
// (170007 for 17.7), or an error when the parser does not run.
func ParserVersion() (int, error) {
	p := parseSQL("SELECT 1")
	return p.version, p.err
}

// ParseWrites is every write of one statement's text (or of several
// statements'), or an error when the text is not read to its end.
func ParseWrites(sql string) ([]Write, error) {
	return readWrites(sql, 0)
}

func readWrites(sql string, nesting int) ([]Write, error) {
	if nesting > maxBodyNesting {
		return nil, fmt.Errorf("bodies nested more than %d deep are not read", maxBodyNesting)
	}
	p := parseSQL(sql)
	if p.err != nil {
		return nil, p.err
	}
	r := &reader{sql: sql, nesting: nesting}
	for _, raw := range p.stmts {
		rs, _ := raw.(jnode)
		if kind, _ := wrapped(rs["stmt"]); kind == "" {
			return nil, fmt.Errorf("a statement the parser gave no kind for")
		}
		from, n := jint(rs["stmt_location"]), jint(rs["stmt_len"])
		if from < 0 || from > len(sql) {
			return nil, fmt.Errorf("a statement placed outside its text")
		}
		r.text = sql[from:]
		if n > 0 && from+n <= len(sql) {
			r.text = sql[from : from+n]
		}
		r.base = from
		r.walk(rs["stmt"])
		if r.err != nil {
			return nil, r.err
		}
	}
	sort.SliceStable(r.out, func(i, j int) bool { return r.out[i].loc < r.out[j].loc })
	return r.out, nil
}

type reader struct {
	sql     string
	nesting int
	// text and base: the top-level statement being read, and where it
	// begins in sql.
	text string
	base int
	out  []Write
	err  error
}

func (r *reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf(format, args...)
	}
}

// wrapped reads {"Kind": {...}}, the form every node of the tree has where
// its field can hold a node of any kind: one key, beginning with a capital,
// whose value is an object. ("", nil) for anything else (an object written
// in place, such as a write's "relation", has its fields as its keys, and
// those begin in lower case).
func wrapped(v any) (string, jnode) {
	m, ok := v.(jnode)
	if !ok || len(m) != 1 {
		return "", nil
	}
	for k, b := range m {
		if k == "" || k[0] < 'A' || k[0] > 'Z' {
			return "", nil
		}
		body, ok := b.(jnode)
		if !ok {
			return "", nil
		}
		return k, body
	}
	return "", nil
}

func sortedKeys(m jnode) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// walk visits every node of the tree under v, whatever field holds it.
func (r *reader) walk(v any) {
	if r.err != nil {
		return
	}
	switch t := v.(type) {
	case []any:
		for _, e := range t {
			r.walk(e)
		}
	case jnode:
		if kind, body := wrapped(t); kind != "" {
			r.visit(kind, body)
			t = body
		}
		for _, k := range sortedKeys(t) {
			r.walk(t[k])
		}
	}
}

func jstr(v any) string {
	s, _ := v.(string)
	return s
}

func jint(v any) int {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0
		}
		return int(i)
	case float64:
		return int(n)
	}
	return 0
}

func jlist(v any) []any {
	l, _ := v.([]any)
	return l
}

// sval is the text of a {"String": {"sval": ...}} node, "" for any other.
func sval(v any) (string, bool) {
	if kind, body := wrapped(v); kind == "String" {
		return jstr(body["sval"]), true
	}
	return "", false
}

// target reads a write's relation (a RangeVar written in place).
func (r *reader) target(kind string, rel any) (Write, bool) {
	rv, ok := rel.(jnode)
	if !ok || jstr(rv["relname"]) == "" {
		r.fail("%s: no table where one must stand", strings.ToUpper(kind))
		return Write{}, false
	}
	w := Write{Kind: kind, Schema: jstr(rv["schemaname"]), Table: jstr(rv["relname"]), loc: r.base + jint(rv["location"])}
	if a, ok := rv["alias"].(jnode); ok {
		w.Alias = jstr(a["aliasname"])
	}
	return w, true
}

// sets reads a SET list (a list of ResTarget) into w.
func (r *reader) sets(list any, w *Write) {
	for _, e := range jlist(list) {
		kind, rt := wrapped(e)
		if kind != "ResTarget" || jstr(rt["name"]) == "" {
			r.fail("%s of %s: an assignment that names no column", strings.ToUpper(w.Kind), w.Table)
			return
		}
		item := SetItem{Column: jstr(rt["name"]), val: rt["val"]}
		if k, b := wrapped(rt["val"]); k == "MultiAssignRef" {
			// SET (a, b) = <source>: the source is one row.
			item.Multi, item.val = true, b["source"]
		}
		item.Expr = render(item.val)
		w.Sets = append(w.Sets, item)
		// a.b = v assigns field b of column a. Written with the target's
		// own name or alias in front (which PostgreSQL refuses) it would
		// name column b: both are recorded.
		if ind := jlist(rt["indirection"]); len(ind) > 0 {
			if f, ok := sval(ind[0]); ok {
				second := item
				second.Column = f
				w.Sets = append(w.Sets, second)
			}
		}
	}
}

// visit reads what one node writes.
func (r *reader) visit(kind string, body jnode) {
	if strings.HasSuffix(kind, "Stmt") {
		switch stmtKinds[kind] {
		case kindReads, kindPasses:
		case kindRefused:
			r.fail("a %s in a parse tree: the parser gives none for statement text", kind)
			return
		default:
			r.fail("a statement of a kind this reader has not classified (%s): classify it in stmtKinds", kind)
			return
		}
	}
	switch kind {
	case "UpdateStmt":
		if w, ok := r.target("update", body["relation"]); ok {
			r.sets(body["targetList"], &w)
			w.where = body["whereClause"]
			r.out = append(r.out, w)
		}
	case "InsertStmt":
		w, ok := r.target("insert", body["relation"])
		if !ok {
			return
		}
		w.InsertColumns, w.InsertAll = targetNames(body["cols"])
		r.out = append(r.out, w)
		if oc, ok := body["onConflictClause"].(jnode); ok && jstr(oc["action"]) == "ONCONFLICT_UPDATE" {
			u := Write{Kind: "conflict-update", Schema: w.Schema, Table: w.Table, Alias: w.Alias, loc: w.loc}
			r.sets(oc["targetList"], &u)
			u.where = oc["whereClause"]
			r.out = append(r.out, u)
		}
	case "DeleteStmt":
		if w, ok := r.target("delete", body["relation"]); ok {
			r.out = append(r.out, w)
		}
	case "MergeStmt":
		w, ok := r.target("merge", body["relation"])
		if !ok {
			return
		}
		var updates []Write
		for _, e := range jlist(body["mergeWhenClauses"]) {
			k, c := wrapped(e)
			if k != "MergeWhenClause" {
				r.fail("MERGE of %s: a clause that is not read", w.Table)
				return
			}
			switch jstr(c["commandType"]) {
			case "CMD_UPDATE":
				u := Write{Kind: "merge-update", Schema: w.Schema, Table: w.Table, Alias: w.Alias, loc: w.loc}
				r.sets(c["targetList"], &u)
				updates = append(updates, u)
			case "CMD_INSERT":
				cols, all := targetNames(c["targetList"])
				w.InsertColumns = append(w.InsertColumns, cols...)
				w.InsertAll = w.InsertAll || all
			case "CMD_DELETE", "CMD_NOTHING":
			default:
				r.fail("MERGE of %s: an action that is not read (%s)", w.Table, jstr(c["commandType"]))
				return
			}
		}
		r.out = append(append(r.out, w), updates...)
	case "TruncateStmt":
		for _, e := range jlist(body["relations"]) {
			k, rv := wrapped(e)
			if k != "RangeVar" {
				r.fail("TRUNCATE: a target that is not a table name")
				return
			}
			if w, ok := r.target("truncate", rv); ok {
				r.out = append(r.out, w)
			}
		}
	case "CopyStmt":
		// COPY (query) TO has no relation, and COPY ... TO writes none.
		if body["is_from"] != true {
			return
		}
		w, ok := r.target("copy", body["relation"])
		if !ok {
			return
		}
		w.Alias = ""
		for _, e := range jlist(body["attlist"]) {
			if c, ok := sval(e); ok {
				w.CopyColumns = append(w.CopyColumns, c)
			}
		}
		w.CopyAll = len(w.CopyColumns) == 0
		r.out = append(r.out, w)
	case "AlterTableStmt":
		w, ok := r.target("alter", body["relation"])
		if !ok {
			return
		}
		w.Alias = ""
		for _, e := range jlist(body["cmds"]) {
			k, c := wrapped(e)
			if k != "AlterTableCmd" {
				r.fail("ALTER TABLE %s: an action that is not read", w.Table)
				return
			}
			action, known := alterKinds[jstr(c["subtype"])]
			switch {
			case !known:
				r.fail("ALTER TABLE %s: an action this reader has not classified (%s): classify it in alterKinds", w.Table, jstr(c["subtype"]))
				return
			case action == "refuse":
				r.fail("ALTER TABLE %s: %s makes another table's rows this table's, which is not read", w.Table, jstr(c["subtype"]))
				return
			case action != "":
				w.Alters = append(w.Alters, AlterAction{Column: jstr(c["name"]), Action: action})
			}
		}
		r.out = append(r.out, w)
	case "RenameStmt":
		if jstr(body["renameType"]) != "OBJECT_COLUMN" || body["relation"] == nil {
			return
		}
		if w, ok := r.target("alter", body["relation"]); ok {
			w.Alias = ""
			w.Alters = []AlterAction{{Column: jstr(body["subname"]), Action: "rename"}}
			r.out = append(r.out, w)
		}
	case "DropStmt":
		if t := jstr(body["removeType"]); t != "OBJECT_TABLE" && t != "OBJECT_FOREIGN_TABLE" {
			return
		}
		for _, e := range jlist(body["objects"]) {
			k, l := wrapped(e)
			var parts []string
			for _, it := range jlist(l["items"]) {
				s, ok := sval(it)
				if !ok {
					k = ""
				}
				parts = append(parts, s)
			}
			w := Write{Kind: "drop", loc: r.base}
			switch {
			case k != "List" || len(parts) == 0 || len(parts) > 3:
				r.fail("DROP TABLE: a name that is not read")
				return
			case len(parts) == 1:
				w.Table = parts[0]
			default:
				// [database.]schema.table
				w.Schema, w.Table = parts[len(parts)-2], parts[len(parts)-1]
			}
			r.out = append(r.out, w)
		}
	case "DoStmt":
		lang, text, has := "plpgsql", "", false
		for _, e := range jlist(body["args"]) {
			_, d := wrapped(e)
			switch jstr(d["defname"]) {
			case "language":
				lang, _ = sval(d["arg"])
			case "as":
				text, has = sval(d["arg"])
			}
		}
		if !has {
			r.fail("a DO block with no body")
			return
		}
		r.body(lang, text)
	case "CreateFunctionStmt":
		lang, text, has := "", "", false
		for _, e := range jlist(body["options"]) {
			_, d := wrapped(e)
			switch jstr(d["defname"]) {
			case "language":
				lang, _ = sval(d["arg"])
			case "as":
				_, l := wrapped(d["arg"])
				items := jlist(l["items"])
				if len(items) != 1 {
					// AS 'object file', 'link symbol': compiled code.
					r.fail("a function whose body is not one text is not read")
					return
				}
				text, has = sval(items[0])
			}
		}
		if !has {
			// BEGIN ATOMIC ... END, or RETURN: the body is nodes, which
			// walk reads where they stand. No body at all writes nothing.
			return
		}
		r.body(lang, text)
	}
}

// targetNames reads the column names of an INSERT's list; all when the
// statement lists none.
func targetNames(list any) (cols []string, all bool) {
	for _, e := range jlist(list) {
		if _, rt := wrapped(e); rt != nil {
			cols = append(cols, jstr(rt["name"]))
		}
	}
	return cols, len(cols) == 0
}

// body reads the body of a DO block or a function, written in lang.
func (r *reader) body(lang, text string) {
	switch strings.ToLower(lang) {
	case "sql":
		ws, err := readWrites(text, r.nesting+1)
		if err != nil {
			r.fail("in a body: %w", err)
			return
		}
		r.nested(ws)
	case "plpgsql":
		r.plpgsql()
	default:
		r.fail("a body in the language %q is not read", lang)
	}
}

// nested adds the writes of a body, placed where the statement that holds
// the body begins.
func (r *reader) nested(ws []Write) {
	for _, w := range ws {
		w.loc = r.base
		r.out = append(r.out, w)
	}
}

// plpgsql reads the PL/pgSQL body of the statement at r.text, through the
// server's PL/pgSQL parser: every SQL statement and expression in it is
// handed back as text, and each is read in turn.
func (r *reader) plpgsql() {
	out, err := pgparser.ParsePlPgSqlToJSON(r.text)
	if err != nil {
		r.fail("in a body: PostgreSQL's PL/pgSQL parser does not read it: %w", err)
		return
	}
	var fns []any
	dec := json.NewDecoder(strings.NewReader(out))
	dec.UseNumber()
	if err := dec.Decode(&fns); err != nil || len(fns) != 1 {
		r.fail("in a body: the PL/pgSQL parser gave %d functions for one statement (%v)", len(fns), err)
		return
	}
	_, fn := wrapped(fns[0])
	if fn == nil || fn["action"] == nil {
		r.fail("in a body: the PL/pgSQL parser gave no body for it")
		return
	}
	p := &plReader{r: r, datums: jlist(fn["datums"]), newVarno: -1}
	if n, ok := fn["new_varno"]; ok {
		p.newVarno = jint(n)
	}
	// The declarations (a default value, a cursor's query) are read as
	// what they are; nothing is assigned through one.
	p.walk(fn["datums"], "", "", false)
	p.walk(fn["action"], "", "", true)
}

type plReader struct {
	r        *reader
	datums   []any
	newVarno int
}

// newField: datum n is a field of the trigger's NEW row (its name), or the
// NEW row itself (whole).
func (p *plReader) newField(n int) (field string, whole, is bool) {
	if p.newVarno < 0 {
		return "", false, false
	}
	if n == p.newVarno {
		return "", true, true
	}
	if n < 0 || n >= len(p.datums) {
		return "", false, false
	}
	if kind, d := wrapped(p.datums[n]); kind == "PLpgSQL_recfield" && jint(d["recparentno"]) == p.newVarno {
		return jstr(d["fieldname"]), false, true
	}
	return "", false, false
}

// assigned records that datum n is given a value by something other than
// an assignment whose value was read: SELECT ... INTO, a loop's variable, GET
// DIAGNOSTICS. If it is a column of NEW, the column is written with a value
// nothing here can judge.
func (p *plReader) assigned(n int) {
	field, whole, is := p.newField(n)
	switch {
	case !is:
	case whole:
		p.r.fail("in a body: the whole NEW row is assigned, which is not read")
	default:
		p.r.out = append(p.r.out, Write{Kind: "trigger-assign", Table: "new", loc: p.r.base,
			Sets: []SetItem{{Column: field, Expr: unreadValue}}})
	}
}

// walk visits the PL/pgSQL tree: stmt is the kind of the node v stands in,
// field the name of the field that holds it, and action is false in the
// declarations.
func (p *plReader) walk(v any, stmt, field string, action bool) {
	if p.r.err != nil {
		return
	}
	switch t := v.(type) {
	case []any:
		for _, e := range t {
			p.walk(e, stmt, field, action)
		}
	case jnode:
		kind, body := wrapped(t)
		switch {
		case kind == "PLpgSQL_expr":
			p.expr(body, stmt, field)
			return
		case kind != "":
			if action {
				p.targets(kind, body)
			}
			for _, k := range sortedKeys(body) {
				p.walk(body[k], kind, k, action)
			}
			return
		}
		if action {
			// {"name": ..., "varno": n}: a member of a row that is a
			// target (INTO a, b).
			if n, ok := t["varno"]; ok && (field == "fields") {
				p.assigned(jint(n))
			}
		}
		for _, k := range sortedKeys(t) {
			p.walk(t[k], stmt, k, action)
		}
	}
}

// targets records what one PL/pgSQL node assigns other than through a read
// assignment.
func (p *plReader) targets(kind string, body jnode) {
	switch kind {
	case "PLpgSQL_stmt_assign":
		field, whole, is := p.newField(jint(body["varno"]))
		if !is {
			return
		}
		if whole {
			p.r.fail("in a body: the whole NEW row is assigned, which is not read")
			return
		}
		_, e := wrapped(body["expr"])
		rhs, ok := assignedValue(jstr(e["query"]))
		if !ok {
			p.r.fail("in a body: an assignment to NEW.%s whose value is not read", field)
			return
		}
		val, err := exprTree(rhs)
		if err != nil {
			p.r.fail("in a body: an assignment to NEW.%s: %w", field, err)
			return
		}
		p.r.out = append(p.r.out, Write{Kind: "trigger-assign", Table: "new", loc: p.r.base,
			Sets: []SetItem{{Column: field, Expr: render(val), val: val}}})
	case "PLpgSQL_diag_item":
		p.assigned(jint(body["target"]))
	case "PLpgSQL_rec":
		// A record that is a target (INTO r, FOR r IN ...) and is NEW.
		if _, whole, is := p.newField(jint(body["dno"])); is && whole {
			p.r.fail("in a body: the whole NEW row is assigned, which is not read")
		}
	case "PLpgSQL_var", "PLpgSQL_recfield":
		// A loop's variable, or a FOREACH's, written in place.
		if n, ok := body["dno"]; ok {
			p.assigned(jint(n))
		}
	}
}

// PostgreSQL's raw parse modes, as a PL/pgSQL expression names them.
const (
	plModeStatement  = 0
	plModeExpression = 2
	plModeAssign1    = 3
	plModeAssign3    = 5
)

// expr reads one expression or statement of a PL/pgSQL body.
func (p *plReader) expr(e jnode, stmt, field string) {
	r := p.r
	q, mode := jstr(e["query"]), jint(e["parseMode"])
	dynamic := field == "dynquery" || (field == "query" && (stmt == "PLpgSQL_stmt_dynexecute" || stmt == "PLpgSQL_stmt_dynfors"))
	if dynamic {
		// EXECUTE runs text. One string constant is that statement;
		// anything else is made at run time.
		text, ok := stringConstant(q)
		if !ok {
			r.fail("EXECUTE of text made at run time is not read")
			return
		}
		ws, err := readWrites(text, r.nesting+1)
		if err != nil {
			r.fail("in EXECUTE: %w", err)
			return
		}
		r.nested(ws)
		return
	}
	text := q
	switch {
	case mode == plModeStatement:
	case mode == plModeExpression:
		text = "SELECT " + q
	case mode >= plModeAssign1 && mode <= plModeAssign3:
		rhs, ok := assignedValue(q)
		if !ok {
			r.fail("in a body: an assignment that is not read: %.80s", q)
			return
		}
		text = "SELECT " + rhs
	default:
		r.fail("in a body: text the PL/pgSQL parser read in a mode this reader does not know (%d)", mode)
		return
	}
	ws, err := readWrites(text, r.nesting+1)
	if err != nil {
		r.fail("in a body: %w", err)
		return
	}
	r.nested(ws)
}

// assignedValue is the right side of a PL/pgSQL assignment `target := value`
// (or `target = value`): what follows the first := or = that stands outside
// every bracket, by PostgreSQL's scanner.
func assignedValue(q string) (string, bool) {
	toks, err := scanSQL(q)
	if err != nil {
		return "", false
	}
	depth := 0
	for _, t := range toks {
		switch t.token {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case tokColonEquals, '=':
			if depth == 0 {
				return q[t.end:], true
			}
		}
	}
	return "", false
}

// exprTree is the tree of one SQL expression.
func exprTree(expr string) (any, error) {
	p := parseSQL("SELECT " + expr)
	if p.err != nil {
		return nil, p.err
	}
	if len(p.stmts) != 1 {
		return nil, errors.New("not one expression")
	}
	rs, _ := p.stmts[0].(jnode)
	kind, sel := wrapped(rs["stmt"])
	targets := jlist(sel["targetList"])
	if kind != "SelectStmt" || len(targets) != 1 {
		return nil, errors.New("not one expression")
	}
	// Nothing but the one value: no FROM, WHERE, set operation or lock.
	for k := range sel {
		switch k {
		case "targetList", "limitOption", "op":
		default:
			return nil, errors.New("not one expression")
		}
	}
	_, rt := wrapped(targets[0])
	if rt == nil || rt["val"] == nil || rt["name"] != nil {
		return nil, errors.New("not one expression")
	}
	return rt["val"], nil
}

// stringConstant: expr is one string constant, and its value.
func stringConstant(expr string) (string, bool) {
	v, err := exprTree(expr)
	if err != nil {
		return "", false
	}
	return constString(v)
}

// constString: v is a string constant (of any spelling), and its value.
func constString(v any) (string, bool) {
	kind, c := wrapped(v)
	if kind != "A_Const" {
		return "", false
	}
	s, ok := c["sval"].(jnode)
	if !ok {
		return "", false
	}
	return jstr(s["sval"]), true
}

// columnRef: v names a column, and the parts of the name ([table.]column,
// each as PostgreSQL folds or unquotes it).
func columnRef(v any) ([]string, bool) {
	kind, c := wrapped(v)
	if kind != "ColumnRef" {
		return nil, false
	}
	var parts []string
	for _, f := range jlist(c["fields"]) {
		s, ok := sval(f)
		if !ok {
			return nil, false
		}
		parts = append(parts, s)
	}
	return parts, len(parts) > 0
}

// GuardsEmpty reports whether the write's WHERE lets it touch only rows
// whose col is empty: the WHERE has, AS ONE OF THE CONDITIONS JOINED BY
// AND at its top, exactly
//
//	[q.]col IS NULL          or          COALESCE([q.]col, '') = ''
//
// where q is the target's alias, or its name when it has no alias. A WHERE
// whose top is OR has no such condition (nothing beside an OR is required);
// a condition under NOT or in a subquery is not this condition. What the
// top of a WHERE is, is the parser's reading, not this file's.
func (w Write) GuardsEmpty(col string) bool {
	if w.Kind != "update" && w.Kind != "conflict-update" && w.Kind != "merge-update" {
		return false
	}
	for _, c := range conjuncts(w.where) {
		if w.isEmptyGuard(c, col) {
			return true
		}
	}
	return false
}

// conjuncts are the conditions all of which v requires.
func conjuncts(v any) []any {
	if kind, b := wrapped(v); kind == "BoolExpr" && jstr(b["boolop"]) == "AND_EXPR" {
		var out []any
		for _, a := range jlist(b["args"]) {
			out = append(out, conjuncts(a)...)
		}
		return out
	}
	if v == nil {
		return nil
	}
	return []any{v}
}

func (w Write) isEmptyGuard(c any, col string) bool {
	stored := func(v any) bool {
		parts, ok := columnRef(v)
		switch {
		case !ok:
			return false
		case len(parts) == 1:
			return parts[0] == col
		case len(parts) == 2:
			q := parts[0]
			return parts[1] == col && (q == w.Alias || (w.Alias == "" && q == w.Table))
		}
		return false
	}
	empty := func(v any) bool {
		s, ok := constString(v)
		return ok && s == ""
	}
	switch kind, b := wrapped(c); kind {
	case "NullTest":
		return jstr(b["nulltesttype"]) == "IS_NULL" && b["argisrow"] == nil && stored(b["arg"])
	case "A_Expr":
		if jstr(b["kind"]) != "AEXPR_OP" || len(jlist(b["name"])) != 1 {
			return false
		}
		if op, _ := sval(jlist(b["name"])[0]); op != "=" {
			return false
		}
		k, co := wrapped(b["lexpr"])
		args := jlist(co["args"])
		return k == "CoalesceExpr" && len(args) == 2 && stored(args[0]) && empty(args[1]) && empty(b["rexpr"])
	}
	return false
}

// ViewSources are the relations each plain view defined in sql reads
// from ("schema.view" to the "schema.table" names its query reads). A
// write through such a view is a write of the tables behind it, under
// column names the view chooses.
func ViewSources(sql string) map[string][]string {
	p := parseSQL(sql)
	if p.err != nil {
		return nil
	}
	out := map[string][]string{}
	for _, raw := range p.stmts {
		rs, _ := raw.(jnode)
		kind, v := wrapped(rs["stmt"])
		if kind != "ViewStmt" {
			continue
		}
		view, _ := v["view"].(jnode)
		if jstr(view["schemaname"]) == "" {
			continue
		}
		name := jstr(view["schemaname"]) + "." + jstr(view["relname"])
		var find func(any)
		find = func(n any) {
			switch t := n.(type) {
			case []any:
				for _, e := range t {
					find(e)
				}
			case jnode:
				if k, rv := wrapped(t); k == "RangeVar" && jstr(rv["schemaname"]) != "" {
					out[name] = append(out[name], jstr(rv["schemaname"])+"."+jstr(rv["relname"]))
				}
				for _, k := range sortedKeys(t) {
					find(t[k])
				}
			}
		}
		find(v["query"])
	}
	return out
}

// render writes a value's tree out for a person to read in a failure. It is
// not PostgreSQL's deparser and need not be exact: nothing is judged on it.
func render(v any) string {
	list := func(l any) string {
		var parts []string
		for _, e := range jlist(l) {
			parts = append(parts, render(e))
		}
		return strings.Join(parts, ", ")
	}
	kind, b := wrapped(v)
	switch kind {
	case "ColumnRef":
		var parts []string
		for _, f := range jlist(b["fields"]) {
			if s, ok := sval(f); ok {
				parts = append(parts, s)
			} else {
				parts = append(parts, "*")
			}
		}
		return strings.Join(parts, ".")
	case "ParamRef":
		return "$" + strconv.Itoa(jint(b["number"]))
	case "A_Const":
		switch {
		case b["isnull"] == true:
			return "NULL"
		case b["sval"] != nil:
			s, _ := constString(v)
			return "'" + strings.ReplaceAll(s, "'", "''") + "'"
		case b["ival"] != nil:
			n, _ := b["ival"].(jnode)
			return strconv.Itoa(jint(n["ival"]))
		case b["fval"] != nil:
			n, _ := b["fval"].(jnode)
			return jstr(n["fval"])
		case b["boolval"] != nil:
			n, _ := b["boolval"].(jnode)
			if n["boolval"] == true {
				return "TRUE"
			}
			return "FALSE"
		}
	case "FuncCall":
		var name []string
		for _, f := range jlist(b["funcname"]) {
			s, _ := sval(f)
			name = append(name, s)
		}
		if b["agg_star"] == true {
			return strings.Join(name, ".") + "(*)"
		}
		return strings.Join(name, ".") + "(" + list(b["args"]) + ")"
	case "CoalesceExpr":
		return "COALESCE(" + list(b["args"]) + ")"
	case "MinMaxExpr":
		if jstr(b["op"]) == "IS_LEAST" {
			return "LEAST(" + list(b["args"]) + ")"
		}
		return "GREATEST(" + list(b["args"]) + ")"
	case "A_Expr":
		op := ""
		for _, f := range jlist(b["name"]) {
			s, _ := sval(f)
			op += s
		}
		switch jstr(b["kind"]) {
		case "AEXPR_NULLIF":
			return "NULLIF(" + render(b["lexpr"]) + ", " + render(b["rexpr"]) + ")"
		case "AEXPR_OP":
			if b["lexpr"] == nil {
				return op + render(b["rexpr"])
			}
			return render(b["lexpr"]) + " " + op + " " + render(b["rexpr"])
		}
		return render(b["lexpr"]) + " " + strings.TrimPrefix(jstr(b["kind"]), "AEXPR_") + " " + render(b["rexpr"])
	case "BoolExpr":
		var parts []string
		for _, e := range jlist(b["args"]) {
			parts = append(parts, render(e))
		}
		switch jstr(b["boolop"]) {
		case "NOT_EXPR":
			return "NOT (" + strings.Join(parts, ", ") + ")"
		case "OR_EXPR":
			return "(" + strings.Join(parts, " OR ") + ")"
		}
		return "(" + strings.Join(parts, " AND ") + ")"
	case "NullTest":
		if jstr(b["nulltesttype"]) == "IS_NULL" {
			return render(b["arg"]) + " IS NULL"
		}
		return render(b["arg"]) + " IS NOT NULL"
	case "TypeCast":
		tn, _ := b["typeName"].(jnode)
		var name []string
		for _, f := range jlist(tn["names"]) {
			if s, _ := sval(f); s != "pg_catalog" {
				name = append(name, s)
			}
		}
		return render(b["arg"]) + "::" + strings.Join(name, ".")
	case "CaseExpr":
		out := "CASE"
		if b["arg"] != nil {
			out += " " + render(b["arg"])
		}
		for _, e := range jlist(b["args"]) {
			_, cw := wrapped(e)
			out += " WHEN " + render(cw["expr"]) + " THEN " + render(cw["result"])
		}
		if b["defresult"] != nil {
			out += " ELSE " + render(b["defresult"])
		}
		return out + " END"
	case "SubLink":
		return "(subquery)"
	case "RowExpr":
		return "(" + list(b["args"]) + ")"
	case "List":
		return "(" + list(b["items"]) + ")"
	case "SetToDefault":
		return "DEFAULT"
	case "SQLValueFunction":
		return strings.TrimPrefix(jstr(b["op"]), "SVFOP_")
	}
	if kind == "" {
		return "(a value not written out)"
	}
	return "(" + kind + ")"
}
