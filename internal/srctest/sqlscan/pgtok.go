// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	pgparser "github.com/wasilibs/go-pgquery/parser"
)

// PostgreSQL's own scanner (0.29.212), for the two places that need the
// words of text that is not a whole statement, or where one token ends: a
// fragment of a statement (FragmentWords) and a PL/pgSQL assignment
// (assignedValue). Nothing here decides what a token is: the scanner does.

// scanTok is one token as PostgreSQL's scanner gives it: where it stands,
// which token it is (the grammar's token number; a single character is its
// own code), and whether it is a key word.
type scanTok struct {
	start, end int
	token      int
	keyword    bool
}

// The grammar's token numbers this file names (PostgreSQL 17;
// TestTheScannersTokenNumbersAreTheOnesNamed scans one of each).
const (
	tokIdent        = 258
	tokUIdent       = 259 // U&"..."
	tokColonEquals  = 270
	tokSQLComment   = 275
	tokBlockComment = 276
)

// scanSQL is sql as the scanner's tokens, or its error (an open string,
// comment or quoted name).
func scanSQL(sql string) ([]scanTok, error) {
	b, err := pgparser.ScanToProtobuf(sql)
	if err != nil {
		return nil, err
	}
	// ScanResult { int32 version = 1; repeated ScanToken tokens = 2; }
	// ScanToken { int32 start = 1; int32 end = 2; Token token = 4;
	// KeywordKind keyword_kind = 5; }: every field a varint.
	var toks []scanTok
	err = protoFields(b, func(field int, n uint64, sub []byte) error {
		if field != 2 || sub == nil {
			return nil
		}
		var t scanTok
		if err := protoFields(sub, func(field int, n uint64, sub []byte) error {
			switch field {
			case 1:
				t.start = int(n)
			case 2:
				t.end = int(n)
			case 4:
				t.token = int(n)
			case 5:
				t.keyword = n != 0
			}
			return nil
		}); err != nil {
			return err
		}
		if t.start < 0 || t.end < t.start || t.end > len(sql) {
			return errors.New("a token placed outside its text")
		}
		toks = append(toks, t)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("the scanner's result is not read: %w", err)
	}
	return toks, nil
}

// protoFields calls f for each field of one protocol-buffer message: a
// varint's value in n, or a length-delimited field's bytes in sub.
func protoFields(b []byte, f func(field int, n uint64, sub []byte) error) error {
	varint := func() (uint64, error) {
		var v uint64
		for shift := uint(0); shift < 64; shift += 7 {
			if len(b) == 0 {
				return 0, errors.New("a number cut short")
			}
			c := b[0]
			b = b[1:]
			v |= uint64(c&0x7f) << shift
			if c < 0x80 {
				return v, nil
			}
		}
		return 0, errors.New("a number too long")
	}
	for len(b) > 0 {
		key, err := varint()
		if err != nil {
			return err
		}
		switch key & 7 {
		case 0:
			n, err := varint()
			if err != nil {
				return err
			}
			if err := f(int(key>>3), n, nil); err != nil {
				return err
			}
		case 2:
			n, err := varint()
			if err != nil {
				return err
			}
			if n > uint64(len(b)) {
				return errors.New("a field cut short")
			}
			sub := b[:n:n]
			b = b[n:]
			if sub == nil {
				sub = []byte{}
			}
			if err := f(int(key>>3), 0, sub); err != nil {
				return err
			}
		default:
			return fmt.Errorf("a field of a kind the scanner's result does not have (%d)", key&7)
		}
	}
	return nil
}

// identName is the name a quoted identifier token names ("A""b" is A"b;
// U&"d\0061t" is dat), by the parser: the token is read as a column's name.
func identName(raw string) (string, bool) {
	v, err := exprTree(raw)
	if err != nil {
		return "", false
	}
	parts, ok := columnRef(v)
	if !ok || len(parts) != 1 {
		return "", false
	}
	return parts[0], true
}

// FragmentWords are the unquoted words and quoted names of a piece of SQL
// that need not be a whole statement, lower-cased where SQL folds them.
// It never fails: what the scanner refuses ends the reading of that piece.
func FragmentWords(piece string) []string {
	var out []string
	for len(piece) > 0 {
		toks, err := scanSQL(piece)
		if err == nil {
			for _, t := range toks {
				raw := piece[t.start:t.end]
				switch {
				case t.token == tokUIdent, t.token == tokIdent && strings.HasPrefix(raw, `"`):
					if name, ok := identName(raw); ok {
						out = append(out, name)
					}
				case t.token == tokIdent, t.keyword:
					out = append(out, asciiLower(raw))
				}
			}
			return out
		}
		// Read what stands before the trouble, word by word.
		i := 0
		for i < len(piece) && !wordStart(piece[i]) {
			i++
		}
		j := i
		for j < len(piece) && (wordStart(piece[j]) || piece[j] == '$' || (piece[j] >= '0' && piece[j] <= '9')) {
			j++
		}
		if j > i {
			out = append(out, strings.ToLower(piece[i:j]))
		}
		piece = piece[j:]
	}
	return out
}

func wordStart(c byte) bool {
	return c == '_' || c >= 0x80 || (c|0x20 >= 'a' && c|0x20 <= 'z')
}

// treeShape is a statement's parse tree with every number in it replaced
// by a mark (a negative one by another), every run of digits in a name or
// a string by one mark, and the places of its tokens left out: two texts
// with one shape are one statement but for their digits. false when the
// parser refuses the text.
func treeShape(sql string) (string, bool) {
	p := parseSQL(sql)
	if p.err != nil {
		return "", false
	}
	var mask func(v any) any
	number := func(s string) string {
		if strings.HasPrefix(strings.TrimSpace(s), "-") {
			return "-#"
		}
		return "#"
	}
	mask = func(v any) any {
		switch t := v.(type) {
		case []any:
			out := make([]any, len(t))
			for i, e := range t {
				out[i] = mask(e)
			}
			return out
		case jnode:
			// A numeric constant, whichever of the parser's forms its
			// size gave it (an integer, or digits kept as text).
			if kind, b := wrapped(t); kind == "A_Const" || kind == "Integer" || kind == "Float" {
				for _, k := range []string{"ival", "fval"} {
					c, ok := b[k]
					if !ok {
						continue
					}
					if inner, ok := c.(jnode); ok {
						c = inner[k]
					}
					s := "0"
					switch n := c.(type) {
					case json.Number:
						s = n.String()
					case string:
						s = n
					}
					return jnode{"number": number(s)}
				}
			}
			out := jnode{}
			for k, e := range t {
				if k == "stmt_len" || strings.HasSuffix(strings.ToLower(k), "location") {
					continue
				}
				out[k] = mask(e)
			}
			return out
		case json.Number:
			return number(t.String())
		case string:
			var b strings.Builder
			digits := false
			for _, c := range t {
				if c >= '0' && c <= '9' {
					if !digits {
						b.WriteByte('#')
					}
					digits = true
					continue
				}
				digits = false
				b.WriteRune(c)
			}
			return b.String()
		}
		return v
	}
	out, err := json.Marshal(mask(p.stmts))
	if err != nil {
		return "", false
	}
	return string(out), true
}
