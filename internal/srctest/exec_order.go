// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"regexp"
	"sort"
)

// ExecOrder orders byte offsets in a package's source by EXECUTION position
// from one root function, not by file position (v0.29.98, stage 0.5 P-1;
// lifted from internal/db's migration column-order analyzer, v0.28.15).
// An event inside a function other than the root sits at its enclosing
// function's first call site, resolved recursively, so file order and
// helper files cannot hide a step that runs before what it depends on
// (the v0.28.18 "decorative gate" lesson: text order across files sorted
// by name is not execution order).
//
// The corpus is comment-STRIPPED source (StripGoComments) keyed by file
// name: a doc-comment mention of helper( must not become the helper's
// call site, and a caller that forgets to strip gets exactly that. Calls
// are matched by name (regexp, depth ≤ 8); a function value, an interface
// call or a function never called from the root is unresolvable, and
// Key reports it as !ok — the caller FAILS on that (SR-16: an unresolved
// order is never "fine").
type ExecOrder struct {
	root  string
	names []string
	files map[string]string
	funcs []execFunc
}

type execFunc struct {
	name       string
	file       string
	start, end int
}

var execFuncRe = regexp.MustCompile(`(?m)^func (?:\([^)]*\)\s*)?(\w+)\(`)

// NewExecOrder indexes the corpus's top-level functions. root is the
// function every execution path starts from (RunMigrations for the
// migrate corpus).
func NewExecOrder(files map[string]string, root string) *ExecOrder {
	o := &ExecOrder{root: root, files: files}
	for f := range files {
		o.names = append(o.names, f)
	}
	sort.Strings(o.names)
	for _, f := range o.names {
		src := files[f]
		ms := execFuncRe.FindAllStringSubmatchIndex(src, -1)
		for i, m := range ms {
			end := len(src)
			if i+1 < len(ms) {
				end = ms[i+1][0]
			}
			o.funcs = append(o.funcs, execFunc{name: src[m[2]:m[3]], file: f, start: m[0], end: end})
		}
	}
	return o
}

// Enclosing names the top-level function holding (file, pos); ok is
// false outside every function.
func (o *ExecOrder) Enclosing(file string, pos int) (name string, ok bool) {
	if fn := o.enclosing(file, pos); fn != nil {
		return fn.name, true
	}
	return "", false
}

func (o *ExecOrder) enclosing(file string, pos int) *execFunc {
	for i := range o.funcs {
		if o.funcs[i].file == file && o.funcs[i].start <= pos && pos < o.funcs[i].end {
			return &o.funcs[i]
		}
	}
	return nil
}

// Key resolves (file, pos) to its execution path from the root: the byte
// offsets of each call site on the way down, ending with pos. Compare
// keys with KeyLess. ok is false when the position is outside any
// function or its function is never (transitively) called from the root
// by name.
func (o *ExecOrder) Key(file string, pos int) ([]int, bool) {
	return o.key(file, pos, 0)
}

func (o *ExecOrder) key(file string, pos, depth int) ([]int, bool) {
	fn := o.enclosing(file, pos)
	if fn == nil || depth > 8 {
		return nil, false
	}
	if fn.name == o.root {
		return []int{pos}, true
	}
	callRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(fn.name) + `\(`)
	var best []int
	found := false
	for _, cf := range o.names {
		for _, m := range callRe.FindAllStringIndex(o.files[cf], -1) {
			def := o.enclosing(cf, m[0])
			if def != nil && def.name == fn.name {
				continue // the definition line itself, or recursion
			}
			k, ok := o.key(cf, m[0], depth+1)
			if !ok {
				continue
			}
			if !found || KeyLess(k, best) {
				best, found = k, true
			}
		}
	}
	if !found {
		return nil, false
	}
	return append(append([]int{}, best...), pos), true
}

// KeyLess orders two execution keys: the first differing call site
// decides; a prefix executes before what it leads to.
func KeyLess(a, b []int) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}
