// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The 2026-09-12 scorecard subprocess ceiling, pinned (review round on the
// admission change: the changelog named this pin and it did not exist).
//
// Two halves. (1) ORDER: runScorecardPhase takes a subprocess slot BEFORE
// it borrows tokens, so a phase that is merely queued for a slot never
// holds keys it is not using — and gives the slot back on every exit.
// (2) CONSTRUCTION: the semaphore exists only because NewWithKeys builds
// it; a Scheduler assembled any other way would send on a nil channel
// and park the job forever. The constructor is therefore pinned to
// produce a channel with the configured capacity, and it must stay the
// package's only Scheduler literal.
func TestScorecardPhaseTakesSlotBeforeBorrowing(t *testing.T) {
	src := srctest.Read(t, "internal/scheduler/scheduler.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Scheduler) runScorecardPhase("))
	slot := strings.Index(body, "s.scorecardSem <- struct{}{}")
	give := strings.Index(body, "<-s.scorecardSem")
	borrow := strings.Index(body, "collector.ScorecardTokens(")
	if slot < 0 || give < 0 || borrow < 0 {
		t.Fatalf("runScorecardPhase must take the subprocess slot, give it back and borrow tokens (slot=%d give=%d borrow=%d)", slot, give, borrow)
	}
	if !(slot < give && give < borrow) {
		t.Errorf("slot must be taken (%d) and its deferred release registered (%d) BEFORE tokens are borrowed (%d) — a queued phase must never hold keys", slot, give, borrow)
	}
	if !strings.Contains(body[slot:borrow], "ctx.Done()") {
		t.Error("the slot wait must be ctx-aware so a shutdown while queued returns without starting anything")
	}
	// The construction pin is on the AST across the whole package, not a
	// substring of one file: a comment mentioning `&Scheduler{` must not
	// fire, and a `Scheduler{}` / `new(Scheduler)` in a sibling file must
	// (L10 pass on the review-round fixes).
	if got := schedulerConstructionSites(t); len(got) != 1 {
		t.Errorf("exactly one Scheduler construction site (NewWithKeys) may exist — a second path would ship a nil scorecardSem and hang every scorecard phase; found %v", got)
	}
}

// schedulerConstructionSites lists every non-test site in the package that
// builds a Scheduler value: a composite literal `Scheduler{…}` (with or
// without `&`), an elided element literal inside a `[]Scheduler{…}` /
// `[N]Scheduler{…}` / `map[K]Scheduler{…}` (also over `*Scheduler`
// elements, where an elided `{}` is `&Scheduler{}`), `new(Scheduler)`, a
// zero-value `var s Scheduler`, or a by-value `Scheduler` struct field
// (constructing the outer type constructs the Scheduler) — every one of
// them yields a nil scorecardSem (L10 passes 2 and 3: each shape escaped
// in turn).
func schedulerConstructionSites(t *testing.T) []string {
	t.Helper()
	files := srctest.PackageFiles(t, "internal/scheduler", 10)
	var sites []string
	fset := token.NewFileSet()
	for rel, src := range files {
		f, err := parser.ParseFile(fset, rel, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		isSched := func(e ast.Expr) bool {
			id, ok := e.(*ast.Ident)
			return ok && id.Name == "Scheduler"
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.CompositeLit:
				if isSched(v.Type) {
					sites = append(sites, rel+":"+fset.Position(v.Pos()).String())
				}
				var elem ast.Expr
				switch ct := v.Type.(type) {
				case *ast.ArrayType:
					elem = ct.Elt
				case *ast.MapType:
					elem = ct.Value
				}
				if star, ok := elem.(*ast.StarExpr); ok {
					elem = star.X // []*Scheduler{{}} elides &Scheduler{}
				}
				if elem != nil && isSched(elem) {
					for _, e := range v.Elts {
						kv, ok := e.(*ast.KeyValueExpr)
						if ok {
							e = kv.Value
						}
						if cl, ok := e.(*ast.CompositeLit); ok && cl.Type == nil {
							sites = append(sites, rel+":"+fset.Position(cl.Pos()).String())
						}
					}
				}
			case *ast.ValueSpec:
				if isSched(v.Type) {
					for range v.Names {
						sites = append(sites, rel+":"+fset.Position(v.Pos()).String())
					}
				}
			case *ast.StructType:
				for _, fld := range v.Fields.List {
					if isSched(fld.Type) {
						sites = append(sites, rel+":"+fset.Position(fld.Pos()).String())
					}
				}
			case *ast.CallExpr:
				if fn, ok := v.Fun.(*ast.Ident); ok && fn.Name == "new" && len(v.Args) == 1 {
					if id, ok := v.Args[0].(*ast.Ident); ok && id.Name == "Scheduler" {
						sites = append(sites, rel+":"+fset.Position(v.Pos()).String())
					}
				}
			}
			return true
		})
	}
	return sites
}

func TestNewWithKeysBuildsScorecardSemAtConfiguredCapacity(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	coll := config.DefaultConfig().Collection
	coll.ScorecardMaxConcurrent = 3
	s := NewWithKeys(nil, nil, nil, platform.NewKeyPool([]string{"t"}, logger), logger, Config{Collection: &coll})
	if s.scorecardSem == nil {
		t.Fatal("NewWithKeys must build scorecardSem — a nil channel blocks every send forever")
	}
	if got := cap(s.scorecardSem); got != 3 {
		t.Errorf("scorecardSem capacity = %d, want the configured scorecard_max_concurrent (3)", got)
	}
	// The accessor is the one default layer: an absent knob yields 8.
	s = NewWithKeys(nil, nil, nil, nil, logger, Config{})
	if got := cap(s.scorecardSem); got != 8 {
		t.Errorf("default scorecardSem capacity = %d, want 8", got)
	}
}
