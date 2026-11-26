// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"strings"
	"testing"
)

// chainOfCallers is a builder whose input is a parameter handed down
// through n unexported helpers from the one caller that writes the constant.
func chainOfCallers(n int) string {
	var b strings.Builder
	b.WriteString("func stmt(col string) string { return \"SELECT \" + col + \" FROM s.t\" }\n")
	b.WriteString("func h0(ctx context.Context, tx pool, col string) { tx.Exec(ctx, stmt(col)) }\n")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "func h%d(ctx context.Context, tx pool, col string) { h%d(ctx, tx, col) }\n", i, i-1)
	}
	fmt.Fprintf(&b, "func f(ctx context.Context, tx pool) { h%d(ctx, tx, \"a\") }\n", n)
	return b.String()
}

// The constants every caller passes are followed through at most
// callerDepthLimit functions; one more and the argument is a hole, with the
// reason said. The bound is what ends the search (callers can call each
// other in a ring), and it is on the refusing side: past it nothing is read.
// Review round 7 raised it tenfold and no test noticed.
func TestCallerConstantsAreFollowedToTheDepthLimitAndNoFurther(t *testing.T) {
	// The number itself, which doc.go states ("to four calls"): the loop
	// below follows whatever the constant is.
	if callerDepthLimit != 4 {
		t.Fatalf("callerDepthLimit is %d; doc.go says four calls, and a larger one reads more through helpers than anyone reviewed", callerDepthLimit)
	}
	deepest := -1
	for n := 0; n <= callerDepthLimit+3; n++ {
		a := one(t, chainOfCallers(n), Env{})
		if a.Builder != "stmt" {
			t.Fatalf("%d helpers: builder %q", n, a.Builder)
		}
		if a.CallerTuples != nil {
			if deepest != n-1 {
				t.Fatalf("%d helpers are read after %d were refused", n, n-1)
			}
			deepest = n
			if got := fmt.Sprint(a.CallerTuples); got != "[[a]]" {
				t.Errorf("%d helpers: caller tuples %s", n, got)
			}
			continue
		}
		if !strings.Contains(a.CallerWhy, "calls away") {
			t.Errorf("%d helpers: refused with %q; want the depth named", n, a.CallerWhy)
		}
	}
	// n helpers above h0 is n+1 functions between the builder's call and
	// the constant.
	if deepest+1 != callerDepthLimit {
		t.Errorf("the constants are followed through %d functions; callerDepthLimit is %d", deepest+1, callerDepthLimit)
	}
	// A ring of callers ends, and gives nothing.
	ring := `func stmt(col string) string { return "SELECT " + col + " FROM s.t" }
func h0(ctx context.Context, tx pool, col string) { tx.Exec(ctx, stmt(col)); if col == "" { h1(ctx, tx, col) } }
func h1(ctx context.Context, tx pool, col string) { h0(ctx, tx, col) }
`
	if a := one(t, ring, Env{}); a.CallerTuples != nil {
		t.Errorf("a ring of callers gave tuples: %v", a.CallerTuples)
	}
}
