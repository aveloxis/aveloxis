// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package capacity

import (
	"strconv"
	"testing"
	"time"
)

func TestNotifierIsDueOncePerPeriodPerKey(t *testing.T) {
	now := t0
	n := NewNotifier(time.Minute, clockAt(&now), 100)
	if !n.Due("token_out_of_scope|7") || n.Due("token_out_of_scope|7") {
		t.Fatal("first due, second not")
	}
	if !n.Due("token_out_of_scope|8") || !n.Due("auto_add_cap|7") {
		t.Fatal("another user, another kind: due")
	}
	now = now.Add(time.Minute)
	if !n.Due("token_out_of_scope|7") {
		t.Fatal("due again after the period")
	}
	for i := 0; i < 500; i++ {
		n.Due("k" + strconv.Itoa(i))
	}
	if n.Len() > 100 {
		t.Fatalf("notifier holds %d keys, bound is 100", n.Len())
	}
}
