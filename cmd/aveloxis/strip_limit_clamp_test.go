// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Copilot round on PR #193, C4: --limit was checked only AFTER a full
// batch, so `--limit 1` with the default batch modified 5,000 rows (the
// v0.28.9 heal-messages class). The batch must clamp to the remaining
// limit BEFORE the read.
func TestStripQuotedHistoryClampsBatchToLimit(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/strip_quoted_history.go")
	body := srctest.StripGoComments(src)
	if !strings.Contains(body, "limit-total < int64(b)") &&
		!strings.Contains(body, "limit-total < int64(batch)") {
		t.Error("strip-quoted-history must clamp the batch to the remaining --limit before reading (C4)")
	}
	idxClamp := strings.Index(body, "limit - total")
	idxRead := strings.Index(body, "GetMailingListBodiesForStrip(")
	if idxClamp == -1 || idxRead == -1 || idxClamp > idxRead {
		t.Error("the clamp must precede the batch read — a post-batch check overshoots the limit by up to one full batch")
	}
}
