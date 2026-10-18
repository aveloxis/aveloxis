// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
)

// usersEmailWriters is the reviewed set of functions that may write
// aveloxis_ops.users.email, keyed dir|func (v0.29.98, stage 0.5 P-4 /
// DBS-9): the sign-in upsert and the confirmation of a pending address.
// An unconfirmed address never lands in the column: the old
// UpdateUserEmail wrote it straight from a form and had no caller left
// (v0.19.10's handler became the confirmation flow); its pin was a
// positive "must exist", which this ban replaces.
var usersEmailWriters = map[string]string{
	"internal/db|SignInOAuthUser":   "sign-in: the address the forge reports (new account)",
	"internal/db|updateOAuthUser":   "sign-in: the address the forge reports (returning account)",
	"internal/db|ConfirmEmailToken": "confirmation: email_pending becomes email once the token is proven",
}

// TestUsersEmailIsWrittenOnlyBySignInAndConfirmation bans the OPERATION
// (an INSERT or UPDATE of users.email) outside the reviewed writers, in
// both directions: a new writer fails, and a reviewed writer that no
// longer writes the column fails (a stale permit excuses nothing).
func TestUsersEmailIsWrittenOnlyBySignInAndConfirmation(t *testing.T) {
	bodies := map[string]string{}
	for name, src := range srctest.PackageFiles(t, "internal/db", 100) {
		for fn, part := range srctest.FuncParts(t, srctest.StripGoComments(src)) {
			bodies[srctest.SiteKey("internal/db", fn)+"@"+name] = part
		}
	}
	writers := map[string]bool{}
	for _, st := range sqlscan.FindWrites(sqlscan.Statements(bodies), "aveloxis_ops.users") {
		if st.WritesColumn("aveloxis_ops.users", "email") {
			writers[strings.SplitN(st.File, "@", 2)[0]] = true
		}
	}
	var got []string
	for k := range writers {
		got = append(got, k)
	}
	sort.Strings(got)
	for _, k := range got {
		if _, ok := usersEmailWriters[k]; !ok {
			t.Errorf("%s writes users.email and is not a reviewed writer: an address must come from the forge at sign-in or from a confirmed token", k)
		}
	}
	for k, why := range usersEmailWriters {
		if !writers[k] {
			t.Errorf("reviewed writer %s (%s) no longer writes users.email: update the list", k, why)
		}
	}
	srctest.MinCount(t, "functions scanned in internal/db", len(bodies), 500)
}
