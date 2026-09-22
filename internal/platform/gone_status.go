// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"encoding/json"
	"errors"
	"net/http"
)

// ErrLegallyBlocked marks a 451 Unavailable For Legal Reasons answer — on
// GitHub a DMCA takedown ("Repository access blocked", block.reason
// "dmca"). It is always wrapped together with ErrGone, so every caller
// that skips a gone resource skips a blocked one too, and the reason
// stays reachable through errors.Is for logs and the GUI (v0.29.58,
// 2026-09-22 log review, finding 3: with no arm for it the status took
// the generic ten-attempt retry on every endpoint, every cycle).
var ErrLegallyBlocked = errors.New("blocked for legal reasons")

// IsRepoGoneStatus is the ONE rule for "this repository is definitively
// not available" shared by every probe that sidelines — prelim, the gone
// recheck, reconcile-repos, mark-gone-repos and the Apache importer
// (SR-17; the import-augur existence check keeps its own "< 400 exists"
// rule, worklist 47): 404 (deleted
// or private), 410 (gone) and 451 (blocked for legal reasons). Anything
// else — 403, 429, 5xx, an unresolved 3xx — is not an answer.
func IsRepoGoneStatus(code int) bool {
	switch code {
	case http.StatusNotFound, http.StatusGone, http.StatusUnavailableForLegalReasons:
		return true
	}
	return false
}

// legalBlockReason extracts GitHub's block reason and notice URL from a
// 451 body ({"message":..., "block":{"reason":"dmca","html_url":...}}).
// Best effort: an unparseable body yields empty strings, never an error.
func legalBlockReason(body []byte) (reason, noticeURL string) {
	var b struct {
		Block struct {
			Reason  string `json:"reason"`
			HTMLURL string `json:"html_url"`
		} `json:"block"`
	}
	if json.Unmarshal(body, &b) != nil {
		return "", ""
	}
	return b.Block.Reason, b.Block.HTMLURL
}
