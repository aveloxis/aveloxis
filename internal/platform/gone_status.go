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
// recheck, reconcile-repos, mark-gone-repos, the Apache importer and,
// since v0.29.59, the import-augur existence probe (SR-17): 404 (deleted
// or private), 410 (gone) and 451 (blocked for legal reasons). Anything
// else — 403, 429, 5xx, an unresolved 3xx — is not an answer.
func IsRepoGoneStatus(code int) bool {
	switch code {
	case http.StatusNotFound, http.StatusGone, http.StatusUnavailableForLegalReasons:
		return true
	}
	return false
}

// blockNotice extracts GitHub's block object from an error body
// ({"message":..., "block":{"reason":"dmca","html_url":...}}) — the ONE
// parser for it (SR-17), shared by the 451 and 403 arms (worklist item
// 82). ok is false when the body carries no block object: an ordinary
// 403 ("Resource not accessible …") is not the forge's notice about the
// repository. Best effort: an unparseable body is "no block", never an
// error.
func blockNotice(body []byte) (ForgeNotice, bool) {
	var b struct {
		Message string `json:"message"`
		Block   *struct {
			Reason  string `json:"reason"`
			HTMLURL string `json:"html_url"`
		} `json:"block"`
	}
	if json.Unmarshal(body, &b) != nil || b.Block == nil {
		return ForgeNotice{}, false
	}
	return ForgeNotice{Message: b.Message, Reason: b.Block.Reason, URL: b.Block.HTMLURL}, true
}
