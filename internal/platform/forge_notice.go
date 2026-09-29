// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import "errors"

// ForgeNotice is the forge's own explanation for refusing a repository —
// GitHub's block object on a 451 (a DMCA takedown) or a 403 (disabled by
// staff), or the `remote:` text git prints when a clone is refused.
// Worklist item 82: the repository page repeats it, verbatim, instead of
// a generic "no longer available".
type ForgeNotice struct {
	Message string // the forge's text ("Repository access blocked")
	Reason  string // GitHub's block.reason ("dmca", "tos"); empty elsewhere
	URL     string // GitHub's block.html_url (the notice); empty elsewhere
}

// Text is the notice as the page shows it: the message with GitHub's
// reason code after it, or whichever of the two exists.
func (n ForgeNotice) Text() string {
	switch {
	case n.Message != "" && n.Reason != "":
		return n.Message + " (" + n.Reason + ")"
	case n.Message != "":
		return n.Message
	}
	return n.Reason
}

// NoticeError carries a ForgeNotice on the error a refusal already
// returned. It changes no classification: Unwrap exposes the original, so
// every errors.Is the callers make (ErrGone, ErrLegallyBlocked,
// ErrForbidden) answers as before.
type NoticeError struct {
	Notice ForgeNotice
	Err    error
}

func (e *NoticeError) Error() string { return e.Err.Error() }
func (e *NoticeError) Unwrap() error { return e.Err }

// NoticeOf returns the forge notice anywhere in err's chain.
func NoticeOf(err error) (ForgeNotice, bool) {
	var ne *NoticeError
	if errors.As(err, &ne) {
		return ne.Notice, true
	}
	return ForgeNotice{}, false
}
