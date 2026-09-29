// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"regexp"
	"strings"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// refusedStatus matches git's own report of an HTTP refusal (http.c: "The
// requested URL returned error: %d") for the two statuses that mean the
// forge refused THIS repository: 403 (GitHub's disabled-by-staff) and 451
// (a legal block). Any other status's remote text — a 500's "Internal
// Server Error" — is not a notice about the repository.
var refusedStatus = regexp.MustCompile(`The requested URL returned error: (403|451)\b`)

// remoteNotice extracts the forge's notice from a failed clone or fetch's
// stderr (worklist item 82): the `remote:` lines git prints from a refused
// request's text/plain body, joined, when git's own line reports a 403 or
// 451. It is the only source of GitHub's "Access to this repository has
// been disabled by GitHub staff." and the only one on GitLab. The store
// caps the text (SetRepoUnavailable).
func remoteNotice(stderr string) (platform.ForgeNotice, bool) {
	if !refusedStatus.MatchString(stderr) {
		return platform.ForgeNotice{}, false
	}
	var parts []string
	for _, line := range strings.Split(stderr, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "remote:")
		if !ok {
			continue
		}
		if rest = strings.TrimSpace(rest); rest != "" {
			parts = append(parts, rest)
		}
	}
	if len(parts) == 0 {
		return platform.ForgeNotice{}, false
	}
	return platform.ForgeNotice{Message: strings.Join(parts, " ")}, true
}

// withRemoteNotice wraps err with the forge's notice when stderr carries
// one; otherwise err is returned unchanged.
func withRemoteNotice(err error, stderr string) error {
	if n, ok := remoteNotice(stderr); ok {
		return &platform.NoticeError{Notice: n, Err: err}
	}
	return err
}
