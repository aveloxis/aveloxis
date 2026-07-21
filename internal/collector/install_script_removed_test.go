// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"testing"
)

// TestInstallScorecardScriptStaysDeleted is the v0.27.23 negative
// tripwire (house remove-don't-deprecate rule). The repo-root
// install-scorecard.sh built a release-asset URL with no version
// segment and no .tar.gz extension (`scorecard_darwin_arm64`), which
// 404s against every ossf/scorecard release — tools.go's
// scorecardDownloadURL (`scorecard_5.4.0_darwin_arm64.tar.gz`) is the
// one correct implementation, invoked via `aveloxis install-tools`.
// A broken script in the repo root is a trap for anyone who finds it
// before finding the command; it must not quietly return.
func TestInstallScorecardScriptStaysDeleted(t *testing.T) {
	if _, err := os.Stat("../../install-scorecard.sh"); err == nil {
		t.Error("install-scorecard.sh is back in the repo root — it was deleted in v0.27.23 (broken asset URL); `aveloxis install-tools` is the only install path")
	}
}
