// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// realistic libmagic spam captured from the 2026-06-09 aveloxis_large (Ubuntu)
// incident — the warning cites the compiled magic.mgc DB by path. The wedging
// bug repeats this fingerprint thousands of times (it saturates the preflight's
// 1 MB stderr cap), so the broken-test fixture repeats it well past the
// systemic-spam threshold.
var libmagicStderr = strings.Repeat("/usr/share/misc/magic.mgc, 3673: Warning: offset `' invalid\n", 200)

// macOS-shaped libmagic corruption: Homebrew's libmagic can emit the same
// 'Warning: offset ... invalid' shape without naming a .mgc file (it may cite
// the magic source dir / a non-compiled path). The OS-independent fingerprint
// (magic + Warning + offset + invalid) must still classify this as broken at
// volume.
var libmagicStderrMacOS = strings.Repeat("magic, 412: Warning: offset `' invalid\n", 200)

// benignLibmagicStderr is the 2026-06-10 false-positive scenario: a repaired
// libmagic (typecode-libmagic injected) emits only a HANDFUL of these warnings
// while scans complete fine. This must NOT be classified as broken — volume,
// not presence, is the signal.
const benignLibmagicStderr = "/usr/share/misc/magic.mgc, 3673: Warning: offset `' invalid\n" +
	"/usr/share/misc/magic.mgc, 3677: Warning: offset `' invalid\n" +
	"/usr/share/misc/magic.mgc, 3678: Warning: offset `\\x05' invalid\n"

func TestClassifyScancodeHealth(t *testing.T) {
	cases := []struct {
		name       string
		installed  bool
		goos       string
		stderr     string
		jsonValid  bool
		wantStatus string
		detailHas  string
	}{
		{"not installed", false, "linux", "", false, db.StatusNotInstalled, "install-tools"},
		{"libmagic corrupt (linux)", true, "linux", libmagicStderr, false, db.StatusBroken, "libmagic"},
		// linux remediation names the apt path.
		{"libmagic detail names apt on linux", true, "linux", libmagicStderr, false, db.StatusBroken, "apt-get"},
		// macOS-shaped corruption (no .mgc path) is still caught by the generic fingerprint at volume.
		{"libmagic corrupt (macOS, no .mgc)", true, "darwin", libmagicStderrMacOS, false, db.StatusBroken, "libmagic"},
		// darwin remediation names the brew path.
		{"libmagic detail names brew on darwin", true, "darwin", libmagicStderrMacOS, false, db.StatusBroken, "brew reinstall"},
		// libmagic spam wins even if JSON happened to be produced (volume case).
		{"libmagic wins over json", true, "linux", libmagicStderr, true, db.StatusBroken, "upgrade-tools"},
		// 2026-06-10 false-positive guard: a handful of warnings + valid JSON is a
		// WORKING install, not the wedging bug. Must be OK.
		{"few benign warnings + valid json is healthy", true, "linux", benignLibmagicStderr, true, db.StatusOK, ""},
		// a handful of warnings but NO json is broken on the no-json signal, NOT
		// the libmagic signal — the detail must name the generic JSON failure.
		{"few benign warnings, no json", true, "linux", benignLibmagicStderr, false, db.StatusBroken, "valid JSON"},
		{"repeated generic error", true, "linux", strings.Repeat("ERROR: cannot load plugin xyz\n", 60), false, db.StatusBroken, "repeated"},
		{"no json, no signature", true, "linux", "some one-off warning\n", false, db.StatusBroken, "valid JSON"},
		{"healthy", true, "linux", "", true, db.StatusOK, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, detail := classifyScancodeHealth(c.installed, c.goos, c.stderr, c.jsonValid)
			if status != c.wantStatus {
				t.Errorf("status = %q, want %q", status, c.wantStatus)
			}
			if c.detailHas != "" && !strings.Contains(detail, c.detailHas) {
				t.Errorf("detail %q must mention %q", detail, c.detailHas)
			}
			if c.wantStatus == db.StatusOK && detail != "" {
				t.Errorf("ok status must have empty detail, got %q", detail)
			}
		})
	}
}

func TestCountLibmagicWarnings(t *testing.T) {
	if n := countLibmagicWarnings(""); n != 0 {
		t.Errorf("empty input must count 0, got %d", n)
	}
	if n := countLibmagicWarnings(benignLibmagicStderr); n != 3 {
		t.Errorf("benign fixture has 3 warning lines, got %d", n)
	}
	if n := countLibmagicWarnings(libmagicStderr); n < scancodePreflightRepeatN {
		t.Errorf("broken fixture must exceed the spam threshold, got %d", n)
	}
	// macOS-shaped (no .mgc) lines are counted by the generic fingerprint.
	if n := countLibmagicWarnings(libmagicStderrMacOS); n < scancodePreflightRepeatN {
		t.Errorf("macOS broken fixture must exceed the spam threshold, got %d", n)
	}
	// non-libmagic noise must not be counted.
	if n := countLibmagicWarnings(strings.Repeat("ERROR: cannot load plugin xyz\n", 60)); n != 0 {
		t.Errorf("non-libmagic lines must not count, got %d", n)
	}
}

func TestMostRepeatedLine(t *testing.T) {
	s := "a\nb\nb\nb\n\n  b  \nc\n"
	line, n := mostRepeatedLine(s)
	if line != "b" || n != 4 {
		t.Errorf("mostRepeatedLine = (%q,%d), want (b,4)", line, n)
	}
	if l, n := mostRepeatedLine(""); l != "" || n != 0 {
		t.Errorf("empty input must be (\"\",0); got (%q,%d)", l, n)
	}
}

func TestCapWriterBounded(t *testing.T) {
	w := &capWriter{cap: 10}
	n, _ := w.Write([]byte("0123456789ABCDEF")) // 16 bytes into a 10-byte cap
	if n != 16 {
		t.Errorf("Write must report all bytes consumed (so the pipe never blocks); got %d", n)
	}
	if len(w.buf) != 10 || string(w.buf) != "0123456789" {
		t.Errorf("capWriter must keep only the first cap bytes; got %q", w.buf)
	}
}

// TestScancodePreflightWiredIntoRun pins that Run runs the preflight before
// dispatching, and the not-installed branch records status.
func TestScancodePreflightWiredIntoRun(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "w.preflight(ctx)") {
		t.Error("Run must call w.preflight(ctx) on startup")
	}
	// preflight must run before the dispatcher starts claiming work.
	pf := strings.Index(s, "w.preflight(ctx)")
	rec := strings.Index(s, "w.recoverOrphans(ctx)")
	if pf < 0 || rec < 0 || pf > rec {
		t.Error("w.preflight must run before w.recoverOrphans/dispatch")
	}
	if !strings.Contains(s, "w.recordScancodeStatus(ctx, st, detail)") {
		t.Error("the not-installed branch must record status")
	}
}
