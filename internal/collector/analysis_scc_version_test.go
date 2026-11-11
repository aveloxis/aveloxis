// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// 0.29.144: every repo_labor row records the scc that produced it
// (data_source "scc <version>"), and the install, the upgrade and the
// monthly update all install the one scc version the canaries validated.
// scc 4.x counts lines differently from 3.x (it fixed quote handling that
// made 3.x count comments and blank lines as code), so a row's numbers are
// read against the scc that produced them.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestParseSCCVersion(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		ok       bool
	}{
		{"scc version 3.7.0\n", "3.7.0", true},
		{"scc version 4.1.0", "4.1.0", true},
		{"scc version 4.2.0 (beta)\n", "4.2.0 (beta)", true},
		{"scc version v4.0.0-rc1\n", "v4.0.0-rc1", true},
		{"  scc version 4.1.0  \r\n", "4.1.0", true},
		// Only the first line counts: a later line is not the version.
		{"scc version 4.1.0\nwarning: something\n", "4.1.0", true},
		{"", "", false},
		{"scc version \n", "", false},
		{"4.1.0\n", "", false},
		{"[{\"Name\":\"Go\"}]", "", false},
		{"scc version 4.1.0; rm -rf /", "", false},
		{"scc version 4.1.0 (beta) trailing", "", false},
		{"\nscc version 4.1.0", "", false},
	} {
		got, ok := parseSCCVersion([]byte(tc.in))
		if got != tc.want || ok != tc.ok {
			t.Errorf("parseSCCVersion(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// versionAwareSCC is a fake scc answering --version with versionScript and
// anything else with a minimal report.
func versionAwareSCC(t *testing.T, versionScript string) {
	t.Helper()
	fakeSCC(t, `if [ "$1" = "--version" ]; then
`+versionScript+`
fi
printf '`+sccMinimalReport+`'`)
}

// statSCC stats the scc that PATH finds, as scanSCC does before it starts
// the scan.
func statSCC(t *testing.T) (string, os.FileInfo) {
	t.Helper()
	path, err := exec.LookPath("scc")
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, fi
}

func versionCollector() (*AnalysisCollector, *bytes.Buffer) {
	var buf bytes.Buffer
	return capturingCollector(&buf), &buf
}

func TestSCCDataSourceRecordsTheInstalledVersion(t *testing.T) {
	versionAwareSCC(t, `echo 'scc version 4.2.0 (beta)'; exit 0`)
	ac, logs := versionCollector()
	path, scanned := statSCC(t)
	if got := ac.sccDataSource(context.Background(), path, 7, scanned, nil); got != "scc 4.2.0 (beta)" {
		t.Errorf("sccDataSource = %q, want %q", got, "scc 4.2.0 (beta)")
	}
	if strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("a readable version logs nothing: %s", logs)
	}
}

// A version probe that fails never fails the scan (an optional attempt is
// never terminal): the rows record "scc", the value every row carried
// before 0.29.144, and the failure is logged with its cause.
func TestSCCDataSourceFallsBackAndLogsOnAFailedProbe(t *testing.T) {
	for name, script := range map[string]string{
		"non-zero exit": `echo 'boom' >&2; exit 3`,
		"unreadable":    `echo 'something else entirely'; exit 0`,
		"no output":     `exit 0`,
	} {
		t.Run(name, func(t *testing.T) {
			versionAwareSCC(t, script)
			ac, logs := versionCollector()
			path, scanned := statSCC(t)
			if got := ac.sccDataSource(context.Background(), path, 7, scanned, nil); got != sccDataSourceUnknown {
				t.Errorf("sccDataSource = %q, want the unknown-version value %q", got, sccDataSourceUnknown)
			}
			if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "repo_id=7") {
				t.Errorf("a failed version probe is logged at WARN with the repository: %s", logs)
			}
		})
	}
}

// A wedged binary cannot hold the worker: the probe is bounded, and the
// bound kills the whole process group.
func TestProbeSCCVersionIsBounded(t *testing.T) {
	versionAwareSCC(t, `sleep 30`)
	start := time.Now()
	if _, err := probeSCCVersion(context.Background(), "scc", 300*time.Millisecond); err == nil {
		t.Fatal("a version probe that never answers must fail")
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("the bounded probe returned after %s; the bound did not end it", took)
	}
}

// A shutdown during the probe is not a failure: nothing is logged (the
// store write that follows reports the cancellation).
func TestSCCDataSourceLogsNothingOnShutdown(t *testing.T) {
	versionAwareSCC(t, `sleep 30`)
	ac, logs := versionCollector()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path, scanned := statSCC(t)
	if got := ac.sccDataSource(ctx, path, 7, scanned, nil); got != sccDataSourceUnknown {
		t.Errorf("sccDataSource = %q on shutdown, want %q", got, sccDataSourceUnknown)
	}
	if logs.Len() != 0 {
		t.Errorf("a shutdown is not a failure; logged: %s", logs)
	}
}

// An upgrade during a scan (review F1): the scan ran the old binary, so the
// version the probe reads afterwards from the new file is not the scan's.
// go install renames the new binary into place; a copy overwrites it in
// place. Either way the file at the path is no longer the one that ran,
// and the rows record "scc" (version unknown) with a WARN.
func TestSCCDataSourceRefusesTheVersionOfAReplacedBinary(t *testing.T) {
	newer := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'scc version 9.9.9'; exit 0; fi\n"
	// Each case changes the file in one more way than the identity check
	// compares, and three change it in exactly ONE way (inode, size, mtime),
	// so dropping any single comparison fails a case.
	for name, replace := range map[string]func(t *testing.T, path string, orig os.FileInfo){
		"renamed into place (go install)": func(t *testing.T, path string, _ os.FileInfo) {
			tmp := path + ".new"
			if err := os.WriteFile(tmp, []byte(newer), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(tmp, path); err != nil {
				t.Fatal(err)
			}
		},
		"only the inode differs": func(t *testing.T, path string, orig os.FileInfo) {
			same, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			tmp := path + ".new"
			if err := os.WriteFile(tmp, same, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(tmp, orig.ModTime(), orig.ModTime()); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(tmp, path); err != nil {
				t.Fatal(err)
			}
		},
		"only the mtime differs": func(t *testing.T, path string, orig os.FileInfo) {
			same, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, same, 0o755); err != nil {
				t.Fatal(err)
			}
			later := orig.ModTime().Add(2 * time.Second)
			if err := os.Chtimes(path, later, later); err != nil {
				t.Fatal(err)
			}
		},
		"only the size differs": func(t *testing.T, path string, orig os.FileInfo) {
			if err := os.WriteFile(path, []byte(newer), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(path, orig.ModTime(), orig.ModTime()); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			versionAwareSCC(t, `echo 'scc version 3.7.0'; exit 0`)
			ac, logs := versionCollector()
			path, scanned := statSCC(t)
			replace(t, path, scanned)
			if got := ac.sccDataSource(context.Background(), path, 7, scanned, nil); got != sccDataSourceUnknown {
				t.Errorf("sccDataSource = %q after the binary was replaced, want %q", got, sccDataSourceUnknown)
			}
			if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "changed") {
				t.Errorf("a replaced binary is logged at WARN: %s", logs)
			}
		})
	}
	// The binary the scan ran could not be identified: no version either.
	versionAwareSCC(t, `echo 'scc version 3.7.0'; exit 0`)
	ac, logs := versionCollector()
	path, _ := statSCC(t)
	if got := ac.sccDataSource(context.Background(), path, 7, nil, os.ErrNotExist); got != sccDataSourceUnknown {
		t.Errorf("sccDataSource = %q with no record of the scanned binary, want %q", got, sccDataSourceUnknown)
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("an unidentified scanned binary is logged at WARN: %s", logs)
	}
}

// The probe runs after a successful scan and its answer reaches the
// snapshot write: wiring pin for the end-to-end test below, which needs a
// database.
func TestScanSCCPassesTheProbedVersionToTheSnapshot(t *testing.T) {
	body := scanSCCBody(t)
	stat := strings.Index(body, "scanned, scannedErr := os.Stat(sccPath)")
	start := strings.Index(body, "startSweptCommand(cmd)")
	if stat < 0 || start < 0 || stat > start {
		t.Errorf("scanSCC must stat the binary before it starts the scan (stat at %d, start at %d)", stat, start)
	}
	probe := strings.Index(body, "dataSource := ac.sccDataSource(ctx, sccPath, repoID, scanned, scannedErr)")
	write := strings.Index(body, "ReplaceRepoLaborSnapshot(ctx, repoID, dataSource, laborRows)")
	if probe < 0 || write < 0 {
		t.Fatalf("scanSCC must probe the scc version and pass it to the snapshot write (probe at %d, write at %d)", probe, write)
	}
	if probe > write {
		t.Error("the version probe must run before the snapshot write")
	}
	if wait := strings.Index(body, "swept.Wait()"); wait < 0 || probe < wait {
		t.Errorf("the version probe runs only after scc has been reaped (a failed scan needs no version; wait at %d, probe at %d)", wait, probe)
	}
}

// End to end: a scan by an scc that reports a version writes that version
// on every row of the snapshot.
func TestScanSCCRecordsTheSccVersionOnEveryRow(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	const repoGit = "https://github.com/_av_sccversion/repo"
	cleanup := func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_data.repo_labor WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`,
			`DELETE FROM aveloxis_data.repo_labor_history WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git=$1`,
		} {
			if _, err := pool.Exec(context.Background(), q, repoGit); err != nil {
				t.Logf("cleanup: %v", err)
			}
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, '_av_sccversion', 'repo', 1) RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}

	scan := func(version string) {
		t.Helper()
		versionAwareSCC(t, `echo 'scc version `+version+`'; exit 0`)
		ac := NewAnalysisCollector(store, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir())
		if err := ac.scanSCC(ctx, repoID, t.TempDir(), &AnalysisResult{}); err != nil {
			t.Fatalf("scanSCC: %v", err)
		}
	}
	sources := func(table string) string {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx, `SELECT COALESCE(string_agg(DISTINCT data_source, ','), '') FROM `+table+` WHERE repo_id = $1`, repoID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	scan("4.1.0")
	if got := sources("aveloxis_data.repo_labor"); got != "scc 4.1.0" {
		t.Errorf("repo_labor data_source = %q, want %q", got, "scc 4.1.0")
	}
	scan("4.2.0 (beta)")
	if got := sources("aveloxis_data.repo_labor"); got != "scc 4.2.0 (beta)" {
		t.Errorf("after a rescan by another scc: repo_labor data_source = %q, want %q", got, "scc 4.2.0 (beta)")
	}

	if got := sources("aveloxis_data.repo_labor_history"); got != "scc 4.1.0" {
		t.Errorf("the rotated snapshot keeps its own scc: repo_labor_history data_source = %q, want %q", got, "scc 4.1.0")
	}

	// An upgrade lands during the scan: the scan's binary renames a newer
	// one over itself (what go install does), so the version read
	// afterwards is not the scan's and the rows say "scc".
	fakeSCC(t, `if [ "$1" = "--version" ]; then echo 'scc version 3.7.0'; exit 0; fi
printf '#!/bin/sh\necho "scc version 9.9.9"\n' > "$0.next" && chmod 755 "$0.next" && mv "$0.next" "$0"
printf '`+sccMinimalReport+`'`)
	ac := NewAnalysisCollector(store, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir())
	if err := ac.scanSCC(ctx, repoID, t.TempDir(), &AnalysisResult{}); err != nil {
		t.Fatalf("scanSCC: %v", err)
	}
	if got := sources("aveloxis_data.repo_labor"); got != sccDataSourceUnknown {
		t.Errorf("a scan during which the binary was replaced: repo_labor data_source = %q, want %q", got, sccDataSourceUnknown)
	}
}

// The install is pinned to one validated scc: an exact module version
// (a release tag or a full commit), never @latest or a branch, so the
// install, the upgrade and the monthly update all deliver the version the
// canaries were run against, and every host counts lines the same way.
func TestSCCInstallIsPinnedToAValidatedVersion(t *testing.T) {
	var scc *ExternalTool
	for _, tool := range ExternalTools() {
		if tool.Name == "scc" {
			scc = &tool
		}
	}
	if scc == nil {
		t.Fatal("scc missing from ExternalTools")
	}
	if scc.InstallCmd != SCCInstallCmd {
		t.Errorf("scc's InstallCmd = %q, want SCCInstallCmd %q (one owner of the pin)", scc.InstallCmd, SCCInstallCmd)
	}
	if want := "go install " + sccModule + "@" + sccPin; SCCInstallCmd != want {
		t.Errorf("SCCInstallCmd = %q, want %q", SCCInstallCmd, want)
	}
	pin := regexp.MustCompile(`^v([0-9]+)\.[0-9]+\.[0-9]+(-0\.[0-9]{14}-[0-9a-f]{12})?$`).FindStringSubmatch(sccPin)
	if pin == nil {
		t.Fatalf("sccPin %q must be an exact release tag or a pseudo-version naming one commit — @latest or a branch lets an upgrade install a version nobody validated", sccPin)
	}
	m := regexp.MustCompile(`^github\.com/boyter/scc/v([0-9]+)$`).FindStringSubmatch(sccModule)
	if m == nil {
		t.Fatalf("sccModule %q is not scc's versioned module path", sccModule)
	}
	if pin[1] != m[1] {
		t.Errorf("sccPin %q is a v%s version of the v%s module path", sccPin, pin[1], m[1])
	}
	if !sccValidatedMajors[m[1]] {
		t.Errorf("the install delivers scc %s.x, which the canary's validated set does not hold", m[1])
	}
	newest := 0
	for major := range sccValidatedMajors {
		if n, err := strconv.Atoi(major); err == nil && n > newest {
			newest = n
		}
	}
	if m[1] != strconv.Itoa(newest) {
		t.Errorf("the install delivers scc %s.x but %d.x is validated: an upgrade would downgrade every host that already runs it", m[1], newest)
	}
}

// The README and the weekly canary job install the same pin: the canary
// validates what the fleet runs, and an operator copying the README gets it.
func TestSCCPinIsTheSameEverywhere(t *testing.T) {
	root := srctest.Root(t)
	for _, rel := range []string{"README.md", filepath.Join(".github", "workflows", "network-canary.yml"),
		filepath.Join("docs", "architecture", "analysis.md")} {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), SCCInstallCmd) {
			t.Errorf("%s must install scc with %q", rel, SCCInstallCmd)
		}
		if regexp.MustCompile(`boyter/scc(/v[0-9]+)?@latest|boyter/scc/v3`).Match(b) {
			t.Errorf("%s still names an unpinned or older scc install", rel)
		}
	}
	// The weekly job runs both scc canaries: the shape check and the
	// streaming decoder production uses.
	wf := srctest.Read(t, ".github/workflows/network-canary.yml")
	for _, name := range []string{"TestLiveSCCOutputShape", "TestLiveSCCStreamingDecodeMatchesUnmarshal"} {
		if !strings.Contains(wf, name) {
			t.Errorf("network-canary.yml must run %s", name)
		}
	}
}

// scc 4.x reads $SCC_CONFIG_PATH and ./.sccconfig in its working directory
// on every run (3.x read neither), and either can change what it counts —
// excluded directories, languages counted as others, complexity off — with
// the rows still labelled with the pinned version (0.29.144 review round 3).
// The scan and the version probe run with neither: the variable stripped and
// the working directory a fresh empty one (never the clone, whose own
// .sccconfig would steer its counts).
func TestSCCRunsWithoutTheHostsConfig(t *testing.T) {
	host := t.TempDir()
	if err := os.WriteFile(filepath.Join(host, ".sccconfig"), []byte("--no-complexity\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SCC_CONFIG_PATH", filepath.Join(host, ".sccconfig"))
	t.Chdir(host)
	fakeSCC(t, `if [ -n "$SCC_CONFIG_PATH" ] || [ -e ./.sccconfig ]; then echo "scc saw a config" >&2; exit 7; fi
if [ "$1" = "--version" ]; then echo 'scc version 4.2.0 (beta)'; exit 0; fi
printf '`+sccMinimalReport+`'`)

	if err := runScanSCC(t, context.Background(), 20*time.Second); !errors.Is(err, errStoreReached) {
		t.Errorf("the scan must not see the host's scc config: %v", err)
	}
	if v, err := probeSCCVersion(context.Background(), "scc", sccVersionProbeTimeout); err != nil || v != "4.2.0 (beta)" {
		t.Errorf("the version probe must not see the host's scc config: %q, %v", v, err)
	}
	// The repository being counted may carry its own .sccconfig: the scan
	// does not run inside the checkout either (review round 4: the scanned
	// directory itself carries the file, so scc running there would see it).
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, ".sccconfig"), []byte("--no-complexity\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runScanSCCIn(t, quietCollector(), context.Background(), 20*time.Second, work); !errors.Is(err, errStoreReached) {
		t.Errorf("the scan must not run inside the checkout, which carries a .sccconfig: %v", err)
	}
}
