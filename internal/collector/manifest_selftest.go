// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// manifestSelfTestTimeout is how long serve's start waits for the reader to
// answer about an empty directory (0.29.216, review round 9: the self-test
// ran under the reader's whole clock, thirteen minutes at the defaults,
// before serve had opened its pool or its monitor). PROVISIONAL: the answer
// takes some tens of milliseconds and the first start of a newly written
// binary under a second (measured on macOS, which validates it then); thirty
// seconds is a host too loaded to answer, and is said as a failure. A
// variable for a test; TestTheManifestReaderSelfTestDoesNotWaitLong pins it.
var manifestSelfTestTimeout = 30 * time.Second

// ManifestReaderSelfTest starts the manifest reader once, on an empty
// directory, and says whether it answered (0.29.215). serve runs it at its
// start, so that a host on which no manifest can be read says so in the
// first lines of its log, once, instead of in every analysis; and so that
// an operator has one line to look for after an upgrade:
//
//	manifest reader ok          the program, its version and build, the limits
//	manifest reader self-test FAILED   why, and what to do
//
// It also takes this process's build identity before anything can replace
// the file (manifestParentBuild). A failure is logged and returned; it does
// not stop serve, which collects everything else without the reader.
func ManifestReaderSelfTest(ctx context.Context, logger *slog.Logger) error {
	limits := manifestLimits().effective()
	build := manifestParentBuild()
	dir, err := os.MkdirTemp("", "aveloxis-manifest-selftest-*")
	if err != nil {
		err = fmt.Errorf("no temporary directory could be made (in %s): %w", os.TempDir(), err)
		logger.Error("manifest reader self-test FAILED — the reader keeps what it reads of lockfiles in a temporary file, so no repository's lockfiles can be read on this host as it is",
			"error", err, "to_do", "give serve a writable temporary directory (TMPDIR), then restart serve")
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	// The program said is the one that RAN (the fallback's, when the link
	// could not be started); the one that would have been, when none did.
	exe, _, _ := manifestChildCommand()
	testCtx, cancel := context.WithTimeout(ctx, manifestSelfTestTimeout)
	defer cancel()
	run, err := runManifestChild(testCtx, manifestRequest{Passes: manifestPassesAll, Dir: dir}, limits, nil, logger)
	if run != nil {
		defer run.close()
		if run.program != "" {
			exe = run.program
		}
	}
	err = manifestSelfTestVerdict(run, err)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // serve is stopping: not the reader's failure
		}
		if testCtx.Err() != nil {
			err = fmt.Errorf("the reader did not answer about an empty directory within %s", manifestSelfTestTimeout)
		}
		logger.Error("manifest reader self-test FAILED — no repository's manifests or lockfiles are read until this is put right",
			append([]any{"program", exe, "version", manifestParentVersion, "build", build, "error", err,
				"to_do", "if the aveloxis binary was replaced while serve ran, restart serve; otherwise the error names what the host lacks (the program must be able to start itself: " + procSelfExe + " on Linux, its own path elsewhere)"},
				limits.logLimits()...)...)
		return err
	}
	logger.Info("manifest reader ok", append([]any{"program", exe, "version", manifestParentVersion, "build", build}, limits.logLimits()...)...)
	return nil
}

// manifestSelfTestVerdict is the self-test's judgement of one run: the
// run's own error, else the first pass that did not finish or could not
// read the directory it was given.
func manifestSelfTestVerdict(run *manifestRun, err error) error {
	if err != nil {
		return err
	}
	for _, pass := range manifestPassesAll {
		if perr := run.passErr(pass, nil); perr != nil {
			return fmt.Errorf("the %s pass: %w", pass, perr)
		}
	}
	return nil
}
