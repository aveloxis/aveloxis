// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

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
	exe, _, _ := manifestChildCommand()
	run, err := runManifestChild(ctx, manifestRequest{Passes: manifestPassesAll, Dir: dir}, limits, nil, logger)
	if run != nil {
		defer run.close()
	}
	if err == nil {
		for _, pass := range manifestPassesAll {
			if perr := run.passErr(pass, nil); perr != nil {
				err = fmt.Errorf("the %s pass: %w", pass, perr)
				break
			}
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
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
