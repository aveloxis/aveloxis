// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"

	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// cloneRead is one analysis's reading of its clone (0.29.213): the
// dependency names, the declarations with their versions, and the lockfiles,
// read by ONE child process (manifest_child.go) under one pair of limits,
// before any phase writes. The phases take their part from it:
// scanDependencies the names, scanLibyear the declarations, scanLockfiles
// the lockfiles.
//
// One child and not three: the three readings are of the same clone, in the
// same phase, and need nothing from each other or from the phases between
// them, so three starts and three sets of limits bought nothing but two more
// ways to be stopped. Each pass ends with its own done frame, and a pass
// that finished stands when the child is stopped in a later one
// (manifestRun.passErr), so the phases still fail one by one.
type cloneRead struct {
	run    *manifestRun
	runErr error
	// poms answered the child's requests for the Maven parents and BOMs
	// that are not in the clone. pomsErr: it could not be made (which
	// repository hosts may be used is not known), and the declarations were
	// not asked for.
	poms    *mavenPomSource
	pomsErr error
	// limits are the limits the child ran under.
	limits ManifestLimits
}

// readClone runs the child. It writes nothing and logs nothing about what
// was not read: each phase says what its own missing part means.
func (ac *AnalysisCollector) readClone(ctx context.Context, repoID int64, workDir string) *cloneRead {
	read := &cloneRead{limits: manifestLimits().effective()}
	// The registry layer's own conditions (a deep pacing queue) report
	// through this collection's logger, not slog's default.
	ctx = withRegistryLogger(ctx, ac.logger)
	// v0.29.182: a Maven parent or BOM that is not in the clone is obtained
	// from the configured Maven repositories, and stored.
	read.poms, read.pomsErr = newMavenPomSource(ctx, ac.store, ac.logger)
	req := manifestRequest{Passes: manifestPassesAll, Dir: workDir, RepoID: repoID,
		DevBuildDeps: ac.DevBuildDeps, GitHubActionsDeps: ac.GitHubActionsDeps, RemotePomLimit: maxRemotePomCoordinates}
	if read.pomsErr != nil {
		req.Passes = []string{manifestPassNames, manifestPassLockfiles}
		read.run, read.runErr = runManifestChild(ctx, req, read.limits, nil, ac.logger)
		return read
	}
	read.run, read.runErr = runManifestChild(ctx, req, read.limits, read.poms, ac.logger)
	return read
}

// lockfiles is the lockfile pass's result as the lockfile phase takes it. A
// pass that did not finish is a walk that failed: nothing is known about the
// repository's lockfiles, and the stored snapshot stands.
func (r *cloneRead) lockfiles() lockfileSet {
	var set lockfileSet
	if err := r.run.passErr(manifestPassLockfiles, r.runErr); err != nil {
		set = lockfileSet{WalkErr: err}
	} else {
		set = *r.run.locks
	}
	// The lockfiles the clone holds, when the name pass listed them: what
	// the stored rows are held to if the lockfiles went unread.
	if r.run != nil && r.run.passErr(manifestPassNames, r.runErr) == nil {
		set.Present, set.PresentKnown = r.run.present, r.run.presentKnown
	}
	return set
}

// declaredKeys are the match keys of the dependencies the declarations pass
// read: what the inventory will ask the lockfiles about (lockfilesInUse).
// nil when that pass has no result (the inventory is then not replaced, and
// nothing is asked).
func (r *cloneRead) declaredKeys() map[string]bool {
	if r.run.passErr(manifestPassDeclarations, r.runErr) != nil {
		return map[string]bool{}
	}
	keys := make(map[string]bool, len(r.run.decl.Deps))
	for _, d := range r.run.decl.Deps {
		keys[pkgid.MatchKey(d.Manager, d.Name)] = true
	}
	return keys
}

// close lets go of what the reading holds outside memory (the lockfile
// pass's result, on disk), when the phases are done with it.
func (r *cloneRead) close() { r.run.close() }

// logLimits are the limits in force, for the log line of a reading that was
// stopped.
func (l ManifestLimits) logLimits() []any {
	return []any{slog.Int("manifest_read_cpu_seconds", int(l.CPU.Seconds())), slog.Int64("manifest_read_memory_mb", l.HeapBytes>>20)}
}
