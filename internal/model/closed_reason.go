// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// Why a finding closed, when the closure is NOT a fix
// (repo_deps_vulnerabilities.closed_reason; summary/70, section 3). A closed
// finding with no reason is one whose dependency is gone or was moved past
// the advisory. A reason never selects a finding to close; it labels one that
// closes, so that it is not counted or shown as a fix.
const (
	// ClosedSuperseded: the finding was at a purl with no version; a version
	// has been worked out, and the same advisory is now reported at it.
	ClosedSuperseded = "superseded"
	// ClosedVersionDetermined: the finding was at a purl with no version (so
	// it listed every advisory the package ever had); a version has been
	// worked out, and this advisory does not affect it.
	ClosedVersionDetermined = "version_determined"
	// ClosedVersionLost: the finding was at a version; the dependency is
	// still there, but its version is no longer known (a lockfile was
	// deleted, say).
	ClosedVersionLost = "version_lost"
	// ClosedVersionChanged: the dependency moved to another version that the
	// same advisory still affects.
	ClosedVersionChanged = "version_changed"
	// ClosedNeverApplied: the finding was on a managed entry that no module
	// uses (summary/70, 4.5).
	ClosedNeverApplied = "never_applied"
	// ClosedUnsendable: the finding's purl can no longer be sent: the name
	// was rejected, or the wire gate dropped the purl.
	ClosedUnsendable = "unsendable"
)

// OpenFinding is a current (not closed) finding as the closing rule reads it
// before a scan writes anything: its identity, and what decides its label.
type OpenFinding struct {
	VulnID          string
	Purl            string
	Kind            string // dependency_kind
	Ecosystem       string
	FirstDetectedAt time.Time
	// VersionResolution and EnteredExposureAt (0.29.166) decide whether a
	// successor takes this finding's time of entering exposure.
	VersionResolution string
	EnteredExposureAt *time.Time
	// DecidedAskedAt (0.29.171) is the asked-at time of the answer that
	// last decided the finding; nil on a finding never decided under the
	// rule.
	DecidedAskedAt *time.Time
}

// DecisionGuard is what a closing scan knows about its own age: when it read
// its targets, and when each target's answer was asked (keyed by purl; a
// purl with no entry had no answer, which is the case of a finding whose
// dependency is gone). A close acts on a finding only if neither time is
// older than the finding's last decision. The zero value guards nothing.
type DecisionGuard struct {
	TargetsReadAt time.Time
	AnswerAskedAt map[string]time.Time
}
