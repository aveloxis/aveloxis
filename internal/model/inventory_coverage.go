// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// InventoryCoverage is the analysis half of a repository's coverage record
// (repo_dependency_coverage; summary/70, 4.7): what one analysis found while
// building the inventory it stored. It is written in the transaction that
// replaces the inventory, so it always describes the rows stored beside it.
type InventoryCoverage struct {
	// ManifestsFound is the number of files a manifest kind claimed;
	// ManifestsParsed how many of those were read and parsed. The rest are
	// in ManifestsFailed.
	ManifestsFound  int
	ManifestsParsed int
	// ManifestsFailed names each manifest that could not be read or parsed.
	ManifestsFailed []ManifestFailure
	// DependenciesDeclared is the number of dependencies the parsed
	// manifests declared. DependenciesWithoutRow counts those given no
	// inventory row, by reason (the NoRow* constants); nil or empty when
	// every declared dependency has a row.
	DependenciesDeclared   int
	DependenciesWithoutRow map[string]int
	// MavenTreesNotWhole (0.29.183) names each Maven tree that holds
	// dependencyManagement entries and was not read whole, with why: its
	// managed entries are still inventoried.
	MavenTreesNotWhole []MavenTreeGap
	// MavenTreeHosts (0.29.185) names each Maven tree whose parent and BOM
	// chains used fetched poms, with the repository hosts they came from.
	MavenTreeHosts []MavenTreeHosts
	// DeclaredRepositories (0.29.184) are the package repositories the
	// repository's build files declare. Not a column of the coverage
	// record: they are written to repo_declared_repositories by the same
	// replace, in its transaction, so the record matches the inventory.
	DeclaredRepositories []DeclaredRepository
}

// DeclaredRepository is one package repository a build file names
// (repo_declared_repositories, 0.29.184).
type DeclaredRepository struct {
	ManifestPath string // relative to the clone
	URL          string // no trailing slash, no credentials; as written when unreadable
	Host         string // lower case; "" when the address could not be read
	Kind         string // what it serves: dependencies or plugins
	NotFetched   string // why it is never fetched from; "" for one that may be
}

// MavenTreeHosts is one Maven tree and the repository hosts its chains used.
type MavenTreeHosts struct {
	Root  string   `json:"root"`
	Hosts []string `json:"hosts"`
}

// MavenTreeGap is one Maven tree that was not read whole.
type MavenTreeGap struct {
	Root   string `json:"root"`   // the tree's root-most pom, relative to the clone
	Reason string `json:"reason"` // why it is not whole
}

// ManifestFailure is one manifest that contributed nothing because it could
// not be read or parsed.
type ManifestFailure struct {
	Path   string `json:"path"`   // relative to the clone's root
	Kind   string `json:"kind"`   // the manifest kind's name
	Reason string `json:"reason"` // ManifestFailedRead or ManifestFailedParse
	// RowsCarried is how many of the manifest's previous inventory rows were
	// carried into this inventory because it failed. Set by the replace,
	// not by the caller. 0 when the file never parsed, or when its rows were
	// written before 0.29.160 and name no manifest.
	RowsCarried int `json:"rows_carried"`
	// NoCarry tells the replace to carry nothing for this entry: its Path is
	// a SANITIZED spelling of a path that could not be stored unchanged, and
	// could match another manifest's rows. An instruction to the writer, not
	// part of the stored record.
	NoCarry bool `json:"-"`
	// TooLarge says the file was not read because reading it would have
	// taken more memory than the reader had left (0.29.215): the one kind
	// of failure that raising collection.manifest_read_memory_mb cures. Not
	// part of the stored record (the reason there is ManifestFailedRead);
	// it is how the lockfile phase knows to record that the lockfiles were
	// not all read, and why.
	TooLarge bool `json:"-"`
}

// Why a manifest failed.
const (
	ManifestFailedRead  = "read"
	ManifestFailedParse = "parse"
)

// Why a declared dependency was given no inventory row.
const (
	// NoRowNonRegistry: it comes from a path, a workspace, git or a URL, so
	// it is not the registry package of the same name.
	NoRowNonRegistry = "non_registry"
	// NoRowManagedUnused (0.29.183): a Maven dependencyManagement entry, in
	// a tree read whole, whose version was applied to no dependency, or a
	// BOM import's own coordinate. Not a dependency; a finding recorded on
	// it closes as never_applied.
	NoRowManagedUnused = "managed_unused"
	// NoRowNameRejected: its name cannot be stored unchanged.
	NoRowNameRejected = "name_rejected"
	// NoRowLookupFailed: no resolver exists for its package manager. (Until
	// 0.29.162 also: the registry lookup gave no row; such a dependency now
	// keeps its row, with the reason in libyear_status.)
	NoRowLookupFailed = "lookup_failed"
	// NoRowNoNamespace: no purl can be built for it: a coordinate with no
	// group where the purl type requires a namespace (a Maven artifact with
	// no groupId), or a SwiftPM package whose address is not a repository
	// URL. It is not garbage, and it is not "name rejected", which would
	// keep a repository from ever being marked scanned. A purl-less row is
	// never written.
	NoRowNoNamespace = "no_namespace"
)

// InventoryNote records a declared dependency that was given no inventory
// row, and why (repo_inventory_notes; summary/70, 4.2). Its Kind is one of
// the NoRow* constants. A note is a record, not identity: it closes nothing
// and preserves nothing by itself.
type InventoryNote struct {
	Kind           string
	PackageManager string
	// Name is the declared name, sanitized and cut to pkgid.MaxNameBytes.
	Name string
	// PackageKey is pkgid.Key of the stored name.
	PackageKey string
	// Purl is the purl of the declared dependency when one can be built and
	// stored unchanged; "" otherwise, and always for a non-registry
	// dependency, which is NOT the registry package of the same name.
	Purl string
	// ManifestPath is the manifest that declared it, as on inventory rows.
	ManifestPath string
}

// Why an analysis did not replace the inventory (the coverage record's
// attempt columns).
const (
	// InventoryAttemptCloneUnavailable: there was no clone to analyse.
	InventoryAttemptCloneUnavailable = "clone_unavailable"
	// InventoryAttemptRootUnreadable: the clone's root could not be read.
	InventoryAttemptRootUnreadable = "root_unreadable"
	// InventoryAttemptReplaceFailed: the replace transaction failed.
	InventoryAttemptReplaceFailed = "replace_failed"
	// The manifests were not read (0.29.213): the process that reads them
	// used more CPU time than collection.manifest_read_cpu_seconds allows;
	// held more memory than collection.manifest_read_memory_mb allows; ran
	// too long by the clock and was stopped by the analysis; or failed (it
	// could not be started, crashed, or sent something that was refused).
	InventoryAttemptReadCPULimit    = "manifest_read_cpu_limit"
	InventoryAttemptReadMemoryLimit = "manifest_read_memory_limit"
	InventoryAttemptReadTimedOut    = "manifest_read_timed_out"
	InventoryAttemptReadFailed      = "manifest_read_failed"
)

// InventoryAttemptResults are every value the coverage record's attempt
// result takes: the list the store accepts and a reader explains.
func InventoryAttemptResults() []string {
	return []string{InventoryAttemptCloneUnavailable, InventoryAttemptRootUnreadable, InventoryAttemptReplaceFailed,
		InventoryAttemptReadCPULimit, InventoryAttemptReadMemoryLimit, InventoryAttemptReadTimedOut, InventoryAttemptReadFailed}
}

// LockfileReading is how one analysis's reading of a repository's lockfiles
// ended (0.29.214): the collector makes it in one place and the store keeps
// it on the coverage record (RecordLockfileReading). Exactly one of
// Completed and NotCompleted is set.
//
//   - Completed: the lockfiles were read and the stored snapshot replaced.
//     Lockfiles says whether the clone has any; false is a repository with
//     no lockfile, for which there is nothing to be behind on.
//   - NotCompleted: they were not read, and the stored rows stand. The
//     value is one of LockfileUnreadReasons. Lockfiles is then true only
//     when the clone is KNOWN to hold a lockfile (the reader listed them
//     before it stopped); false means it is not known. A reading that did
//     not finish in a clone known to hold NO lockfile is recorded as
//     Completed with Lockfiles false: there was nothing to read.
type LockfileReading struct {
	Completed    bool
	Lockfiles    bool
	NotCompleted string
}

// LockfileUnreadNotReached (0.29.215) is why a lockfile reading did not
// finish when it never STARTED: the reader was stopped while it read the
// names or the declarations, before it reached the lockfiles. The four
// manifest_read_* reasons say what stopped a reading of the lockfiles
// themselves; given for one that did not start, a caution said "reading
// them took more memory than allowed" about lockfiles nothing had read.
const LockfileUnreadNotReached = "lockfiles_not_reached"

// LockfileUnreadTooLarge (0.29.216) is why a lockfile reading is recorded
// as not completed although it REPLACED the snapshot: one or more lockfiles
// were left unread because reading them would have taken more memory than
// the reader had left, and every other lockfile was read. 0.29.215 recorded
// this as manifest_read_memory_limit, the reason of a reading that was
// stopped and replaced nothing, and the page then dated today's rows by an
// older reading. The record's two columns do not tell the two apart in
// every case (a first reading that leaves one lockfile unread has no
// lockfiles_read_at, like rows stored before the column existed), and a
// client is sent only the reason and a date, so the reason says which it
// is.
const LockfileUnreadTooLarge = "lockfiles_too_large"

// LockfileUnreadReasons are the values LockfileReading.NotCompleted takes:
// the four ways the process that reads a clone can end without a result,
// the clone's root being unreadable, the reader being stopped before it
// reached the lockfiles, and some lockfiles being too large to read.
func LockfileUnreadReasons() []string {
	return []string{InventoryAttemptReadCPULimit, InventoryAttemptReadMemoryLimit, InventoryAttemptReadTimedOut,
		InventoryAttemptReadFailed, InventoryAttemptRootUnreadable, LockfileUnreadNotReached, LockfileUnreadTooLarge}
}

// DependencyCoverage is a repository's stored coverage record.
type DependencyCoverage struct {
	// AnalysedAt is when the inventory was last replaced whole; nil when it
	// has not been since the record existed (0.29.159).
	AnalysedAt *time.Time
	// Inventory is the analysis half; meaningful only when AnalysedAt is set.
	Inventory InventoryCoverage
	// LockfilesFailed (0.29.176) are the lockfiles that exist and could not
	// be read or parsed at the last lockfile phase; RowsCarried is how many
	// of each one's previous package rows were kept.
	LockfilesFailed []ManifestFailure
	// LastAttemptAt and LastAttemptResult describe the last analysis that
	// did NOT replace the inventory. An attempt later than AnalysedAt means
	// the inventory is older than the last attempt to refresh it.
	LastAttemptAt     *time.Time
	LastAttemptResult string
	// LockfilesReadAt (0.29.214) is the column: when a reading of the
	// repository's lockfiles last finished and found some; nil when none
	// has since the column existed (it is not backfilled), or when the last
	// reading found the repository has none. LockfilesUnreadReason is why
	// the LATEST reading did not finish (one of LockfileUnreadReasons), and
	// is empty when it did.
	//
	// LockfilesLastRead (0.29.215) is what a reader shows: when the lockfile
	// rows that are STORED were read, and nil when none is stored. The store
	// decides it from what is stored, not from the column alone: the column
	// when it is set, else the date of the stored snapshot (rows written
	// before the column existed), and nothing when no lockfile row is
	// stored, whatever the column says. So a reason with a date means the
	// stored rows are that old ("behind"), and a reason with none means
	// there are no lockfile rows and transitive dependencies are not
	// checked ("never read").
	LockfilesReadAt       *time.Time
	LockfilesLastRead     *time.Time
	LockfilesUnreadReason string
	// Scan is the scan half; ScanAttemptAt is nil when no scan has written
	// one (a repository not scanned since 0.29.161).
	ScanAttemptAt *time.Time
	Scan          ScanCoverage
	// FirstScanAt is when the repository was first scanned, for a repository
	// first scanned by 0.29.161 or later; nil otherwise, for good.
	FirstScanAt *time.Time
}

// The outcomes of a vulnerability scan (the coverage record's scan half;
// summary/70, 4.7). Where two could apply, ScanFailed is first and
// ScanInventoryIncomplete is before ScanNothingToSend.
const (
	// ScanSent: at least one target was sent and answered.
	ScanSent = "sent"
	// ScanNothingToSend: no target, and nothing that could not be sent. The
	// repository declares nothing to check.
	ScanNothingToSend = "nothing_to_send"
	// ScanUnsendable: no target could be sent (only dependencies with no
	// purl, or every target dropped by the wire gate).
	ScanUnsendable = "unsendable"
	// ScanUnanswered (0.29.208): targets were sent and OSV gave no complete
	// answer for any of them (every query refused, or cut short). Nothing
	// was decided and the repository is not marked scanned. Until this
	// value such a scan was recorded as ScanUnsendable, which reads "no
	// dependency could be sent" about dependencies that were all sent.
	ScanUnanswered = "unanswered"
	// ScanInventoryIncomplete: no target, and nothing has ever said what the
	// repository depends on (no stored inventory rows, no lockfile rows, no
	// completed analysis). Not "clean": not known.
	ScanInventoryIncomplete = "inventory_incomplete"
	// ScanFailed: the scan returned an error.
	ScanFailed = "failed"
)

// ScanCoverage is the scan half of a repository's coverage record.
type ScanCoverage struct {
	Outcome string
	// Partial: the outcome is ScanSent and the closing statement failed (in
	// later releases also: an answer was truncated or rejected, or closures
	// were held back).
	Partial bool
	// TargetsSent and TargetsDropped are nil when the scan failed before its
	// targets were built.
	TargetsSent    *int
	TargetsDropped *int
	// TargetsRejected (0.29.168) counts the sent targets OSV refused to
	// answer; ClosuresHeld the open findings the scan neither reported nor
	// closed because an answer for their package was not complete. Both nil
	// when the scan stopped before its answers were in.
	TargetsRejected *int
	ClosuresHeld    *int
	// TargetsTruncated (0.29.169) counts the sent targets whose answer was
	// longer than the page cap and was not read to its end.
	TargetsTruncated *int
	// DependenciesUnknown is nil when the scan's target reads did not
	// complete; otherwise whether nothing has ever said what the repository
	// depends on.
	DependenciesUnknown *bool
	// FirstScan asks the writer to set first_scan_at if it is not set: this
	// scan's outcome is ScanSent or ScanNothingToSend and the repository had
	// no scan stamp and no finding before it began. An instruction to the
	// writer; not read back.
	FirstScan bool
}

// InventoryEvidence is what is stored about a repository's dependencies and
// earlier scans, read once with a scan's target reads.
type InventoryEvidence struct {
	// InventoryRows and LockfileRows count the repository's stored
	// inventory rows and lockfile package rows.
	InventoryRows int
	LockfileRows  int
	// AnalysisHalf is true when a whole inventory has been written since
	// 0.29.159 (repo_dependency_coverage.analysed_at is set).
	AnalysisHalf bool
	// ScanStamp and Findings say whether the repository had a completed
	// scan stamp, and any finding, before this scan began.
	ScanStamp bool
	Findings  bool
}

// DependenciesUnknown reports whether nothing has ever said what the
// repository depends on: no stored inventory rows, no lockfile rows and no
// analysis half. It is the precondition of the closing rule (summary/70,
// section 3, rule 1): no dependency finding closes for such a repository.
func (e InventoryEvidence) DependenciesUnknown() bool {
	return e.InventoryRows == 0 && e.LockfileRows == 0 && !e.AnalysisHalf
}
