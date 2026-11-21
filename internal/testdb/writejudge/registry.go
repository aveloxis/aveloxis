// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package writejudge

import "github.com/aveloxis/aveloxis/internal/srctest/sqlscan"

// The ONE registry of column write policies and their Exceptions (SR-11).
// It was internal/db's test data until 0.29.213; it lives here so that the
// run-time judge of every DB-tier package and the store's static check
// (internal/db/column_write_policy_test.go) judge by the same entries.

// Policies is the registry. Reasons are REQUIRED and cite the
// release that established the policy.
var Policies = []sqlscan.Registered{
	{Table: "aveloxis_data.repos", Column: "platform_repo_id", Policy: sqlscan.FillEmptyOnly,
		Reason: "the forge numeric ID never changes for a repo; an overwrite would erase the delete-and-recreate mismatch signal (v0.27.116/117)"},
	{Table: "aveloxis_data.repos", Column: "created_at", Policy: sqlscan.FillEmptyOnly,
		Reason: "a repository's creation date is an immutable fact (v0.27.104)"},
	{Table: "aveloxis_data.repos", Column: "updated_at", Policy: sqlscan.GreatestNonNull,
		Reason: "the forge's last-update time only increases; out-of-order refreshes must not regress it (v0.27.122)"},
	{Table: "aveloxis_data.repos", Column: "added_at", Policy: sqlscan.InsertOnly,
		Reason: "fleet-entry timestamp — INSERT-only so it can never degrade into last-touched (v0.27.60)"},
	{Table: "aveloxis_data.repos", Column: "forked_from", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "an id-less re-upsert must not wipe captured lineage; empty incoming preserves stored (v0.27.78)"},
	// v0.29.68 (worklist follow-up 8, review rounds 1-2): the three
	// collection-side metadata columns are INSERT-only for the identity
	// writer. UpsertRepo's conflict arm wrote them from its callers' zero
	// values (no caller carries them from the forge), blanking a collected
	// repository on every re-add; Phase 0's UpdateRepoMetadata is the
	// collection writer and the lifecycle writers (SidelineRepo, the
	// v0.27.50 migrate step) set repo_archived — each an Exception.
	{Table: "aveloxis_data.repos", Column: "repo_description", Policy: sqlscan.InsertOnly,
		Reason: "collected by Phase 0 (UpdateRepoMetadata) every cycle; the identity upsert never carries it, so its conflict arm must not touch it (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repos", Column: "primary_language", Policy: sqlscan.InsertOnly,
		Reason: "same as repo_description (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", Policy: sqlscan.InsertOnly,
		Reason: "collected by Phase 0 every cycle, both directions; set by the lifecycle writers on their own paths; the identity upsert never carries it (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repo_deps_vulnerabilities", Column: "first_detected_at", Policy: sqlscan.EarliestOfStoredAndIncoming,
		Reason: "v0.29.166: a finding's first detection never moves forward; a successor is handed its predecessor's date (summary/70, 4.3), and a plain re-observation offers NOW(), which is never earlier"},
	{Table: "aveloxis_data.repo_deps_vulnerabilities", Column: "entered_exposure_at", Policy: sqlscan.KeepCarryOrNow,
		Reason: "v0.29.166: a write may keep the stored time, take a predecessor's carried one, or stamp now, by the finding upsert's class-transition CASE; WHEN each applies is pinned by TestEnteredExposureIsSetWhenTheClassBecomesExposure and TestEnteredExposureIsCarried"},
	{Table: "aveloxis_data.repo_deps_libyear", Column: "package_key", Policy: sqlscan.FillEmptyOnly,
		Reason: "v0.29.173: written on insert (0.29.172) and never changed; the fill's UPDATE is guarded on the key being empty, so it cannot overwrite a key or race the insert"},
	{Table: "aveloxis_data.repo_lockfile_packages", Column: "package_key", Policy: sqlscan.FillEmptyOnly,
		Reason: "v0.29.173: as repo_deps_libyear.package_key"},
	{Table: "aveloxis_data.repos", Column: "vuln_scan_last_run", Policy: sqlscan.AlwaysRefresh,
		Reason: "completed-scan stamp — every completed vulnerability scan re-stamps NOW(); never written on error paths (v0.28.1 A4)"},
	{Table: "aveloxis_data.messages", Column: "msg_text_clean", Policy: sqlscan.AlwaysRefresh,
		Reason: "quote-stripped body — recomputed from the incoming raw body on every email upsert (Part B); a stale clean beside a refreshed raw would silently serve old text through the COALESCE read path"},
	{Table: "aveloxis_data.messages", Column: "msg_text_clean_rule", Policy: sqlscan.AlwaysRefresh,
		Reason: "the pattern-library version that produced msg_text_clean — travels with it on every write so --rule-rerun can find stale-rule rows"},
	{Table: "aveloxis_data.messages", Column: "msg_updated", Policy: sqlscan.GreatestNonNull,
		Reason: "the provider's comment edit time only increases; a stale replayed envelope must not regress it — it is the comparator the msg_text freshness guard reads (round 14)"},
	{Table: "aveloxis_data.contributors", Column: "cntrb_login", Policy: sqlscan.FillEmptyOnly,
		Reason: "R2 audit trail — the login as FIRST observed, so an established value never changes. " +
			"Tightened from PreferNonemptyIncoming on 2026-09-11 (F4): the commit-resolver branch that " +
			"re-wrote it was the sole source of all 555 idx_contributors_login violations in the " +
			"2026-09-06..09-11 production log and is gone, and the two remaining writers — the " +
			"ON CONFLICT (cntrb_id) race arms in commit_resolver_store.go and contributors.go — now " +
			"preserve the stored login and fill only an empty one, which also makes them structurally " +
			"unable to trip the partial unique index (v0.18.29 Fix 3, v0.22.0)"},
	{Table: "aveloxis_data.contributor_identities", Column: "node_id", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "pre-v0.27.103 GraphQL-rail rows carry empty node_id; a non-empty observation heals them, empty never clobbers (v0.27.103)"},
	{Table: "aveloxis_data.contributor_identities", Column: "user_type", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "same healing contract as node_id (v0.27.103)"},
	// v0.29.118 GitHub traffic (aveloxis/aveloxis#234): the latest read
	// wins. GitHub can still revise the newest day of a read, so a day,
	// a 14-day window or a week read again takes the later read's
	// figures and the later collection date.
	{Table: "aveloxis_data.repo_clones", Column: "total_clones", Policy: sqlscan.AlwaysRefresh,
		Reason: "latest read of the day wins: GitHub revises the newest day after the first read (v0.29.118)"},
	{Table: "aveloxis_data.repo_clones", Column: "unique_clones", Policy: sqlscan.AlwaysRefresh,
		Reason: "as total_clones; a read without a distinct count (NULL, never 0) replaces a stored number too, since the later read is the one that counts (v0.29.118)"},
	{Table: "aveloxis_data.repo_clones", Column: "data_collection_date", Policy: sqlscan.AlwaysRefresh,
		Reason: "the day's figures are those of the read made at this date (v0.29.118)"},
	{Table: "aveloxis_data.repo_views", Column: "total_views", Policy: sqlscan.AlwaysRefresh,
		Reason: "latest read of the day wins, as repo_clones (v0.29.118)"},
	{Table: "aveloxis_data.repo_views", Column: "unique_views", Policy: sqlscan.AlwaysRefresh,
		Reason: "as total_views (v0.29.118)"},
	{Table: "aveloxis_data.repo_views", Column: "data_collection_date", Policy: sqlscan.AlwaysRefresh,
		Reason: "as repo_clones.data_collection_date (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_windows", Column: "uniques", Policy: sqlscan.AlwaysRefresh,
		Reason: "a window read again takes the later read's distinct count (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_windows", Column: "data_collection_date", Policy: sqlscan.AlwaysRefresh,
		Reason: "as repo_clones.data_collection_date (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_weeks", Column: "uniques", Policy: sqlscan.AlwaysRefresh,
		Reason: "a complete week lies in the window of 7 to 8 daily reads; the later read's distinct count wins (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_weeks", Column: "data_collection_date", Policy: sqlscan.AlwaysRefresh,
		Reason: "as repo_clones.data_collection_date (v0.29.118)"},
	// The path and referrer snapshot of a date is replaced as a whole
	// (delete the date's rows, then insert the read's), never edited in
	// place: an UPDATE would keep an entry that dropped out of the top
	// 10 between two reads of one day (v0.29.118).
	{Table: "aveloxis_data.repo_traffic_paths", Column: "title", Policy: sqlscan.InsertOnly,
		Reason: "snapshot row: replaced with its date's snapshot, never updated in place (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_paths", Column: "total_views", Policy: sqlscan.InsertOnly,
		Reason: "as title (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_paths", Column: "unique_views", Policy: sqlscan.InsertOnly,
		Reason: "as title (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_referrers", Column: "total_views", Policy: sqlscan.InsertOnly,
		Reason: "snapshot row, as repo_traffic_paths (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_referrers", Column: "unique_views", Policy: sqlscan.InsertOnly,
		Reason: "as total_views (v0.29.118)"},
}

// Exceptions are the deliberate, reviewed deviations. An entry that
// excuses nothing fails the run as STALE.
//
// Each PINS THE STATEMENT it excuses, whole (0.29.214): Statement is the
// statement as SQL, compared as PostgreSQL's parse tree (spacing, comments
// and the case of keywords aside), its WHERE included; Fragment says which
// assignment of it is the excused one. Before, an Exception gave the
// assigned expression alone, and went on excusing the statement after the
// WHERE its reason rested on was removed: the v0.27.50 step below, without
// `AND r.repo_gone_at IS NULL`, is the 0.29.140 regression that un-archived
// 426 gone repositories, and this registry would have let it through.
//
// To change an excused statement, change it here in the same commit, and
// read the Reason again while you do: it has to be true of the new text.
// The comments of the store's own copy are left out of these (a comment is
// not part of the tree).
var Exceptions = append(append([]sqlscan.Exception{
	{Table: "aveloxis_data.repos", Column: "platform_repo_id", Kind: sqlscan.KindAssign, File: "internal/db/repo_forge_id_changes.go", Decl: "PostgresStore.adoptForgeIDInTx",
		Fragment: "$3",
		Statement: `UPDATE aveloxis_data.repos SET platform_repo_id = $3
			WHERE repo_id = $1 AND platform_repo_id = $2`,
		Reason: "v0.29.62 AdoptForgeID — the operator's explicit adoption of a re-created upstream (2026-09-23 decision), guarded WHERE platform_repo_id = the expected old ID (pinned here: the statement without that condition is another statement) in the same transaction that records the change in repo_forge_id_changes; the mismatch signal the fill-empty policy protects is preserved there, not erased"},
	{Table: "aveloxis_data.repos", Column: "added_at", Kind: sqlscan.KindAssign, File: "internal/db/migrate.go", Decl: "migrateStage10RecentReleases",
		Fragment: "COALESCE(data_collection_date, created_at, NOW())",
		Statement: `UPDATE aveloxis_data.repos
			SET added_at = COALESCE(data_collection_date, created_at, NOW())
			WHERE added_at IS NULL`,
		Reason: "the v0.27.60 one-shot backfill for pre-column rows — WHERE added_at IS NULL (pinned here) makes it self-disabling, so it can never degrade a real stamp"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", Kind: sqlscan.KindAssign, File: "internal/db/repo_gone.go", Decl: "PostgresStore.SidelineRepo",
		Fragment: "TRUE",
		Statement: `WITH
			marked AS (
			UPDATE aveloxis_data.repos
			SET repo_archived = TRUE, repo_gone_at = NOW(), repo_gone_checked_at = NOW(),
			data_collection_date = NOW(), data_changed_at = NOW()
			WHERE repo_id = $1
			RETURNING repo_id, repo_gone_at
			),
			episode AS (
			INSERT INTO aveloxis_data.repo_gone_history (repo_id, gone_at, gone_status, gone_source)
			SELECT repo_id, repo_gone_at, $2, $3 FROM marked
			ON CONFLICT (repo_id) WHERE restored_at IS NULL DO NOTHING
			RETURNING repo_id
			)
			SELECT COUNT(*) FROM episode`,
		Reason: "SidelineRepo — the gone lifecycle sets the flag with the gone stamp (v0.29.68, follow-up 8: a lifecycle writer)"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", Kind: sqlscan.KindAssign, File: "internal/db/migrate.go", Decl: "migrateStage10RecentReleases",
		Fragment: "(latest.status = 'Archived')",
		Statement: `UPDATE aveloxis_data.repos r
			SET repo_archived = (latest.status = 'Archived')
			FROM (
			SELECT DISTINCT ON (repo_id) repo_id, status
			FROM aveloxis_data.repo_info
			ORDER BY repo_id, data_collection_date DESC NULLS LAST, repo_info_id DESC
			) latest
			WHERE r.repo_id = latest.repo_id
			AND r.repo_gone_at IS NULL
			AND COALESCE(r.repo_archived, FALSE) IS DISTINCT FROM (latest.status = 'Archived')`,
		Reason: "the v0.27.50 one-shot backfill from repo_info.status for rows collected before Phase 0 propagated the flag. It must skip gone rows (AND r.repo_gone_at IS NULL, pinned here): a gone repository's flag is the gone lifecycle's, and the step without that condition is the 0.29.140 regression"},
	// RETIRED (2026-09-11, F4): the cntrb_login exception on
	// commit_resolver_store.go's row-exists-by-deterministic-ID branch
	// is gone because the WRITE is gone. Its reason argued the
	// freshly-resolved login "IS the truth" for a cntrb_id-keyed row —
	// true about the person, but cntrb_login is not the person's
	// current name, it is the first-observed one (R2), and gh_login
	// already carries the current name. Production settled the
	// argument: that statement produced all 555 idx_contributors_login
	// unique violations in the 2026-09-06..09-11 log, and its recovery
	// dropped the gh_login update too, so every affected rename was
	// lost outright. Deliberately NOT re-added as a differently-worded
	// exception — the code now agrees with the doc.
	{Table: "aveloxis_data.contributor_identities", Column: "node_id", Kind: sqlscan.KindAssign, File: "internal/db/contributors.go", Decl: "ContributorResolver.Resolve",
		Fragment: "COALESCE(NULLIF(node_id, ''), $3)", Statement: identityHealStatement,
		Reason: "the v0.27.112 step-2 IN-PLACE heal deliberately fills empties only — node_id never changes for a platform user, so fill-empty is a strictly more conservative shape than the registered prefer-nonempty (which governs the full upserts)"},
	{Table: "aveloxis_data.contributor_identities", Column: "user_type", Kind: sqlscan.KindAssign, File: "internal/db/contributors.go", Decl: "ContributorResolver.Resolve",
		Fragment: "COALESCE(NULLIF(user_type, ''), $4)", Statement: identityHealStatement,
		Reason: "same in-place-heal rationale as node_id (v0.27.112)"},
	// Operator decision, 2026-11-19 (option 1 of three): the pre-1970 repair
	// (cleanupBadTimestamps, a ledgered migrate step) clears a stored
	// created_at / updated_at, which each column's policy forbids. The
	// exception holds only while the statement's own condition limits it to
	// a year before 1970, which no forge date has (the value is Go's zero
	// time, written by an old bug). The next collection then stores the
	// real date, which both policies allow over an empty value.
	{Table: "aveloxis_data.repos", Column: "created_at", Kind: sqlscan.KindAssign, File: "internal/db/migrate.go", Decl: "cleanupBadTimestamps", Fragment: "NULL",
		Statement: `UPDATE aveloxis_data.repos SET "created_at" = NULL WHERE "created_at" IS NOT NULL AND EXTRACT(YEAR FROM "created_at") < 1970`,
		Reason:    "one-time repair of a value that is not a date (year before 1970); the condition is in the statement and the whole statement is matched"},
	{Table: "aveloxis_data.repos", Column: "updated_at", Kind: sqlscan.KindAssign, File: "internal/db/migrate.go", Decl: "cleanupBadTimestamps", Fragment: "NULL",
		Statement: `UPDATE aveloxis_data.repos SET "updated_at" = NULL WHERE "updated_at" IS NOT NULL AND EXTRACT(YEAR FROM "updated_at") < 1970`,
		Reason:    "one-time repair of a value that is not a date (year before 1970); the condition is in the statement and the whole statement is matched"},
}, updateRepoMetadataExceptions...), goneClearExceptions()...)

// identityHealStatement is ContributorResolver.Resolve's step-2 heal, which
// two Exceptions above excuse (one per column it fills).
const identityHealStatement = `UPDATE aveloxis_data.contributor_identities
	SET node_id = COALESCE(NULLIF(node_id, ''), $3),
	user_type = COALESCE(NULLIF(user_type, ''), $4)
	WHERE platform_id = $1 AND platform_user_id = $2`

// updateRepoMetadataStatement is Phase 0's one statement
// (PostgresStore.UpdateRepoMetadata): the collection writer of four columns
// the identity upsert may not touch. v0.29.68 (follow-up 8): the three
// collection-side columns are InsertOnly for the identity upsert; these are
// their real writers.
const updateRepoMetadataStatement = `UPDATE aveloxis_data.repos
	SET repo_description = $2,
	primary_language = $3,
	languages        = $4::jsonb,
	repo_archived    = $5,
	forked_from      = $6,
	platform_repo_id = COALESCE(NULLIF(repos.platform_repo_id, ''), $7),
	created_at = COALESCE(repos.created_at, $8),
	data_changed_at = CASE WHEN repos.created_at IS NULL AND $8::timestamptz IS NOT NULL THEN NOW() ELSE repos.data_changed_at END,
	updated_at = GREATEST(COALESCE($9, repos.updated_at), COALESCE(repos.updated_at, $9)),
	data_collection_date = NOW()
	WHERE repo_id = $1
	RETURNING COALESCE(platform_repo_id, '')`

var updateRepoMetadataExceptions = []sqlscan.Exception{
	{Table: "aveloxis_data.repos", Column: "repo_description", Kind: sqlscan.KindAssign, File: "internal/db/repo_metadata.go", Decl: "PostgresStore.UpdateRepoMetadata",
		Fragment: "$2", Statement: updateRepoMetadataStatement,
		Reason: "Phase 0's UpdateRepoMetadata is the collection writer: the forge's current description every cycle, an emptied one included (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repos", Column: "primary_language", Kind: sqlscan.KindAssign, File: "internal/db/repo_metadata.go", Decl: "PostgresStore.UpdateRepoMetadata",
		Fragment: "$3", Statement: updateRepoMetadataStatement,
		Reason: "Phase 0's UpdateRepoMetadata, as repo_description (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", Kind: sqlscan.KindAssign, File: "internal/db/repo_metadata.go", Decl: "PostgresStore.UpdateRepoMetadata",
		Fragment: "$5", Statement: updateRepoMetadataStatement,
		Reason: "Phase 0's UpdateRepoMetadata propagates the forge's archived status both directions (v0.27.50)"},
	{Table: "aveloxis_data.repos", Column: "forked_from", Kind: sqlscan.KindAssign, File: "internal/db/repo_metadata.go", Decl: "PostgresStore.UpdateRepoMetadata",
		Fragment: "$6", Statement: updateRepoMetadataStatement,
		Reason: "Phase 0's FetchRepoInfo is AUTHORITATIVE for fork lineage — a repo detaching from its upstream honestly clears the column (v0.27.78); the prefer-nonempty policy protects the id-less org-scan re-upsert path only"},
}

// goneClearStatements are the statements clearInTxUnderRowLock runs, one
// for each KIND of clearer of the gone stamp (the store's goneClear values:
// what the row must be, and whether the repository is queued again). The
// store builds each from a goneClear; each is pinned here whole, so a
// clearer added or changed is a statement no Exception names until a
// person has read it. Every one puts repo_archived back to the forge's
// latest snapshot with the same expression.
var goneClearStatements = []string{
	`WITH
		cleared AS (
		UPDATE aveloxis_data.repos
		SET repo_gone_at = NULL, repo_gone_checked_at = NULL,
		repo_unavailable_reason = NULL, repo_unavailable_url = NULL,
		repo_archived = COALESCE((SELECT ri.status = 'Archived' FROM aveloxis_data.repo_info ri
		WHERE ri.repo_id = $1
		ORDER BY ri.data_collection_date DESC NULLS LAST, ri.repo_info_id DESC
		LIMIT 1), FALSE),
		data_changed_at = NOW()
		WHERE repo_id = $1 AND repo_gone_at IS NULL AND COALESCE(repo_archived, FALSE)
		AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue q WHERE q.repo_id = $1)
		RETURNING repo_id
		),
		closed AS (
		UPDATE aveloxis_data.repo_gone_history h
		SET restored_at = NOW(), restored_source = $2
		FROM cleared
		WHERE h.repo_id = cleared.repo_id AND h.restored_at IS NULL
		RETURNING h.repo_id
		)
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
		SELECT $1, $3, 'queued', NOW()
		WHERE $4::boolean OR EXISTS (SELECT 1 FROM cleared)
		ON CONFLICT (repo_id) DO UPDATE SET
		priority = LEAST(collection_queue.priority, EXCLUDED.priority),
		status = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.status ELSE 'queued' END,
		due_at = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.due_at ELSE NOW() END,
		updated_at = NOW()`,
	`WITH
		cleared AS (
		UPDATE aveloxis_data.repos
		SET repo_gone_at = NULL, repo_gone_checked_at = NULL,
		repo_unavailable_reason = NULL, repo_unavailable_url = NULL,
		repo_archived = COALESCE((SELECT ri.status = 'Archived' FROM aveloxis_data.repo_info ri
		WHERE ri.repo_id = $1
		ORDER BY ri.data_collection_date DESC NULLS LAST, ri.repo_info_id DESC
		LIMIT 1), FALSE),
		data_changed_at = NOW()
		WHERE repo_id = $1 AND repo_gone_at IS NOT NULL
		RETURNING repo_id
		),
		closed AS (
		UPDATE aveloxis_data.repo_gone_history h
		SET restored_at = NOW(), restored_source = $2
		FROM cleared
		WHERE h.repo_id = cleared.repo_id AND h.restored_at IS NULL
		RETURNING h.repo_id
		)
		SELECT COUNT(*) FROM closed`,
	`WITH
		cleared AS (
		UPDATE aveloxis_data.repos
		SET repo_gone_at = NULL, repo_gone_checked_at = NULL,
		repo_unavailable_reason = NULL, repo_unavailable_url = NULL,
		repo_archived = COALESCE((SELECT ri.status = 'Archived' FROM aveloxis_data.repo_info ri
		WHERE ri.repo_id = $1
		ORDER BY ri.data_collection_date DESC NULLS LAST, ri.repo_info_id DESC
		LIMIT 1), FALSE),
		data_changed_at = NOW()
		WHERE repo_id = $1 AND repo_gone_at IS NOT NULL
		RETURNING repo_id
		),
		closed AS (
		UPDATE aveloxis_data.repo_gone_history h
		SET restored_at = NOW(), restored_source = $2
		FROM cleared
		WHERE h.repo_id = cleared.repo_id AND h.restored_at IS NULL
		RETURNING h.repo_id
		)
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
		SELECT $1, $3, 'queued', NOW()
		WHERE $4::boolean OR EXISTS (SELECT 1 FROM cleared)
		ON CONFLICT (repo_id) DO UPDATE SET
		priority = LEAST(collection_queue.priority, EXCLUDED.priority),
		status = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.status ELSE 'queued' END,
		due_at = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.due_at ELSE NOW() END,
		updated_at = NOW()`,
	`WITH
		cleared AS (
		UPDATE aveloxis_data.repos
		SET repo_gone_at = NULL, repo_gone_checked_at = NULL,
		repo_unavailable_reason = NULL, repo_unavailable_url = NULL,
		repo_archived = COALESCE((SELECT ri.status = 'Archived' FROM aveloxis_data.repo_info ri
		WHERE ri.repo_id = $1
		ORDER BY ri.data_collection_date DESC NULLS LAST, ri.repo_info_id DESC
		LIMIT 1), FALSE),
		data_changed_at = NOW()
		WHERE repo_id = $1 AND repo_gone_at IS NOT NULL AND platform_repo_id = $5
		AND NOT EXISTS (
		SELECT 1 FROM (SELECT h.restored_source, h.restored_at FROM aveloxis_data.repo_gone_history h
		WHERE h.repo_id = $1 AND h.restored_at IS NOT NULL
		ORDER BY h.restored_at DESC, h.episode_id DESC LIMIT 1) last
		WHERE last.restored_source = 'org-scan'
		AND NOT EXISTS (SELECT 1 FROM aveloxis_data.repo_info ri
		WHERE ri.repo_id = $1 AND ri.data_collection_date > last.restored_at
		UNION ALL
		SELECT 1 FROM aveloxis_data.repo_info_history rh
		WHERE rh.repo_id = $1 AND rh.data_collection_date > last.restored_at))
		RETURNING repo_id
		),
		closed AS (
		UPDATE aveloxis_data.repo_gone_history h
		SET restored_at = NOW(), restored_source = $2
		FROM cleared
		WHERE h.repo_id = cleared.repo_id AND h.restored_at IS NULL
		RETURNING h.repo_id
		)
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
		SELECT $1, $3, 'queued', NOW()
		WHERE $4::boolean OR EXISTS (SELECT 1 FROM cleared)
		ON CONFLICT (repo_id) DO UPDATE SET
		priority = LEAST(collection_queue.priority, EXCLUDED.priority),
		status = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.status ELSE 'queued' END,
		due_at = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.due_at ELSE NOW() END,
		updated_at = NOW()`,
}

func goneClearExceptions() []sqlscan.Exception {
	var out []sqlscan.Exception
	for _, st := range goneClearStatements {
		out = append(out, sqlscan.Exception{Table: "aveloxis_data.repos", Column: "repo_archived", Kind: sqlscan.KindAssign, File: "internal/db/repo_gone.go", Decl: "clearInTxUnderRowLock",
			Fragment: `COALESCE((SELECT ri.status = 'Archived' FROM aveloxis_data.repo_info ri
				WHERE ri.repo_id = $1
				ORDER BY ri.data_collection_date DESC NULLS LAST, ri.repo_info_id DESC
				LIMIT 1), FALSE)`,
			Statement: st,
			Reason:    "clearGoneCTE — every clearer of the gone stamp puts the flag back to the forge's latest snapshot (0.29.140, Copilot PR #252 eighth review: it stayed TRUE after a recovery)"})
	}
	return out
}

// TempTables are session tables that one text of the store creates and
// ANOTHER writes (two Exec calls on one connection), by bare name. A write
// of pg_temp.<name> is then a write of that session's own table and not of
// the schema (sqlscan.Catalog.Temp; the static check and the run-time judge
// both declare these). A temporary table created and written in ONE text
// needs no entry. None today: the store uses no temporary table.
var TempTables = []string{}
