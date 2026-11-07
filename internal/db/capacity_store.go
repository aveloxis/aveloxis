// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.89 (summary/53): the store's side of account capacity. The model
// and every decision live in internal/capacity; here are the stored quotas
// (one row each), the contact address, per-account overrides, the shared
// per-day request counts, and the repository allocation at the writers of
// a new group link (linkWithinCap, addPasteWithinCap), which both decide
// new repositories through decideWithinCap.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/jackc/pgx/v5"
)

// seedCapacityContactPlaceholder: the row is seeded with NO address. A new
// install takes mail.operator_email through SeedCapacityContact (serve and
// migrate call it); a row an administrator set, or one seeded by 0.29.89
// with the Aveloxis project's address, is left alone (C6, stage 1f of
// summary/59: a self-hoster must never send users to aveloxis.io).
const seedCapacityContactPlaceholder = ""

// validContactEmail is the ONE rule for the refusal contact address
// (0.29.106, whole-branch review DB-F1: the seed and the administrator's
// save each spelled it): a plausible address of at most
// MaxContactEmailBytes with nothing that could break a header or a page,
// and no ?, & or % — the address becomes a mailto link, which must not
// carry injected parameters (ASVS V1.2.2). The empty address is the
// caller's decision (the save clears; the seed seeds nothing).
func validContactEmail(email string) bool {
	return len(email) <= MaxContactEmailBytes && strings.Contains(email, "@") && !strings.ContainsAny(email, "\r\n<>\"' ?&%")
}

// SeedCapacityContact fills the refusal contact from mail.operator_email on
// a deployment whose contact was never set (empty, no administrator's
// stamp). A row an administrator saved, or 0.29.89's seed, is left alone;
// an empty address seeds nothing. Reports whether it wrote.
func (s *PostgresStore) SeedCapacityContact(ctx context.Context, operatorEmail string) (bool, error) {
	operatorEmail = strings.TrimSpace(operatorEmail)
	if operatorEmail == "" {
		return false, nil
	}
	if !validContactEmail(operatorEmail) {
		return false, fmt.Errorf("seed capacity contact from mail.operator_email: %w", ErrInvalidCapacitySettings)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.capacity_settings SET contact_email = $1, updated_at = NOW()
		WHERE id = 1 AND contact_email = '' AND updated_by IS NULL`, operatorEmail)
	if err != nil {
		return false, fmt.Errorf("seed capacity contact: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// capacityConfig is a store's own capacity words, when set (tests); a
// store without them uses this process's aveloxis.json words.
type capacityConfig struct {
	mu      sync.RWMutex
	sources map[string]capacity.Source // nil: the process's words
}

// processCapacitySources are this process's aveloxis.json "capacity" words
// (summary/53 §9), set once by the command's config load
// (SetProcessCapacitySources) and read by every store the process opens.
var processCapacitySources atomic.Pointer[map[string]capacity.Source]

// SetProcessCapacitySources records this process's aveloxis.json
// "capacity" words; nil clears them (every quota WEB). cmd/aveloxis's
// loadConfig is the one caller outside tests.
func SetProcessCapacitySources(sources map[string]capacity.Source) {
	if sources == nil {
		processCapacitySources.Store(nil)
		return
	}
	m := make(map[string]capacity.Source, len(sources))
	for k, v := range sources {
		m[k] = v
	}
	processCapacitySources.Store(&m)
}

// SetCapacitySources gives this store its own words in place of the
// process's (tests); nil goes back to the process's.
func (s *PostgresStore) SetCapacitySources(sources map[string]capacity.Source) {
	s.capacity.mu.Lock()
	defer s.capacity.mu.Unlock()
	if sources == nil {
		s.capacity.sources = nil
		return
	}
	s.capacity.sources = make(map[string]capacity.Source, len(sources))
	for k, v := range sources {
		s.capacity.sources[k] = v
	}
}

// CapacitySource is the word in effect for one quota: the store's own,
// else the process's aveloxis.json word, else WEB.
func (s *PostgresStore) CapacitySource(name string) capacity.Source {
	s.capacity.mu.RLock()
	own := s.capacity.sources
	s.capacity.mu.RUnlock()
	if own != nil {
		if src, ok := own[name]; ok {
			return src
		}
		return capacity.SourceWeb
	}
	if p := processCapacitySources.Load(); p != nil {
		if src, ok := (*p)[name]; ok {
			return src
		}
	}
	return capacity.SourceWeb
}

// LogCapacityInForce logs each quota as this process applies it (the
// EFFECTIVE value and mode, after the aveloxis.json word; SR-10). A failed
// read is logged as such.
func (s *PostgresStore) LogCapacityInForce(ctx context.Context, component string) {
	quotas, err := s.EffectiveCapacityQuotas(ctx)
	if errors.Is(err, context.Canceled) {
		return // a stop during startup, not a failure
	}
	if err != nil {
		s.logger.Warn("could not read the capacity quotas for the startup log", "component", component, "error", err)
		return
	}
	for _, name := range capacity.QuotaNames() {
		q := quotas[name]
		s.logger.Info("capacity quota in force", "component", component, "quota", name,
			"allowed", q.Allowed, "mode", string(q.Mode), "source", string(q.Source))
	}
}

// GetCapacityQuotas reads the stored quota rows (name → value and mode).
func (s *PostgresStore) GetCapacityQuotas(ctx context.Context) (map[string]capacity.Stored, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, allowed, mode FROM aveloxis_ops.capacity_quotas`)
	if err != nil {
		return nil, fmt.Errorf("read capacity quotas: %w", err)
	}
	defer rows.Close()
	out := map[string]capacity.Stored{}
	for rows.Next() {
		var name, mode string
		var allowed int
		if err := rows.Scan(&name, &allowed, &mode); err != nil {
			return nil, fmt.Errorf("read capacity quotas: %w", err)
		}
		m, err := capacity.ParseMode(mode)
		if err != nil {
			return nil, err
		}
		out[name] = capacity.Stored{Allowed: allowed, Mode: m}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read capacity quotas: %w", err)
	}
	return out, nil
}

// EffectiveCapacityQuotas applies this process's config words to the
// stored rows: every quota, as applied.
func (s *PostgresStore) EffectiveCapacityQuotas(ctx context.Context) (map[string]capacity.EffectiveQuota, error) {
	stored, err := s.GetCapacityQuotas(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]capacity.EffectiveQuota, len(capacity.Shipped))
	for _, name := range capacity.QuotaNames() {
		var st *capacity.Stored
		if v, ok := stored[name]; ok {
			st = &v
		}
		out[name] = capacity.Effective(name, s.CapacitySource(name), st)
	}
	return out, nil
}

// ErrInvalidCapacitySettings is a capacity value out of range, an unknown
// quota, or a quota this process's config does not let the page change.
var ErrInvalidCapacitySettings = errors.New("invalid capacity settings")

// SetCapacityQuota saves one quota's value and mode (the Capacity page).
// Only a quota whose config word is WEB may be changed.
func (s *PostgresStore) SetCapacityQuota(ctx context.Context, name string, allowed int, mode capacity.Mode, by int) error {
	if err := s.validateCapacityQuota(name, allowed, mode); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("save capacity quota: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.writeCapacityQuotasTx(ctx, tx, []quotaWrite{{name: name, allowed: allowed, mode: mode}}, by); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("save capacity quota: %w", err)
	}
	return nil
}

// validateCapacityQuota is the one admission rule for a quota write: a
// shipped quota, a positive value, a known mode, and a config word of WEB.
// It reads only this process's config (no pool read), so the writer can
// run it inside a transaction.
func (s *PostgresStore) validateCapacityQuota(name string, allowed int, mode capacity.Mode) error {
	if _, ok := capacity.Shipped[name]; !ok || allowed <= 0 || s.CapacitySource(name) != capacity.SourceWeb {
		return ErrInvalidCapacitySettings
	}
	if _, err := capacity.ParseMode(string(mode)); err != nil {
		return ErrInvalidCapacitySettings
	}
	return nil
}

// quotaWrite is one capacity_quotas row to save.
type quotaWrite struct {
	name    string
	allowed int
	mode    capacity.Mode
}

// writeCapacityQuotasTx is the ONE writer of capacity_quotas: the upserts
// and the rollback mirror inside the caller's transaction. SetCapacityQuota
// wraps it for one quota; SetAPITokenSettings writes its two quotas and
// the lifetime through it in one transaction (Copilot review 5485402444:
// separate commits left a mix the administrator never submitted).
//
// The admission rule runs HERE (SR-18): every write is validated before
// any is made, so a caller cannot land an unknown or a config-owned quota
// by skipping it (the callers' earlier call is the refuse-before-Begin
// fast path).
//
// Lock order (L10 round 3 F1): the quota rows are written in NAME order
// and api_token_settings — the mirror here, the lifetime in
// SetAPITokenSettings — only after the last quota row. Every transaction
// that touches these rows takes them in that one order, so two
// administrators saving at once wait on each other at most once and never
// in a cycle (40P01). Writing the mirror between two quota rows did
// deadlock a hour+day save against a day-only save.
func (s *PostgresStore) writeCapacityQuotasTx(ctx context.Context, tx pgx.Tx, writes []quotaWrite, by int) error {
	for _, w := range writes {
		if err := s.validateCapacityQuota(w.name, w.allowed, w.mode); err != nil {
			return err
		}
	}
	ordered := append([]quotaWrite(nil), writes...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].name < ordered[j].name })
	mirrorHour := 0
	for _, w := range ordered {
		if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_ops.capacity_quotas (name, allowed, mode, updated_at, updated_by)
		VALUES ($1, $2, $3, NOW(), $4)
		ON CONFLICT (name) DO UPDATE SET allowed = EXCLUDED.allowed, mode = EXCLUDED.mode,
		    updated_at = NOW(), updated_by = EXCLUDED.updated_by`, w.name, w.allowed, string(w.mode), nullIfZero(by)); err != nil {
			return fmt.Errorf("save capacity quota: %w", err)
		}
		if w.name == capacity.QuotaTokenRequestsPerHour {
			mirrorHour = w.allowed
		}
		if s.capacityQuotaWritten != nil {
			s.capacityQuotaWritten(w.name)
		}
	}
	// The rollback mirror (v0.29.89): a 0.29.82-0.29.88 binary reads its
	// API-token default from api_token_settings. Last, after every quota
	// row (the lock order above).
	if mirrorHour > 0 {
		if _, err := tx.Exec(ctx, `UPDATE aveloxis_ops.api_token_settings SET default_rate_limit_per_hour = $1 WHERE id = 1`, mirrorHour); err != nil {
			return fmt.Errorf("save capacity quota: rollback mirror: %w", err)
		}
	}
	return nil
}

// ErrCapacitySettingsMissing is the capacity_settings row absent: the
// database has not been migrated to this version.
var ErrCapacitySettingsMissing = errors.New("the capacity settings row is missing (run aveloxis migrate)")

// MaxContactEmailBytes bounds the contact address an administrator saves.
const MaxContactEmailBytes = 254

// GetCapacityContact is the address heavy users are asked to write to.
func (s *PostgresStore) GetCapacityContact(ctx context.Context) (string, error) {
	var c string
	if err := s.pool.QueryRow(ctx, `SELECT contact_email FROM aveloxis_ops.capacity_settings WHERE id = 1`).Scan(&c); err != nil {
		// A missing settings row is a deployment that has not migrated, not
		// an empty contact (SR-5): said the way SetCapacityContact says it
		// (v0.29.91: the bare "no rows in result set" failed every link).
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrCapacitySettingsMissing
		}
		return "", fmt.Errorf("read capacity contact: %w", err)
	}
	return c, nil
}

// SetCapacityContact saves it: empty (cleared), or an address
// validContactEmail accepts.
func (s *PostgresStore) SetCapacityContact(ctx context.Context, email string, by int) error {
	email = strings.TrimSpace(email)
	if email != "" && !validContactEmail(email) {
		return ErrInvalidCapacitySettings
	}
	tag, err := s.pool.Exec(ctx, `UPDATE aveloxis_ops.capacity_settings SET contact_email = $1, updated_at = NOW(), updated_by = $2 WHERE id = 1`,
		email, nullIfZero(by))
	if err != nil {
		return fmt.Errorf("save capacity contact: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("save capacity contact: %w", ErrCapacitySettingsMissing)
	}
	return nil
}

// CapacityOverride is an administrator's change to one account's quotas; a
// nil field means "the quota's value".
type CapacityOverride struct {
	UserID          int       `json:"user_id"`
	ReposAllowed    *int      `json:"repos_allowed"`
	RequestsPerHour *int      `json:"requests_per_hour"`
	RequestsPerDay  *int      `json:"requests_per_day"`
	LinksPerDay     *int      `json:"links_per_day"`
	Note            string    `json:"note"`
	UpdatedAt       time.Time `json:"updated_at"`
	UpdatedBy       *int      `json:"updated_by"`
}

// MaxCapacityNoteLength bounds an override's note, in characters.
const MaxCapacityNoteLength = 500

// SetCapacityOverride saves one account's overrides; no values and no note
// remove the row (the account is back on the quotas' values).
func (s *PostgresStore) SetCapacityOverride(ctx context.Context, o CapacityOverride, by int) error {
	for _, v := range []*int{o.ReposAllowed, o.RequestsPerHour, o.RequestsPerDay, o.LinksPerDay} {
		if v != nil && (*v <= 0 || *v > math.MaxInt32) { // the columns are INT
			return ErrInvalidCapacitySettings
		}
	}
	if len([]rune(o.Note)) > MaxCapacityNoteLength {
		return ErrInvalidCapacitySettings
	}
	if o.ReposAllowed == nil && o.RequestsPerHour == nil && o.RequestsPerDay == nil && o.LinksPerDay == nil && strings.TrimSpace(o.Note) == "" {
		// Removing the overrides of an account that does not exist is
		// ErrUserNotFound, as saving them is (58 §7 item 17: it answered
		// {saved: true}); an account with no overrides is a no-op.
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM aveloxis_ops.users WHERE user_id = $1)`, o.UserID).Scan(&exists); err != nil {
			return fmt.Errorf("remove capacity override: %w", err)
		}
		if !exists {
			return ErrUserNotFound
		}
		if _, err := s.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_capacity WHERE user_id = $1`, o.UserID); err != nil {
			return fmt.Errorf("remove capacity override: %w", err)
		}
		return nil
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_capacity (user_id, repos_allowed, requests_per_hour, requests_per_day, links_per_day, note, updated_at, updated_by)
		SELECT u.user_id, $2, $3, $4, $7, $5, NOW(), $6 FROM aveloxis_ops.users u WHERE u.user_id = $1
		ON CONFLICT (user_id) DO UPDATE SET repos_allowed = EXCLUDED.repos_allowed,
		    requests_per_hour = EXCLUDED.requests_per_hour, requests_per_day = EXCLUDED.requests_per_day,
		    links_per_day = EXCLUDED.links_per_day,
		    note = EXCLUDED.note, updated_at = NOW(), updated_by = EXCLUDED.updated_by`,
		o.UserID, o.ReposAllowed, o.RequestsPerHour, o.RequestsPerDay, strings.TrimSpace(o.Note), nullIfZero(by), o.LinksPerDay)
	if err != nil {
		return fmt.Errorf("save capacity override: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrUserNotFound
	}
	return nil
}

// GetCapacityOverride reads one account's overrides (nil fields: none).
func (s *PostgresStore) GetCapacityOverride(ctx context.Context, userID int) (CapacityOverride, error) {
	o := CapacityOverride{UserID: userID}
	err := s.pool.QueryRow(ctx, `
		SELECT repos_allowed, requests_per_hour, requests_per_day, links_per_day, note, updated_at, updated_by
		FROM aveloxis_ops.user_capacity WHERE user_id = $1`, userID).Scan(
		&o.ReposAllowed, &o.RequestsPerHour, &o.RequestsPerDay, &o.LinksPerDay, &o.Note, &o.UpdatedAt, &o.UpdatedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, nil
	}
	if err != nil {
		return CapacityOverride{}, fmt.Errorf("read capacity override: %w", err)
	}
	return o, nil
}

// ListCapacityOverrides reads every account's overrides (the api process's
// capacity policy reads them all at once: the table holds only the accounts
// an administrator changed).
func (s *PostgresStore) ListCapacityOverrides(ctx context.Context) (map[int]CapacityOverride, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT user_id, repos_allowed, requests_per_hour, requests_per_day, links_per_day, note, updated_at, updated_by
		FROM aveloxis_ops.user_capacity`)
	if err != nil {
		return nil, fmt.Errorf("list capacity overrides: %w", err)
	}
	defer rows.Close()
	out := map[int]CapacityOverride{}
	for rows.Next() {
		var o CapacityOverride
		if err := rows.Scan(&o.UserID, &o.ReposAllowed, &o.RequestsPerHour, &o.RequestsPerDay, &o.LinksPerDay, &o.Note, &o.UpdatedAt, &o.UpdatedBy); err != nil {
			return nil, fmt.Errorf("list capacity overrides: %w", err)
		}
		out[o.UserID] = o
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list capacity overrides: %w", err)
	}
	return out, nil
}

// AddRequestCounts is the capacity.Persister for UTC-day request counts:
// it adds each key's delta to the shared count for day and answers the
// totals (other api processes' requests included).
func (s *PostgresStore) AddRequestCounts(ctx context.Context, day time.Time, deltas map[string]int64) (map[string]int64, error) {
	// Sorted: two api processes saving overlapping subjects lock the rows
	// in the same order (review round 1 R2-3: map order risked 40P01).
	keys := make([]string, 0, len(deltas))
	for k := range deltas {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	vals := make([]int64, 0, len(deltas))
	for _, k := range keys {
		vals = append(vals, deltas[k])
	}
	rows, err := s.pool.Query(ctx, `
		INSERT INTO aveloxis_ops.request_counts (subject, day, requests)
		SELECT k, $1::date, v FROM unnest($2::text[], $3::bigint[]) AS t(k, v)
		ON CONFLICT (subject, day) DO UPDATE SET requests = aveloxis_ops.request_counts.requests + EXCLUDED.requests
		RETURNING subject, requests`, capacity.UTCDate(day), keys, vals)
	if err != nil {
		return nil, fmt.Errorf("add request counts: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64, len(deltas))
	for rows.Next() {
		var k string
		var n int64
		if err := rows.Scan(&k, &n); err != nil {
			return nil, fmt.Errorf("add request counts: %w", err)
		}
		out[k] = n
	}
	return out, rows.Err()
}

// PruneRequestCounts deletes per-day counts older than yesterday (UTC).
func (s *PostgresStore) PruneRequestCounts(ctx context.Context, now time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM aveloxis_ops.request_counts WHERE day < $1::date`,
		capacity.UTCDate(capacity.UTCDay.Start(now).AddDate(0, 0, -1)))
	if err != nil {
		return 0, fmt.Errorf("prune request counts: %w", err)
	}
	return tag.RowsAffected(), nil
}

// heldItemsSQL is the FROM/WHERE of account $1's add-request items still
// holding a place: an item not yet processed (repo_id IS NULL) of a pending
// or approved request in a group that is not rejected (operator decision
// 2026-10-10: pending counts). An approved item keeps its place until the
// approval pass links it (linkHeld), so the place a pending item held does
// not come free at the approval's flip.
const heldItemsSQL = `aveloxis_ops.collection_add_request_items i
	        JOIN aveloxis_ops.collection_add_requests r USING (request_id)
	        JOIN aveloxis_ops.user_groups g ON g.group_id = r.group_id
	        WHERE r.user_id = $1 AND r.kind = 'repos' AND r.status IN ('pending', 'approved')
	          AND i.repo_id IS NULL AND g.status IS DISTINCT FROM 'rejected'`

// normalizedRepoURLSQL is model.NormalizeRepoGitURL in SQL for a stored
// (already trimmed) URL: the trailing run of "/" and ".git" removed
// (TestItemURLNormalizationIsTheModelRule pins the two on a corpus).
func normalizedRepoURLSQL(urlExpr string) string {
	return `regexp_replace(` + urlExpr + `, '(/|\.git)+$', '')`
}

// resolvedRepoIDSQL is the repository a URL resolves to — FindRepoByURL's
// one match (it runs this fragment): the exact spelling, or the same apart
// from letter case on GitHub and GitLab (platforms 1 and 2), the exact one
// first, then the lowest id; NULL when none. urlExpr must already be
// normalized (model.NormalizeRepoGitURL in Go, normalizedRepoURLSQL in SQL).
// v0.29.91 review round 2 (R2-3, SR-17): the capacity SQL matched raw item
// text against raw repo_git, so an item approval resolves to a NEW
// repository could match a linked legacy row and ask no place.
func resolvedRepoIDSQL(urlExpr string) string {
	return `(SELECT rr.repo_id FROM aveloxis_data.repos rr
	        WHERE rr.repo_git = ` + urlExpr + ` OR (LOWER(rr.repo_git) = LOWER(` + urlExpr + `) AND rr.platform_id IN (1, 2))
	        ORDER BY (rr.repo_git = ` + urlExpr + `) DESC, rr.repo_id LIMIT 1)`
}

// itemRepoIDSQL is the repository an add item's stored URL will link at
// approval (NULL: approval creates it).
func itemRepoIDSQL(urlExpr string) string {
	return resolvedRepoIDSQL(normalizedRepoURLSQL(urlExpr))
}

// itemKeySQL identifies what an item holds a place for: the repository it
// resolves to, else its normalized URL (approval creates that repository).
// Two items with one key hold one place.
func itemKeySQL(urlExpr string) string {
	return `COALESCE('repo:' || ` + itemRepoIDSQL(urlExpr) + `::text, 'url:' || ` + normalizedRepoURLSQL(urlExpr) + `)`
}

// ownerLinksURL is true when account $1 already links the repository an
// item URL resolves to.
func ownerLinksURL(urlExpr string) string {
	return `EXISTS (SELECT 1 FROM aveloxis_ops.user_repos lr
	        JOIN aveloxis_ops.user_groups lg ON lg.group_id = lr.group_id
	        WHERE lg.user_id = $1 AND lr.repo_id = ` + itemRepoIDSQL(urlExpr) + `)`
}

// reachSQL counts what an account reaches through its groups: distinct
// linked repositories plus the distinct places its items still hold
// (heldItemsSQL; one per itemKeySQL) for repositories it does not already
// link (v0.29.91, whole-PR review: a legacy pending group's items are
// repositories its owner already links, and a pending URL another
// approval made tracked is linked by a later paste — each was counted
// twice, so an allowance that fits refused the legacy conversion on every
// migrate).
var reachSQL = `
	SELECT (SELECT COUNT(DISTINCT ur.repo_id) FROM aveloxis_ops.user_repos ur
	        JOIN aveloxis_ops.user_groups g USING (group_id) WHERE g.user_id = $1)
	     + (SELECT COUNT(DISTINCT ` + itemKeySQL("i.repo_url") + `) FROM ` + heldItemsSQL + `
	          AND NOT ` + ownerLinksURL("i.repo_url") + `)`

// heldNewSQL counts the URLs of $2 that would hold a NEW place for
// account $1: distinct places, for repositories it does not link, not
// already held by one of its items.
var heldNewSQL = `
	SELECT COUNT(DISTINCT ` + itemKeySQL("t.u") + `) FROM unnest($2::text[]) AS t(u)
	WHERE NOT ` + ownerLinksURL("t.u") + `
	  AND NOT EXISTS (SELECT 1 FROM ` + heldItemsSQL + ` AND ` + itemKeySQL("i.repo_url") + ` = ` + itemKeySQL("t.u") + `)`

// heldAddsSQL counts the URLs of $2 that are new additions for account $1
// when linked: distinct repositories (itemKeySQL) it does not link yet,
// whether or not one of its items already holds the place. Daily
// additions are counted apart from places (v0.29.91 review round 3
// R3-1): a URL a PENDING item holds takes no new place, but an
// auto-approved re-paste of it links it now. A URL an earlier
// auto-approved, not yet processed item holds counts again: the
// conservative direction.
var heldAddsSQL = `
	SELECT COUNT(DISTINCT ` + itemKeySQL("t.u") + `) FROM unnest($2::text[]) AS t(u)
	WHERE NOT ` + ownerLinksURL("t.u") + ``

// heldFreeSQL is the repositories of $2 (new to account $1) whose place
// one of its items already holds: linking one takes no new place (the
// item then resolves to a linked repository and stops counting).
var heldFreeSQL = `
	SELECT rp.repo_id FROM aveloxis_data.repos rp
	WHERE rp.repo_id = ANY($2::bigint[])
	  AND EXISTS (SELECT 1 FROM ` + heldItemsSQL + ` AND ` + itemRepoIDSQL("i.repo_url") + ` = rp.repo_id)`

// AccountReach is the number of repositories an account reaches through
// its groups, pending additions included.
func (s *PostgresStore) AccountReach(ctx context.Context, userID int) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, reachSQL, userID).Scan(&n); err != nil {
		return 0, fmt.Errorf("account reach: %w", err)
	}
	return n, nil
}

// reposAllocation is an account's repository allocation: the quota as
// applied (config word, stored row, the account's override), whether the
// account is exempt (an administrator — not through an API token), and the
// contact address. A lookup error is returned as such (SR-5).
func (s *PostgresStore) reposAllocation(ctx context.Context, userID int) (capacity.EffectiveQuota, capacity.Allocation, error) {
	quotas, err := s.EffectiveCapacityQuotas(ctx)
	if err != nil {
		return capacity.EffectiveQuota{}, capacity.Allocation{}, err
	}
	q := quotas[capacity.QuotaReposPerAccount]
	o, err := s.GetCapacityOverride(ctx, userID)
	if err != nil {
		return capacity.EffectiveQuota{}, capacity.Allocation{}, err
	}
	if o.ReposAllowed != nil {
		q.Allowed = *o.ReposAllowed
	}
	admin, err := s.IsUserAdmin(ctx, userID)
	if err != nil {
		return capacity.EffectiveQuota{}, capacity.Allocation{}, fmt.Errorf("admin flag: %w", err)
	}
	contact, err := s.GetCapacityContact(ctx)
	if err != nil {
		return capacity.EffectiveQuota{}, capacity.Allocation{}, err
	}
	return q, capacity.Allocation{Allowed: q.Allowed, Exempt: admin, Contact: contact}, nil
}

// RepoAllocationStatus is an account's repository allocation as applied:
// what it reaches (linked plus held places), the value and mode in force,
// whether it is exempt (an administrator's own session), and whom to write.
type RepoAllocationStatus struct {
	Used    int           `json:"used"`
	Allowed int           `json:"allowed"`
	Mode    capacity.Mode `json:"mode"`
	Exempt  bool          `json:"exempt"`
	Contact string        `json:"contact"`
}

// AccountRepoAllocation reads an account's repository allocation (the
// profile panel, /me, the web group page's refusal notice).
func (s *PostgresStore) AccountRepoAllocation(ctx context.Context, userID int) (RepoAllocationStatus, error) {
	quota, alloc, err := s.reposAllocation(ctx, userID)
	if err != nil {
		return RepoAllocationStatus{}, err
	}
	used, err := s.AccountReach(ctx, userID)
	if err != nil {
		return RepoAllocationStatus{}, err
	}
	return RepoAllocationStatus{Used: used, Allowed: alloc.Allowed, Mode: quota.Mode, Exempt: alloc.Exempt, Contact: alloc.Contact}, nil
}

// AccountDailyAdditions is an account's repo_links_per_day as applied:
// today's additions, the value, the mode and the contact (the web group
// page's notice after a paste this quota refused; closing review r3 F1).
func (s *PostgresStore) AccountDailyAdditions(ctx context.Context, userID int) (DailyAdditionsStatus, error) {
	q, err := s.linksQuota(ctx, userID)
	if err != nil {
		return DailyAdditionsStatus{}, err
	}
	contact, err := s.GetCapacityContact(ctx)
	if err != nil {
		return DailyAdditionsStatus{}, err
	}
	var used int64
	if err := s.pool.QueryRow(ctx, requestCountOnDaySQL, linksTodayKey(userID), capacity.UTCDate(s.capacityNow())).Scan(&used); err != nil {
		return DailyAdditionsStatus{}, fmt.Errorf("daily additions: %w", err)
	}
	return DailyAdditionsStatus{Used: int(used), Allowed: q.Allowed, Mode: q.Mode, Contact: contact}, nil
}

// DailyAdditionsStatus is an account's repo_links_per_day as applied.
type DailyAdditionsStatus struct {
	Used    int           `json:"used"`
	Allowed int           `json:"allowed"`
	Mode    capacity.Mode `json:"mode"`
	Contact string        `json:"contact"`
}

// Refusal is this status's refusal (the one message, Exceeded.Message).
func (st DailyAdditionsStatus) Refusal(now time.Time) *capacity.Exceeded {
	return &capacity.Exceeded{Kind: capacity.KindRate, Quota: capacity.QuotaRepoLinksPerDay, Window: capacity.UTCDay,
		Used: st.Used, Allowed: st.Allowed, ResetAt: capacity.UTCDay.End(now), Contact: st.Contact}
}

// Refusal is the allocation's refusal for this status: the one message
// every surface shows (capacity.Exceeded.Message).
func (st RepoAllocationStatus) Refusal() *capacity.Exceeded {
	return &capacity.Exceeded{Kind: capacity.KindAllocation, Quota: capacity.QuotaReposPerAccount, Used: st.Used, Allowed: st.Allowed, Contact: st.Contact}
}

// holdRepoPlaces admits new add-request items (their URLs) for owner inside
// the request writer's transaction, under the per-owner lock: the place
// each item holds is decided where it is taken (SR-18). quota and alloc
// come from reposAllocation, read BEFORE the transaction began: a pool read
// while the transaction holds a connection deadlocks the pool under load
// (review round 1 R2-2; linkWithinCap's order). The items written here
// (pending items an administrator will approve, and the legacy-group
// conversion's) are not daily additions, so only the allocation decides;
// an auto-approved paste's items ARE counted, by addPasteWithinCap (v0.29.91
// round 2 R2-4). A URL resolving to a repository the owner already links,
// or one it already holds, asks for no new place (v0.29.91).
func (s *PostgresStore) holdRepoPlaces(ctx context.Context, tx pgx.Tx, owner int, urls []string, quota capacity.EffectiveQuota, alloc capacity.Allocation) error {
	if len(urls) == 0 {
		return nil
	}
	d, err := s.decideWithinCap(ctx, tx, owner, ownerPolicy{quota: quota, alloc: alloc, links: offQuota}, nil, urls, false, false)
	if err != nil {
		return err
	}
	return refusalOrNil(d.refused)
}

// ownerPolicy is an account's quotas as applied, read before the deciding
// transaction begins (no pool read inside it: review round 1 R2-2).
type ownerPolicy struct {
	quota capacity.EffectiveQuota // repos_per_account
	alloc capacity.Allocation     // its value, exemption and contact
	links capacity.EffectiveQuota // repo_links_per_day (offQuota when not counted)
}

// offQuota is a quota not applied (a decision that does not count it).
var offQuota = capacity.EffectiveQuota{Mode: capacity.Off}

// readOwnerPolicy reads owner's policy; countsAdds reads repo_links_per_day.
func (s *PostgresStore) readOwnerPolicy(ctx context.Context, owner int, countsAdds bool) (ownerPolicy, error) {
	if s.linkPolicyRead != nil {
		if err := s.linkPolicyRead(); err != nil {
			return ownerPolicy{}, err
		}
	}
	quota, alloc, err := s.reposAllocation(ctx, owner)
	if err != nil {
		return ownerPolicy{}, err
	}
	p := ownerPolicy{quota: quota, alloc: alloc, links: offQuota}
	if countsAdds {
		if p.links, err = s.linksQuota(ctx, owner); err != nil {
			return ownerPolicy{}, err
		}
	}
	return p, nil
}

// capDecision is one decision of what fits.
type capDecision struct {
	fresh   []int64            // the repositories new to the account: those whose place an item holds first, then input order
	admit   int                // how many of fresh are admitted
	counted int                // how many count toward today's additions
	day     time.Time          // the UTC day the additions are counted on
	refused *capacity.Exceeded // the enforced refusal, if any
}

// refusalOrNil returns the refusal as an error (nil when there is none:
// a nil *Exceeded is not a nil error).
func refusalOrNil(ex *capacity.Exceeded) error {
	if ex == nil {
		return nil
	}
	return ex
}

// requestCountOnDaySQL is one subject's shared count for one UTC day (the
// daily additions, the Capacity page's requests today): one spelling.
const requestCountOnDaySQL = `SELECT COALESCE(SUM(requests), 0) FROM aveloxis_ops.request_counts WHERE subject = $1 AND day = $2::date`

// capacityNow is the decisions' clock (the capacityClock seam in tests).
func (s *PostgresStore) capacityNow() time.Time {
	if s.capacityClock != nil {
		return s.capacityClock()
	}
	return time.Now()
}

// decideWithinCap is the ONE decision of what an account may add (lens L18:
// linkWithinCap, the paste writer and holdRepoPlaces all decide here),
// inside the writing transaction under the per-owner lock: repoIDs new to
// the account plus the held URLs that would take a new place
// (heldNewSQL) against repos_per_account, and the new repositories —
// with those held URLs when heldAreAdditions (an auto-approved paste) —
// against repo_links_per_day (when p.links is applied). A new repository
// whose place one of the account's items already holds takes no new
// place (heldFreeSQL; v0.29.91). All or none, or as many as fit (fill).
// Shadow logs and admits; Off does not count. Exempt accounts and quotas
// that are all Off take no lock; callers skip it when there is nothing to
// decide. The UTC day is read once (d.day): the count is written on the
// day it was decided against.
func (s *PostgresStore) decideWithinCap(ctx context.Context, tx pgx.Tx, owner int, p ownerPolicy, repoIDs []int64, held []string, heldAreAdditions, fill bool) (capDecision, error) {
	d := capDecision{day: capacity.UTCDay.Start(s.capacityNow())}
	checkRepos := p.quota.Mode != capacity.Off
	checkLinks := p.links.Mode != capacity.Off
	if p.alloc.Exempt || !(checkRepos || checkLinks) {
		return d, nil
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(repoCapLockClass), int32(owner)); err != nil {
		return d, fmt.Errorf("allocation lock: %w", err)
	}
	if len(repoIDs) > 0 {
		rows, err := tx.Query(ctx, freshSQL, owner, repoIDs)
		if err != nil {
			return d, fmt.Errorf("allocation: new repositories: %w", err)
		}
		fresh, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return d, fmt.Errorf("allocation: new repositories: %w", err)
		}
		d.fresh = distinctIDs(fresh)
	}
	free, heldNew := 0, 0
	if checkRepos && len(d.fresh) > 0 {
		rows, err := tx.Query(ctx, heldFreeSQL, owner, d.fresh)
		if err != nil {
			return d, fmt.Errorf("allocation: held places: %w", err)
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return d, fmt.Errorf("allocation: held places: %w", err)
		}
		d.fresh, free = heldFirst(d.fresh, ids)
	}
	// The held URLs' new places are also today's additions when they are
	// the account's own decision (an auto-approved paste; round 2 R2-4).
	if checkRepos && len(held) > 0 {
		if err := tx.QueryRow(ctx, heldNewSQL, owner, held).Scan(&heldNew); err != nil {
			return d, fmt.Errorf("allocation: new held places: %w", err)
		}
	}
	// The held URLs' additions, counted apart from their places when they
	// are the account's own decision (an auto-approved paste; round 2
	// R2-4, round 3 R3-1: heldAddsSQL, not heldNewSQL).
	added := 0
	if checkLinks && heldAreAdditions && len(held) > 0 {
		if err := tx.QueryRow(ctx, heldAddsSQL, owner, held).Scan(&added); err != nil {
			return d, fmt.Errorf("daily additions: held URLs: %w", err)
		}
	}
	d.admit = len(d.fresh)
	subject := capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(owner)}
	if wanted := d.admit - free + heldNew; checkRepos && wanted > 0 {
		alloc := p.alloc
		if err := tx.QueryRow(ctx, reachSQL, owner).Scan(&alloc.Used); err != nil {
			return d, fmt.Errorf("allocation: reach: %w", err)
		}
		n, ex := alloc.Admit(capacity.QuotaReposPerAccount, wanted, fill)
		if ex != nil {
			ex.Subject = subject
			if p.quota.Mode == capacity.Shadow {
				s.noteShadowAllocation(owner, ex)
			} else {
				s.noteAllocationRefused(owner, ex)
				d.refused = ex
				// All or none admits nothing; fill (never with held items)
				// admits the free repositories and what fits after them.
				d.admit = 0
				if fill {
					d.admit = free + n
				}
			}
		}
	}
	// An all-or-none addition the allocation refused stands: no daily
	// decision is made for it (round 3 R3-2: it replaced the refusal, and a
	// shadow daily quota logged a would-refuse). A fill decision (never
	// with held URLs) still applies the daily quota to what it admitted.
	if checkLinks && (d.refused == nil || fill) && d.admit+added > 0 {
		var used int64
		if err := tx.QueryRow(ctx, requestCountOnDaySQL, linksTodayKey(owner), capacity.UTCDate(d.day)).Scan(&used); err != nil {
			return d, fmt.Errorf("daily additions: %w", err)
		}
		daily := capacity.Allocation{Used: int(used), Allowed: p.links.Allowed, Contact: p.alloc.Contact}
		n, ex := daily.Admit(capacity.QuotaRepoLinksPerDay, d.admit+added, fill)
		if ex != nil {
			ex.Kind, ex.Window, ex.ResetAt, ex.Subject = capacity.KindRate, capacity.UTCDay, capacity.UTCDay.End(d.day), subject
			if p.links.Mode == capacity.Shadow {
				s.noteShadowLinks(owner, ex)
			} else {
				s.noteLinksRefused(owner, ex)
				// Held URLs are all or none with the rest (no caller fills
				// with held URLs).
				d.refused, d.admit = ex, max(n-added, 0)
				// What was admitted counts, nothing refused (round 4 R4-2):
				// 0 for all or none; fill (no held URLs) its admitted n.
				added = 0
			}
		}
		d.counted = d.admit + added
	}
	if s.allocationDecided != nil {
		s.allocationDecided()
	}
	return d, nil
}

// heldFirst orders fresh with the repositories in free first (each group
// in input order) and reports how many those are.
func heldFirst(fresh, free []int64) ([]int64, int) {
	if len(free) == 0 {
		return fresh, 0
	}
	in := make(map[int64]bool, len(free))
	for _, id := range free {
		in[id] = true
	}
	out := make([]int64, 0, len(fresh))
	for _, id := range fresh {
		if in[id] {
			out = append(out, id)
		}
	}
	n := len(out)
	for _, id := range fresh {
		if !in[id] {
			out = append(out, id)
		}
	}
	return out, n
}

// countAdditions adds n to owner's additions on day (the decision's UTC
// day, so a decision at 23:59:59.999 is not counted on the next one), in
// the writing transaction.
func countAdditions(ctx context.Context, tx pgx.Tx, owner, n int, day time.Time) error {
	if n <= 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_ops.request_counts (subject, day, requests) VALUES ($1, $2::date, $3)
		ON CONFLICT (subject, day) DO UPDATE SET requests = aveloxis_ops.request_counts.requests + EXCLUDED.requests`,
		linksTodayKey(owner), capacity.UTCDate(day), n); err != nil {
		return fmt.Errorf("daily additions: count: %w", err)
	}
	return nil
}

// linkMode is how linkWithinCap admits new repositories. It is a pointer
// identity, not a number (v0.29.91 review round 4 R4-1): no arithmetic,
// conversion or literal yields one of the modes below — a mode is only
// ever named — and a value minted any other way (linkMode{}, a new
// &linkModeDef) equals none of them, so it is decided and counted like
// linkAll, the strictest mode (fill, org and held are opt-in by identity;
// countsAdds is opt-out).
type linkMode struct{ def *linkModeDef }

// linkModeDef is a mode's identity (its name is for debugging only).
type linkModeDef struct{ name string }

var (
	// linkAll admits all the new repositories or none (a bulk add, a copy).
	linkAll = linkMode{&linkModeDef{"all"}}
	// linkFill links as many as fit (a comparison record).
	linkFill = linkMode{&linkModeDef{"fill"}}
	// linkHeld links repositories whose places an approved add-request
	// item already holds (counted by reachSQL until the item is stamped):
	// no new place is asked for. Only the approval pass's link
	// (ensureRepoCollectedInGroup) uses it.
	linkHeld = linkMode{&linkModeDef{"held"}}
	// linkOrg and linkOrgFill are linkAll and linkFill for repositories an
	// organization registration brings (the scans, the reconcile, add-repo's
	// org expansion): within the allocation, and not counted as the
	// account's daily additions only when an administrator decided the
	// registration that brings them (orgLinksCounted; v0.29.91, whole-PR
	// review: an auto-approved registration of an organization already
	// tracked elsewhere is the account's own decision, and linked up to
	// 1,000 repositories uncounted, repeatably).
	linkOrg     = linkMode{&linkModeDef{"org"}}
	linkOrgFill = linkMode{&linkModeDef{"org-fill"}}
	// linkOperator is linkAll for a link the operator's own command makes
	// (the foundation loader's dashboard groups): within the allocation,
	// not counted.
	linkOperator = linkMode{&linkModeDef{"operator"}}
)

func (m linkMode) fill() bool { return m == linkFill || m == linkOrgFill }

func (m linkMode) org() bool { return m == linkOrg || m == linkOrgFill }

// countsAdds reports whether new links count toward repo_links_per_day:
// every mode but the ones that say otherwise (an organization link is
// decided per registration, orgLinksCounted; an operator's link and a held
// place are not additions the account decided now).
func (m linkMode) countsAdds() bool { return !(m.org() || m == linkOperator || m == linkHeld) }

// orgLinksCountedSQL is true unless every repository of $2 is brought into
// group $1 by an organization registration an administrator decided: an
// approved kind 'org' add request for that group and organization with a
// real decider (decided_by > 0; 0 marks an auto-approval). A repository
// the registrations do not cover (or that does not exist) counts. A batch
// mixing both kinds (the reconcile, a group registering two organizations
// decided differently) counts whole: the conservative direction.
var orgLinksCountedSQL = `
	SELECT EXISTS (
		SELECT 1 FROM unnest($2::bigint[]) AS t(id)
		LEFT JOIN aveloxis_data.repos r ON r.repo_id = t.id
		WHERE NOT EXISTS (
			SELECT 1 FROM aveloxis_ops.user_org_requests o
			WHERE o.group_id = $1 AND ` + orgCoversRepoSQL + `
			  AND EXISTS (SELECT 1 FROM aveloxis_ops.collection_add_requests q
			              WHERE q.group_id = o.group_id AND q.kind = 'org' AND q.status = 'approved'
			                AND q.decided_by > 0 AND LOWER(q.org_url) = LOWER(o.org_url))))`

// orgCoversRepoSQL is the registration o covers repository r:
// ReconcileOrgRepoLinks's prefix rule (its SQL carries this text exactly;
// TestOrgCoverageIsTheReconcileRule).
const orgCoversRepoSQL = `starts_with(LOWER(r.repo_git), LOWER(rtrim(o.org_url, '/')) || '/')`

// orgLinksCounted reports whether linking fresh into groupID counts toward
// repo_links_per_day (orgLinksCountedSQL). A failed read is an error, not
// "not counted" (SR-5).
func (s *PostgresStore) orgLinksCounted(ctx context.Context, groupID int64, fresh []int64) (bool, error) {
	var counted bool
	if err := s.pool.QueryRow(ctx, orgLinksCountedSQL, groupID, fresh).Scan(&counted); err != nil {
		return false, fmt.Errorf("organization links: who decided the registration: %w", err)
	}
	return counted, nil
}

// repoCapLockClass is the first key of the per-owner advisory lock that
// serializes allocation decisions (two concurrent adds at 999 must not
// both pass).
const repoCapLockClass = 0x617678 // "avx"

// freshSQL is the repositories of $2 the owner $1 does not reach yet, in
// input order.
const freshSQL = `
	SELECT id FROM unnest($2::bigint[]) WITH ORDINALITY AS t(id, ord)
	WHERE NOT EXISTS (SELECT 1 FROM aveloxis_ops.user_repos ur
	                  JOIN aveloxis_ops.user_groups g USING (group_id)
	                  WHERE g.user_id = $1 AND ur.repo_id = t.id)
	ORDER BY ord`

// insertLinksSQL is the one INSERT of a new group link. Its only users
// (TestOneWriterOfNewGroupLinks): linkWithinCap — its fast path when no
// repository is new to the account, linkHeld when an approved item already
// holds the place, otherwise behind decideWithinCap — and addPasteWithinCap,
// behind decideWithinCap.
//
// The target repository rows are selected FOR SHARE, in id order (Copilot
// PR #252, twelfth review): user_repos.repo_id has no FK to repos, so
// nothing refused a link to a repository a dedup merge or a rename heal was
// deleting — inserted after the merge's repoint and before its delete
// committed, it left the group pointing at a deleted id. SHARE conflicts
// with the merge's FOR NO KEY UPDATE on both rows (lockRepoRowsSQL, held
// from its first statement to commit): a link either commits first and is
// moved by the repoint, or waits and then finds the loser gone and inserts
// nothing for it (the next org refresh links the winner). KEY SHARE would
// not do: it is compatible with NO KEY UPDATE, so the merge's delete would
// only wait for the link to commit. An id with no repos row is never linked.
//
// Only a NEW link takes the lock (the L10 pass on this fix): the org scan
// re-links every repository into every group on each pass (9.3 M no-op
// re-links in one run), and a FOR SHARE on each target row sets xmax,
// dirties the page and logs a record on the hot repos table — summary/37's
// class. An existing link is filtered out before the lock (NOT EXISTS); a
// concurrent insert of the same link is ON CONFLICT's, and an existing link
// to a merge's loser is the repoint's to move. Not retried inside (the L10
// pass's F3, declined): a NEW link's FOR SHARE in id order can meet a
// multi-row repos UPDATE that locks in plan order (the CVSS rescore's stamp,
// heal-libyear's windows) — a 40P01 returns to the caller, and every caller
// owns its retry (the approval pass leaves the item for the next pass, an
// auto-approved add marks it failed, the org fill retries next refresh); the
// store's withRetry ladder, ~100 s on a persistent failure, stalled them
// (census33: 411 s in the approval test's injected failures). Decided: a
// re-link racing an uncommitted REMOVAL of the same link reads it as present
// and inserts nothing, so the group ends unlinked — the add observed the link
// and the removal then took it out, a valid order of two opposite intents;
// before, ON CONFLICT waited and the add won. Neither is wrong.
const insertLinksSQL = `
	INSERT INTO aveloxis_ops.user_repos (group_id, repo_id)
	SELECT $1, r.repo_id FROM aveloxis_data.repos r
	WHERE r.repo_id = ANY($2::bigint[])
	  AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.user_repos ur WHERE ur.group_id = $1 AND ur.repo_id = r.repo_id)
	ORDER BY r.repo_id
	FOR SHARE OF r
	ON CONFLICT DO NOTHING`

// goneIfUnlinked turns a link that inserted nothing into ErrRepoNotFound when
// the repository no longer exists — a merge or a rename heal deleted it while
// the link waited, and insertLinksSQL skips a deleted row (the L10 pass: the
// user-facing single-repository paths returned (false, nil), "already
// linked"). Probed only when nothing was inserted; the org refresh's hot
// path does not call it (its next pass links the winner).
func (s *PostgresStore) goneIfUnlinked(ctx context.Context, inserted int, repoID int64) error {
	if inserted > 0 {
		return nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM aveloxis_data.repos WHERE repo_id = $1)`, repoID).Scan(&exists); err != nil {
		return fmt.Errorf("verify repository %d after linking: %w", repoID, err)
	}
	if !exists {
		return fmt.Errorf("link repository %d: %w", repoID, ErrRepoNotFound)
	}
	return nil
}

// linksQuota is an account's repo_links_per_day as applied (the account's
// override replacing the value) and today's count key.
func (s *PostgresStore) linksQuota(ctx context.Context, owner int) (capacity.EffectiveQuota, error) {
	quotas, err := s.EffectiveCapacityQuotas(ctx)
	if err != nil {
		return capacity.EffectiveQuota{}, err
	}
	q := quotas[capacity.QuotaRepoLinksPerDay]
	o, err := s.GetCapacityOverride(ctx, owner)
	if err != nil {
		return capacity.EffectiveQuota{}, err
	}
	if o.LinksPerDay != nil {
		q.Allowed = *o.LinksPerDay
	}
	return q, nil
}

func linksTodayKey(owner int) string {
	return capacity.CountKey(capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(owner)}, capacity.QuotaRepoLinksPerDay)
}

// linkWithinCap links repoIDs into groupID within the group owner's
// repository allocation and daily additions. With addPasteWithinCap (a
// non-administrator's paste, links and add request in one transaction) it
// writes every NEW group link, both deciding through decideWithinCap
// (summary/53 A8, SR-18): repositories the owner already reaches
// link freely (and cheaply: no policy read, lock or count — closing review
// W1, an organization refresh re-links every repository each pass); new
// ones are admitted by both repos_per_account (what the account holds) and,
// for the account's own additions, repo_links_per_day (how many it adds per
// UTC day) — all or none (linkAll, linkOrg, linkOperator) or as many as fit
// (linkFill, linkOrgFill) — or take the place an approved item holds
// (linkHeld: one plain insert, no policy read or transaction; v0.29.91 —
// the policy it read was never used, and a failed read stamped an
// auto-approved item -1 for good). Under Shadow a quota's decision is
// logged and does not limit; under Off it is not counted. It returns the
// links inserted and, when an enforced quota refused anything, the
// *capacity.Exceeded.
func (s *PostgresStore) linkWithinCap(ctx context.Context, groupID int64, repoIDs []int64, mode linkMode) (int, error) {
	if len(repoIDs) == 0 {
		return 0, nil
	}
	if mode == linkHeld {
		// The place is already held (reachSQL counts the item until the
		// pass stamps it), so nothing is decided.
		tag, err := s.pool.Exec(ctx, insertLinksSQL, groupID, repoIDs)
		if err != nil {
			return 0, fmt.Errorf("link repositories: %w", err)
		}
		return int(tag.RowsAffected()), nil
	}
	var owner int
	if err := s.pool.QueryRow(ctx, `SELECT user_id FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&owner); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("link into group %d: %w", groupID, ErrGroupNotOwned)
		}
		return 0, fmt.Errorf("group owner: %w", err)
	}
	rows, err := s.pool.Query(ctx, freshSQL, owner, repoIDs)
	if err != nil {
		return 0, fmt.Errorf("allocation: new repositories: %w", err)
	}
	fresh, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return 0, fmt.Errorf("allocation: new repositories: %w", err)
	}
	if len(fresh) == 0 {
		// Nothing new to the account: no quota is asked. (A link removed
		// concurrently makes one repository "new" again; it is re-linked
		// into the reach it just left.)
		tag, err := s.pool.Exec(ctx, insertLinksSQL, groupID, repoIDs)
		if err != nil {
			return 0, fmt.Errorf("link repositories: %w", err)
		}
		return int(tag.RowsAffected()), nil
	}
	counts := mode.countsAdds()
	if mode.org() {
		if counts, err = s.orgLinksCounted(ctx, groupID, fresh); err != nil {
			return 0, err
		}
	}
	pol, err := s.readOwnerPolicy(ctx, owner, counts)
	if err != nil {
		return 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	d, err := s.decideWithinCap(ctx, tx, owner, pol, repoIDs, nil, false, mode.fill())
	if err != nil {
		return 0, err
	}
	link := repoIDs
	if d.refused != nil {
		if d.admit == 0 && !mode.fill() {
			return 0, d.refused
		}
		link = keepAdmitted(repoIDs, d.fresh, d.admit)
	}
	tag, err := tx.Exec(ctx, insertLinksSQL, groupID, link)
	if err != nil {
		return 0, fmt.Errorf("link repositories: %w", err)
	}
	if err := countAdditions(ctx, tx, owner, d.counted, d.day); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if d.refused != nil {
		return int(tag.RowsAffected()), d.refused
	}
	return int(tag.RowsAffected()), nil
}

// addPasteWithinCap writes a non-administrator's paste in ONE transaction
// under the owner's lock (Copilot review 5479554271: an unlocked precheck
// followed by one transaction per link let a concurrent add turn a refused
// paste into a partial one): the tracked repositories' links and, when
// urls is not empty, the add request holding their places (status
// "pending", or "approved" for an auto-approved add whose items the caller
// then processes). The decision covers both, all or nothing; on a refusal
// nothing is written. It returns the links inserted and the request id.
func (s *PostgresStore) addPasteWithinCap(ctx context.Context, owner int, groupID int64, trackedIDs []int64, urls []string, status string) (int, int64, error) {
	if len(trackedIDs) == 0 && len(urls) == 0 {
		return 0, 0, nil // nothing to decide or write (a paste of blanks and duplicates)
	}
	pol, err := s.readOwnerPolicy(ctx, owner, true)
	if err != nil {
		return 0, 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The decision is owner's (quota and lock), so the writer itself checks
	// that owner owns the group (SR-18; L10 round 2 R2): a caller that
	// skipped verifyGroupOwned cannot link into another account's group.
	var owned bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM aveloxis_ops.user_groups WHERE group_id = $1 AND user_id = $2)`, groupID, owner).Scan(&owned); err != nil {
		return 0, 0, fmt.Errorf("group owner: %w", err)
	}
	if !owned {
		// Only a caller that skipped verifyGroupOwned reaches this: said
		// loudly here, since the api answers it as a plain 400.
		s.logger.Error("paste refused: the group is not the account's (a caller skipped the ownership check)",
			"user_id", owner, "group_id", groupID)
		return 0, 0, fmt.Errorf("paste into group %d: %w", groupID, ErrGroupNotOwned)
	}
	// An auto-approved paste's new repositories are the account's own
	// additions (round 2 R2-4): decided against repo_links_per_day with the
	// tracked ones, all or nothing, and counted on the decision's day.
	d, err := s.decideWithinCap(ctx, tx, owner, pol, trackedIDs, urls, status == "approved", false)
	if err != nil {
		return 0, 0, err
	}
	if d.refused != nil {
		return 0, 0, d.refused
	}
	linked := int64(0)
	if len(trackedIDs) > 0 {
		tag, err := tx.Exec(ctx, insertLinksSQL, groupID, trackedIDs)
		if err != nil {
			return 0, 0, fmt.Errorf("link repositories: %w", err)
		}
		linked = tag.RowsAffected()
	}
	if err := countAdditions(ctx, tx, owner, d.counted, d.day); err != nil {
		return 0, 0, err
	}
	var requestID int64
	if len(urls) > 0 {
		if requestID, err = insertAddRequest(ctx, tx, owner, groupID, "repos", "", urls, status); err != nil {
			return 0, 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return int(linked), requestID, nil
}

// keepAdmitted is repoIDs without the new repositories past the first admit.
func keepAdmitted(repoIDs, fresh []int64, admit int) []int64 {
	drop := make(map[int64]bool, len(fresh)-admit)
	for _, id := range fresh[admit:] {
		drop[id] = true
	}
	out := make([]int64, 0, len(repoIDs))
	for _, id := range repoIDs {
		if !drop[id] {
			out = append(out, id)
		}
	}
	return out
}

// noteShadowAllocation logs, once a day per account, what an enforced
// repository quota would have refused (the dark launch).
func (s *PostgresStore) noteShadowAllocation(owner int, ex *capacity.Exceeded) {
	if s.capacityNotifier().Due(fmt.Sprintf("repos_per_account|%d", owner)) {
		s.logger.Warn("repository quota (shadow): an enforced quota would have refused this addition",
			append([]any{"user_id", owner}, ex.LogAttrs()...)...)
	}
}

// noteShadowLinks logs, once a day per account, what an enforced
// repo_links_per_day would have refused.
func (s *PostgresStore) noteShadowLinks(owner int, ex *capacity.Exceeded) {
	if s.capacityNotifier().Due(fmt.Sprintf("repo_links_per_day|%d", owner)) {
		s.logger.Warn("daily additions quota (shadow): an enforced quota would have refused this addition",
			append([]any{"user_id", owner}, ex.LogAttrs()...)...)
	}
}

// noteLinksRefused logs, once a day per account, that repo_links_per_day
// refused an addition.
func (s *PostgresStore) noteLinksRefused(owner int, ex *capacity.Exceeded) {
	if s.capacityNotifier().Due(fmt.Sprintf("links_refused|%d", owner)) {
		s.logger.Info("daily additions quota reached: an addition was refused (logged once a day per account)",
			append([]any{"user_id", owner}, ex.LogAttrs()...)...)
	}
}

// noteAllocationRefused logs, once a day per account, that an enforced
// repository quota refused an addition — callers that link in loops (the
// organization scans) skip a refusal without a line per repository.
func (s *PostgresStore) noteAllocationRefused(owner int, ex *capacity.Exceeded) {
	if s.capacityNotifier().Due(fmt.Sprintf("repos_refused|%d", owner)) {
		s.logger.Info("repository quota reached: an addition was refused (logged once a day per account)",
			append([]any{"user_id", owner}, ex.LogAttrs()...)...)
	}
}

func (s *PostgresStore) capacityNotifier() *capacity.Notifier {
	s.capacityOnce.Do(func() { s.capacityNote = capacity.NewNotifier(24*time.Hour, nil, 0) })
	return s.capacityNote
}

// signupAddressBytes is what one network address is for the sign-up
// quota: an IPv4 address, or an IPv6 /64 (one home or office network; a
// host picks any address inside its /64 freely).
func signupAddressBytes(addr netip.Addr) []byte {
	addr = addr.Unmap()
	if addr.Is4() {
		b := addr.As4()
		return b[:]
	}
	b := addr.As16()
	return b[:8]
}

// signupAddressKey is the keyed hash of the address the sign-up record
// keeps (hex, 64 characters) under that UTC day's secret. The secret is
// deleted once its day has passed (and the key cleared), so after its day
// a key can be matched to an address by no one (ASVS review I1); during
// its day a reader of the database could, which is why the day is short.
func signupAddressKey(secret []byte, addr netip.Addr) string {
	m := hmac.New(sha256.New, secret)
	m.Write(signupAddressBytes(addr))
	return hex.EncodeToString(m.Sum(nil))
}

// signupLockClass is the first key of the per-address advisory lock that
// serializes sign-up decisions (two at 2-of-3 must not both pass).
const signupLockClass = 0x617679 // "avy"

// signupPolicy is what admitSignup reads from the pool, read BEFORE the
// creating transaction begins (review round 1 R2-2: a pool read inside it
// deadlocks the pool when as many sign-ups as connections run at once).
type signupPolicy struct {
	quota   capacity.EffectiveQuota
	contact string
}

func (s *PostgresStore) readSignupPolicy(ctx context.Context) (signupPolicy, error) {
	quotas, err := s.EffectiveCapacityQuotas(ctx)
	if err != nil {
		return signupPolicy{}, err
	}
	contact, err := s.GetCapacityContact(ctx)
	if err != nil {
		return signupPolicy{}, err
	}
	return signupPolicy{quota: quotas[capacity.QuotaSignupsPerAddressPerDay], contact: contact}, nil
}

// signupSecretForDay returns the sign-up quota's secret for day, creating
// it on first use (32 bytes from crypto/rand), inside the creating
// transaction.
func signupSecretForDay(ctx context.Context, tx pgx.Tx, day time.Time) ([]byte, error) {
	fresh, err := newSecretBytes()
	if err != nil {
		return nil, fmt.Errorf("sign-up quota: secret: %w", err)
	}
	d := capacity.UTCDate(day)
	if _, err := tx.Exec(ctx, `INSERT INTO aveloxis_ops.signup_key_secrets (day, secret) VALUES ($1::date, $2) ON CONFLICT (day) DO NOTHING`, d, fresh); err != nil {
		return nil, fmt.Errorf("sign-up quota: secret (run aveloxis migrate): %w", err)
	}
	var secret []byte
	if err := tx.QueryRow(ctx, `SELECT secret FROM aveloxis_ops.signup_key_secrets WHERE day = $1::date`, d).Scan(&secret); err != nil {
		return nil, fmt.Errorf("sign-up quota: secret: %w", err)
	}
	return secret, nil
}

// admitSignup decides one account creation against
// signups_per_address_per_day inside the creating transaction and returns
// the address key to record ("" for none). Not counted, and no key kept:
// an unknown address, the first account of a deployment (the bootstrap
// administrator), and every sign-up while the quota is off (decided before
// any secret is read — ASVS review I6). An allowlisted network is counted
// but never refused. Enforced past the value: a *capacity.Exceeded;
// shadow: logged once a day per address.
func (s *PostgresStore) admitSignup(ctx context.Context, tx pgx.Tx, addr netip.Addr, first bool, pol signupPolicy) (string, error) {
	q := pol.quota
	if !addr.IsValid() || first || q.Mode == capacity.Off {
		return "", nil
	}
	day := capacity.UTCDay.Start(s.capacityNow())
	secret, err := signupSecretForDay(ctx, tx, day)
	if err != nil {
		return "", err
	}
	key := signupAddressKey(secret, addr)
	var allowlisted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM aveloxis_ops.signup_allowlist WHERE cidr >>= $1::inet)`, addr.Unmap().String()).Scan(&allowlisted); err != nil {
		return "", fmt.Errorf("sign-up quota: allowlist: %w", err)
	}
	if allowlisted {
		return key, nil
	}
	lockKey := int32(binary.BigEndian.Uint32(signupAddressBytes(addr)[:4]))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(signupLockClass), lockKey); err != nil {
		return "", fmt.Errorf("sign-up quota lock: %w", err)
	}
	var used int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.account_signups WHERE address_key = $1 AND created_at >= $2`,
		key, day).Scan(&used); err != nil {
		return "", fmt.Errorf("sign-up quota: count: %w", err)
	}
	if used < q.Allowed {
		return key, nil
	}
	ex := &capacity.Exceeded{Kind: capacity.KindRate, Subject: capacity.Subject{Kind: capacity.KindSignupAddress, ID: key[:12]},
		Quota: q.Name, Window: capacity.UTCDay, Used: used, Allowed: q.Allowed, Wanted: 1, ResetAt: capacity.UTCDay.End(day), Contact: pol.contact}
	if q.Mode == capacity.Shadow {
		if s.capacityNotifier().Due("signups|" + key) {
			s.logger.Warn("sign-up quota (shadow): an enforced quota would have refused this new account",
				append([]any{"address_key", key[:12]}, ex.LogAttrs()...)...)
		}
		return key, nil
	}
	if s.capacityNotifier().Due("signups-refused|" + key) {
		s.logger.Info("sign-up quota reached: a new account was refused (logged once a day per address)",
			append([]any{"address_key", key[:12]}, ex.LogAttrs()...)...)
	}
	return "", ex
}

// recordSignup writes the sign-up record in the creating transaction: the
// day's address key (or none) and the sealed address (or none).
func recordSignup(ctx context.Context, tx pgx.Tx, key string, sealed []byte, userID int) error {
	if _, err := tx.Exec(ctx, `INSERT INTO aveloxis_ops.account_signups (address_key, address_sealed, user_id) VALUES (NULLIF($1, ''), $2, $3)`,
		key, sealed, userID); err != nil {
		return fmt.Errorf("record sign-up: %w", err)
	}
	return nil
}

// logAccountCreated logs a new account with the UTC day's running count
// (operator 2026-10-10: log new accounts per day). A failed count is logged
// as such; the account stands.
func (s *PostgresStore) logAccountCreated(ctx context.Context, userID int) {
	var today int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.account_signups WHERE created_at >= $1`,
		capacity.UTCDay.Start(s.capacityNow())).Scan(&today); err != nil {
		if errors.Is(err, context.Canceled) {
			return // the browser left after the account was made: not a failure
		}
		s.logger.Warn("account created; could not count today's new accounts", "user_id", userID, "error", err)
		return
	}
	s.logger.Info("account created", "user_id", userID, "new_accounts_today_utc", today)
}

// SignupDay is one UTC day's new accounts.
type SignupDay struct {
	Day      time.Time `json:"day"`
	Accounts int       `json:"accounts"`
}

// SignupsPerDay is the new accounts of each of the last days UTC days
// (today last; days with none left out).
func (s *PostgresStore) SignupsPerDay(ctx context.Context, days int) ([]SignupDay, error) {
	if days < 1 {
		days = 1
	}
	since := capacity.UTCDay.Start(s.capacityNow()).AddDate(0, 0, -(days - 1))
	rows, err := s.pool.Query(ctx, `
		SELECT (created_at AT TIME ZONE 'UTC')::date AS d, COUNT(*)
		FROM aveloxis_ops.account_signups WHERE created_at >= $1
		GROUP BY d ORDER BY d`, since)
	if err != nil {
		return nil, fmt.Errorf("sign-ups per day: %w", err)
	}
	defer rows.Close()
	out := []SignupDay{} // D6: no sign-up in the window is []
	for rows.Next() {
		var d SignupDay
		if err := rows.Scan(&d.Day, &d.Accounts); err != nil {
			return nil, fmt.Errorf("sign-ups per day: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func distinctIDs(ids []int64) []int64 {
	seen := make(map[int64]bool, len(ids))
	out := ids[:0:0]
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

func nullIfZero(id int) *int {
	if id == 0 {
		return nil
	}
	return &id
}
