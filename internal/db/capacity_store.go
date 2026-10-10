// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.89 (summary/53): the store's side of account capacity. The model
// and every decision live in internal/capacity; here are the stored quotas
// (one row each), the contact address, per-account overrides, the shared
// per-day request counts, and the repository allocation at the one writer
// of a new group link (linkWithinCap).

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
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

// DefaultCapacityContactEmail is where a heavy user is asked to write
// (operator 2026-10-10); editable on the Capacity page.
const DefaultCapacityContactEmail = "aveloxis.io@gmail.com"

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
	if _, ok := capacity.Shipped[name]; !ok || allowed <= 0 || s.CapacitySource(name) != capacity.SourceWeb {
		return ErrInvalidCapacitySettings
	}
	if _, err := capacity.ParseMode(string(mode)); err != nil {
		return ErrInvalidCapacitySettings
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("save capacity quota: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_ops.capacity_quotas (name, allowed, mode, updated_at, updated_by)
		VALUES ($1, $2, $3, NOW(), $4)
		ON CONFLICT (name) DO UPDATE SET allowed = EXCLUDED.allowed, mode = EXCLUDED.mode,
		    updated_at = NOW(), updated_by = EXCLUDED.updated_by`, name, allowed, string(mode), nullIfZero(by)); err != nil {
		return fmt.Errorf("save capacity quota: %w", err)
	}
	// The rollback mirror (v0.29.89): a 0.29.82-0.29.88 binary reads its
	// API-token default from api_token_settings.
	if name == capacity.QuotaTokenRequestsPerHour {
		if _, err := tx.Exec(ctx, `UPDATE aveloxis_ops.api_token_settings SET default_rate_limit_per_hour = $1 WHERE id = 1`, allowed); err != nil {
			return fmt.Errorf("save capacity quota: rollback mirror: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("save capacity quota: %w", err)
	}
	return nil
}

// MaxContactEmailBytes bounds the contact address an administrator saves.
const MaxContactEmailBytes = 254

// GetCapacityContact is the address heavy users are asked to write to.
func (s *PostgresStore) GetCapacityContact(ctx context.Context) (string, error) {
	var c string
	if err := s.pool.QueryRow(ctx, `SELECT contact_email FROM aveloxis_ops.capacity_settings WHERE id = 1`).Scan(&c); err != nil {
		return "", fmt.Errorf("read capacity contact: %w", err)
	}
	return c, nil
}

// SetCapacityContact saves it: empty, or a plausible address of at most
// MaxContactEmailBytes with nothing that could break a header or a page.
func (s *PostgresStore) SetCapacityContact(ctx context.Context, email string, by int) error {
	email = strings.TrimSpace(email)
	// No ?, & or %: the address becomes a mailto link, which must not carry
	// injected parameters (ASVS V1.2.2).
	if len(email) > MaxContactEmailBytes || (email != "" && !strings.Contains(email, "@")) || strings.ContainsAny(email, "\r\n<>\"' ?&%") {
		return ErrInvalidCapacitySettings
	}
	tag, err := s.pool.Exec(ctx, `UPDATE aveloxis_ops.capacity_settings SET contact_email = $1, updated_at = NOW(), updated_by = $2 WHERE id = 1`,
		email, nullIfZero(by))
	if err != nil {
		return fmt.Errorf("save capacity contact: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("save capacity contact: the settings row is missing (run aveloxis migrate)")
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

// ErrNoSuchUser is an account id that does not exist.
var ErrNoSuchUser = errors.New("no such account")

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
		_, err := s.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_capacity WHERE user_id = $1`, o.UserID)
		return err
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
		return ErrNoSuchUser
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
		RETURNING subject, requests`, day.UTC().Format(time.DateOnly), keys, vals)
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
		now.UTC().AddDate(0, 0, -1).Format(time.DateOnly))
	if err != nil {
		return 0, fmt.Errorf("prune request counts: %w", err)
	}
	return tag.RowsAffected(), nil
}

// reachSQL counts what an account reaches through its groups: distinct
// linked repositories plus every repository addition still holding a
// place — an item not yet processed (repo_id IS NULL) of a pending or
// approved request in a group that is not rejected (operator decision
// 2026-10-10: pending counts). An approved item keeps its place until the
// approval pass links it (linkHeld), so the place a pending item held does
// not come free at the approval's flip.
const reachSQL = `
	SELECT (SELECT COUNT(DISTINCT ur.repo_id) FROM aveloxis_ops.user_repos ur
	        JOIN aveloxis_ops.user_groups g USING (group_id) WHERE g.user_id = $1)
	     + (SELECT COUNT(*) FROM aveloxis_ops.collection_add_request_items i
	        JOIN aveloxis_ops.collection_add_requests r USING (request_id)
	        JOIN aveloxis_ops.user_groups g ON g.group_id = r.group_id
	        WHERE r.user_id = $1 AND r.kind = 'repos' AND r.status IN ('pending', 'approved')
	          AND i.repo_id IS NULL AND g.status IS DISTINCT FROM 'rejected')`

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
	if err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(requests), 0) FROM aveloxis_ops.request_counts WHERE subject = $1 AND day = $2::date`,
		linksTodayKey(userID), time.Now().UTC().Format(time.DateOnly)).Scan(&used); err != nil {
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

// precheckReposAllocation decides a whole bulk addition up front: the
// tracked repositories the account does not reach yet plus the new
// pending items must fit, or nothing is written (an enforced quota
// returns the *capacity.Exceeded; a shadow quota logs). It takes no lock:
// the writers decide again under it (linkWithinCap for a link,
// holdRepoPlaces for a request's items), so a concurrent add is refused
// there.
func (s *PostgresStore) precheckReposAllocation(ctx context.Context, userID int, trackedIDs []int64, pending int) error {
	quota, alloc, err := s.reposAllocation(ctx, userID)
	if err != nil {
		return err
	}
	links, err := s.linksQuota(ctx, userID)
	if err != nil {
		return err
	}
	if (quota.Mode == capacity.Off && links.Mode == capacity.Off) || alloc.Exempt || (len(trackedIDs) == 0 && pending == 0) {
		return nil
	}
	var fresh int
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT t.id) FROM unnest($2::bigint[]) AS t(id)
		WHERE NOT EXISTS (SELECT 1 FROM aveloxis_ops.user_repos ur
		                  JOIN aveloxis_ops.user_groups g USING (group_id)
		                  WHERE g.user_id = $1 AND ur.repo_id = t.id)`, userID, trackedIDs).Scan(&fresh); err != nil {
		return fmt.Errorf("allocation: new repositories: %w", err)
	}
	subject := capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(userID)}
	if quota.Mode != capacity.Off {
		if alloc.Used, err = s.AccountReach(ctx, userID); err != nil {
			return err
		}
		if _, ex := alloc.Admit(capacity.QuotaReposPerAccount, fresh+pending, false); ex != nil {
			ex.Subject = subject
			if quota.Mode != capacity.Shadow {
				return ex
			}
			s.noteShadowAllocation(userID, ex)
		}
	}
	// The paste's tracked repositories new to the account count as today's
	// additions (pending items are counted when an administrator approves
	// them: not at all — approval decides those).
	if links.Mode != capacity.Off && fresh > 0 {
		day := time.Now().UTC().Truncate(24 * time.Hour)
		var used int64
		if err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(requests), 0) FROM aveloxis_ops.request_counts WHERE subject = $1 AND day = $2::date`,
			linksTodayKey(userID), day.Format(time.DateOnly)).Scan(&used); err != nil {
			return fmt.Errorf("daily additions: %w", err)
		}
		daily := capacity.Allocation{Used: int(used), Allowed: links.Allowed, Contact: alloc.Contact}
		if _, ex := daily.Admit(capacity.QuotaRepoLinksPerDay, fresh, false); ex != nil {
			ex.Kind, ex.Window, ex.ResetAt, ex.Subject = capacity.KindRate, capacity.UTCDay, day.AddDate(0, 0, 1), subject
			if links.Mode != capacity.Shadow {
				return ex
			}
			s.noteShadowLinks(userID, ex)
		}
	}
	return nil
}

// holdRepoPlaces admits wanted new add-request items for owner inside the
// request writer's transaction, under the per-owner lock: the place each
// item holds is decided where it is taken (SR-18). quota and alloc come from
// reposAllocation, read BEFORE the transaction began: a pool read while the
// transaction holds a connection deadlocks the pool under load (review
// round 1 R2-2; linkWithinCap's order).
func (s *PostgresStore) holdRepoPlaces(ctx context.Context, tx pgx.Tx, owner, wanted int, quota capacity.EffectiveQuota, alloc capacity.Allocation) error {
	if wanted == 0 {
		return nil
	}
	if quota.Mode == capacity.Off || alloc.Exempt {
		return nil
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(repoCapLockClass), int32(owner)); err != nil {
		return fmt.Errorf("allocation lock: %w", err)
	}
	if err := tx.QueryRow(ctx, reachSQL, owner).Scan(&alloc.Used); err != nil {
		return fmt.Errorf("allocation: reach: %w", err)
	}
	if _, ex := alloc.Admit(capacity.QuotaReposPerAccount, wanted, false); ex != nil {
		ex.Subject = capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(owner)}
		if quota.Mode == capacity.Shadow {
			s.noteShadowAllocation(owner, ex)
			return nil
		}
		s.noteAllocationRefused(owner, ex)
		return ex
	}
	return nil
}

// linkMode is how linkWithinCap admits new repositories.
type linkMode int

const (
	// linkAll admits all the new repositories or none (a bulk add, a copy).
	linkAll linkMode = iota
	// linkFill links as many as fit (a comparison record).
	linkFill
	// linkHeld links repositories whose places an approved add-request
	// item already holds (counted by reachSQL until the item is stamped):
	// no new place is asked for. Only the approval pass's link
	// (ensureRepoCollectedInGroup) uses it.
	linkHeld
	// linkOrg and linkOrgFill are linkAll and linkFill for repositories an
	// organization registration brings (the scans, the reconcile, the CLI
	// loaders): within the allocation, but not counted as the account's
	// daily additions — an administrator approved the organization
	// (operator 2026-10-10).
	linkOrg
	linkOrgFill
)

func (m linkMode) fill() bool { return m == linkFill || m == linkOrgFill }

// countsAdds reports whether new links count toward repo_links_per_day.
func (m linkMode) countsAdds() bool { return m == linkAll || m == linkFill }

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

// insertLinksSQL is the one INSERT of a new group link.
const insertLinksSQL = `
	INSERT INTO aveloxis_ops.user_repos (group_id, repo_id)
	SELECT $1, id FROM unnest($2::bigint[]) AS t(id)
	ON CONFLICT DO NOTHING`

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
// repository allocation and daily additions. It is the one writer of a NEW
// group link (summary/53 A8, SR-18): repositories the owner already reaches
// link freely (and cheaply: no policy read, lock or count — closing review
// W1, an organization refresh re-links every repository each pass); new
// ones are admitted by both repos_per_account (what the account holds) and,
// for the account's own additions, repo_links_per_day (how many it adds per
// UTC day) — all or none (linkAll, linkOrg) or as many as fit (linkFill,
// linkOrgFill) — or take the place an approved item holds (linkHeld). Under
// Shadow a quota's decision is logged and does not limit; under Off it is
// not counted. It returns the links inserted and, when an enforced quota
// refused anything, the *capacity.Exceeded.
func (s *PostgresStore) linkWithinCap(ctx context.Context, groupID int64, repoIDs []int64, mode linkMode) (int, error) {
	if len(repoIDs) == 0 {
		return 0, nil
	}
	var owner int
	if err := s.pool.QueryRow(ctx, `SELECT user_id FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&owner); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("link into group %d: %w", groupID, ErrGroupNotOwned)
		}
		return 0, fmt.Errorf("group owner: %w", err)
	}
	if mode != linkHeld {
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
	}
	quota, alloc, err := s.reposAllocation(ctx, owner)
	if err != nil {
		return 0, err
	}
	links := capacity.EffectiveQuota{Mode: capacity.Off}
	if mode.countsAdds() {
		if links, err = s.linksQuota(ctx, owner); err != nil {
			return 0, err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	link := repoIDs
	var refused *capacity.Exceeded
	fill := mode.fill()
	checkRepos := quota.Mode != capacity.Off
	checkLinks := links.Mode != capacity.Off
	counted := 0 // new repositories that count toward today's additions
	if mode != linkHeld && !alloc.Exempt && (checkRepos || checkLinks) {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(repoCapLockClass), int32(owner)); err != nil {
			return 0, fmt.Errorf("allocation lock: %w", err)
		}
		rows, err := tx.Query(ctx, freshSQL, owner, repoIDs)
		if err != nil {
			return 0, fmt.Errorf("allocation: new repositories: %w", err)
		}
		fresh, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return 0, fmt.Errorf("allocation: new repositories: %w", err)
		}
		fresh = distinctIDs(fresh)
		admit := len(fresh)
		subject := capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(owner)}
		if checkRepos {
			if err := tx.QueryRow(ctx, reachSQL, owner).Scan(&alloc.Used); err != nil {
				return 0, fmt.Errorf("allocation: reach: %w", err)
			}
			n, ex := alloc.Admit(capacity.QuotaReposPerAccount, admit, fill)
			if ex != nil {
				ex.Subject = subject
				if quota.Mode == capacity.Shadow {
					s.noteShadowAllocation(owner, ex)
				} else {
					refused, admit = ex, n
					s.noteAllocationRefused(owner, ex)
				}
			}
		}
		if checkLinks && admit > 0 {
			day := time.Now().UTC().Truncate(24 * time.Hour)
			var used int64
			if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM(requests), 0) FROM aveloxis_ops.request_counts WHERE subject = $1 AND day = $2::date`,
				linksTodayKey(owner), day.Format(time.DateOnly)).Scan(&used); err != nil {
				return 0, fmt.Errorf("daily additions: %w", err)
			}
			daily := capacity.Allocation{Used: int(used), Allowed: links.Allowed, Contact: alloc.Contact}
			n, ex := daily.Admit(capacity.QuotaRepoLinksPerDay, admit, fill)
			if ex != nil {
				ex.Kind, ex.Window, ex.ResetAt, ex.Subject = capacity.KindRate, capacity.UTCDay, day.AddDate(0, 0, 1), subject
				if links.Mode == capacity.Shadow {
					s.noteShadowLinks(owner, ex)
				} else {
					refused, admit = ex, n
					s.noteLinksRefused(owner, ex)
				}
			}
			counted = admit
		}
		if s.allocationDecided != nil {
			s.allocationDecided()
		}
		if refused != nil {
			if admit == 0 && !fill {
				return 0, refused
			}
			link = keepAdmitted(repoIDs, fresh, admit)
		}
	}
	tag, err := tx.Exec(ctx, insertLinksSQL, groupID, link)
	if err != nil {
		return 0, fmt.Errorf("link repositories: %w", err)
	}
	if counted > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_ops.request_counts (subject, day, requests) VALUES ($1, $2::date, $3)
			ON CONFLICT (subject, day) DO UPDATE SET requests = aveloxis_ops.request_counts.requests + EXCLUDED.requests`,
			linksTodayKey(owner), time.Now().UTC().Format(time.DateOnly), counted); err != nil {
			return 0, fmt.Errorf("daily additions: count: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if refused != nil {
		return int(tag.RowsAffected()), refused
	}
	return int(tag.RowsAffected()), nil
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
			"user_id", owner, "reach", ex.Used, "allowed", ex.Allowed, "adding", ex.Wanted)
	}
}

// noteShadowLinks logs, once a day per account, what an enforced
// repo_links_per_day would have refused.
func (s *PostgresStore) noteShadowLinks(owner int, ex *capacity.Exceeded) {
	if s.capacityNotifier().Due(fmt.Sprintf("repo_links_per_day|%d", owner)) {
		s.logger.Warn("daily additions quota (shadow): an enforced quota would have refused this addition",
			"user_id", owner, "added_today", ex.Used, "allowed", ex.Allowed, "adding", ex.Wanted)
	}
}

// noteLinksRefused logs, once a day per account, that repo_links_per_day
// refused an addition.
func (s *PostgresStore) noteLinksRefused(owner int, ex *capacity.Exceeded) {
	if s.capacityNotifier().Due(fmt.Sprintf("links_refused|%d", owner)) {
		s.logger.Info("daily additions quota reached: an addition was refused (logged once a day per account)",
			"user_id", owner, "added_today", ex.Used, "allowed", ex.Allowed, "adding", ex.Wanted)
	}
}

// noteAllocationRefused logs, once a day per account, that an enforced
// repository quota refused an addition — callers that link in loops (the
// organization scans) skip a refusal without a line per repository.
func (s *PostgresStore) noteAllocationRefused(owner int, ex *capacity.Exceeded) {
	if s.capacityNotifier().Due(fmt.Sprintf("repos_refused|%d", owner)) {
		s.logger.Info("repository quota reached: an addition was refused (logged once a day per account)",
			"user_id", owner, "reach", ex.Used, "allowed", ex.Allowed, "adding", ex.Wanted)
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
	fresh := make([]byte, 32)
	if _, err := rand.Read(fresh); err != nil {
		return nil, fmt.Errorf("sign-up quota: secret: %w", err)
	}
	d := day.Format(time.DateOnly)
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
	day := time.Now().UTC().Truncate(24 * time.Hour)
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
		Quota: q.Name, Window: capacity.UTCDay, Used: used, Allowed: q.Allowed, ResetAt: day.AddDate(0, 0, 1), Contact: pol.contact}
	if q.Mode == capacity.Shadow {
		if s.capacityNotifier().Due("signups|" + key) {
			s.logger.Warn("sign-up quota (shadow): an enforced quota would have refused this new account",
				"address_key", key[:12], "signups_today", used, "allowed", q.Allowed)
		}
		return key, nil
	}
	if s.capacityNotifier().Due("signups-refused|" + key) {
		s.logger.Info("sign-up quota reached: a new account was refused (logged once a day per address)",
			"address_key", key[:12], "signups_today", used, "allowed", q.Allowed)
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
		time.Now().UTC().Truncate(24*time.Hour)).Scan(&today); err != nil {
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
	since := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -(days - 1))
	rows, err := s.pool.Query(ctx, `
		SELECT (created_at AT TIME ZONE 'UTC')::date AS d, COUNT(*)
		FROM aveloxis_ops.account_signups WHERE created_at >= $1
		GROUP BY d ORDER BY d`, since)
	if err != nil {
		return nil, fmt.Errorf("sign-ups per day: %w", err)
	}
	defer rows.Close()
	var out []SignupDay
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
