// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package forgekeys

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// Interval is how often a running serve reloads keys and saves its report —
// the pickup bound the API-keys admin page promises ("within a minute").
// Worker_oauth changes are rare and human-driven; the cost per tick is one
// read of the stored keys (tens of rows) and one report upsert. The admin
// API treats a report older than three intervals as stale.
const Interval = time.Minute

// githubWebURL is how the report names GitHub's one instance.
const githubWebURL = "https://github.com"

// ReportStore persists a process's key report.
type ReportStore interface {
	SaveForgeKeyReport(ctx context.Context, reporter string, report []byte) error
}

// MaintainerConfig wires a Maintainer to the pools a process built.
type MaintainerConfig struct {
	GitHubConfigTokens []string
	GitHubAPIURL       string
	GitHub             *platform.KeyPool // nil: no GitHub pool in this process
	Instances          []config.GitLabInstance
	GitLab             *gitlab.Instances // nil: a GitHub-only process (web's org scans)
	Loader             StoredLoader
	Reports            ReportStore // nil: Report is a no-op
	Reporter           string
	// StartupNotLoaded is what the startup resolution already logged, so the
	// first reload does not log the same orphans and conflicts again.
	StartupNotLoaded []NotLoaded
	Logger           *slog.Logger
}

// Maintainer keeps a running process's pools in line with the stored keys
// (Reload) and reports their state (Report). One mutex covers a whole read
// and apply, so two reloads can never interleave and apply an older read
// over a newer one.
type Maintainer struct {
	cfg MaintainerConfig

	mu        sync.Mutex
	meta      map[string]keyMeta // key_id → what the report says about it
	notLoaded []NotLoaded
	orphanSig string
	conflSig  string
}

type keyMeta struct {
	masked      string
	source      Source
	oauthID     int64
	platform    string
	platformID  int
	instanceURL string
}

// NewMaintainer returns a Maintainer for cfg's pools.
func NewMaintainer(cfg MaintainerConfig) *Maintainer {
	m := &Maintainer{cfg: cfg, meta: map[string]keyMeta{}}
	for _, tok := range cfg.GitHubConfigTokens {
		if tok != "" {
			m.meta[platform.KeyID(tok)] = keyMeta{masked: platform.MaskToken(tok), source: SourceConfig, platform: "github", platformID: int(model.PlatformGitHub), instanceURL: githubWebURL}
		}
	}
	ids := m.instanceIDs()
	for _, in := range cfg.Instances {
		for _, tok := range in.APIKeys {
			if tok != "" {
				m.meta[platform.KeyID(tok)] = keyMeta{masked: platform.MaskToken(tok), source: SourceConfig, platform: "gitlab", platformID: ids[in.WebBase], instanceURL: in.WebBase}
			}
		}
	}
	m.notLoaded = cfg.StartupNotLoaded
	m.orphanSig, m.conflSig = notLoadedSignatures(cfg.StartupNotLoaded)
	return m
}

func (m *Maintainer) instanceIDs() map[string]int {
	ids := map[string]int{}
	for _, ik := range m.cfg.GitLab.KeySnapshots() {
		ids[ik.WebBase] = int(ik.ID)
	}
	return ids
}

// Reload reads the stored keys and reconciles every pool with them: GitHub's
// pool with config ∪ stored ∪ Augur, and each GitLab instance's pool with
// its own partition entry (gitlab.Instances.ReconcileKeys pairs them).
// Nothing changes unless BOTH reads succeed (SR-5): a read error is returned
// unlogged (the caller classifies cancellation and logs), and the pools keep
// what they had. Pool changes are logged at INFO; orphaned and conflicting
// stored keys once per change.
func (m *Maintainer) Reload(ctx context.Context) error {
	if m.cfg.Loader == nil {
		return errNoLoader
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	ghStored, err := m.cfg.Loader.Load(ctx, "github")
	if err != nil {
		return fmt.Errorf("reload API keys: read stored github keys (pools unchanged): %w", err)
	}
	var glStored Stored
	if m.cfg.GitLab != nil {
		if glStored, err = m.cfg.Loader.Load(ctx, "gitlab"); err != nil {
			return fmt.Errorf("reload API keys: read stored gitlab keys (pools unchanged): %w", err)
		}
	}
	ghKeys := GitHubKeys(m.cfg.GitHubConfigTokens, ghStored)
	pools, part, err := PartitionGitLabTokens(m.cfg.Instances, glStored)
	if err != nil {
		// Only a config-vs-config conflict, which startup already refused.
		return fmt.Errorf("reload API keys (pools unchanged): %w", err)
	}

	meta := map[string]keyMeta{}
	if m.cfg.GitHub != nil {
		reconcileGitHub(m.cfg.GitHub, ghKeys, m.cfg.Logger)
		for _, k := range ghKeys {
			meta[platform.KeyID(k.Token)] = keyMeta{masked: platform.MaskToken(k.Token), source: k.Source, oauthID: k.OAuthID, platform: "github", platformID: int(model.PlatformGitHub), instanceURL: githubWebURL}
		}
	}
	results := m.cfg.GitLab.ReconcileKeys(pools)
	ids := m.instanceIDs()
	bases := make([]string, 0, len(results))
	for base := range results {
		bases = append(bases, base)
	}
	sort.Strings(bases)
	for _, base := range bases {
		if r := results[base]; r != (platform.ReconcileResult{}) {
			m.cfg.Logger.Info("API keys reloaded", "platform", "gitlab", "web_url", base, "platform_id", ids[base],
				"added", r.Added, "removed", r.Removed, "restored", r.Restored, "active_keys", len(pools[base]))
		}
	}
	for base, keys := range part.Keys {
		for _, k := range keys {
			meta[platform.KeyID(k.Token)] = keyMeta{masked: platform.MaskToken(k.Token), source: k.Source, oauthID: k.OAuthID, platform: "gitlab", platformID: ids[base], instanceURL: base}
		}
	}
	// A removed key keeps its description while it drains; Report prunes
	// descriptions of keys no pool holds any more.
	for id, v := range m.meta {
		if _, ok := meta[id]; !ok {
			meta[id] = v
		}
	}
	m.meta = meta
	m.notLoaded = part.NotLoaded

	orphanSig, conflSig := notLoadedSignatures(part.NotLoaded)
	if orphanSig != m.orphanSig {
		m.orphanSig = orphanSig
		LogOrphans(m.cfg.Logger, part.Orphans)
	}
	if conflSig != m.conflSig {
		m.conflSig = conflSig
		LogConflicts(m.cfg.Logger, part.NotLoaded)
	}
	return nil
}

// reconcileGitHub is the one place GitHub tokens are paired with a pool
// (TestKeyPoolReconcileHasOnlyReviewedCallers). Serve's Maintainer and web's
// GitHub-only Maintainer both go through it; WHICH pool each passes is the
// GitHub pool its process built, pinned by TestServeWiresKeyMaintainer.
func reconcileGitHub(pool *platform.KeyPool, keys []Key, logger *slog.Logger) {
	if r := pool.Reconcile(Tokens(keys)); r != (platform.ReconcileResult{}) {
		logger.Info("API keys reloaded", "platform", "github", "added", r.Added, "removed", r.Removed, "restored", r.Restored, "active_keys", pool.Len())
	}
}

// LogOrphans logs one WARN per unconfigured instance tag that stored keys
// name (startup and reload share it).
func LogOrphans(logger *slog.Logger, orphans map[string]int) {
	tags := make([]string, 0, len(orphans))
	for tag := range orphans {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for _, tag := range tags {
		logger.Warn("stored GitLab keys name an instance that is not configured — not loaded",
			"instance_url", tag, "keys", orphans[tag], "fix", "add the instance to gitlab.instances, or remove the keys and add them again for a configured instance")
	}
}

// LogConflicts logs one ERROR per stored key refused because the same token
// is another instance's config key (startup and reload share it).
func LogConflicts(logger *slog.Logger, notLoaded []NotLoaded) {
	for _, nl := range notLoaded {
		if nl.Reason != ReasonConflict {
			continue
		}
		logger.Error("stored GitLab key conflicts with a config key — not loaded",
			"oauth_id", nl.OAuthID, "key_id", nl.KeyID, "token", nl.Masked, "source", nl.Source,
			"instance_url", nl.Tag, "detail", nl.Detail)
	}
}

func notLoadedSignatures(notLoaded []NotLoaded) (orphans, conflicts string) {
	var o, c []string
	for _, nl := range notLoaded {
		entry := nl.Tag + "|" + nl.KeyID
		switch nl.Reason {
		case ReasonOrphan:
			o = append(o, entry)
		case ReasonConflict:
			c = append(c, entry)
		}
	}
	sort.Strings(o)
	sort.Strings(c)
	return strings.Join(o, ","), strings.Join(c, ",")
}

// Report is one process's key report, as saved in
// aveloxis_ops.forge_key_reports and read by the admin API. It never holds
// a token.
type Report struct {
	Reporter        string           `json:"reporter"`
	IntervalSeconds int              `json:"interval_seconds"`
	Instances       []ReportInstance `json:"instances"`
	// Unregistered lists configured GitLab instances this process could not
	// serve because the platforms registry has no id for them yet (run
	// aveloxis migrate): no pool, no client, repositories not collected
	// over the API.
	Unregistered []ReportInstance `json:"unregistered"`
	Keys         []ReportKey      `json:"keys"`
	NotLoaded    []NotLoaded      `json:"not_loaded"`
}

// ReportInstance is one forge instance a process serves: GitHub, or a
// GitLab instance with its effective API URL.
type ReportInstance struct {
	Platform   string `json:"platform"`
	PlatformID int    `json:"platform_id"`
	WebURL     string `json:"web_url"`
	APIURL     string `json:"api_url"`
	Main       bool   `json:"main"`
	ActiveKeys int    `json:"active_keys"`
}

// ReportKey is one key a pool holds (active, or draining after removal).
// Health is classified in the reporting process, on its clock.
type ReportKey struct {
	KeyID            string             `json:"key_id"`
	Masked           string             `json:"masked"`
	OAuthID          int64              `json:"oauth_id,omitempty"`
	Source           Source             `json:"source"`
	Platform         string             `json:"platform"`
	PlatformID       int                `json:"platform_id"`
	InstanceURL      string             `json:"instance_url"`
	State            platform.KeyState  `json:"state"`
	Health           platform.KeyHealth `json:"health"`
	CoreRemaining    int                `json:"core_remaining"`
	CoreResetAt      *time.Time         `json:"core_reset_at,omitempty"`
	GraphQLRemaining *int               `json:"graphql_remaining,omitempty"`
	GraphQLResetAt   *time.Time         `json:"graphql_reset_at,omitempty"`
	Inflight         int                `json:"inflight"`
	Lent             int                `json:"lent"`
	SecondaryHits    int                `json:"secondary_hits"`
	SecondaryUntil   *time.Time         `json:"secondary_until,omitempty"`
	QuarantineUntil  *time.Time         `json:"quarantine_until,omitempty"`
	QuarantineCount  int                `json:"quarantine_count"`
	Invalid          bool               `json:"invalid"`
}

// Report builds this process's key report and saves it.
func (m *Maintainer) Report(ctx context.Context) error {
	if m.cfg.Reports == nil {
		return nil
	}
	b, err := json.Marshal(m.buildReport(time.Now()))
	if err != nil {
		return fmt.Errorf("encode key report: %w", err)
	}
	return m.cfg.Reports.SaveForgeKeyReport(ctx, m.cfg.Reporter, b)
}

func (m *Maintainer) buildReport(now time.Time) Report {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := Report{Reporter: m.cfg.Reporter, IntervalSeconds: int(Interval.Seconds()), NotLoaded: m.notLoaded}
	if r.NotLoaded == nil {
		r.NotLoaded = []NotLoaded{}
	}
	held := map[string]bool{}
	if m.cfg.GitHub != nil {
		snaps, _ := m.cfg.GitHub.Snapshot()
		r.Instances = append(r.Instances, ReportInstance{Platform: "github", PlatformID: int(model.PlatformGitHub), WebURL: githubWebURL,
			APIURL: m.cfg.GitHubAPIURL, Main: true, ActiveKeys: m.cfg.GitHub.Len()})
		for _, s := range snaps {
			held[s.KeyID] = true
			r.Keys = append(r.Keys, m.reportKey(s, now, true, "github", int(model.PlatformGitHub), githubWebURL))
		}
	}
	for _, ik := range m.cfg.GitLab.KeySnapshots() {
		r.Instances = append(r.Instances, ReportInstance{Platform: "gitlab", PlatformID: int(ik.ID), WebURL: ik.WebBase,
			APIURL: ik.APIURL, Main: ik.Primary, ActiveKeys: ik.ActiveKeys})
		for _, s := range ik.Keys {
			held[s.KeyID] = true
			r.Keys = append(r.Keys, m.reportKey(s, now, false, "gitlab", int(ik.ID), ik.WebBase))
		}
	}
	registered := map[string]bool{}
	for _, in := range r.Instances {
		registered[in.WebURL] = true
	}
	r.Unregistered = []ReportInstance{}
	for _, in := range m.cfg.Instances {
		if !registered[in.WebBase] {
			r.Unregistered = append(r.Unregistered, ReportInstance{Platform: "gitlab", WebURL: in.WebBase, APIURL: in.APIURL, Main: in.Primary})
		}
	}
	for id := range m.meta {
		if !held[id] {
			delete(m.meta, id)
		}
	}
	if r.Keys == nil {
		r.Keys = []ReportKey{}
	}
	return r
}

func (m *Maintainer) reportKey(s platform.KeySnapshot, now time.Time, graphQL bool, platformName string, platformID int, instanceURL string) ReportKey {
	meta, ok := m.meta[s.KeyID]
	if !ok {
		meta = keyMeta{masked: s.Prefix}
	}
	k := ReportKey{
		KeyID: s.KeyID, Masked: meta.masked, OAuthID: meta.oauthID, Source: meta.source,
		Platform: platformName, PlatformID: platformID, InstanceURL: instanceURL,
		State: s.State, Health: s.Health(now, graphQL),
		CoreRemaining: s.Core, CoreResetAt: timePtr(s.CoreResetAt),
		Inflight: s.Inflight, Lent: s.Lent, SecondaryHits: s.SecondaryHits,
		SecondaryUntil: futurePtr(s.SecondaryUntil, now), QuarantineUntil: futurePtr(s.QuarantineUntil, now),
		QuarantineCount: s.QuarantineCount, Invalid: s.Invalid,
	}
	if graphQL {
		gql := s.GraphQL
		k.GraphQLRemaining = &gql
		k.GraphQLResetAt = timePtr(s.GraphQLResetAt)
	}
	return k
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// futurePtr keeps a rest or quarantine deadline only while it is ahead.
func futurePtr(t, now time.Time) *time.Time {
	if !now.Before(t) {
		return nil
	}
	return &t
}

var (
	bootOnce sync.Once
	bootID   string
)

// ReporterID names this process in forge_key_reports:
// <component>@<host marker>#<boot id>. The boot id is random per process, so
// two processes on one host (serve --allow-second-serve) never overwrite
// each other's report, and a restarted process starts a new row (the old
// one ages out after a day).
func ReporterID(component string) string {
	bootOnce.Do(func() {
		b := make([]byte, 4)
		if _, err := rand.Read(b); err != nil {
			bootID = fmt.Sprintf("%x", time.Now().UnixNano())
			return
		}
		bootID = hex.EncodeToString(b)
	})
	return component + "@" + db.HostMarker() + "#" + bootID
}
