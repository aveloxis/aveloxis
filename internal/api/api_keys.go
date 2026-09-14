// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.30.0 Phase C — API-key administration for the aveloxis-gui "API keys"
// page. One GET describes the whole fleet (GitHub plus every GitLab
// instance); POST adds a stored key; POST .../delete removes one. Running
// serve processes pick changes up within forgekeys.Interval and report the
// keys they hold (aveloxis_ops.forge_key_reports), which this file merges
// with the stored rows.
//
// The api process ACCEPTS a token on the way in, never reads one back (the
// store's admin reads return key_id and key_mask only; TestAPIPackageNever
// ReadsTokens), and never sends one anywhere — no forge call is made here: a
// URL taken from the database must never receive a secret.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/forgekeys"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// apiKeyStore is the narrow store surface the key admin endpoints use.
type apiKeyStore interface {
	ListAdminAPIKeys(ctx context.Context) ([]db.AdminAPIKey, error)
	InsertAPIKey(ctx context.Context, name, token, platformName, instanceURL string) (int64, error)
	DeleteAPIKey(ctx context.Context, oauthID int64) (db.AdminAPIKey, error)
	LoadForgeKeyReports(ctx context.Context) ([]db.ForgeKeyReportRow, error)
	LoadGitLabInstanceRegistry(ctx context.Context) (map[string]model.Platform, error)
	CountReposByPlatform(ctx context.Context) (map[model.Platform]int, error)
	MisroutedGitLabRepos(ctx context.Context) ([]db.MisroutedRepo, error)
}

const (
	// apiKeyBodyLimit bounds an add request: a token (≤ maxTokenBytes), a
	// name (≤ maxKeyNameBytes), a platform and a web URL fit with room.
	apiKeyBodyLimit = 8 << 10
	maxTokenBytes   = 512
	maxKeyNameBytes = 200
	// reportStaleAfter: a report older than three reload intervals is from a
	// process that has stopped (or cannot reach the database).
	reportStaleAfter = 3 * forgekeys.Interval
	githubWebURL     = "https://github.com"
)

// Key statuses on the page, and why a stored key is not loaded.
const (
	keyStatusLoaded    = "loaded"
	keyStatusDraining  = "draining"
	keyStatusNotLoaded = "not_loaded"

	reasonPending    = "pending"     // stored after the latest reload
	reasonNoReporter = "no_reporter" // no serve process has reported recently
)

// gitHubTokenPrefixes / gitLabTokenPrefixes are token formats issued by one
// forge. A token filed under the other forge would be sent to that forge's
// API host, so it is refused. Classic 40-hex GitHub tokens have no prefix
// and are accepted.
var (
	gitHubTokenPrefixes = []string{"ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_"}
	gitLabTokenPrefixes = []string{"glpat-", "gloas-", "gldt-", "glrt-", "glcbt-", "glptt-", "glft-", "glimt-", "glagent-", "glsoat-"}
)

type reporterJSON struct {
	Reporter   string    `json:"reporter"`
	ReportedAt time.Time `json:"reported_at"`
	AgeSeconds float64   `json:"age_seconds"`
	Stale      bool      `json:"stale"`
}

type loadJSON struct {
	Reporter string `json:"reporter"`
	forgekeys.ReportKey
}

type keyJSON struct {
	OAuthID     int64      `json:"oauth_id,omitempty"`
	Name        string     `json:"name"`
	Source      string     `json:"source"`
	KeyID       string     `json:"key_id"`
	Masked      string     `json:"masked"`
	InstanceURL string     `json:"instance_url"`
	CreatedAt   *time.Time `json:"created_at,omitempty"`
	Removable   bool       `json:"removable"`
	Status      string     `json:"status"`
	Health      string     `json:"health,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	Detail      string     `json:"detail,omitempty"`
	LoadedBy    []loadJSON `json:"loaded_by"`
}

type forgeJSON struct {
	Platform       string         `json:"platform"`
	PlatformID     int            `json:"platform_id"`
	WebURL         string         `json:"web_url"`
	APIURL         string         `json:"api_url"`
	Main           bool           `json:"main"`
	Registered     bool           `json:"registered"`
	Reported       bool           `json:"reported"`
	ActiveKeys     int            `json:"active_keys"`
	RepoCount      int            `json:"repo_count"`
	MisroutedCount int            `json:"misrouted_count"`
	Health         map[string]int `json:"health"`
	Keys           []keyJSON      `json:"keys"`
}

type apiKeysResponse struct {
	IntervalSeconds   int                        `json:"interval_seconds"`
	StaleAfterSeconds int                        `json:"stale_after_seconds"`
	Reporters         []reporterJSON             `json:"reporters"`
	Forges            []forgeJSON                `json:"forges"`
	Orphaned          []keyJSON                  `json:"orphaned"`
	Unregistered      []forgekeys.ReportInstance `json:"unregistered"`
}

// handleAdminAPIKeys — GET /api/v1/admin/api-keys.
func (s *Server) handleAdminAPIKeys(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	ctx := r.Context()
	fail := func(what string, err error) {
		s.logger.Error("API keys admin list failed", "step", what, "error", err)
		http.Error(w, "could not load API keys", http.StatusInternalServerError)
	}
	rows, err := s.apiKeys.ListAdminAPIKeys(ctx)
	if err != nil {
		fail("stored keys", err)
		return
	}
	reportRows, err := s.apiKeys.LoadForgeKeyReports(ctx)
	if err != nil {
		fail("key reports", err)
		return
	}
	registry, err := s.apiKeys.LoadGitLabInstanceRegistry(ctx)
	if err != nil {
		fail("instance registry", err)
		return
	}
	repos, err := s.apiKeys.CountReposByPlatform(ctx)
	if err != nil {
		fail("repository counts", err)
		return
	}
	misrouted, err := s.apiKeys.MisroutedGitLabRepos(ctx)
	if err != nil {
		fail("misrouted repositories", err)
		return
	}
	jsonResponse(w, buildAPIKeysResponse(rows, s.freshReports(reportRows), registry, repos, misrouted))
}

// decodedReports is the fresh reports plus every reporter's freshness.
type decodedReports struct {
	fresh     []forgekeys.Report
	reporters []reporterJSON
}

// freshReports decodes the stored reports; a report older than
// reportStaleAfter (judged by the database clock) is listed as a stale
// reporter but not merged.
func (s *Server) freshReports(rows []db.ForgeKeyReportRow) decodedReports {
	var out decodedReports
	for _, row := range rows {
		stale := row.AgeSeconds > reportStaleAfter.Seconds()
		out.reporters = append(out.reporters, reporterJSON{Reporter: row.Reporter, ReportedAt: row.ReportedAt, AgeSeconds: row.AgeSeconds, Stale: stale})
		if stale {
			continue
		}
		var rep forgekeys.Report
		if err := json.Unmarshal(row.Report, &rep); err != nil {
			s.logger.Warn("unreadable API key report skipped", "reporter", logSafe(row.Reporter), "error", err)
			continue
		}
		if rep.Reporter == "" {
			rep.Reporter = row.Reporter
		}
		out.fresh = append(out.fresh, rep)
	}
	if out.reporters == nil {
		out.reporters = []reporterJSON{}
	}
	return out
}

func buildAPIKeysResponse(rows []db.AdminAPIKey, reports decodedReports, registry map[string]model.Platform,
	repos map[model.Platform]int, misrouted []db.MisroutedRepo) apiKeysResponse {
	resp := apiKeysResponse{
		IntervalSeconds: int(forgekeys.Interval.Seconds()), StaleAfterSeconds: int(reportStaleAfter.Seconds()),
		Reporters: reports.reporters, Orphaned: []keyJSON{}, Unregistered: []forgekeys.ReportInstance{},
	}

	forges, byBase, gitlabInstances := resolveForges(registry, reports)
	seenUnregistered := map[string]bool{}
	for _, rep := range reports.fresh {
		for _, in := range rep.Unregistered {
			if !seenUnregistered[in.WebURL] {
				seenUnregistered[in.WebURL] = true
				resp.Unregistered = append(resp.Unregistered, in)
			}
		}
	}
	for _, f := range forges {
		f.RepoCount = repos[model.Platform(f.PlatformID)]
		f.Health = map[string]int{}
		f.Keys = []keyJSON{}
	}
	if gl := forges[int(model.PlatformGitLab)]; gl != nil {
		gl.MisroutedCount = len(misrouted)
	}

	// What the fresh reports say about each key, per forge: the same token
	// can be held by one instance (a config key) and refused for another (a
	// stored copy), so a key is matched within its own forge only (review
	// of Phase C, finding 2).
	loads := map[string][]loadJSON{} // platform_id|key_id → per-reporter view
	loadKey := func(platformID int, keyID string) string { return strconv.Itoa(platformID) + "|" + keyID }
	notLoaded := map[int64]forgekeys.NotLoaded{}
	for _, rep := range reports.fresh {
		for _, k := range rep.Keys {
			id := loadKey(k.PlatformID, k.KeyID)
			loads[id] = append(loads[id], loadJSON{Reporter: rep.Reporter, ReportKey: k})
		}
		for _, nl := range rep.NotLoaded {
			if nl.OAuthID > 0 {
				notLoaded[nl.OAuthID] = nl
			}
		}
	}

	matched := map[string]bool{}
	for _, row := range rows {
		created := row.CreatedAt
		k := keyJSON{OAuthID: row.OAuthID, Name: row.Name, Source: string(forgekeys.SourceDatabase), KeyID: row.KeyID,
			Masked: row.KeyMask, InstanceURL: row.InstanceURL, CreatedAt: &created, Removable: true, LoadedBy: []loadJSON{}}
		var forge *forgeJSON
		switch row.Platform {
		case "github":
			forge = forges[int(model.PlatformGitHub)]
		case "gitlab":
			if base, ok := forgekeys.InstanceForTag(gitlabInstances, row.InstanceURL); ok {
				forge = byBase[base]
			}
		}
		var l []loadJSON
		id := ""
		if forge != nil {
			id = loadKey(forge.PlatformID, row.KeyID)
			l = loads[id]
		}
		switch {
		case len(l) > 0:
			matched[id] = true
			applyLoads(&k, l)
			for _, v := range l {
				if v.Source == forgekeys.SourceConfig {
					k.Detail = "also in the config file — removing this stored copy leaves the key loaded"
				}
			}
		case notLoaded[row.OAuthID].Reason != "":
			nl := notLoaded[row.OAuthID]
			k.Status, k.Reason, k.Detail = keyStatusNotLoaded, nl.Reason, nl.Detail
		case forge == nil:
			k.Status, k.Reason = keyStatusNotLoaded, forgekeys.ReasonOrphan
			k.Detail = "no configured GitLab instance has this web URL"
		case len(reports.fresh) == 0:
			k.Status, k.Reason = keyStatusNotLoaded, reasonNoReporter
			k.Detail = "no running serve process has reported in the last few minutes"
		default:
			k.Status, k.Reason = keyStatusNotLoaded, reasonPending
			k.Detail = fmt.Sprintf("not picked up yet — running serve processes reload keys every %s", forgekeys.Interval)
		}
		if forge == nil || k.Reason == forgekeys.ReasonOrphan {
			resp.Orphaned = append(resp.Orphaned, k)
			continue
		}
		forge.Keys = append(forge.Keys, k)
	}

	// Keys only the reports know: config and Augur keys, and stored keys
	// removed since the reports were written (still draining).
	ids := make([]string, 0, len(loads))
	for id := range loads {
		if !matched[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		l := loads[id]
		first := l[0]
		forge := forges[first.PlatformID]
		if forge == nil {
			continue
		}
		k := keyJSON{Source: string(first.Source), KeyID: first.KeyID, Masked: first.Masked, InstanceURL: first.InstanceURL, LoadedBy: []loadJSON{}}
		applyLoads(&k, l)
		if first.Source == forgekeys.SourceDatabase {
			k.Detail = "removed from the database — finishing its in-flight requests"
		}
		forge.Keys = append(forge.Keys, k)
	}

	for _, f := range forges {
		for _, k := range f.Keys {
			if k.Status == keyStatusNotLoaded {
				f.Health[keyStatusNotLoaded]++
			} else {
				f.Health[k.Health]++
			}
		}
		resp.Forges = append(resp.Forges, *f)
	}
	sort.Slice(resp.Forges, func(i, j int) bool { return resp.Forges[i].PlatformID < resp.Forges[j].PlatformID })
	return resp
}

// resolveForges builds the forge list: GitHub, then every GitLab instance
// the registry or a fresh report knows. Which GitLab instance is main comes
// from the first fresh report that names instances (the config decides);
// with none, the historical instance (platform_id 2, whose registry row is
// stamped from the main instance's web URL). gitlabInstances is the same set
// in the shape forgekeys.InstanceForTag resolves stored tags against — the
// ONE tag resolution the list and the add handler share (SR-17).
func resolveForges(registry map[string]model.Platform, reports decodedReports) (forges map[int]*forgeJSON, byBase map[string]*forgeJSON, gitlabInstances []config.GitLabInstance) {
	forges = map[int]*forgeJSON{int(model.PlatformGitHub): {Platform: "github", PlatformID: int(model.PlatformGitHub), WebURL: githubWebURL, Main: true, Registered: true}}
	byBase = map[string]*forgeJSON{githubWebURL: forges[int(model.PlatformGitHub)]}
	for base, id := range registry {
		f := &forgeJSON{Platform: "gitlab", PlatformID: int(id), WebURL: base, Registered: true, Main: id == model.PlatformGitLab}
		forges[int(id)], byBase[base] = f, f
	}
	mainFromReport := false
	for _, rep := range reports.fresh {
		for _, in := range rep.Instances {
			f, ok := forges[in.PlatformID]
			if !ok {
				f = &forgeJSON{Platform: in.Platform, PlatformID: in.PlatformID, WebURL: in.WebURL}
				forges[in.PlatformID], byBase[in.WebURL] = f, f
			}
			if f.Reported {
				continue
			}
			f.Reported, f.APIURL, f.ActiveKeys = true, in.APIURL, in.ActiveKeys
			if in.Platform != "gitlab" {
				continue
			}
			if !mainFromReport {
				mainFromReport = true
				for _, g := range forges {
					if g.Platform == "gitlab" {
						g.Main = false
					}
				}
			}
			f.Main = in.Main
		}
	}
	for _, f := range forges {
		if f.Platform == "gitlab" {
			gitlabInstances = append(gitlabInstances, config.GitLabInstance{WebBase: f.WebURL, Primary: f.Main})
		}
	}
	sort.Slice(gitlabInstances, func(i, j int) bool { return gitlabInstances[i].WebBase < gitlabInstances[j].WebBase })
	return forges, byBase, gitlabInstances
}

// healthRank orders key health from best to worst.
var healthRank = map[platform.KeyHealth]int{
	platform.HealthOK: 0, platform.HealthExhausted: 1, platform.HealthResting: 2,
	platform.HealthQuarantined: 3, platform.HealthInvalid: 4,
}

// applyLoads sets a key's status and health from the processes holding it:
// loaded when any holds it active, draining when every one is draining; the
// worst health any of them reports.
func applyLoads(k *keyJSON, l []loadJSON) {
	k.LoadedBy = l
	k.Status = keyStatusDraining
	worst := platform.HealthOK
	for _, v := range l {
		if v.State != platform.KeyDraining {
			k.Status = keyStatusLoaded
		}
		if healthRank[v.Health] > healthRank[worst] {
			worst = v.Health
		}
	}
	k.Health = string(worst)
}

type apiKeyAddRequest struct {
	Platform    string `json:"platform"`
	InstanceURL string `json:"instance_url"`
	Name        string `json:"name"`
	Token       string `json:"token"`
}

type apiKeyAddResponse struct {
	OAuthID       int64  `json:"oauth_id"`
	KeyID         string `json:"key_id"`
	Masked        string `json:"masked"`
	Platform      string `json:"platform"`
	InstanceURL   string `json:"instance_url"`
	APIURL        string `json:"api_url,omitempty"`
	PickupSeconds int    `json:"pickup_seconds"`
}

// handleAdminAPIKeyAdd — POST /api/v1/admin/api-keys with
// {"platform","instance_url","name","token"} in the JSON body.
func (s *Server) handleAdminAPIKeyAdd(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	badRequest := func(msg string) { http.Error(w, msg, http.StatusBadRequest) }
	if r.URL.Query().Has("token") {
		badRequest("send the token in the JSON body only — a query string ends up in access logs")
		return
	}
	var req apiKeyAddRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, apiKeyBodyLimit)).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		badRequest("invalid JSON body")
		return
	}
	token := strings.TrimSpace(req.Token)
	name := strings.TrimSpace(req.Name)
	switch {
	case req.Platform != "github" && req.Platform != "gitlab":
		badRequest(`platform must be "github" or "gitlab"`)
		return
	case token == "":
		badRequest("token is required")
		return
	case len(token) > maxTokenBytes:
		badRequest(fmt.Sprintf("token is longer than %d bytes", maxTokenBytes))
		return
	case !printableASCII(token):
		badRequest("token must be printable ASCII with no spaces")
		return
	case len(name) > maxKeyNameBytes:
		badRequest(fmt.Sprintf("name is longer than %d bytes", maxKeyNameBytes))
		return
	}
	if req.Platform == "gitlab" && hasAnyPrefix(token, gitHubTokenPrefixes) {
		badRequest("this looks like a GitHub token — a GitLab instance would receive it; add it with platform github")
		return
	}
	if req.Platform == "github" && hasAnyPrefix(token, gitLabTokenPrefixes) {
		badRequest("this looks like a GitLab token — GitHub would receive it; add it with platform gitlab and its instance")
		return
	}

	ctx := r.Context()
	instance := ""
	var registry map[string]model.Platform
	switch req.Platform {
	case "github":
		if strings.TrimSpace(req.InstanceURL) != "" {
			badRequest("GitHub keys take no instance_url (GitHub has one instance)")
			return
		}
	case "gitlab":
		if strings.TrimSpace(req.InstanceURL) == "" {
			badRequest("instance_url is required for a GitLab key — the web URL of the GitLab instance that issued it")
			return
		}
		nb, err := model.NormalizeInstanceWebBase(req.InstanceURL)
		if err != nil {
			badRequest("instance_url is not a valid GitLab web URL")
			return
		}
		registry, err = s.apiKeys.LoadGitLabInstanceRegistry(ctx)
		if err != nil {
			s.logger.Error("API key add: instance registry read failed", "error", err)
			http.Error(w, "could not check the GitLab instance", http.StatusInternalServerError)
			return
		}
		for base := range registry {
			if model.SchemelessWebBase(base) == model.SchemelessWebBase(nb) {
				instance = base
			}
		}
		if instance == "" {
			badRequest(fmt.Sprintf("%s is not a registered GitLab instance — add it to gitlab.instances in aveloxis.json and run aveloxis migrate", nb))
			return
		}
	}

	keyID := platform.KeyID(token)
	masked := platform.MaskToken(token)
	reports := s.loadFreshReportsForCheck(ctx)
	apiURL := ""
	for _, rep := range reports.fresh {
		for _, in := range rep.Instances {
			if apiURL == "" && ((req.Platform == "github" && in.Platform == "github") || (in.Platform == "gitlab" && in.WebURL == instance)) {
				apiURL = in.APIURL
			}
		}
		for _, k := range rep.Keys {
			if k.KeyID != keyID || k.Platform != req.Platform || k.Source != forgekeys.SourceConfig {
				continue
			}
			where := k.InstanceURL
			if req.Platform == "gitlab" && k.InstanceURL != instance {
				http.Error(w, fmt.Sprintf("this token is a config file key of %s — a key belongs to the one instance that issued it", where), http.StatusConflict)
				return
			}
			http.Error(w, fmt.Sprintf("this token is already loaded from the config file for %s", where), http.StatusConflict)
			return
		}
	}

	id, err := s.apiKeys.InsertAPIKey(ctx, name, token, req.Platform, instance)
	var exists *db.APIKeyExistsError
	switch {
	case errors.As(err, &exists):
		if sameStoredInstance(exists.InstanceURL, instance, registry, reports) {
			http.Error(w, fmt.Sprintf("this key is already stored (oauth_id %d)", exists.OAuthID), http.StatusConflict)
			return
		}
		where := exists.InstanceURL
		if where == "" {
			where = "the main GitLab instance"
		}
		http.Error(w, fmt.Sprintf("this key is already stored for %s (oauth_id %d) — remove it first, then add it for the new instance", where, exists.OAuthID), http.StatusConflict)
		return
	case err != nil:
		s.logger.Error("API key add failed", "platform", req.Platform, "instance_url", instance, "key_id", keyID, "error", err)
		http.Error(w, "could not store the key", http.StatusInternalServerError)
		return
	}
	s.logger.Info("API key added via the admin API", "admin_user_id", info.UserID, "platform", req.Platform,
		"instance_url", instance, "oauth_id", id, "key_id", keyID, "token", masked, "name", logSafe(name))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(apiKeyAddResponse{OAuthID: id, KeyID: keyID, Masked: masked, Platform: req.Platform,
		InstanceURL: instance, APIURL: apiURL, PickupSeconds: int(forgekeys.Interval.Seconds())})
}

type apiKeyDeleteResponse struct {
	OAuthID               int64  `json:"oauth_id"`
	KeyID                 string `json:"key_id"`
	Masked                string `json:"masked"`
	Platform              string `json:"platform"`
	InstanceURL           string `json:"instance_url"`
	StillLoadedFromConfig bool   `json:"still_loaded_from_config"`
	DrainSeconds          int    `json:"drain_seconds"`
}

// handleAdminAPIKeyDelete — POST /api/v1/admin/api-keys/{oauthID}/delete.
func (s *Server) handleAdminAPIKeyDelete(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("oauthID"), 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "invalid key id", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	removed, err := s.apiKeys.DeleteAPIKey(ctx, id)
	if errors.Is(err, db.ErrAPIKeyNotFound) {
		http.Error(w, "no stored key with that id", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Error("API key removal failed", "oauth_id", id, "error", err)
		http.Error(w, "could not remove the key", http.StatusInternalServerError)
		return
	}
	still := false
	for _, rep := range s.loadFreshReportsForCheck(ctx).fresh {
		for _, k := range rep.Keys {
			if k.KeyID == removed.KeyID && k.Platform == removed.Platform && k.Source == forgekeys.SourceConfig {
				still = true
			}
		}
	}
	s.logger.Info("API key removed via the admin API", "admin_user_id", info.UserID, "platform", removed.Platform,
		"instance_url", removed.InstanceURL, "oauth_id", removed.OAuthID, "key_id", removed.KeyID, "token", removed.KeyMask,
		"still_loaded_from_config", still)
	jsonResponse(w, apiKeyDeleteResponse{OAuthID: removed.OAuthID, KeyID: removed.KeyID, Masked: removed.KeyMask,
		Platform: removed.Platform, InstanceURL: removed.InstanceURL, StillLoadedFromConfig: still,
		DrainSeconds: int(forgekeys.Interval.Seconds())})
}

// loadFreshReportsForCheck is the reports read for a mutation's advisory
// checks: a read failure is logged and treated as "no reports" — the
// running processes' own resolver still refuses a conflicting key.
func (s *Server) loadFreshReportsForCheck(ctx context.Context) decodedReports {
	rows, err := s.apiKeys.LoadForgeKeyReports(ctx)
	if err != nil {
		s.logger.Warn("API key reports unreadable — config-key conflict check skipped", "error", err)
		return decodedReports{}
	}
	return s.freshReports(rows)
}

func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// sameStoredInstance reports whether an existing row's instance tag is the
// instance a new add names. A tag resolves exactly as running processes
// load it — forgekeys.InstanceForTag against the forges resolveForges
// builds — so "" (a key add-key stored without --instance) is the main
// instance (review of Phase C, finding 5). GitHub has one instance.
func sameStoredInstance(existingTag, instance string, registry map[string]model.Platform, reports decodedReports) bool {
	if instance == "" {
		return existingTag == ""
	}
	_, _, gitlabInstances := resolveForges(registry, reports)
	base, ok := forgekeys.InstanceForTag(gitlabInstances, existingTag)
	return ok && base == instance
}
