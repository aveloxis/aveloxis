// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// Supply-chain endpoints (v0.29.60): the package-centred view of the
// vulnerability findings for the GUI's dependencies page.
//
//	GET /api/v1/supply-chain/packages?ecosystem=&q=&sort=&limit=&offset=&group=
//	GET /api/v1/supply-chain/packages/{ecosystem}/{name...}?group=&repos=
//
// Scope: an admin without ?group reads the FLEET (the materialized views);
// a signed-in user without ?group reads their own scope (every repository
// their groups hold) live; ?group=<id> reads that group's repositories
// live (a non-admin must own the group; the group's repositories are
// intersected with the caller's scope). The live path aggregates the
// cohort's findings on request; the 60 s cache the compare endpoints use
// covers the hot case.

// supplyChainScope is the cohort a request resolved to.
type supplyChainScope struct {
	Kind    string `json:"kind"` // "fleet" | "user" | "group"
	GroupID int64  `json:"group_id,omitempty"`
	// Repos is the cohort size for a live scope; 0 for the fleet (the
	// materialized view does not carry it).
	Repos int `json:"repos"`
	// Source says what answered: "matview" (the fleet views) or "live"
	// (a cohort aggregate, or the fleet without its views built).
	Source string `json:"source"`
	ids    []int64
}

func sourceLabel(live bool) string {
	if live {
		return "live"
	}
	return "matview"
}

func (s *Server) resolveSupplyChainScope(w http.ResponseWriter, r *http.Request, info authInfo) (supplyChainScope, bool) {
	groupParam := strings.TrimSpace(r.URL.Query().Get("group"))
	if groupParam == "" {
		if info.IsAdmin {
			return supplyChainScope{Kind: "fleet"}, true
		}
		ids := make([]int64, 0, len(info.Scope))
		for id := range info.Scope {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return supplyChainScope{Kind: "user", Repos: len(ids), ids: ids}, true
	}
	gid, err := strconv.ParseInt(groupParam, 10, 64)
	if err != nil || gid <= 0 {
		http.Error(w, "invalid group", http.StatusBadRequest)
		return supplyChainScope{}, false
	}
	ids, err := s.store.GetGroupRepoIDsForUser(r.Context(), gid, info.UserID, info.IsAdmin)
	if errors.Is(err, db.ErrGroupNotFound) {
		http.Error(w, "group not found", http.StatusNotFound)
		return supplyChainScope{}, false
	}
	if err != nil {
		s.logger.Warn("supply-chain group scope failed", "group_id", gid, "error", err)
		http.Error(w, "group lookup failed", http.StatusInternalServerError)
		return supplyChainScope{}, false
	}
	if !info.IsAdmin {
		kept := ids[:0]
		for _, id := range ids {
			if info.Scope[id] {
				kept = append(kept, id)
			}
		}
		ids = kept
	}
	return supplyChainScope{Kind: "group", GroupID: gid, Repos: len(ids), ids: ids}, true
}

func (s *Server) handleSupplyChainPackages(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	if s.store == nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	scope, ok := s.resolveSupplyChainScope(w, r, info)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	sortKey := q.Get("sort")
	if sortKey == "" {
		sortKey = "repos"
	}
	if _, ok := db.PackageExposureSorts[sortKey]; !ok {
		http.Error(w, "invalid sort", http.StatusBadRequest)
		return
	}
	query := db.PackageExposureQuery{
		RepoIDs:   scope.ids,
		Ecosystem: strings.ToLower(strings.TrimSpace(q.Get("ecosystem"))),
		Search:    strings.TrimSpace(q.Get("q")),
		Sort:      sortKey,
		Limit:     limit,
		Offset:    offset,
	}
	key := fmt.Sprintf("sc-list|%s|%d|%d|%s|%s|%s|%d|%d", scope.Kind, scope.GroupID, info.UserID, query.Ecosystem, query.Search, sortKey, limit, offset)
	if body, ok := s.cmpCache.get(key); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}
	page, err := s.store.ListPackageExposure(r.Context(), query)
	if err != nil {
		s.logger.Warn("supply-chain package list failed", "scope", scope.Kind, "error", err)
		http.Error(w, "package list failed", http.StatusInternalServerError)
		return
	}
	rows := page.Rows
	if rows == nil {
		rows = []*db.PackageExposure{}
	}
	scope.Source = sourceLabel(page.Live)
	body, _ := json.Marshal(map[string]any{
		"scope":        scope,
		"total":        page.Total,
		"packages":     rows,
		"sort":         sortKey,
		"generated_at": time.Now().UTC(),
	})
	s.cmpCache.put(key, body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

type supplyChainAdvisoryJSON struct {
	db.PackageAdvisory
	AdvisoryURL string `json:"advisory_url"`
	CVEURL      string `json:"cve_url,omitempty"`
}

func (s *Server) handleSupplyChainPackage(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	if s.store == nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	ecosystem := strings.ToLower(strings.TrimSpace(r.PathValue("ecosystem")))
	name := strings.TrimSpace(r.PathValue("name"))
	if ecosystem == "" || name == "" {
		http.Error(w, "ecosystem and package name are required", http.StatusBadRequest)
		return
	}
	scope, ok := s.resolveSupplyChainScope(w, r, info)
	if !ok {
		return
	}
	repoLimit, _ := strconv.Atoi(r.URL.Query().Get("repos"))
	key := fmt.Sprintf("sc-pkg|%s|%d|%d|%s|%s|%d", scope.Kind, scope.GroupID, info.UserID, ecosystem, name, repoLimit)
	if body, ok := s.cmpCache.get(key); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}
	ctx := r.Context()
	profile, live, err := s.store.GetPackageExposure(ctx, ecosystem, name, scope.ids)
	scope.Source = sourceLabel(live)
	if errors.Is(err, db.ErrPackageNotFound) {
		http.Error(w, "package has no findings in this scope", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Warn("supply-chain package profile failed", "ecosystem", ecosystem, "package", name, "error", err)
		http.Error(w, "package profile failed", http.StatusInternalServerError)
		return
	}
	advisories, err := s.store.GetPackageAdvisories(ctx, ecosystem, name, scope.ids)
	if err != nil {
		s.logger.Warn("supply-chain advisories failed", "ecosystem", ecosystem, "package", name, "error", err)
		http.Error(w, "package advisories failed", http.StatusInternalServerError)
		return
	}
	versions, err := s.store.GetPackageVersionsInUse(ctx, ecosystem, name, scope.ids)
	if err != nil {
		s.logger.Warn("supply-chain versions failed", "ecosystem", ecosystem, "package", name, "error", err)
		http.Error(w, "package versions failed", http.StatusInternalServerError)
		return
	}
	repos, err := s.store.GetPackageExposedRepos(ctx, ecosystem, name, scope.ids, repoLimit)
	if err != nil {
		s.logger.Warn("supply-chain repos failed", "ecosystem", ecosystem, "package", name, "error", err)
		http.Error(w, "package repositories failed", http.StatusInternalServerError)
		return
	}
	advOut := make([]supplyChainAdvisoryJSON, 0, len(advisories))
	for _, a := range advisories {
		osv, cve := advisoryURLs(a.VulnID, a.CVEID)
		advOut = append(advOut, supplyChainAdvisoryJSON{PackageAdvisory: a, AdvisoryURL: osv, CVEURL: cve})
	}
	if versions == nil {
		versions = []db.PackageVersionUse{}
	}
	if repos == nil {
		repos = []db.PackageExposedRepo{}
	}
	body, _ := json.Marshal(map[string]any{
		"scope":        scope,
		"package":      profile,
		"advisories":   advOut,
		"versions":     versions,
		"repos":        repos,
		"generated_at": time.Now().UTC(),
	})
	s.cmpCache.put(key, body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}
