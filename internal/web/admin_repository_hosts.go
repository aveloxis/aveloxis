// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"fmt"
	"net/http"
	"time"

	"github.com/aveloxis/aveloxis/internal/admin"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/hostguard"
)

// The repository-hosts page of the administrator console (0.29.188;
// summary/70, 4.5, D14). Thin handlers over internal/admin, which decides
// who may and validates the host; every action has a GET confirm page and a
// POST (D-19).

// WithRepositoryHosts names the hosts of the configured Maven repositories,
// which the page lists whether or not they have ever answered.
func (s *Server) WithRepositoryHosts(hosts []string) *Server {
	s.repositoryHosts = append([]string(nil), hosts...)
	return s
}

// repositoryHostView is one row of the page.
type repositoryHostView struct {
	Host          string
	FirstAnswered string
	LastAnswered  string
	Distrusted    bool
	DistrustedOn  string
	DistrustedBy  string
	Note          string
	Impact        db.RepositoryHostImpact
	// Signals are the standing signals, in words; NotKnown the checks
	// whose last look could not tell. Attention: a signal not yet seen.
	Signals   []string
	NotKnown  []string
	Attention bool
}

// signalWords says what each kind of check found, for the page.
var signalWords = map[string]string{
	"unresolvable":     "the name no longer resolves",
	"refused_address":  "the name resolves to an address Aveloxis does not reach",
	"certificate":      "the certificate is not valid for this host",
	"registered_later": "the domain was registered after Aveloxis first fetched from this host",
	"threat_list":      "it is on the configured threat list",
}

// notKnownWords names each check when its last look could not tell.
var notKnownWords = map[string]string{
	"unresolvable":     "whether the name resolves",
	"refused_address":  "which address the name resolves to",
	"certificate":      "whether the certificate is valid",
	"registered_later": "when the domain was registered",
	"threat_list":      "whether it is on the threat list",
}

func kindWords(words map[string]string, kind string) string {
	if w, ok := words[kind]; ok {
		return w
	}
	return kind
}

// signalLines fills the view's three signal fields from the store's rows.
func (v *repositoryHostView) signalLines(signals []db.RepositoryHostSignal) {
	for _, g := range signals {
		if g.State == "signal" {
			line := kindWords(signalWords, g.Kind) + ", since " + g.Since.UTC().Format("2006-01-02")
			if g.Detail != "" {
				line += " (" + g.Detail + ")"
			}
			v.Signals = append(v.Signals, line)
			v.Attention = v.Attention || g.NeedsAttention()
		}
		if g.UnknownSince != nil || g.State == "unknown" {
			v.NotKnown = append(v.NotKnown, kindWords(notKnownWords, g.Kind))
		}
	}
}

func hostDay(t *time.Time) string {
	if t == nil {
		return "never"
	}
	return t.UTC().Format("2006-01-02")
}

func (s *Server) handleAdminRepositoryHosts(w http.ResponseWriter, r *http.Request, sess *Session) {
	hosts, err := s.admin.ListRepositoryHosts(r.Context(), sessionActor(sess), s.repositoryHosts)
	if err != nil {
		s.serviceErr(w, r, "handleAdminRepositoryHosts", err)
		return
	}
	configured := map[string]bool{}
	for _, h := range s.repositoryHosts {
		configured[hostguard.Normalize(h)] = true
	}
	var attention, distrusted, conf, others []repositoryHostView
	for _, h := range hosts {
		v := repositoryHostView{Host: h.Host, FirstAnswered: hostDay(h.FirstFetchedAt), LastAnswered: hostDay(h.LastAnsweredAt),
			Distrusted: h.DistrustedAt != nil, DistrustedBy: h.DistrustedBy, Note: h.Note, Impact: h.Impact}
		v.signalLines(h.Signals)
		switch {
		case v.Distrusted:
			v.DistrustedOn = hostDay(h.DistrustedAt)
			distrusted = append(distrusted, v)
		case v.Attention:
			attention = append(attention, v)
		case configured[h.Host]:
			conf = append(conf, v)
		default:
			others = append(others, v)
		}
	}
	s.render(w, r, "admin_repository_hosts", map[string]any{
		"Session": sess, "Notice": adminNotice(r), "Attention": attention, "Distrusted": distrusted, "Configured": conf, "Others": others,
	})
}

// countNoun is "1 stored result" / "3 stored results".
func countNoun(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

// distrustEffects says, in sentences, what a distrust will do.
func distrustEffects(im db.RepositoryHostImpact) []string {
	return []string{
		countNoun(im.StoredResults, "stored result", "stored results") + " from this host will be deleted now.",
		countNoun(im.Repositories, "repository depends", "repositories depend") + " on it for a version or a Maven tree.",
		countNoun(im.Findings, "current finding", "current findings") + " will close as \"version no longer known\" at the next analysis.",
		countNoun(im.Trees, "Maven tree", "Maven trees") + " will no longer be read whole, so managed entries return to the inventory.",
	}
}

func (s *Server) handleAdminRepositoryHostDistrustConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	// The service's own rule for a host, before the page carries the value
	// back in its hidden field.
	host, im, err := s.admin.PreviewRepositoryHostDistrust(r.Context(), sessionActor(sess), r.URL.Query().Get("host"))
	if err != nil {
		s.serviceErr(w, r, "handleAdminRepositoryHostDistrustConfirm", err)
		return
	}
	effects := distrustEffects(im)
	for _, h := range s.repositoryHosts {
		if hostguard.Normalize(h) == host {
			// A configured repository: say that the configuration's list
			// is being shortened by this decision.
			effects = append([]string{"This host is one of the configured repositories (collection.maven_repositories). It stays in aveloxis.json and is no longer asked: the list in use is the configured one without it."}, effects...)
			break
		}
	}
	s.render(w, r, "admin_repository_host_distrust", map[string]any{
		"Session": sess, "Host": host, "Effects": effects, "MaxNote": admin.MaxRepositoryHostNoteBytes,
	})
}

func (s *Server) handleAdminRepositoryHostDistrust(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	_, changed, err := s.admin.DistrustRepositoryHost(r.Context(), sessionActor(sess), r.FormValue("host"), r.FormValue("note"))
	if err != nil {
		s.serviceErr(w, r, "handleAdminRepositoryHostDistrust", err)
		return
	}
	ok := "host_distrusted"
	if !changed {
		ok = "host_already_distrusted"
	}
	http.Redirect(w, r, "/admin/repository-hosts?ok="+ok, http.StatusFound)
}

func (s *Server) handleAdminRepositoryHostDismiss(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	if _, err := s.admin.DismissRepositoryHostSignals(r.Context(), sessionActor(sess), r.FormValue("host")); err != nil {
		s.serviceErr(w, r, "handleAdminRepositoryHostDismiss", err)
		return
	}
	http.Redirect(w, r, "/admin/repository-hosts?ok=host_dismissed", http.StatusFound)
}

func (s *Server) handleAdminRepositoryHostRestoreConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	host, err := admin.RepositoryHostName(r.URL.Query().Get("host"))
	if err != nil {
		s.serviceErr(w, r, "handleAdminRepositoryHostRestoreConfirm", err)
		return
	}
	s.confirmPage(w, r, sess, "Restore a repository host",
		"Restore "+host+"? It is fetched from again like any other host, and versions that came through it return as each repository is next analysed.",
		"/admin/repository-hosts/restore", "Restore", "/admin/repository-hosts", map[string]string{"host": host})
}

func (s *Server) handleAdminRepositoryHostRestore(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	changed, err := s.admin.RestoreRepositoryHost(r.Context(), sessionActor(sess), r.FormValue("host"))
	if err != nil {
		s.serviceErr(w, r, "handleAdminRepositoryHostRestore", err)
		return
	}
	if !changed {
		http.Error(w, "that host is not distrusted", http.StatusNotFound)
		return
	}
	http.Redirect(w, r, "/admin/repository-hosts?ok=host_restored", http.StatusFound)
}
