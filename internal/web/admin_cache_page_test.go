// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

// D4 (0.29.123, summary/63 §9): the Cache console page — administrators
// only, linked from the console, and a shell whose script reads the api's
// cache view through the /api/ proxy and renders it as text.

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestAdminCachePageIsAnAdministratorConsolePage(t *testing.T) {
	src := srctest.StripGoComments(srctest.PackageFunc(t, "internal/web", "func (s *Server) Handler("))
	if !strings.Contains(src, `mux.HandleFunc("GET /admin/cache", s.requireAdmin(s.handleAdminCachePage))`) {
		t.Error("GET /admin/cache must be registered through requireAdmin")
	}
	links := srctest.Read(t, "internal/web/templates/adminLinks.html")
	if !strings.Contains(links, `<a href="/admin/cache">Cache</a>`) {
		t.Error("the console links must carry the Cache page")
	}
}

// The script reads the three view routes — each one the API registers — and
// renders through avxEl only: a cached request's URI carries its query, which
// must never become markup.
func TestAdminCacheScriptReadsTheViewAsText(t *testing.T) {
	js := pageScripts(t)["admin-cache.js"]
	if js == "" {
		t.Fatal("admin-cache.js is not embedded")
	}
	for _, route := range []string{"'/api/v1/admin/cache'", "'/api/v1/admin/cache/repos?sort='", "'/api/v1/admin/cache/repos/'"} {
		if !strings.Contains(js, route) {
			t.Errorf("admin-cache.js must read %s", route)
		}
	}
	for _, banned := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"} {
		if strings.Contains(js, banned) {
			t.Errorf("admin-cache.js uses %s: render through avxEl (text only)", banned)
		}
	}
	if !regexp.MustCompile(`avxEl\('td'`).MatchString(js) {
		t.Error("admin-cache.js must build its cells with avxEl")
	}
	api := srctest.StripGoComments(srctest.PackageSource(t, "internal/api", 20))
	for _, route := range []string{`"GET /api/v1/admin/cache"`, `"GET /api/v1/admin/cache/repos"`, `"GET /api/v1/admin/cache/repos/{repoID}"`} {
		if !strings.Contains(api, route) {
			t.Errorf("the API does not register %s, which the page reads", route)
		}
	}
}
