// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package static embeds the shared static assets: the icon the monitor
// and the web GUI serve, and (0.29.102, stage 2e of the admin-console
// program, WEB-9/B6) the web GUI's page assets — the stylesheet, the page
// scripts and the vendored Chart.js — served by Handler at /static/. No
// page carries an inline script or an on*= attribute, and no page loads a
// CDN, so the Content-Security-Policy names one script source.
package static

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed icon.png
var IconPNG []byte

//go:embed app.css avx.js repo-search.js compare.js repo-detail.js monitor.js admin-cache.js vendor/chart.umd.js
var pages embed.FS

// Asset is one served page asset: its path under /static/, its media type
// and its content.
type Asset struct {
	Path        string
	ContentType string
	Body        []byte
	etag        string
}

// Assets lists every page asset Handler serves, in a fixed order (the
// tests read the set; the templates reference it by path).
func Assets() []Asset {
	var out []Asset
	_ = fs.WalkDir(pages, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || p == "icon.png" {
			return err
		}
		b, _ := pages.ReadFile(p)
		sum := sha256.Sum256(b)
		out = append(out, Asset{Path: p, ContentType: contentType(p), Body: b, etag: `"` + hex.EncodeToString(sum[:16]) + `"`})
		return nil
	})
	return out
}

// etagMatches: the browser's If-None-Match names the served ETag, in its
// strong form or the weak form (W/"…") a compressing proxy such as nginx
// turns a strong tag into — otherwise every page view behind such a proxy
// re-downloaded Chart.js (review B8). "*" matches too.
func etagMatches(header, etag string) bool {
	for _, c := range strings.Split(header, ",") {
		c = strings.TrimSpace(c)
		if c == "*" || c == etag || strings.TrimPrefix(c, "W/") == etag {
			return true
		}
	}
	return false
}

func contentType(p string) string {
	switch path.Ext(p) {
	case ".css":
		return "text/css; charset=utf-8"
	case ".js":
		return "text/javascript; charset=utf-8"
	case ".md":
		return "text/markdown; charset=utf-8"
	}
	return "application/octet-stream"
}

// Handler serves the page assets at the given prefix ("/static/"): an
// exact name from the embedded set, its media type, a content ETag with
// must-revalidate (a new release's asset is fetched again, an unchanged one
// is a 304), nosniff; anything else is a 404. GET and HEAD only.
func Handler(prefix string) http.Handler {
	byPath := map[string]Asset{}
	for _, a := range Assets() {
		byPath[a.Path] = a
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a, ok := byPath[strings.TrimPrefix(r.URL.Path, prefix)]
		if !ok {
			http.NotFound(w, r)
			return
		}
		h := w.Header()
		h.Set("Content-Type", a.ContentType)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Cache-Control", "public, max-age=0, must-revalidate")
		h.Set("ETag", a.etag)
		if etagMatches(r.Header.Get("If-None-Match"), a.etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = w.Write(a.Body)
	})
}
