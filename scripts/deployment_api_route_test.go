// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestDeploymentRecipeRoutesTheGUIsAPIThroughTheWeb (Copilot on PR #241,
// 0.29.115): the built-in GUI's pages fetch /api/ on their own origin with
// no Authorization header; the web process's /api/ proxy attaches the
// signed-in session's Bearer (0.29.110). A recipe that sends the GUI
// host's /api/ straight to the api process therefore answers 401 to every
// browser outside exempt_cidrs once api.require_auth is true — which the
// same page's configuration sets. The GUI host's server block must not
// reach the api port; a direct-API host (Bearer clients) is a separate
// server block. The API guide must not advise the direct route either.
func TestDeploymentRecipeRoutesTheGUIsAPIThroughTheWeb(t *testing.T) {
	doc := srctest.Read(t, "docs/getting-started/deployment.md")
	const guiHost = "server_name aveloxis.example.org;"
	blocks := nginxServerBlocks(doc)
	if len(blocks) == 0 {
		t.Fatal("deployment.md carries no nginx server block")
	}
	found := false
	for _, b := range blocks {
		if !strings.Contains(b, guiHost) {
			continue
		}
		found = true
		// Every proxy_pass of the GUI host targets the web process — any
		// spelling of the api (localhost, another port) is refused, not
		// only the one literal (L10 round 1 of 0.29.115: a localhost:8383
		// location passed the literal check).
		targets := 0
		for _, line := range strings.Split(b, "\n") {
			f := strings.Fields(strings.TrimSpace(line))
			if len(f) < 2 || f[0] != "proxy_pass" {
				continue
			}
			targets++
			if target := strings.TrimSuffix(f[1], ";"); target != "http://127.0.0.1:8082" {
				t.Errorf("the GUI host proxies to %s; its every path, /api/ included, must go to the web process (http://127.0.0.1:8082), which attaches the session's Bearer", target)
			}
		}
		if targets == 0 {
			t.Errorf("the GUI host's server block has no proxy_pass:\n%s", b)
		}
	}
	if !found {
		t.Fatalf("no nginx server block names %q", guiHost)
	}
	if strings.Contains(doc, `"require_auth": true`) && !strings.Contains(doc, "api.aveloxis.example.org") {
		t.Error("the recipe sets require_auth true but documents no direct-API host for Bearer clients")
	}
	api := strings.Join(strings.Fields(srctest.Read(t, "docs/guide/api.md")), " ") // prose wraps: match across line breaks
	if strings.Contains(api, "send `/api/` to the API directly") {
		t.Error("api.md still advises sending the GUI host's /api/ to the API directly")
	}
}

// nginxServerBlocks returns each top-level `server { … }` block of the
// document's ```nginx fences, braces balanced.
func nginxServerBlocks(doc string) []string {
	var out []string
	for _, fence := range strings.Split(doc, "```nginx")[1:] {
		end := strings.Index(fence, "```")
		if end < 0 {
			continue
		}
		body := fence[:end]
		for {
			i := strings.Index(body, "server {")
			if i < 0 {
				break
			}
			depth, j := 0, i
			for ; j < len(body); j++ {
				if body[j] == '{' {
					depth++
				} else if body[j] == '}' {
					depth--
					if depth == 0 {
						break
					}
				}
			}
			if j >= len(body) {
				break
			}
			out = append(out, body[i:j+1])
			body = body[j+1:]
		}
	}
	return out
}

// L10 round 3 of 0.29.125: the API host's plain-HTTP refusal was an ignored
// duplicate — `certbot --nginx` adds `listen 80;` to a block with no listen
// and splits the port-80 half off only with its redirect, so the proxy
// block and the `return 403` block both took port 80 for one name and nginx
// kept the first (a warning, not an error: `nginx -t` passes). The recipe
// now states every listen of the API host itself. Two blocks sharing a
// server_name must never both take port 80 (an explicit `listen 80`, or no
// listen at all), and the block that proxies the api states `listen 443 ssl`.
func TestDeploymentRecipeRefusesPlainHTTPOnTheAPIHostUnambiguously(t *testing.T) {
	doc := srctest.Read(t, "docs/getting-started/deployment.md")
	blocks := nginxServerBlocks(doc)
	srctest.MinCount(t, "nginx server blocks in deployment.md", len(blocks), 3)
	listen80 := map[string]int{}
	listenRE := regexp.MustCompile(`(?m)^\s*listen\s+([^;]+);`)
	nameRE := regexp.MustCompile(`(?m)^\s*server_name\s+([^;]+);`)
	apiBlocks := 0
	for _, b := range blocks {
		name := nameRE.FindStringSubmatch(b)
		if name == nil {
			t.Errorf("a server block without server_name:\n%s", b)
			continue
		}
		listens := listenRE.FindAllStringSubmatch(b, -1)
		on80 := len(listens) == 0 // nginx's implicit *:80 for the superuser
		for _, l := range listens {
			// The address is the first token (`80`, `[::]:80`, `0.0.0.0:80`,
			// `443 ssl`); its port is what follows the last colon, or the
			// whole token. A prefix test read 8080 as 80 (the mutant that
			// survived).
			addr := strings.Fields(l[1])[0]
			if port := addr[strings.LastIndex(addr, ":")+1:]; port == "80" {
				on80 = true
			}
		}
		if on80 {
			listen80[strings.TrimSpace(name[1])]++
		}
		if strings.Contains(b, "127.0.0.1:8383") {
			apiBlocks++
			if !regexp.MustCompile(`(?m)^\s*listen\s+443 ssl;`).MatchString(b) {
				t.Errorf("the API host's proxy block must state `listen 443 ssl;` itself (certbot --nginx would add listen 80 and keep serving in clear):\n%s", b)
			}
			if !strings.Contains(b, "ssl_certificate ") || !strings.Contains(b, "ssl_certificate_key ") {
				t.Errorf("the API host's proxy block states its certificate lines (certonly fetches, edits nothing):\n%s", b)
			}
		}
		if strings.Contains(b, "return 403") && !on80 {
			t.Errorf("the plain-HTTP refusal must listen on 80:\n%s", b)
		}
	}
	srctest.MinCount(t, "blocks proxying the api", apiBlocks, 1)
	for name, n := range listen80 {
		if n > 1 {
			t.Errorf("%d server blocks for %q take port 80; nginx keeps one and ignores the rest with a warning `nginx -t` does not fail on", n, name)
		}
	}
	if !strings.Contains(doc, "certbot certonly --nginx -d api.") {
		t.Error("the API host's certificate comes from `certbot certonly --nginx` (the challenge only); `certbot --nginx` rewrites the block")
	}
}
