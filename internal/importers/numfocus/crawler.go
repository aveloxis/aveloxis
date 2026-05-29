// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package numfocus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

// URLs we crawl. Stable since numfocus.org launched the current
// listing pages in ~2022.
const (
	SponsoredListingURL  = "https://numfocus.org/sponsored-projects"
	AffiliatedListingURL = "https://numfocus.org/sponsored-projects/affiliated-projects"
)

// ScrapedProject is what the crawler extracts from a listing page.
// Less rich than catalog.Project — the listing pages don't carry
// reliable GitHub org/repo info; that's what the catalog YAML is for.
// The crawler only produces enough signal for --detect-new to flag
// catalog drift.
type ScrapedProject struct {
	Slug    string // numfocus.org URL slug for sponsored (from /project/{slug}); normalized name for affiliated
	Name    string // display name as shown on numfocus.org
	URL     string // primary link from the listing tile (project website OR /project/{slug})
	Section string // "sponsored" or "affiliated"
}

// Crawl fetches both listing pages and returns every project
// observed. Errors out only on HTTP/parse failure; the returned
// slice may be empty if both pages parsed but contained no
// recognizable entries (a strong signal that numfocus.org changed
// its layout).
func Crawl(ctx context.Context, client *http.Client) ([]ScrapedProject, error) {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	sponsored, err := crawlURL(ctx, client, SponsoredListingURL, parseSponsored, "sponsored")
	if err != nil {
		return nil, fmt.Errorf("crawl sponsored: %w", err)
	}
	affiliated, err := crawlURL(ctx, client, AffiliatedListingURL, parseAffiliated, "affiliated")
	if err != nil {
		return nil, fmt.Errorf("crawl affiliated: %w", err)
	}

	out := append([]ScrapedProject(nil), sponsored...)
	out = append(out, affiliated...)
	return out, nil
}

// DetectNew compares scraped projects against the catalog and
// returns projects in numfocus.org's listings that are NOT in the
// catalog. This is the --detect-new output: a punch list of
// catalog updates the operator needs to make.
//
// Comparison uses TWO axes — normalized slug AND normalized name
// — so the catalog can use short readable slugs (e.g. "mesa")
// while the crawler derives longer normalizations from full
// display names ("mesa-agent-based-modeling-in-python"); a scraped
// entry matches the catalog if either its derived slug OR its
// display name normalizes to a known catalog identifier. This
// keeps the catalog editable for humans without producing false
// positives in drift detection.
func DetectNew(scraped []ScrapedProject, c *Catalog) []ScrapedProject {
	// Build a single set of normalization keys from BOTH catalog
	// slugs AND catalog display names. Scraped entries are
	// considered known if either their slug or name normalizes
	// into this set.
	known := make(map[string]bool, 512)
	for _, p := range c.Sponsored {
		known[normalizeSlug(p.Slug)] = true
		known[normalizeSlug(p.Name)] = true
	}
	for _, p := range c.Affiliated {
		known[normalizeSlug(p.Slug)] = true
		known[normalizeSlug(p.Name)] = true
	}
	for _, p := range c.NeedsReview {
		known[normalizeSlug(p.Slug)] = true
		known[normalizeSlug(p.Name)] = true
	}

	var missing []ScrapedProject
	for _, sp := range scraped {
		if known[normalizeSlug(sp.Slug)] {
			continue
		}
		if sp.Name != "" && known[normalizeSlug(sp.Name)] {
			continue
		}
		missing = append(missing, sp)
	}
	sort.Slice(missing, func(i, j int) bool {
		if missing[i].Section != missing[j].Section {
			return missing[i].Section < missing[j].Section
		}
		return missing[i].Slug < missing[j].Slug
	})
	return missing
}

// crawlURL is the shared HTTP+parse wrapper.
func crawlURL(ctx context.Context, client *http.Client, url string, parse func(string, string) []ScrapedProject, section string) ([]ScrapedProject, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	// numfocus.org doesn't have a robots policy that prohibits us;
	// identify ourselves as a courtesy so they can route diagnostics.
	req.Header.Set("User-Agent", "aveloxis-numfocus-importer (https://github.com/aveloxis)")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: HTTP %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", url, err)
	}
	return parse(string(body), section), nil
}

// reSponsoredSlug matches /project/{slug} links on the sponsored
// listing page. Slug = whatever's between /project/ and the closing
// quote, with an optional trailing slash.
var reSponsoredSlug = regexp.MustCompile(`href="https://numfocus\.org/project/([^"/]+)/?"`)

// parseSponsored extracts /project/{slug} entries from the
// sponsored listing page. The listing is server-rendered so a
// regex against the raw HTML is sufficient — the structure has
// been stable since 2022.
func parseSponsored(html, section string) []ScrapedProject {
	seen := map[string]bool{}
	var out []ScrapedProject
	for _, m := range reSponsoredSlug.FindAllStringSubmatch(html, -1) {
		slug := m[1]
		if seen[slug] {
			continue
		}
		seen[slug] = true
		out = append(out, ScrapedProject{
			Slug:    slug,
			Name:    "", // not extracted from the listing (would need detail-page fetch); name lives in the catalog
			URL:     "https://numfocus.org/project/" + slug,
			Section: section,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out
}

// reAffiliatedTile matches the affiliated listing's per-project
// tile header: <h3 class="et_pb_module_header"><a href="...">NAME</a></h3>
// where the href is the project's primary website (or, occasionally,
// a direct GitHub repo). Each tile appears twice in the rendered HTML
// (mobile + desktop layouts); the dedupe pass collapses them.
var reAffiliatedTile = regexp.MustCompile(`<h3 class="et_pb_module_header"><a href="([^"]+)"[^>]*>([^<]+)</a></h3>`)

// parseAffiliated extracts (name, url) pairs from the affiliated
// listing's tile headers. Affiliated projects don't have
// /project/{slug} pages on numfocus.org — the listing tile is the
// only authoritative reference. The slug is derived from the name
// (normalized lowercase, ascii-only) so it can be matched against
// the catalog's affiliated section.
func parseAffiliated(html, section string) []ScrapedProject {
	seen := map[string]bool{}
	var out []ScrapedProject
	for _, m := range reAffiliatedTile.FindAllStringSubmatch(html, -1) {
		url := m[1]
		name := strings.TrimSpace(m[2])
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, ScrapedProject{
			Slug:    deriveAffiliatedSlug(name),
			Name:    name,
			URL:     url,
			Section: section,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out
}

// deriveAffiliatedSlug normalizes a display name to a slug for
// matching against the catalog. The catalog's affiliated slugs
// were chosen to match what this function would produce, so the
// --detect-new comparison stays accurate.
//
// Rules:
//   - Lowercase.
//   - Replace any non-alphanumeric character with a hyphen.
//   - Collapse runs of hyphens.
//   - Strip leading/trailing hyphens.
//
// Examples:
//
//	"PyTorch-Ignite"           → "pytorch-ignite"
//	"CB-Geo MPM"               → "cb-geo-mpm"
//	"Python(X,Y)"              → "python-x-y"
//	"Mesa: Agent-Based ..."    → "mesa-agent-based-"
//
// The catalog hand-curates slugs to be SHORTER than full
// normalization where it makes sense (e.g. "mesa" not
// "mesa-agent-based-modeling-in-python"); --detect-new tolerates
// this via the catalog.AllSlugs lookup, which uses the catalog's
// own slug as the comparison key after both sides are normalized.
func deriveAffiliatedSlug(name string) string {
	name = strings.ToLower(name)
	var b strings.Builder
	prevHyphen := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevHyphen = false
		} else if !prevHyphen {
			b.WriteByte('-')
			prevHyphen = true
		}
	}
	out := strings.Trim(b.String(), "-")
	return out
}

// normalizeSlug is the comparison key used by DetectNew. Applied
// to BOTH catalog slugs and scraped slugs so a catalog entry
// `mesa` matches a scraped entry derived from "Mesa: Agent-Based
// Modeling In Python". Strips everything non-alphanumeric.
func normalizeSlug(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
