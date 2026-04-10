package web

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestDashboardTemplateHasComparisonCard verifies the dashboard has a prominent
// comparison section, not just a small link in the breadcrumb.
func TestDashboardTemplateHasComparisonCard(t *testing.T) {
	if !strings.Contains(allTemplates, `id="compare-card"`) {
		t.Error("dashboard template missing comparison card (id='compare-card')")
	}
}

// TestDashboardTemplateComparisonCardHasSearchHint verifies the comparison card
// explains how to use it.
func TestDashboardTemplateComparisonCardHasSearchHint(t *testing.T) {
	if !strings.Contains(allTemplates, "Search and select") || !strings.Contains(allTemplates, "compare-card") {
		t.Error("comparison card should include search instructions for usability")
	}
}

// TestCompareTemplateHasVisibleDropdown verifies the compare page search has
// a styled dropdown that appears on typing, not just a bare input.
func TestCompareTemplateHasVisibleDropdown(t *testing.T) {
	if !strings.Contains(allTemplates, "search-results") {
		t.Error("compare template missing visible search results container (id='search-results')")
	}
}

// TestCompareTemplateHasPlaceholderHint verifies the search input has a
// descriptive placeholder telling users what to do.
func TestCompareTemplateHasPlaceholderHint(t *testing.T) {
	if !strings.Contains(allTemplates, "Type to search") {
		t.Error("compare search input should have 'Type to search' placeholder hint")
	}
}

// TestContainerWidthIsWideEnough verifies the main container max-width is at
// least 1100px so the repo list table with 10+ columns doesn't require
// awkward horizontal scrolling.
func TestContainerWidthIsWideEnough(t *testing.T) {
	re := regexp.MustCompile(`\.container\{[^}]*max-width:\s*(\d+)px`)
	m := re.FindStringSubmatch(allTemplates)
	if m == nil {
		t.Fatal("could not find .container max-width in CSS")
	}
	width, _ := strconv.Atoi(m[1])
	if width < 1100 {
		t.Errorf(".container max-width is %dpx, want at least 1100px to avoid cramped repo table", width)
	}
}

// TestRepoNameColumnNotOverlyTruncated verifies the repo name cell allows
// enough width that common repo names are fully visible without ellipsis.
func TestRepoNameColumnNotOverlyTruncated(t *testing.T) {
	// The old value was max-width:350px which was too narrow. Verify it's
	// either removed or widened. We check that no cell has max-width < 450px
	// combined with text-overflow:ellipsis on the repo link cell.
	re := regexp.MustCompile(`max-width:(\d+)px;overflow:hidden;text-overflow:ellipsis`)
	m := re.FindStringSubmatch(allTemplates)
	if m != nil {
		w, _ := strconv.Atoi(m[1])
		if w < 450 {
			t.Errorf("repo name cell max-width is %dpx with ellipsis, want at least 450px or no truncation", w)
		}
	}
	// If no match, truncation was removed entirely — that's fine.
}

// TestComparePrePopulateDoesNotNestStatsFetch verifies the compare page
// pre-populate code fetches timeseries directly instead of nesting inside
// a stats fetch (which fails silently if the stats call errors).
func TestComparePrePopulateDoesNotNestStatsFetch(t *testing.T) {
	// The old code had: fetch(API_BASE + '/api/v1/repos/' + id + '/stats')
	// nested with fetch(...timeseries) inside. The stats fetch is unnecessary
	// and breaks the chain if it fails. We search the full template for the
	// pre-populate block (identified by "urlRepos") and check no /stats fetch follows.
	idx := strings.Index(allTemplates, "urlRepos")
	if idx < 0 {
		t.Fatal("compare template missing urlRepos pre-populate block")
	}
	end := idx + 1000
	if end > len(allTemplates) {
		end = len(allTemplates)
	}
	prePopulate := allTemplates[idx:end]
	if strings.Contains(prePopulate, "/stats'") || strings.Contains(prePopulate, `/stats"`) {
		t.Error("compare pre-populate should not fetch /stats — fetch /timeseries directly for owner/name")
	}
}

// TestGroupTemplateHasCompareSearch verifies the group detail page includes
// the API-powered compare search widget, not just the server-side filter.
func TestGroupTemplateHasCompareSearch(t *testing.T) {
	// The group template must contain the compare search input and the
	// API fetch call. We search between the group define and the next
	// template define to isolate the group section.
	start := strings.Index(allTemplates, `{{define "group"}}`)
	end := strings.Index(allTemplates, `{{define "repo_detail"}}`)
	if start < 0 || end < 0 || end <= start {
		t.Fatal("could not locate group template boundaries")
	}
	groupSection := allTemplates[start:end]
	if !strings.Contains(groupSection, "grp-repo-search") {
		t.Error("group template should include an API-powered compare search widget (grp-repo-search)")
	}
	if !strings.Contains(groupSection, "/api/v1/repos/search") {
		t.Error("group template compare search should call the repos search API")
	}
}

// extractTemplateSection returns the content between the first occurrence of
// startMarker and the next matching end marker in the Go template string.
func extractTemplateSection(tmpl, startMarker, endMarker string) string {
	start := strings.Index(tmpl, startMarker)
	if start < 0 {
		return ""
	}
	rest := tmpl[start+len(startMarker):]
	end := strings.Index(rest, endMarker)
	if end < 0 {
		return ""
	}
	return rest[:end]
}
