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
// As of v0.18.18 the widget lives in the shared compareSearchWidget template;
// we assert the group template invokes that shared definition.
func TestGroupTemplateHasCompareSearch(t *testing.T) {
	start := strings.Index(allTemplates, `{{define "group"}}`)
	end := strings.Index(allTemplates, `{{define "repo_detail"}}`)
	if start < 0 || end < 0 || end <= start {
		t.Fatal("could not locate group template boundaries")
	}
	groupSection := allTemplates[start:end]
	if !strings.Contains(groupSection, `{{template "compareSearchWidget" (dict "Prefix" "grp")}}`) {
		t.Error(`group template must invoke the shared compareSearchWidget template with Prefix "grp"`)
	}
}

// TestCompareSearchWidgetIsShared pins the recommendation #5 contract: the
// dashboard and group pages both invoke a single shared compareSearchWidget
// template. If a future refactor duplicates the markup back into each page
// (the pre-v0.18.18 state), this fails before ship. That duplication was
// the shape that produced "fixed on home page, not on group page" drift.
func TestCompareSearchWidgetIsShared(t *testing.T) {
	if !strings.Contains(allTemplates, `{{define "compareSearchWidget"}}`) {
		t.Fatal("missing shared compareSearchWidget template definition")
	}

	dashStart := strings.Index(allTemplates, `{{define "dashboard"}}`)
	dashEnd := strings.Index(allTemplates[dashStart+1:], `{{define "`)
	if dashStart < 0 || dashEnd < 0 {
		t.Fatal("could not locate dashboard template boundaries")
	}
	dashSection := allTemplates[dashStart : dashStart+1+dashEnd]
	if !strings.Contains(dashSection, `{{template "compareSearchWidget" (dict "Prefix" "dash")}}`) {
		t.Error(`dashboard template must invoke compareSearchWidget with Prefix "dash" — do not re-inline the markup`)
	}

	grpStart := strings.Index(allTemplates, `{{define "group"}}`)
	grpEnd := strings.Index(allTemplates[grpStart+1:], `{{define "`)
	if grpStart < 0 || grpEnd < 0 {
		t.Fatal("could not locate group template boundaries")
	}
	grpSection := allTemplates[grpStart : grpStart+1+grpEnd]
	if !strings.Contains(grpSection, `{{template "compareSearchWidget" (dict "Prefix" "grp")}}`) {
		t.Error(`group template must invoke compareSearchWidget with Prefix "grp" — do not re-inline the markup`)
	}

	// The old #dash-repo-search and #grp-repo-search IDs must not appear
	// as inline hardcoded strings outside the shared template. The shared
	// template builds them via the Prefix parameter, so a hardcoded
	// literal inside dashboard/group indicates a regression.
	widgetStart := strings.Index(allTemplates, `{{define "compareSearchWidget"}}`)
	widgetEnd := strings.Index(allTemplates[widgetStart:], `{{end}}`) + widgetStart
	inWidget := allTemplates[widgetStart:widgetEnd]
	for _, literal := range []string{"'dash-repo-search'", "'grp-repo-search'", "'dash-search-results'", "'grp-search-results'"} {
		// Literal string occurrences outside the shared widget would mean
		// someone started hand-rolling the JS again. The widget itself
		// builds IDs from the prefix so doesn't contain these literals.
		if strings.Contains(inWidget, literal) {
			t.Errorf("compareSearchWidget should derive %s from the Prefix parameter, not hardcode it", literal)
		}
		// Count occurrences across the whole template string: anything
		// outside the shared widget is a duplicate.
		total := strings.Count(allTemplates, literal)
		if total > 0 {
			t.Errorf("found %d hardcoded occurrences of %s — the shared widget builds these from Prefix", total, literal)
		}
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
