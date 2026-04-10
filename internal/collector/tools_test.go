package collector

import (
	"strings"
	"testing"
	"time"
)

// TestToolDefinitionsComplete verifies all external tools are defined.
func TestToolDefinitionsComplete(t *testing.T) {
	tools := ExternalTools()
	if len(tools) < 3 {
		t.Fatalf("ExternalTools() returned %d tools, want at least 3 (scc, scorecard, scancode)", len(tools))
	}

	// Verify required tools are present.
	names := make(map[string]bool)
	for _, tool := range tools {
		names[tool.Name] = true
	}

	for _, required := range []string{"scc", "scorecard", "scancode"} {
		if !names[required] {
			t.Errorf("ExternalTools() missing %q", required)
		}
	}
}

// TestToolDefinitionsHaveInstallCmd verifies every tool has an install command.
func TestToolDefinitionsHaveInstallCmd(t *testing.T) {
	for _, tool := range ExternalTools() {
		if tool.InstallCmd == "" {
			t.Errorf("tool %q has empty InstallCmd", tool.Name)
		}
		if tool.Description == "" {
			t.Errorf("tool %q has empty Description", tool.Name)
		}
		if tool.CheckBinary == "" {
			t.Errorf("tool %q has empty CheckBinary", tool.Name)
		}
	}
}

// TestToolDefinitionsHavePurpose verifies the purpose field describes what phase uses the tool.
func TestToolDefinitionsHavePurpose(t *testing.T) {
	for _, tool := range ExternalTools() {
		if tool.Purpose == "" {
			t.Errorf("tool %q has empty Purpose", tool.Name)
		}
	}
}

// TestScorecardToolHasInstallFunc verifies that the scorecard tool uses
// InstallFunc (binary download) rather than relying solely on InstallCmd
// (go install), because scorecard v5 does not expose a go-installable cmd package.
func TestScorecardToolHasInstallFunc(t *testing.T) {
	for _, tool := range ExternalTools() {
		if tool.Name == "scorecard" {
			if tool.InstallFunc == nil {
				t.Error("scorecard tool must have InstallFunc set (go install does not work for scorecard v5)")
			}
			return
		}
	}
	t.Error("scorecard tool not found in ExternalTools()")
}

// TestScorecardDownloadURL verifies the URL construction for downloading
// pre-built scorecard binaries from GitHub releases.
// Actual release assets use the format: scorecard_<version-without-v>_<os>_<arch>.tar.gz
// e.g., scorecard_5.4.0_darwin_arm64.tar.gz
func TestScorecardDownloadURL(t *testing.T) {
	tests := []struct {
		goos, goarch string
		version      string
		wantSuffix   string
	}{
		{"darwin", "arm64", "v5.4.0", "scorecard_5.4.0_darwin_arm64.tar.gz"},
		{"darwin", "amd64", "v5.4.0", "scorecard_5.4.0_darwin_amd64.tar.gz"},
		{"linux", "amd64", "v5.4.0", "scorecard_5.4.0_linux_amd64.tar.gz"},
		{"linux", "arm64", "v5.1.0", "scorecard_5.1.0_linux_arm64.tar.gz"},
	}
	for _, tt := range tests {
		url := scorecardDownloadURL(tt.version, tt.goos, tt.goarch)
		if url == "" {
			t.Errorf("scorecardDownloadURL(%q, %q, %q) returned empty", tt.version, tt.goos, tt.goarch)
			continue
		}
		if !strings.HasSuffix(url, tt.wantSuffix) {
			t.Errorf("scorecardDownloadURL(%q, %q, %q) = %q, want suffix %q",
				tt.version, tt.goos, tt.goarch, url, tt.wantSuffix)
		}
		if !strings.Contains(url, tt.version) {
			t.Errorf("scorecardDownloadURL(%q, %q, %q) = %q, want to contain version tag %q",
				tt.version, tt.goos, tt.goarch, url, tt.version)
		}
	}
}

// TestInstallFuncTakesPriority verifies that when both InstallFunc and
// InstallCmd are set, the tool still has a valid InstallCmd for display
// purposes (manual install instructions) but also has the func.
func TestInstallFuncTakesPriority(t *testing.T) {
	for _, tool := range ExternalTools() {
		if tool.InstallFunc != nil && tool.InstallCmd == "" {
			t.Errorf("tool %q has InstallFunc but empty InstallCmd (need InstallCmd for manual install display)", tool.Name)
		}
	}
}

// TestToolUpdateCheckDue verifies the 30-day check logic.
func TestToolUpdateCheckDue(t *testing.T) {
	now := time.Now()
	thirtyOneDaysAgo := now.Add(-31 * 24 * time.Hour)
	twentyDaysAgo := now.Add(-20 * 24 * time.Hour)

	if !IsToolUpdateCheckDue(thirtyOneDaysAgo) {
		t.Error("31 days ago should be due for update check")
	}
	if IsToolUpdateCheckDue(twentyDaysAgo) {
		t.Error("20 days ago should NOT be due for update check")
	}
	if !IsToolUpdateCheckDue(time.Time{}) {
		t.Error("zero time (never checked) should be due")
	}
}
