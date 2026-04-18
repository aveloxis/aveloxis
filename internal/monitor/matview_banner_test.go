package monitor

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/augurlabs/aveloxis/internal/scheduler"
)

func mustReadMonitorSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatalf("read monitor.go: %v", err)
	}
	return string(data)
}

// TestRenderMatviewBannerWhenActive verifies the banner HTML renders when the
// shared rebuild flag is set. This is the user-visible signal that collection
// is paused while the weekly rebuild completes.
func TestRenderMatviewBannerWhenActive(t *testing.T) {
	prev := scheduler.MatviewRebuildActive.Load()
	scheduler.MatviewRebuildActive.Store(true)
	t.Cleanup(func() { scheduler.MatviewRebuildActive.Store(prev) })

	var buf bytes.Buffer
	renderMatviewBanner(&buf)

	out := buf.String()
	if out == "" {
		t.Fatal("renderMatviewBanner wrote nothing while MatviewRebuildActive=true")
	}
	// Banner must announce the pause so operators don't assume the scheduler
	// is hung (the whole point of this work).
	lowered := strings.ToLower(out)
	mustContain := []string{"materialized view", "rebuild"}
	for _, term := range mustContain {
		if !strings.Contains(lowered, term) {
			t.Errorf("banner must mention %q so operators understand why " +
				"collection is paused; got: %s", term, out)
		}
	}
	if !strings.Contains(lowered, "paused") && !strings.Contains(lowered, "pause") {
		t.Error("banner must convey that collection is paused — without this " +
			"wording the banner reads like an info note, not a stop signal")
	}
}

// TestRenderMatviewBannerWhenInactive verifies the banner stays hidden when
// the flag is clear, so the dashboard isn't cluttered during normal runs.
func TestRenderMatviewBannerWhenInactive(t *testing.T) {
	prev := scheduler.MatviewRebuildActive.Load()
	scheduler.MatviewRebuildActive.Store(false)
	t.Cleanup(func() { scheduler.MatviewRebuildActive.Store(prev) })

	var buf bytes.Buffer
	renderMatviewBanner(&buf)

	if buf.Len() != 0 {
		t.Errorf("renderMatviewBanner must write nothing when flag is false, got %q", buf.String())
	}
}

// TestDashboardEmbedsBannerHelper verifies the dashboard HTML path actually
// invokes renderMatviewBanner. A helper that nobody calls is as bad as no
// helper at all — the banner has to appear on the live page.
func TestDashboardEmbedsBannerHelper(t *testing.T) {
	// Source-level check: handleDashboard must reference the helper.
	data := mustReadMonitorSource(t)
	if !strings.Contains(data, "renderMatviewBanner") {
		t.Error("monitor.go must call renderMatviewBanner from handleDashboard " +
			"so the pause message appears at the top of the live page")
	}
}
