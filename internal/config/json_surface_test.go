// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.37 JSON-surface freeze. aveloxis.json's `collection` block is
// the operator contract; the Action-1 config collapse (and any future
// refactor) must not change it. This test pins the exact key set —
// adding a knob is a deliberate act (add it here in the same commit);
// removing or renaming one is a breaking change that must be called
// out, never a refactoring side effect.

package config

import (
	"reflect"
	"strings"
	"testing"
)

var frozenCollectionKeys = []string{
	"days_until_recollect",
	"workers",
	"repo_clone_dir",
	"force_full",
	"matview_rebuild_day",
	"matview_rebuild_on_startup",
	"pr_child_mode",
	"listing_mode",
	"threading_mode",
	"shard_size",
	"issue_child_mode",
	"enrich_interval_minutes",
	"search_resolve_interval_minutes",
	"affiliation_interval_minutes",
	"breadth_interval_minutes",
	"breadth_batch_size",
	"breadth_cooldown_days",
	"breadth_fetch_concurrency",
	"shutdown_grace_seconds",
	"scancode_workers",
	"scancode_start_interval_s",
	"scancode_cadence_days",
	"scancode_clone_dir",
	"scancode_shutdown_grace_minutes",
	"scancode_run_timeout_hours",
	"scancode_run_timeout_cap_hours",
	"scancode_max_in_memory",
	"scorecard_timeout_minutes",
	"scorecard_token_count",
	"scancode_timeout_cap_strikes",
	"scancode_ignore_globs",
	"staging_retention_hours",
	"phase_watchdog_minutes",
	"distribution_tracking_enabled",
	"distribution_tracking_interval_days",
	"distribution_tracking_workers",
	"distribution_tracking_start_interval_s",
	"distribution_tracking_polite_email",
	"distribution_tracking_user_agent",
	"distribution_tracking_cross_check_sources",
	"distribution_tracking_immediate_partial_reclaim",
	"mailing_list_enabled",
	"mailing_list_workers",
	"mailing_list_cadence_days",
	"mailing_list_backfill_months",
	"mailing_list_polite_email",
	"mailing_list_mirror_handling",
	"mailing_list_processor_workers",
}

func TestCollectionJSONSurfaceIsFrozen(t *testing.T) {
	declared := map[string]bool{}
	typ := reflect.TypeOf(CollectionConfig{})
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		declared[strings.Split(tag, ",")[0]] = true
	}

	frozen := map[string]bool{}
	for _, k := range frozenCollectionKeys {
		frozen[k] = true
		if !declared[k] {
			t.Errorf("frozen key %q no longer exists on CollectionConfig — removing/"+
				"renaming an aveloxis.json key is a BREAKING operator change; if "+
				"deliberate, update frozenCollectionKeys in the same commit and call "+
				"it out in the changelog.", k)
		}
	}
	for k := range declared {
		if !frozen[k] {
			t.Errorf("CollectionConfig declares json key %q that is not in "+
				"frozenCollectionKeys — new knobs are welcome, but freezing the key "+
				"here (same commit) is what makes accidental surface changes "+
				"impossible.", k)
		}
	}
}
