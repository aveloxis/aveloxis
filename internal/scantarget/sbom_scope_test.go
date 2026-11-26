// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scantarget

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestVulnScanTargetsCarryScope(t *testing.T) {
	dep := model.VulnScanDep{
		Name: "pytest", CurrentVersion: "7.4.0", PackageManager: "pypi",
		Purl: "pkg:pypi/pytest@7.4.0", Requirement: "==7.4.0", Type: model.ScopeDev,
	}
	targets := DirectTargets(dep, nil)
	if len(targets) == 0 {
		t.Fatal("no targets")
	}
	for _, tgt := range targets {
		if tgt.Scope != model.ScopeDev {
			t.Errorf("direct target scope %q, want dev — findings' dependency_scope must work for direct deps", tgt.Scope)
		}
	}
	// v0.27.51: runtime deps stamp the WORD explicitly.
	dep.Type = "runtime"
	for _, tgt := range DirectTargets(dep, nil) {
		if tgt.Scope != model.ScopeRuntime {
			t.Errorf("runtime dep target scope %q, want 'runtime' (v0.27.51 StoredScope convention)", tgt.Scope)
		}
	}
}

// TestMergeDirectTargetRuntimeWinsCollision pins the local-canary
// catch (2026-07-21, pipenv/setuptools): when the same purl is
// reachable from a non-runtime AND a runtime declaration, the target
// scope must fold to runtime (the empty string) regardless of iteration order — a
// non-runtime first-writer was re-stamping runtime findings off the
// headline and out of the digest.
func TestMergeDirectTargetRuntimeWinsCollision(t *testing.T) {
	m := map[string]model.VulnScanTarget{}
	var purls []string
	buildFirst := model.VulnScanTarget{Purl: "pkg:pypi/setuptools@67", Scope: model.ScopeBuild}
	runtimeSecond := model.VulnScanTarget{Purl: "pkg:pypi/setuptools@67", Scope: model.ScopeRuntime}
	purls = MergeDirectTarget(m, purls, buildFirst)
	purls = MergeDirectTarget(m, purls, runtimeSecond)
	if len(purls) != 1 {
		t.Fatalf("collision must not duplicate the purl list, got %d", len(purls))
	}
	if got := m["pkg:pypi/setuptools@67"].Scope; !model.IsRuntimeScope(got) {
		t.Errorf("runtime declaration must win the scope fold, got %q", got)
	}
	// Reverse order: runtime first stays runtime.
	m2 := map[string]model.VulnScanTarget{}
	purls2 := MergeDirectTarget(m2, nil, runtimeSecond)
	purls2 = MergeDirectTarget(m2, purls2, buildFirst)
	if len(purls2) != 1 || !model.IsRuntimeScope(m2["pkg:pypi/setuptools@67"].Scope) {
		t.Error("runtime-first must stay runtime on later non-runtime collision")
	}
	// v0.27.51: a LEGACY ''-scope target (pre-word rows in flight)
	// must fold identically to the explicit word.
	m4 := map[string]model.VulnScanTarget{}
	legacyRuntime := model.VulnScanTarget{Purl: "pkg:pypi/setuptools@67", Scope: ""}
	_ = MergeDirectTarget(m4, nil, buildFirst)
	_ = MergeDirectTarget(m4, nil, legacyRuntime)
	if got := m4["pkg:pypi/setuptools@67"].Scope; !model.IsRuntimeScope(got) {
		t.Errorf("legacy '' runtime must win the fold too (IsRuntimeScope contract), got %q", got)
	}
	// Two non-runtime declarations keep the first scope (no runtime
	// evidence — nothing to fold toward).
	m3 := map[string]model.VulnScanTarget{}
	dev := model.VulnScanTarget{Purl: "pkg:npm/x@1", Scope: model.ScopeDev}
	test := model.VulnScanTarget{Purl: "pkg:npm/x@1", Scope: model.ScopeTest}
	_ = MergeDirectTarget(m3, nil, dev)
	_ = MergeDirectTarget(m3, nil, test)
	if got := m3["pkg:npm/x@1"].Scope; got != model.ScopeDev {
		t.Errorf("non-runtime collision keeps the first scope, got %q", got)
	}
}
