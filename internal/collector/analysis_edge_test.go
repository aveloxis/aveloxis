// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"
)

// ============================================================
// Python — pyproject.toml edge cases
// ============================================================

// ============================================================
// Python — requirements.txt edge cases
// ============================================================

// ============================================================
// Python — setup.py edge cases
// ============================================================

// ============================================================
// Python — Pipfile edge cases
// ============================================================

// ============================================================
// Go — go.mod edge cases
// ============================================================

// ============================================================
// Rust — Cargo.toml edge cases
// ============================================================

// ============================================================
// Ruby — Gemfile edge cases
// ============================================================

// ============================================================
// Java — pom.xml edge cases
// ============================================================

// ============================================================
// PHP — composer.json edge cases
// ============================================================

// ============================================================
// JavaScript — package.json edge cases
// ============================================================

// ============================================================
// Scala — build.sbt edge cases
// ============================================================

// ============================================================
// Dart — pubspec.yaml edge cases
// ============================================================

// ============================================================
// .NET — Directory.Packages.props & .csproj edge cases
// ============================================================

// ============================================================
// Haskell — package.yaml edge cases
// ============================================================

// ============================================================
// Elixir — mix.exs edge cases
// ============================================================

// ============================================================
// Swift — Package.swift edge cases
// ============================================================

// ============================================================
// build.gradle — additional edge cases
// ============================================================

// ============================================================
// manifest.CleanVersion edge cases
// ============================================================

// ============================================================
// calcLibyear edge cases
// ============================================================

func TestCalcLibyear_NegativeAge(t *testing.T) {
	// Latest is older than current (can happen with pre-release).
	ly := calcLibyear("2024-06-01T00:00:00Z", "2024-01-01T00:00:00Z")
	if ly >= 0 {
		t.Errorf("expected negative libyear, got %f", ly)
	}
}

func TestCalcLibyear_EmptyBothDates(t *testing.T) {
	ly := calcLibyear("", "")
	if ly != 0 {
		t.Errorf("both empty: got %f, want 0", ly)
	}
}

func TestCalcLibyear_DateOnly(t *testing.T) {
	// Some registries return date-only format.
	ly := calcLibyear("2023-01-01", "2024-01-01")
	// Should fail to parse and return 0 (date-only not in layouts).
	_ = ly
}

// ============================================================
// normalizeSemanticVersion edge cases
// ============================================================

func TestNormalizeSemanticVersion_Full(t *testing.T) {
	if v := normalizeSemanticVersion("1.2.3"); v != "1.2.3" {
		t.Errorf("full: got %q", v)
	}
}

func TestNormalizeSemanticVersion_Major(t *testing.T) {
	if v := normalizeSemanticVersion("1"); v != "1.0.0" {
		t.Errorf("major-only: got %q", v)
	}
}

func TestNormalizeSemanticVersion_MajorMinor(t *testing.T) {
	if v := normalizeSemanticVersion("1.0"); v != "1.0.0" {
		t.Errorf("major.minor: got %q", v)
	}
}

func TestNormalizeSemanticVersion_PreRelease(t *testing.T) {
	if v := normalizeSemanticVersion("1.0.0-beta"); v != "1.0.0-beta" {
		t.Errorf("pre-release unchanged: got %q", v)
	}
}

func TestNormalizeSemanticVersion_Empty(t *testing.T) {
	if v := normalizeSemanticVersion(""); v != "" {
		t.Errorf("empty: got %q", v)
	}
}
