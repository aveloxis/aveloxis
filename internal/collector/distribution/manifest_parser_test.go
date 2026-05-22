// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"testing"
)

// v0.24.0 — manifest parsers extract the *declared* package name
// from a manifest file's content. Used by the DistributionWorker
// to populate repo_distribution_manifest.package_name_declared so
// the headline analysis query can detect "this repo declares name X
// in its setup.py but X is not in PyPI".
//
// Conservative contract: a parser returns "" when it can't
// reliably extract the name. We never make up a name. Garbage in,
// empty out — the row still records *intent* via manifest_type,
// just not the declared name.

func TestParseManifestNamePackageJSON(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "simple name",
			content: `{"name": "my-package", "version": "1.0.0"}`,
			want:    "my-package",
		},
		{
			name:    "scoped name",
			content: `{"name": "@scope/widget", "version": "2.0.0"}`,
			want:    "@scope/widget",
		},
		{
			name:    "no name field",
			content: `{"version": "1.0.0"}`,
			want:    "",
		},
		{
			name:    "malformed JSON",
			content: `{not valid`,
			want:    "",
		},
		{
			name:    "empty",
			content: "",
			want:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseManifestName("package.json", tt.content); got != tt.want {
				t.Errorf("ParseManifestName = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseManifestNameCargoToml(t *testing.T) {
	content := `[package]
name = "my-crate"
version = "0.1.0"
edition = "2021"

[dependencies]
serde = "1.0"
`
	if got := ParseManifestName("Cargo.toml", content); got != "my-crate" {
		t.Errorf("Cargo.toml name = %q, want my-crate", got)
	}
}

func TestParseManifestNameCargoTomlWorkspaceOnly(t *testing.T) {
	// A workspace-root Cargo.toml may not have a [package] section.
	// Our parser should return "" rather than guess.
	content := `[workspace]
members = ["crates/foo", "crates/bar"]
`
	if got := ParseManifestName("Cargo.toml", content); got != "" {
		t.Errorf("workspace-only Cargo.toml name = %q, want empty", got)
	}
}

func TestParseManifestNamePyprojectToml(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{
			name: "PEP 621 [project]",
			content: `[project]
name = "my-package"
version = "0.1.0"
`,
			want: "my-package",
		},
		{
			name: "Poetry [tool.poetry]",
			content: `[tool.poetry]
name = "poetry-package"
version = "0.1.0"
`,
			want: "poetry-package",
		},
		{
			name:    "no project section",
			content: `[build-system]\nrequires = ["setuptools"]`,
			want:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseManifestName("pyproject.toml", tt.content); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseManifestNameSetupPy(t *testing.T) {
	// setup.py is python; we can't safely execute it, but we can
	// regex-extract setup(name="..."). This is best-effort.
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "double quotes",
			content: `setup(name="my-pkg", version="1.0")`,
			want:    "my-pkg",
		},
		{
			name:    "single quotes",
			content: `setup(name='my-pkg', version='1.0')`,
			want:    "my-pkg",
		},
		{
			name: "multiline",
			content: `from setuptools import setup
setup(
    name="multiline-pkg",
    version="0.1.0",
)`,
			want: "multiline-pkg",
		},
		{
			name:    "no setup call",
			content: `print("hello")`,
			want:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseManifestName("setup.py", tt.content); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseManifestNameSetupCfg(t *testing.T) {
	content := `[metadata]
name = setup-cfg-pkg
version = 1.0.0
`
	if got := ParseManifestName("setup.cfg", content); got != "setup-cfg-pkg" {
		t.Errorf("setup.cfg name = %q, want setup-cfg-pkg", got)
	}
}

func TestParseManifestNamePomXml(t *testing.T) {
	content := `<?xml version="1.0" encoding="UTF-8"?>
<project>
    <groupId>com.example</groupId>
    <artifactId>my-artifact</artifactId>
    <version>1.0.0</version>
</project>`
	// We emit "{groupId}:{artifactId}" — Maven coordinate convention.
	if got := ParseManifestName("pom.xml", content); got != "com.example:my-artifact" {
		t.Errorf("pom.xml name = %q, want com.example:my-artifact", got)
	}
}

func TestParseManifestNameGoMod(t *testing.T) {
	content := `module github.com/example/widget

go 1.21

require github.com/sirupsen/logrus v1.9.0
`
	if got := ParseManifestName("go.mod", content); got != "github.com/example/widget" {
		t.Errorf("go.mod module path = %q, want github.com/example/widget", got)
	}
}

func TestParseManifestNameGemspec(t *testing.T) {
	content := `Gem::Specification.new do |s|
  s.name = "my-gem"
  s.version = "1.0.0"
end`
	if got := ParseManifestName("my-gem.gemspec", content); got != "my-gem" {
		t.Errorf("gemspec name = %q, want my-gem", got)
	}
}

func TestParseManifestNameComposerJson(t *testing.T) {
	content := `{"name": "vendor/package", "version": "1.0.0"}`
	if got := ParseManifestName("composer.json", content); got != "vendor/package" {
		t.Errorf("composer.json name = %q, want vendor/package", got)
	}
}

func TestParseManifestNameUnknownReturnsEmpty(t *testing.T) {
	// Defensive: unknown filenames return "" with no panic.
	if got := ParseManifestName("README.md", "anything"); got != "" {
		t.Errorf("unknown manifest = %q, want empty", got)
	}
}

func TestParseManifestNameEmptyContentReturnsEmpty(t *testing.T) {
	// Empty content (manifest >1MB or binary) — every parser
	// must handle this without panicking.
	for _, filename := range []string{
		"package.json", "Cargo.toml", "pyproject.toml",
		"setup.py", "setup.cfg", "pom.xml", "go.mod",
		"my.gemspec", "composer.json",
	} {
		if got := ParseManifestName(filename, ""); got != "" {
			t.Errorf("%s empty content = %q, want empty", filename, got)
		}
	}
}
