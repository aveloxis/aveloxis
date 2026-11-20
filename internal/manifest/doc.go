// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package manifest reads what a build declares, from the files of a
// repository's working tree: which packages it depends on, at which declared
// requirement, in which scope. One reader per manifest format (package.json,
// requirements files, pyproject.toml, go.mod, Cargo.toml, Gemfile, pom.xml,
// Gradle, .NET project files, and the rest), plus the helpers they share:
// the one file reader that decodes UTF-16 and a UTF-8 BOM, the TOML and XML
// comment strippers, the version normaliser and the floor convention.
//
// It does no network I/O and writes nothing. It says what a file DECLARES;
// which version is actually in use is a different question, answered
// elsewhere from lockfiles and managed sources.
//
// The way in is the roster (roster.go): Walk.Declared for a file's versioned
// declarations and Names for its dependency names. Both of the collector's
// walks ask it, so a manifest format is added in one place. The individual
// readers are unexported except the few that other code calls for a reason
// of its own (the Go and Maven readers for the module graph and the pom
// walk, the Python requirement reader for lockfile matching).
//
// The readers were in internal/collector (analysis.go and five smaller
// files) and moved here unchanged in 0.29.152; the roster replaced the two
// hand-written dispatches in 0.29.153, proven equal to both by
// roster_oracle_test.go (summary/70, 4.1.5). The walks that find manifests
// in a clone, and the registry resolvers that turn a declared dependency
// into a libyear row, stay in internal/collector.
//
// It imports internal/model, and internal/platform for exactly one function:
// platform.CanonicalWebHost, the one spelling of "these two host strings name
// the same web host", which the SwiftPM URL reader shares with the forge
// code. A second copy of that fold here would be the defect (SR-17); platform
// itself imports only internal/model, so the unit stays near the leaves.
package manifest
