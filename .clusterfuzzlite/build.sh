#!/bin/bash -eu
# ClusterFuzzLite build script (v0.27.99). One compile_native_go_fuzzer
# line per native Go fuzz target — the targets live in ordinary
# *_test.go files (their seeds also run in every `go test ./...`).
cd $SRC/aveloxis
go mod download

# compile_native_go_fuzzer's generated shim imports
# github.com/AdamKorcz/go-118-fuzz-build/testing (the bridge from native
# `f *testing.F` targets to libFuzzer). It is NOT a runtime dependency of
# aveloxis, so it is added here INSIDE the build container only — never
# committed to the repo's go.mod. Without this line the first
# compile_native_go_fuzzer call fails with "no required module provides
# package github.com/AdamKorcz/go-118-fuzz-build/testing" (the
# 2026-08-19 PR #181 CI failure).
go get github.com/AdamKorcz/go-118-fuzz-build/testing

compile_native_go_fuzzer ./internal/mailinglist FuzzParseMbox              fuzz_mailinglist_mbox
compile_native_go_fuzzer ./internal/mailinglist FuzzDecodeHeader           fuzz_mailinglist_header
compile_native_go_fuzzer ./internal/collector   FuzzParseLockfile          fuzz_collector_lockfile
compile_native_go_fuzzer ./internal/collector   FuzzNormalizeParsedVersion fuzz_collector_version
compile_native_go_fuzzer ./internal/collector   FuzzPurlHelpers            fuzz_collector_purl
compile_native_go_fuzzer ./internal/collector   FuzzManifestParsers        fuzz_collector_manifests
compile_native_go_fuzzer ./internal/platform    FuzzRepoURLParsers         fuzz_platform_repourl
