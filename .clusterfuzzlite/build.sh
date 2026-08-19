#!/bin/bash -eu
# ClusterFuzzLite build script (v0.27.99). One compile_native_go_fuzzer
# line per native Go fuzz target — the targets live in ordinary
# *_test.go files (their seeds also run in every `go test ./...`).
cd $SRC/aveloxis
go mod download

compile_native_go_fuzzer ./internal/mailinglist FuzzParseMbox              fuzz_mailinglist_mbox
compile_native_go_fuzzer ./internal/mailinglist FuzzDecodeHeader           fuzz_mailinglist_header
compile_native_go_fuzzer ./internal/collector   FuzzParseLockfile          fuzz_collector_lockfile
compile_native_go_fuzzer ./internal/collector   FuzzNormalizeParsedVersion fuzz_collector_version
compile_native_go_fuzzer ./internal/collector   FuzzPurlHelpers            fuzz_collector_purl
compile_native_go_fuzzer ./internal/collector   FuzzManifestParsers        fuzz_collector_manifests
compile_native_go_fuzzer ./internal/platform    FuzzRepoURLParsers         fuzz_platform_repourl
