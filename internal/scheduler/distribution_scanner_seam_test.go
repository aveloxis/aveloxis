// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/collector/distribution"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/platform"
	gh "github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestDistributionScannerKeepsTheGitHubSource is the RUNTIME assertion of
// item 21's review-round-1 decision: the scanner the distribution worker
// runs carries the scheduler's GitHub client whatever the key pool holds.
// With no usable key the GitHub source fails a GitHub repository's scan
// (a strike; the snapshot is kept); without the source a registries-only
// scan COMPLETES and wipes the repository's GitHub-sourced rows for the
// cadence. Every source pin on the wiring so far was escaped by a
// respelling; the seams are where the property is executed.
func TestDistributionScannerKeepsTheGitHubSource(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	ghClient := &gh.Client{}
	s := &Scheduler{
		ghKeys:   platform.NewKeyPool(nil, lg), // GitLab-only keys: non-nil, empty
		ghClient: ghClient,
		logger:   lg,
		cfg:      Config{Collection: &config.CollectionConfig{}},
	}
	if s.githubKeysAvailable() {
		t.Fatal("fixture: the pool must have no usable key")
	}
	opts, err := s.distributionWorkerOptions()
	if err != nil {
		t.Fatal(err)
	}
	scanner, ok := opts.Scanner.(*distribution.CompositeScanner)
	if !ok {
		t.Fatalf("the worker's scanner is a %T; want the composite scanner", opts.Scanner)
	}
	if scanner.GitHub != ghClient {
		t.Errorf("scanner.GitHub = %p; want the scheduler's own GitHub client %p even with no usable key: a registries-only scan completes and wipes the GitHub-sourced rows", scanner.GitHub, ghClient)
	}
	if scanner.DepsDev == nil || scanner.Ecosystems == nil {
		t.Error("the registry sources are missing")
	}
	if opts.Store == nil || opts.Logger == nil || opts.Workers <= 0 || opts.Cadence <= 0 {
		t.Errorf("the options are incomplete: %+v", opts)
	}
	// The one refusal: a scheduler whose GitHub client is not the concrete
	// type (the platform interface has no distribution methods).
	s.ghClient = &gitlab.Client{}
	if _, err := s.distributionWorkerOptions(); !errors.Is(err, errDistributionGitHubClient) {
		t.Errorf("a non-GitHub client: err = %v; want errDistributionGitHubClient", err)
	}
}

// TestDistributionWorkerReceivesTheGitHubSource asserts the property on the
// options the WORKER receives, through the newDistributionWorker seam: a
// recorder replaces the constructor, spawnDistributionWorker runs with an
// empty key pool, and after it returns exactly one worker was built whose
// scanner carries the scheduler's GitHub client (checked after the spawn,
// so a write through the shared scanner pointer would show). Every source
// pin on the wiring (rounds 1–5) was escaped by a respelling; this cannot
// be, because it reads what the constructor was handed.
func TestDistributionWorkerReceivesTheGitHubSource(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	ghClient := &gh.Client{}
	s := &Scheduler{
		ghKeys:   platform.NewKeyPool(nil, lg), // GitLab-only keys: non-nil, empty
		ghClient: ghClient,
		logger:   lg,
		cfg:      Config{Collection: &config.CollectionConfig{}},
	}
	var built []distribution.WorkerOptions
	orig := newDistributionWorker
	newDistributionWorker = func(o distribution.WorkerOptions) distributionRunner {
		built = append(built, o)
		return noopRunner{}
	}
	t.Cleanup(func() { newDistributionWorker = orig })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.spawnDistributionWorker(ctx)
	s.background.Wait()
	if len(built) != 1 {
		t.Fatalf("%d workers built; want exactly one", len(built))
	}
	scanner, ok := built[0].Scanner.(*distribution.CompositeScanner)
	if !ok {
		t.Fatalf("the worker's scanner is a %T; want the composite scanner", built[0].Scanner)
	}
	if scanner.GitHub != ghClient {
		t.Errorf("the worker's scanner.GitHub = %p; want the scheduler's own GitHub client %p even with no usable key: a registries-only scan completes and wipes the GitHub-sourced rows", scanner.GitHub, ghClient)
	}
	if scanner.DepsDev == nil || scanner.Ecosystems == nil {
		t.Error("the worker's registry sources are missing")
	}
	// The seam's production default builds the real worker.
	if _, ok := orig(built[0]).(*distribution.Worker); !ok {
		t.Error("newDistributionWorker's default must build a *distribution.Worker")
	}
}

type noopRunner struct{}

func (noopRunner) Run(context.Context) {}

// TestDistributionWorkerSeamIsTheOnlyConstructor is the sendSignal pattern's
// other half (review rounds 6–7), counting SYMBOLS rather than spellings:
// the identifier `newDistributionWorker` appears exactly twice in production
// (its declaration and its one call — a sibling file's init() re-pointing
// it through a tuple or a pointer, or a wrapper capturing the original,
// would be a third), the selector `.NewWorker` exactly once (the seam's
// default; an alias `nw := distribution.NewWorker`, an import alias or a
// parenthesised `(distribution.NewWorker)(o)` all carry the selector, and a
// second worker built any of those ways is invisible to the recorder), the
// default is the one statement `return distribution.NewWorker(o)`, and the
// worker is launched once under its label (goTracked or safego.Go). The
// pin's SCOPE is its boundary (L16, review rounds 8–9): it holds over
// internal/scheduler's production sources for a constructor reached by its
// selector. Outside it by construction: a bare `go` statement (this pin
// counts the launch label; a bare `go` carries none); a dot import (`.
// ".../distribution"` makes `NewWorker(o)` selector-free) — refused below
// by an assertion over the same corpus (go/parser's import list plus
// strconv.Unquote, so every legal spelling — block, one-line,
// parenthesised, raw-string or escaped path — is one predicate); the
// lint tiers' own dot-import rule (staticcheck ST1001) is not relied on
// here (ledger rounds 8–12 record why: what the analyzer exempts and
// which CI checks are required are pinned by nothing in this tree); and
// any route through another package — a re-export (`var New =
// distribution.NewWorker`), a `//go:linkname`, another package's init().
func TestDistributionWorkerSeamIsTheOnlyConstructor(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/scheduler", 10)
	// go/parser reads the import list itself (review round 10: a regex over
	// the text missed `import (. "…")` and a raw-string path).
	const distributionPath = "github.com/aveloxis/aveloxis/internal/collector/distribution"
	for name, src := range files {
		f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range f.Imports {
			path, perr := strconv.Unquote(imp.Path.Value)
			if perr == nil && imp.Name != nil && imp.Name.Name == "." && path == distributionPath {
				t.Errorf("%s dot-imports the distribution package — NewWorker becomes selector-free and this pin cannot count it", name)
			}
		}
	}
	seamIdent, constructors, launches := 0, 0, 0
	identRe := regexp.MustCompile(`\bnewDistributionWorker\b`)
	launchRe := regexp.MustCompile(`"distribution-worker",\s*func\(\)`)
	for _, src := range files {
		code := srctest.StripGoComments(src)
		seamIdent += len(identRe.FindAllString(code, -1))
		constructors += strings.Count(code, ".NewWorker")
		launches += len(launchRe.FindAllString(code, -1))
	}
	if seamIdent != 2 {
		t.Errorf("the identifier newDistributionWorker appears %d times in production; want exactly 2 (its declaration and its one call) — any other reference re-points or wraps the seam", seamIdent)
	}
	if constructors != 1 {
		t.Errorf(".NewWorker appears %d times in production; want exactly once, inside the seam's default — a worker built any other way is invisible to the recorder", constructors)
	}
	if launches != 1 {
		t.Errorf("the distribution worker is launched %d times; want once", launches)
	}
	decl := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/distribution_wiring.go"))
	i := strings.Index(decl, "var newDistributionWorker = func(o distribution.WorkerOptions) distributionRunner {")
	if i < 0 {
		t.Fatal("the seam's declaration is not in distribution_wiring.go with the expected signature")
	}
	body := decl[i:]
	body = body[strings.Index(body, "{")+1:]
	body = body[:strings.Index(body, "}")]
	if strings.TrimSpace(body) != "return distribution.NewWorker(o)" {
		t.Errorf("the seam's default must be exactly `return distribution.NewWorker(o)`; got %q — a default that changes the options is not the production constructor", strings.TrimSpace(body))
	}
}
