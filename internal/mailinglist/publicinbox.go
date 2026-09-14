// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// PublicInbox is the lore.kernel.org (public-inbox) ArchiveSource backend
// (Phase 3 — the generalization proof for a non-Apache, non-HTTP system).
//
// lore's HTTP is gated by Anubis (anti-scraper proof-of-work), so the
// sanctioned bulk path is `git clone` of the per-list archive repo: a
// public-inbox v2 archive is a git repo where each message is a blob named
// "m" in its own commit. We clone once per list (cached), then walk commits
// by date and cat-file each `:m` blob, parsing with the shared RFC-822
// pipeline. Inboxes are named like "linux-pci"; the list local-part is the
// inbox name.
type PublicInbox struct {
	baseURL   string // https://lore.kernel.org (or a local dir in tests)
	cloneRoot string // where per-list clones live

	mu     sync.Mutex
	clones map[string]string // inbox → local clone dir
}

// NewPublicInbox builds the backend. cloneRoot defaults to a temp dir.
func NewPublicInbox(baseURL, cloneRoot string) *PublicInbox {
	if baseURL == "" {
		baseURL = "https://lore.kernel.org"
	}
	if cloneRoot == "" {
		cloneRoot = filepath.Join(os.TempDir(), "aveloxis-lore")
	}
	return &PublicInbox{
		baseURL:   strings.TrimRight(baseURL, "/"),
		cloneRoot: cloneRoot,
		clones:    map[string]string{},
	}
}

func (p *PublicInbox) Name() string { return "lore_public_inbox" }

// EnumerateLists is not supported: lore's list index is Anubis-gated, so
// kernel lists are configured explicitly (a curated set), not discovered.
// Returns nil so callers fall back to their configured list set.
func (p *PublicInbox) EnumerateLists(ctx context.Context, domain string) ([]ListInfo, error) {
	return nil, nil
}

// archiveURL is the git URL of an inbox's epoch-0 archive. (One epoch is
// enough for a bounded test; busy inboxes have 1.git, 2.git, … which a
// production build would also clone.)
func (p *PublicInbox) archiveURL(inbox string) string {
	return p.baseURL + "/" + inbox + "/git/0.git"
}

// ensureClone clones (once, cached) an inbox archive. --shallow-since bounds
// the clone of a busy list to recent history.
func (p *PublicInbox) ensureClone(ctx context.Context, inbox string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if dir, ok := p.clones[inbox]; ok {
		return dir, nil
	}
	dir := filepath.Join(p.cloneRoot, inbox)
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil {
		if err := os.MkdirAll(p.cloneRoot, 0o755); err != nil {
			return "", err
		}
		cmd := exec.CommandContext(ctx, "git", "clone", "--quiet", "--bare", p.archiveURL(inbox), dir)
		if out, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("clone %s: %w: %w: %s", inbox, ErrTransient, ctxCause(ctx, err), strings.TrimSpace(string(out)))
		}
	}
	p.clones[inbox] = dir
	return dir, nil
}

// FirstMonth returns the earliest commit month in the inbox archive.
func (p *PublicInbox) FirstMonth(ctx context.Context, listAddress string) (string, error) {
	inbox, _ := splitListAddress(listAddress)
	if inbox == "" {
		return "", fmt.Errorf("invalid list address %q", listAddress)
	}
	dir, err := p.ensureClone(ctx, inbox)
	if err != nil {
		return "", err
	}
	out, err := exec.CommandContext(ctx, "git", "-C", dir, "log", "--reverse",
		"--format=%cd", "--date=format:%Y-%m", "--max-count=1").Output()
	if err != nil {
		return "", nil
	}
	return strings.TrimSpace(string(out)), nil
}

// FetchMonth clones (cached) the inbox and returns the messages whose commit
// date falls in the yyyy-mm window, parsing each `:m` blob.
func (p *PublicInbox) FetchMonth(ctx context.Context, listAddress, yyyymm string) ([]ArchiveMessage, time.Duration, error) {
	inbox, _ := splitListAddress(listAddress)
	if inbox == "" {
		return nil, 0, fmt.Errorf("invalid list address %q", listAddress)
	}
	dir, err := p.ensureClone(ctx, inbox)
	if err != nil {
		return nil, 0, err
	}
	start, err := time.Parse("2006-01", yyyymm)
	if err != nil {
		return nil, 0, fmt.Errorf("bad month %q", yyyymm)
	}
	end := start.AddDate(0, 1, 0)
	// git --since is strictly "more recent than", so a commit at exactly the
	// month start would be dropped; widen both bounds by a second to capture
	// [start, end) inclusive of the boundary commit.
	since := start.Add(-time.Second).Format("2006-01-02T15:04:05")
	until := end.Add(-time.Second).Format("2006-01-02T15:04:05")

	out, err := exec.CommandContext(ctx, "git", "-C", dir, "log", "--format=%H",
		"--since="+since, "--until="+until).Output()
	if err != nil {
		return nil, 0, fmt.Errorf("git log %s: %w: %w", inbox, ErrTransient, ctxCause(ctx, err))
	}
	var msgs []ArchiveMessage
	for _, hash := range strings.Fields(string(out)) {
		blob, berr := exec.CommandContext(ctx, "git", "-C", dir, "cat-file", "-p", hash+":m").Output()
		if berr != nil {
			continue // not all commits carry an "m" blob (e.g. tooling commits)
		}
		if am, ok := parseRFC822(blob, listAddress); ok {
			msgs = append(msgs, am)
		}
	}
	return msgs, 0, nil
}

// ctxCause returns the context's error when the subprocess died under a
// done ctx (exec reports `signal: killed`, never context.Canceled) and
// the raw error otherwise, so the ErrTransient wrap keeps the cause
// visible to errors.Is (pass 39).
func ctxCause(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
