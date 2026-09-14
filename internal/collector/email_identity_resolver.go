// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"strings"
)

// email_identity_resolver.go is the SINGLE shared email→GitHub-identity chain
// used by the three "resolve an email to a contributor" call sites (operator
// directive, 2026-06-04 — "one shared piece of code for these two distinct but
// highly similar use cases"):
//
//  1. commit-author resolution (CommitResolver) — uses ResolveEmailViaAPI as
//     its Strategy-4 tail, AFTER its commit-specific SHA lookup (Strategy 3);
//  2. the mailing-list sender-resolve ticker — uses ResolveEmailToIdentity;
//  3. the mailing-list batch Processor's contributor resolution.
//
// The chain (cheapest first): noreply parse → bot/junk filter → DB lookup →
// GitHub Search API by email → GitHub GLOBAL commit-search by author-email.
// The last step is the load-bearing one for private-profile-email users
// (bootstrap 2026-06-04: ~35% vs ~8-11% for Search alone; summary/12 §5g).
//
// Contract mirrors the platform search methods: a no-resolution outcome is
// (`"", 0, "", nil`) — NOT an error. An error is returned only on transport /
// 5xx failures the caller would want to retry.

// emailSearchClient is the narrow slice of platform.Client this resolver
// needs (kept narrow so it is trivially fakeable in tests; *platform.Client
// implementations satisfy it).
type emailSearchClient interface {
	SearchUserByEmail(ctx context.Context, email string) (login string, userID int64, err error)
	SearchCommitByAuthorEmail(ctx context.Context, email string) (login string, userID int64, err error)
}

// emailDBLookup is the narrow DB surface: resolve an email to an existing
// contributor login via cntrb_email/canonical + aliases (FindLoginByEmail).
type emailDBLookup interface {
	FindLoginByEmail(ctx context.Context, email string) (string, error)
}

// Resolution source labels (telemetry + tests).
const (
	EmailSourceNoreply      = "noreply"
	EmailSourceDB           = "db"
	EmailSourceSearch       = "search"
	EmailSourceCommitSearch = "commit-search"
)

// ResolveEmailViaAPI is the shared API tail: Search API by email → global
// commit-search by author-email. The CommitResolver calls THIS (not the full
// chain) so it can keep its commit-specific SHA lookup ahead of the search
// steps without reordering. Returns ("",0,"",nil) on no hit.
func ResolveEmailViaAPI(ctx context.Context, client emailSearchClient, email string) (login string, ghUserID int64, source string, err error) {
	email = strings.Trim(email, `"' `)
	if email == "" || !strings.Contains(email, "@") {
		return "", 0, "", nil
	}
	if login, id, err := client.SearchUserByEmail(ctx, email); err != nil {
		return "", 0, "", err
	} else if login != "" {
		return login, id, EmailSourceSearch, nil
	}
	if login, id, err := client.SearchCommitByAuthorEmail(ctx, email); err != nil {
		return "", 0, "", err
	} else if login != "" {
		return login, id, EmailSourceCommitSearch, nil
	}
	return "", 0, "", nil
}

// ResolveEmailToIdentity runs the full shared chain: noreply → bot filter →
// DB lookup → API tail (Search → global commit-search). Used by the
// mailing-list sender resolver and batch Processor. A nil store skips the DB
// step; a nil client skips the API tail (so a DB-only resolve is valid).
func ResolveEmailToIdentity(ctx context.Context, store emailDBLookup, client emailSearchClient, email string) (login string, ghUserID int64, source string, err error) {
	email = strings.Trim(email, `"' `)

	// Strategy 1: noreply parse (free).
	if info := ParseNoreplyEmail(email); info != nil {
		return info.Login, info.UserID, EmailSourceNoreply, nil
	}
	// Automation/junk/non-email short-circuit (IsAutomationEmail ⊇
	// IsBotEmail; the ticker pre-gates too — belt at the owning layer).
	if IsAutomationEmail(email) || email == "" || !strings.Contains(email, "@") {
		return "", 0, "", nil
	}
	// Strategy 2: DB lookup (cntrb_email/canonical + aliases).
	if store != nil {
		if l, derr := store.FindLoginByEmail(ctx, email); derr == nil && l != "" {
			return l, 0, EmailSourceDB, nil
		}
	}
	// Strategies 3+4: shared API tail.
	if client != nil {
		return ResolveEmailViaAPI(ctx, client, email)
	}
	return "", 0, "", nil
}
