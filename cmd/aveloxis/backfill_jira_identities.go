// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/jira"
	"github.com/spf13/cobra"
)

// backfillJiraIdentitiesCmd registers `aveloxis backfill-jira-identities`
// — the C2 one-shot: for every REGISTERED project (jira_project_serve
// — run `aveloxis register-jira-projects` first), bulk-search that
// registration's Jira Server (identity + state fields, NO comments —
// those are the collector's job) and write reporter identity +
// authoritative state onto the synthetic issues through the same
// provider-precedence writers the collector uses. ~2-3 polite hours
// for the full 844K-issue ASF corpus at 1,000 issues/search.
//
// Copilot round 2 on PR #193 (#1): the registrations are the ONLY
// project source here — never a re-derivation from synthetics, which
// would ignore operator corrections to a registration's repo_id and
// query the wrong instance for a corrected base_url.
//
// Run it SOON regardless of collector enablement: the stable Server-era
// username this matches on (49.2% of issues unambiguous by login,
// +17.6% by display) does not exist in Jira Cloud's API — an ASF cloud
// migration makes the whole corpus's usernames unfetchable at once.
//
// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
// Only serve and the migrate subcommand run migrations.
func backfillJiraIdentitiesCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun  bool
		project string
		limit   int
	)
	cmd := &cobra.Command{
		Use:   "backfill-jira-identities",
		Short: "Bank Jira reporter identity + authoritative state from the Jira Server API (one-shot)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-jira-backfill"), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			fields := []string{"summary", "reporter", "assignee", "status", "resolution", "resolutiondate", "created", "updated"}
			const pageSize = 1000 // identity-only pages measured 1.6MB/8.5s
			const pageSleep = 500 * time.Millisecond

			regs, err := store.ListJiraProjectRegistrations(ctx)
			if err != nil {
				return err
			}
			if len(regs) == 0 {
				return fmt.Errorf("no enabled Jira project registrations — run `aveloxis register-jira-projects` first (the registration's repo mapping and base URL are what this backfill writes against)")
			}
			// Copilot round 17 (suppressed): an unknown --project used to
			// skip every registration and exit 0 with "backfilled 0
			// issues" — a typo looked like a completed targeted
			// backfill. Validate the key against the enabled
			// registrations before entering the loop.
			if project != "" {
				found := false
				for _, c := range regs {
					if c.ProjectKey == project {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("--project %q matches no ENABLED Jira registration (%d registered) — check the key or run `aveloxis register-jira-projects`", project, len(regs))
				}
			}
			attempted, processed, linked, minted, failedProjects, failedIssues, skippedNoRepo := 0, 0, 0, 0, 0, 0, 0
			for _, c := range regs {
				if project != "" && c.ProjectKey != project {
					continue
				}
				if ctx.Err() != nil {
					return fmt.Errorf("interrupted after %d issues — rerun; completed work is idempotent", processed)
				}
				if c.RepoID == nil {
					// The registration is the operator-correctable mapping;
					// without a repo there is nothing to write against.
					logger.Warn("jira backfill: registration has no repo mapping — skipped (set jira_project_serve.repo_id)",
						"project", c.ProjectKey)
					skippedNoRepo++
					continue
				}
				client := jira.New(c.BaseURL, cfg.Collection.JiraPoliteEmail)
				if dryRun {
					total, terr := client.ProjectTotal(ctx, c.ProjectKey)
					if terr != nil {
						if platform.ClassifyError(terr) == platform.ClassSkip {
							logger.Warn("jira backfill: dead project key skipped", "project", c.ProjectKey)
							continue
						}
						logger.Warn("jira backfill: project failed — continuing", "project", c.ProjectKey, "error", terr)
						failedProjects++
						continue
					}
					fmt.Printf("%-16s total=%d\n", c.ProjectKey, total)
					continue
				}
				// Copilot round 3 on PR #193 (#2): the full-history pass
				// walks jira.WalkProjectByUpdated — the ONE drift-safe
				// walk the incremental worker uses (SR-17) — never bare
				// offsets over `ORDER BY updated ASC`, whose membership
				// mutates under the walk and can permanently skip an
				// issue while the command exits 0. Boundary-minute
				// re-lists are deduped below so counters stay honest.
				seen := map[string]struct{}{}
				walkErr := client.WalkProjectByUpdated(ctx, c.ProjectKey, fields, pageSize, pageSleep, time.Time{},
					func(issues []jira.Issue, updated []time.Time) error {
						for _, is := range issues {
							nk := is.Key + "@" + is.Fields.Updated
							if _, dup := seen[nk]; dup {
								continue // a re-listed boundary-minute issue
							}
							seen[nk] = struct{}{}
							// Copilot round 21 (PR #193): --limit must bound
							// IDENTITY MUTATIONS, not successful upserts. The
							// reporter/assignee resolve+mint below commit
							// contributor + jira_identities rows BEFORE the
							// issue upsert, and a failed issue
							// (failedIssues++/continue) never increments
							// `processed` — so a failure-heavy `--limit N`
							// canary would walk the WHOLE corpus, mutating
							// identities for every issue, while `processed`
							// never reached N. Stop BEFORE this issue's first
							// identity write once N issues have been attempted;
							// `processed` stays the successful count for the
							// report.
							if limit > 0 && attempted >= limit {
								return errBackfillLimitHit
							}
							attempted++
							in := db.JiraAPIIssue{RepoID: *c.RepoID, ExternalKey: is.Key, Title: is.Fields.Summary}
							in.JiraIssueID, _ = strconv.ParseInt(is.ID, 10, 64)
							if is.Fields.Status != nil {
								in.Status = is.Fields.Status.Name
							}
							if is.Fields.Resolution != nil {
								in.Resolution = is.Fields.Resolution.Name
							}
							in.Created = jiraParse(is.Fields.Created)
							in.Updated = jiraParse(is.Fields.Updated)
							in.ResolutionDate = jiraParse(is.Fields.ResolutionDate)
							// Copilot round 2 on PR #193 (#4): a resolve/mint
							// ERROR is not "unresolved" — swallowing it made a
							// transient DB failure report success while
							// attribution stayed permanently missing. Failed
							// issues are skipped (no partial write that a rerun
							// would then skip re-attributing) and counted; the
							// command exits nonzero so the operator reruns from
							// a truthful state. Ambiguity remains the
							// intentional no-link case.
							reporter, reporterMinted, rerr := backfillResolveIdentity(ctx, store, is.Fields.Reporter)
							if rerr != nil {
								if errors.Is(rerr, context.Canceled) {
									return rerr // interrupt, not an issue failure
								}
								logger.Warn("jira backfill: reporter identity failed — issue skipped", "key", is.Key, "error", rerr)
								failedIssues++
								continue
							}
							// Copilot round 19 (PR #193): count the reporter mint
							// IMMEDIATELY — the contributor is already committed.
							// If the assignee block below then `continue`s on a
							// transient error, the mint is nonetheless real and
							// must be counted; a rerun cannot re-count it (mint is
							// idempotent, so reporterMinted would be false), so a
							// late count permanently under-reports.
							if reporterMinted {
								minted++
							}
							// Suppressed #1: bank the ASSIGNEE identity too —
							// the Server-era username is the perishable half,
							// and this one-shot is the banking vehicle. The id
							// is not written anywhere yet (issues has no
							// assignee column); the banking is the point.
							if _, am, aerr := backfillResolveIdentity(ctx, store, is.Fields.Assignee); aerr != nil {
								if errors.Is(aerr, context.Canceled) {
									return aerr // interrupt, not an issue failure
								}
								logger.Warn("jira backfill: assignee identity failed — issue skipped", "key", is.Key, "error", aerr)
								failedIssues++
								continue
							} else if am {
								minted++
							}
							if reporter != "" {
								in.ReporterCntrb = reporter
								linked++
							}
							if _, uerr := store.UpsertJiraIssueFromAPI(ctx, in); uerr != nil {
								if errors.Is(uerr, context.Canceled) {
									return uerr // interrupt, not an issue failure
								}
								logger.Warn("jira backfill: issue upsert failed", "key", is.Key, "error", uerr)
								failedIssues++
								continue
							}
							processed++
						}
						return nil
					})
				if walkErr != nil {
					switch {
					case errors.Is(walkErr, errBackfillLimitHit):
						// Copilot round 3 on PR #193 (#3): there is NO
						// persisted resume marker — a rerun starts from
						// the beginning, so "rerun to continue" was a lie
						// (the same --limit reprocesses the same N and
						// stops at the same point). Say what is true.
						fmt.Printf("hit --limit after %d issues attempted (%d fully processed, linked=%d minted=%d) — canary complete. Rerun WITHOUT --limit for the full pass; it restarts from the beginning and every write is idempotent.\n",
							attempted, processed, linked, minted)
						return nil
					case errors.Is(walkErr, context.Canceled):
						return fmt.Errorf("interrupted after %d issues — rerun; completed work is idempotent", processed)
					case platform.ClassifyError(walkErr) == platform.ClassSkip:
						logger.Warn("jira backfill: dead project key skipped", "project", c.ProjectKey)
					default:
						logger.Warn("jira backfill: project failed — continuing", "project", c.ProjectKey, "error", walkErr)
						failedProjects++
					}
				}
			}
			fmt.Printf("backfilled %d issues (reporter linked=%d, contributors minted=%d, failed projects=%d, failed issues=%d, skipped no-repo registrations=%d)\n",
				processed, linked, minted, failedProjects, failedIssues, skippedNoRepo)
			if failedProjects > 0 || failedIssues > 0 {
				return fmt.Errorf("%d project(s) and %d issue(s) failed — rerun retries them (all writes are idempotent)", failedProjects, failedIssues)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "report per-project totals without writing")
	cmd.Flags().StringVar(&project, "project", "", "restrict to one project key")
	cmd.Flags().IntVar(&limit, "limit", 0, "stop after N issues (canary)")
	return cmd
}

// errBackfillLimitHit aborts the walk cleanly when --limit is reached.
var errBackfillLimitHit = errors.New("backfill limit hit")

func jiraParse(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(jira.TimeLayout, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}

// backfillResolveIdentity resolves-or-mints one Jira identity: banked
// via ResolveJiraIdentity regardless of match; unambiguous no-match
// mints (the processor's resolveIdentity contract). Returns the linked
// cntrb_id ("" for absent/ambiguous), whether a mint happened, and any
// ERROR — which the caller must treat as issue failure, never as
// "unresolved" (SR-5).
func backfillResolveIdentity(ctx context.Context, store *db.PostgresStore, u *jira.User) (string, bool, error) {
	if u == nil || u.Name == "" {
		return "", false, nil
	}
	cntrb, _, ambiguous, err := store.ResolveJiraIdentity(ctx, u.Name, u.Key, u.DisplayName)
	if err != nil {
		return "", false, fmt.Errorf("resolve jira identity %q: %w", u.Name, err)
	}
	if cntrb != "" || ambiguous {
		return cntrb, false, nil
	}
	m, err := store.MintJiraContributor(ctx, u.Name, u.DisplayName)
	if err != nil {
		return "", false, fmt.Errorf("mint jira contributor %q: %w", u.Name, err)
	}
	return m, true, nil
}
