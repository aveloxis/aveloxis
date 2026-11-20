// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/hostguard"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// mavenPomSource obtains a Maven parent pom or BOM that is not in the clone
// being analysed, from the configured Maven repositories
// (collection.maven_repositories), in order (v0.29.182; summary/70, 4.5).
// It is the manifest package's PomSource; that package does no network I/O.
//
// Every ANSWER is stored, per (serving repository, group, artifact,
// version): a pom that was obtained, or a repository's "I have none". What
// is stored is used before any request is made, and is what stands when a
// request fails:
//
//   - a stored hit is used, with no request (a released pom does not
//     change);
//   - a stored miss that is not due passes to the next repository, with no
//     request;
//   - nothing stored, or a miss that is due: the repository is asked. A pom
//     is stored and used; a miss is stored and passes on; a FAILURE stores
//     nothing, and a due miss then stands as the stored answer.
//
// So a repository's bad hour cannot change a dependency's version, and with
// it the dependency's purl and every finding on it. A coordinate that has
// never been answered and cannot be asked now is RemoteFailed: unknown, and
// said to be.
type mavenPomSource struct {
	ctx    context.Context
	store  mavenPomStore
	logger *slog.Logger
	// counts for the one log line an analysis writes.
	fetched, served, missed, failed, staleServed, refused int
	// answered are the hosts already recorded as answering in this
	// analysis: once each.
	answered map[string]bool
	// distrusted is the distrusted list as read once for this analysis
	// (0.29.186): the bound on how soon a distrust made in the web process
	// takes effect here.
	distrusted hostguard.Snapshot
	// declaredSpent is how long this analysis's requests to DECLARED
	// repositories have taken; budgetCut counts the requests not made, or
	// cut short, because declaredFetchBudget was used up.
	declaredSpent time.Duration
	budgetCut     int
	// declaredNotAsked counts the declared repositories a lookup left
	// unasked because it had already asked maxDeclaredRepositoriesAsked.
	declaredNotAsked int
	// unaskable counts the coordinates that were not asked for because
	// they are not text a request can be built from.
	unaskable int
}

// maxDeclaredRepositoriesAsked is how many of the repositories a build
// declares ONE lookup asks, nearest pom first. declaredFetchBudget bounds
// the waiting; this bounds the count, because a declared repository that
// the address rule refuses costs no time at all, and a build may declare
// any number of them: every coordinate times every repository was a read
// of the store and a refused dial (review round 2). With the rest unasked
// the coordinate is not known, never "not found".
//
// The value is pomChainLimit's, 32: no build found in the fleet's sample
// declares more than a handful, and a parent chain is the other list in a
// pom that is bounded for the same reason. PROVISIONAL (summary/71,
// section 8); a build past it is named in the log.
const maxDeclaredRepositoriesAsked = 32

// declaredFetchBudget is how long ONE analysis may spend waiting on the
// repositories its build files declare. Those hosts, and how many
// coordinates are asked of them, are chosen by the collected repository,
// and nothing else bounds the sum: each request has its timeout, each host
// its breaker, and a build can name any number of hosts and coordinates. A
// worker would be held for their product, and again at every recollection,
// since a failed request stores nothing.
//
// The value is the time the breaker lets one failing host cost before it is
// rested (its threshold of requests, each to its timeout), applied to the
// analysis as a whole: five minutes. Past it no declared repository is
// asked for the rest of the analysis; what is stored is still served, and a
// coordinate with nothing stored is unknown (never "not found"), so no
// finding closes on it. PROVISIONAL (summary/71, section 8): a build that
// legitimately needs longer shows as the log line below.
var declaredFetchBudget = time.Duration(registryBreakerThreshold) * registryHTTPClient.Timeout

// mavenCoordinatePart is what a group, an artifact or a version may be made
// of to be asked for. The three become path segments of a URL on a host the
// build may have chosen; `?`, `#`, `/` or `%2e%2e` in one would add a query
// or steer the path. Maven's own grammar is narrower than this.
var mavenCoordinatePart = regexp.MustCompile(`^[A-Za-z0-9_+-]+(\.[A-Za-z0-9_+-]+)*$`)

func mavenCoordinateAskable(group, artifact, version string) bool {
	return mavenCoordinatePart.MatchString(group) && mavenCoordinatePart.MatchString(artifact) && mavenCoordinatePart.MatchString(version)
}

// mavenPomStore is the storage the source reads and writes.
type mavenPomStore interface {
	GetMavenPomResult(ctx context.Context, repository, group, artifact, version string) (db.MavenPomResult, bool, error)
	NotePackageRepositoryHostAnswered(ctx context.Context, host string) error
	GetDistrustedRepositoryHosts(ctx context.Context) ([]string, error)
	PutMavenPomResult(ctx context.Context, repository, group, artifact, version string, found bool, extract []byte, ttl time.Duration) error
}

// mavenPomMissTTL is how long a repository's "I have none" holds before it
// is asked again: the registry answer cache's lifetime, so a miss is as old
// as any other registry answer an analysis may act on.
const mavenPomMissTTL = registryCacheTTL

// mavenPomBodyLimit bounds one pom. The largest in common use (the Spring
// Boot dependencies BOM) is a few hundred kilobytes; a megabyte-scale pom is
// not one.
const mavenPomBodyLimit int64 = 8 << 20

// newMavenPomSource reads the distrusted list, once. If it cannot be read
// there is no source: which hosts may be used is not known, and a host an
// operator has distrusted must not be used on a guess (SR-5).
func newMavenPomSource(ctx context.Context, store mavenPomStore, logger *slog.Logger) (*mavenPomSource, error) {
	hosts, err := store.GetDistrustedRepositoryHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading the distrusted repository hosts: %w", err)
	}
	list := hostguard.NewSnapshot(hosts)
	// The snapshot rides the context too: a redirect hop is judged against
	// it (registryCheckRedirect).
	return &mavenPomSource{ctx: withDistrustedHosts(withRegistryBodyLimit(ctx, mavenPomBodyLimit), list), store: store, logger: logger,
		distrusted: list}, nil
}

// mavenPomURL is where a repository keeps a pom.
func mavenPomURL(base, group, artifact, version string) string {
	segments := strings.Split(group, ".")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return base + "/" + strings.Join(segments, "/") + "/" + url.PathEscape(artifact) + "/" +
		url.PathEscape(version) + "/" + url.PathEscape(artifact) + "-" + url.PathEscape(version) + ".pom"
}

// declaredRepositoryFetch is collection.declared_repository_fetch: whether a
// repository a build file declares is asked. Off, nothing is fetched from
// such a host; what it already served stays stored and is still used (the
// knob gates fetching, not reading: switching it must not re-key every
// version resolved through those hosts across the fleet at once). The value
// before loadConfig installs it is off.
var declaredRepositoryFetch atomic.Bool

// SetDeclaredRepositoryFetch installs the knob.
func SetDeclaredRepositoryFetch(on bool) { declaredRepositoryFetch.Store(on) }

// mavenDeclaredHitTTL is how long a pom from a DECLARED repository holds
// before it is asked for again. A configured repository's pom never is: the
// operator named that host, and a released pom does not change. A declared
// host is one a collected repository chose; its answer is re-confirmed once
// a recollection cycle (the default cycle, 21 days), and when it cannot be,
// the stored answer stands. PROVISIONAL (summary/71, section 8).
const mavenDeclaredHitTTL = 21 * 24 * time.Hour

// mavenDeclaredSchemeOK is the source's own check that a declared address
// is https. The manifest reader has already left out any that is not; this
// is the layer that would otherwise act on it (SR-18). A test seam, pinned
// at its default.
var mavenDeclaredSchemeOK = func(raw string) bool { return strings.HasPrefix(raw, "https://") }

// repositoryAnswer is what one repository contributes to a resolution.
type repositoryAnswer int

const (
	answerMiss    repositoryAnswer = iota // it has none (now, or as stored)
	answerHit                             // the pom
	answerUnknown                         // no answer, and none stored
	answerRefused                         // it may not be fetched from, or refused the request
)

// RemotePom implements manifest.PomSource: the configured repositories in
// order, then the repositories the requesting build declares. Found beats
// everything; otherwise unknown beats refused beats not found.
func (s *mavenPomSource) RemotePom(group, artifact, version string, declared []string) (*manifest.RemotePom, manifest.RemoteOutcome) {
	if !mavenCoordinateAskable(group, artifact, version) {
		// Not text to build a URL from: nothing is asked. The answer is
		// the one the reader itself gives a coordinate it never asks for
		// (a range, a snapshot): not found. "Refused" is a REPOSITORY
		// that may not be fetched from, which is not what happened; this
		// is counted and said on its own (round 3).
		s.unaskable++
		return nil, manifest.RemoteNotFound
	}
	unknown, refused := false, false
	settle := func(pom *manifest.RemotePom, ans repositoryAnswer) bool {
		switch ans {
		case answerHit:
			return pom != nil
		case answerUnknown:
			unknown = true
		case answerRefused:
			refused = true
		}
		return false
	}
	configured := mavenRepositoryBases()
	for _, base := range configured {
		pom, ans := s.ask(s.ctx, base, group, artifact, version, true, 0)
		if settle(pom, ans) {
			return pom, manifest.RemoteFound
		}
		if s.ctx.Err() != nil {
			return nil, manifest.RemoteFailed
		}
	}
	fetch := declaredRepositoryFetch.Load()
	declaredCtx := withDeclaredRepositoryHost(s.ctx)
	askedDeclared := 0
	for _, base := range declared {
		if slices.Contains(configured, base) || !mavenDeclaredSchemeOK(base) {
			continue // asked above as a configured one, or not an address this source acts on
		}
		if askedDeclared >= maxDeclaredRepositoriesAsked {
			s.declaredNotAsked++
			unknown = true // one of them might have had it
			continue
		}
		askedDeclared++
		pom, ans := s.ask(declaredCtx, base, group, artifact, version, fetch, mavenDeclaredHitTTL)
		if settle(pom, ans) {
			return pom, manifest.RemoteFound
		}
		if s.ctx.Err() != nil {
			return nil, manifest.RemoteFailed
		}
	}
	switch {
	case unknown:
		return nil, manifest.RemoteFailed
	case refused:
		return nil, manifest.RemoteRefused
	}
	return nil, manifest.RemoteNotFound
}

// ask is one repository's part: the stored answer when there is one that
// holds, a request when fetch allows it and one is needed, and the stored
// answer again when the request fails. hitTTL is how long a pom from this
// repository holds (0: for good).
func (s *mavenPomSource) ask(ctx context.Context, base, group, artifact, version string, fetch bool, hitTTL time.Duration) (*manifest.RemotePom, repositoryAnswer) {
	// The one decision (hostguard.MayUse), at the moment of use and before
	// the store is read: a distrusted host is neither fetched from nor
	// served from.
	if !hostguard.MayUse(s.distrusted, hostOf(base)) {
		s.refused++
		return nil, answerRefused
	}
	res, stored, err := s.store.GetMavenPomResult(ctx, base, group, artifact, version)
	if err != nil {
		// The store could not be read: what this repository said is not
		// known (SR-5), and it is not asked blind either, since its answer
		// could not be stored.
		return nil, answerUnknown
	}
	var storedPom *manifest.RemotePom
	if stored && res.Found {
		if storedPom = decodeStoredPom(res.Extract); storedPom == nil {
			stored = false // an extract that no longer decodes is asked for again
		} else {
			storedPom.ServedBy = hostOf(base)
		}
	}
	if stored && (!res.Expired || !fetch) {
		// It holds; or it is due and this repository is not being fetched
		// from, in which case what it already served is still what is known.
		if storedPom != nil {
			s.served++
			return storedPom, answerHit
		}
		return nil, answerMiss
	}
	if !fetch {
		return nil, answerMiss // never asked, and not to be: it contributes nothing
	}
	declared := registryDeclaredHost(ctx)
	if declared && s.declaredSpent >= declaredFetchBudget {
		// The analysis has waited on declared repositories as long as it
		// may. Not asked: what is stored stands, and with nothing stored
		// the coordinate is unknown.
		s.budgetCut++
		return s.standOn(storedPom, stored)
	}
	// A declared request ends with the budget: one fetch retries inside
	// itself (a 429's Retry-After, each round trip to the client's
	// timeout), and without a deadline of its own it ran several times
	// the budget before the budget was looked at again (round 2).
	fetchCtx := ctx
	if declared {
		var cancel context.CancelFunc
		fetchCtx, cancel = context.WithTimeout(ctx, declaredFetchBudget-s.declaredSpent)
		defer cancel()
	}
	asked := time.Now()
	body, err := fetchRegistryJSON(fetchCtx, mavenPomURL(base, group, artifact, version))
	if declared {
		s.declaredSpent += time.Since(asked)
	}
	switch {
	case err == nil:
		pom, perr := manifest.ParseRemotePom(string(body))
		if perr != nil {
			// The repository answered with something that is not a pom:
			// not an answer about the coordinate, and not stored.
			s.logger.Warn("a Maven repository served a pom that could not be parsed", "repository", hostOf(base),
				"coordinate", group+":"+artifact+":"+version, "error", perr)
			return s.standOn(storedPom, stored)
		}
		s.fetched++
		s.noteAnswered(ctx, base)
		pom.ServedBy = hostOf(base)
		if extract, jerr := json.Marshal(pom); jerr == nil {
			perr := s.store.PutMavenPomResult(ctx, base, group, artifact, version, true, extract, hitTTL)
			if errors.Is(perr, context.Canceled) {
				return nil, answerUnknown // shutdown, not a failure
			}
			if perr != nil {
				s.logger.Warn("a fetched Maven pom could not be stored; it is used now and fetched again next time", "error", perr)
			}
		}
		return pom, answerHit
	case fetchCtx.Err() != nil:
		// Shutdown, or the declared budget ran out mid-request: no answer.
		if ctx.Err() == nil {
			s.budgetCut++
			return s.standOn(storedPom, stored)
		}
		return nil, answerUnknown
	case isDefinitiveRegistryMiss(err):
		s.missed++
		s.noteAnswered(ctx, base)
		perr := s.store.PutMavenPomResult(ctx, base, group, artifact, version, false, nil, mavenPomMissTTL)
		if errors.Is(perr, context.Canceled) {
			return nil, answerUnknown // shutdown, not a failure
		}
		if perr != nil {
			s.logger.Warn("a Maven repository's miss could not be stored; it is asked again next time", "error", perr)
		}
		return nil, answerMiss
	case isRepositoryRefusal(err):
		// Not to be fetched from: the outbound-address rule refused its
		// address, or it answered 401 or 403. A terminal reason, and not a
		// failed lookup: asking again will not help.
		s.refused++
		if stored {
			return s.standOn(storedPom, stored)
		}
		return nil, answerRefused
	}
	// The refresh failed. What is stored stands; with nothing stored the
	// coordinate is unknown.
	if stored {
		s.staleServed++
	} else {
		s.failed++
	}
	return s.standOn(storedPom, stored)
}

// noteAnswered records, once per analysis, that a repository host answered:
// the host registry is what the host watch reads (0.29.185). A failure to
// record it costs the record for this analysis and nothing else.
func (s *mavenPomSource) noteAnswered(ctx context.Context, base string) {
	host := hostOf(base)
	if host == "" || s.answered[host] {
		return
	}
	if s.answered == nil {
		s.answered = map[string]bool{}
	}
	s.answered[host] = true
	err := s.store.NotePackageRepositoryHostAnswered(ctx, host)
	if errors.Is(err, context.Canceled) {
		return // shutdown, not a failure
	}
	if err != nil {
		s.logger.Warn("a package repository host's answer could not be recorded", "host", host, "error", err)
	}
}

// standOn is the answer when a request gave none: the stored one.
func (s *mavenPomSource) standOn(storedPom *manifest.RemotePom, stored bool) (*manifest.RemotePom, repositoryAnswer) {
	switch {
	case storedPom != nil:
		return storedPom, answerHit
	case stored:
		return nil, answerMiss
	}
	return nil, answerUnknown
}

// isRepositoryRefusal reports a request that was refused for good: by the
// outbound-address rule, by a redirect the rule does not follow, or by the
// repository itself (401, 403).
func isRepositoryRefusal(err error) bool {
	if errors.Is(err, platform.ErrPrivateAddress) || errors.Is(err, platform.ErrLinkLocalAddress) ||
		errors.Is(err, platform.ErrRedirectTargetUserinfo) || errors.Is(err, errRegistryRedirectScheme) ||
		errors.Is(err, errRegistryRedirectDistrusted) {
		return true
	}
	var se *registryStatusError
	return errors.As(err, &se) && (se.status == http.StatusUnauthorized || se.status == http.StatusForbidden)
}

func decodeStoredPom(extract []byte) *manifest.RemotePom {
	if len(extract) == 0 {
		return nil
	}
	var pom manifest.RemotePom
	if err := json.Unmarshal(extract, &pom); err != nil || pom.Artifact == "" {
		return nil
	}
	return &pom
}

func hostOf(rawURL string) string { return hostguard.OfURL(rawURL) }

// logSummary writes the one line an analysis says about remote poms, when
// it asked for any.
func (s *mavenPomSource) logSummary(repoID int64) {
	if s.declaredNotAsked > 0 {
		// "Consulted": with collection.declared_repository_fetch off none
		// is asked, and the cap still bounds how many are looked up in
		// the store.
		s.logger.Warn("maven: a build declares more repositories than one lookup consults; the rest were neither asked nor looked up in the store, and versions that depend on them are not known",
			"repo_id", repoID, "consulted_per_lookup", maxDeclaredRepositoriesAsked, "left_unconsulted", s.declaredNotAsked)
	}
	if s.budgetCut > 0 {
		s.logger.Warn("maven: this analysis waited on the repositories its build files declare for as long as it may; the rest were not asked, and versions that depend on them are not known",
			"repo_id", repoID, "waited", s.declaredSpent.Round(time.Second).String(), "budget", declaredFetchBudget.String(), "requests_not_made", s.budgetCut)
	}
	if s.fetched+s.served+s.missed+s.failed+s.staleServed+s.refused+s.unaskable == 0 {
		return
	}
	s.logger.Info("maven: parent and BOM poms outside the clone",
		"repo_id", repoID, "fetched", s.fetched, "served_from_store", s.served,
		"not_on_a_repository", s.missed, "lookups_failed", s.failed, "stored_answer_served_after_a_failed_refresh", s.staleServed,
		"refused", s.refused, "coordinates_not_askable", s.unaskable, "declared_repositories_fetched", declaredRepositoryFetch.Load())
}

// errRepositoryDistrusted: every Maven repository that could be asked is on
// the distrusted list. Not "not on a registry": nothing was asked.
var errRepositoryDistrusted = fmt.Errorf("every configured Maven repository is distrusted: %w", errRegistryCannotBeAsked)

type distrustedHostsKey struct{}

// withDistrustedHosts carries the analysis's snapshot of the distrusted list
// to the registry lookups made under ctx.
func withDistrustedHosts(ctx context.Context, list hostguard.Snapshot) context.Context {
	return context.WithValue(ctx, distrustedHostsKey{}, list)
}

// distrustedHostsFrom is that snapshot, or nil (which distrusts nothing)
// when none was installed.
func distrustedHostsFrom(ctx context.Context) hostguard.Distrusted {
	if list, ok := ctx.Value(distrustedHostsKey{}).(hostguard.Snapshot); ok {
		return list
	}
	return nil
}

// usableMavenRepositories is the EFFECTIVE list of Maven repositories for a
// lookup under ctx: the configured ones minus the distrusted ones, in order.
func usableMavenRepositories(ctx context.Context) []string {
	return hostguard.Allowed(distrustedHostsFrom(ctx), mavenRepositoryBases(), hostOf)
}
