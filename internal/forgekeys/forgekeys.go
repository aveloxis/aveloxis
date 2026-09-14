// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package forgekeys decides which forge API keys each key pool holds — the
// ONE resolver used at startup (cmd/aveloxis buildForgeClients), by the live
// reload in running processes, by add-key and by the API-keys admin page
// (v0.30.0 Phase C; SR-17). A key belongs to exactly one pool: GitHub's, or
// the GitLab instance that issued it.
package forgekeys

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// Source is where a key came from. The resolver's precedence is the
// declaration order: config, then database, then Augur.
type Source string

const (
	SourceConfig   Source = "config"
	SourceDatabase Source = "database"
	SourceAugur    Source = "augur"
)

// Key is one token with its source; OAuthID is set for database rows.
type Key struct {
	Token   string
	Source  Source
	OAuthID int64
}

// Tokens returns keys' tokens in order.
func Tokens(keys []Key) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k.Token)
	}
	return out
}

// Reasons a stored key is not loaded.
const (
	// ReasonOrphan: its instance tag names no configured GitLab instance.
	ReasonOrphan = "orphan"
	// ReasonConflict: the same token is a config key of another instance.
	ReasonConflict = "conflict"
)

// NotLoaded is a stored (database or Augur) key the resolver refused. It
// never carries the token.
type NotLoaded struct {
	OAuthID  int64  `json:"oauth_id,omitempty"`
	KeyID    string `json:"key_id"`
	Masked   string `json:"masked"`
	Platform string `json:"platform"`
	Tag      string `json:"instance_url"`
	Source   Source `json:"source"`
	Reason   string `json:"reason"`
	Detail   string `json:"detail"`
}

// Stored is one platform's stored keys: aveloxis_ops.worker_oauth rows and,
// when this process decided to fall back to Augur, Augur's tokens.
type Stored struct {
	Database []db.StoredAPIKey
	Augur    []string
}

// GitHubKeys is GitHub's pool: config tokens, then stored, then Augur, each
// token once (the first source wins).
func GitHubKeys(configTokens []string, stored Stored) []Key {
	seen := map[string]bool{}
	var out []Key
	add := func(k Key) {
		if k.Token == "" || seen[k.Token] {
			return
		}
		seen[k.Token] = true
		out = append(out, k)
	}
	for _, tok := range configTokens {
		add(Key{Token: tok, Source: SourceConfig})
	}
	for _, row := range stored.Database {
		add(Key{Token: row.Token, Source: SourceDatabase, OAuthID: row.OAuthID})
	}
	for _, tok := range stored.Augur {
		add(Key{Token: tok, Source: SourceAugur})
	}
	return out
}

// Partition is PartitionGitLabTokens' detail: the keys (with sources) each
// instance loads, how many stored keys name an unconfigured instance per
// tag, and every stored key refused with its reason.
type Partition struct {
	Keys      map[string][]Key
	Orphans   map[string]int
	NotLoaded []NotLoaded
}

// PartitionGitLabTokens assigns every GitLab token to exactly one configured
// instance. pools maps each configured instance's web base to its tokens
// (every instance has an entry, empty when it has no keys); part carries the
// same assignment with sources, plus what was refused.
//
//   - Config keys load into their instance entry. A token in two instances'
//     entries is an error naming both: the operator owns that file, and
//     loading it into either pool would send one instance's credential to
//     the other.
//   - Stored keys load into the instance their tag names (InstanceForTag);
//     "" is the main instance, and every Augur token is a main-instance key.
//     A tag no configured instance has is an orphan, never loaded.
//   - A stored token already owned by ANOTHER instance (a config key) is not
//     loaded and is reported as a conflict — config wins — never an error,
//     so a stored row can neither take a working config key out of service
//     nor stop startup or a reload.
//   - A token repeated within one instance is kept once.
func PartitionGitLabTokens(instances []config.GitLabInstance, stored Stored) (pools map[string][]string, part Partition, err error) {
	pools = make(map[string][]string, len(instances))
	part = Partition{Keys: make(map[string][]Key, len(instances)), Orphans: map[string]int{}}
	owner := map[string]string{} // token → web base

	for _, in := range instances {
		pools[in.WebBase] = nil
		part.Keys[in.WebBase] = nil
	}
	for _, in := range instances {
		for _, tok := range in.APIKeys {
			if tok == "" {
				continue
			}
			if prev, ok := owner[tok]; ok {
				if prev != in.WebBase {
					return nil, Partition{}, fmt.Errorf("a GitLab API key is configured for two instances (%s and %s) — a key belongs to the one instance that issued it; remove it from one of them", prev, in.WebBase)
				}
				continue
			}
			owner[tok] = in.WebBase
			pools[in.WebBase] = append(pools[in.WebBase], tok)
			part.Keys[in.WebBase] = append(part.Keys[in.WebBase], Key{Token: tok, Source: SourceConfig})
		}
	}

	type storedKey struct {
		Key
		tag string
	}
	var rows []storedKey
	for _, r := range stored.Database {
		rows = append(rows, storedKey{Key{Token: r.Token, Source: SourceDatabase, OAuthID: r.OAuthID}, r.InstanceURL})
	}
	// Deterministic pool order and report text: by tag, then oauth_id.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].tag < rows[j].tag })
	for _, tok := range stored.Augur {
		rows = append(rows, storedKey{Key{Token: tok, Source: SourceAugur}, ""})
	}
	for _, r := range rows {
		if r.Token == "" {
			continue
		}
		base, ok := InstanceForTag(instances, r.tag)
		if !ok {
			part.Orphans[r.tag]++
			part.NotLoaded = append(part.NotLoaded, notLoaded(r.Key, "gitlab", r.tag, ReasonOrphan,
				"no configured GitLab instance has this web URL — add it to gitlab.instances, or remove the key and add it again for a configured instance"))
			continue
		}
		if prev, taken := owner[r.Token]; taken {
			if prev != base {
				part.NotLoaded = append(part.NotLoaded, notLoaded(r.Key, "gitlab", r.tag, ReasonConflict,
					fmt.Sprintf("the same token is a key of %s in the config file — a key belongs to one instance; remove this stored copy", prev)))
			}
			continue
		}
		owner[r.Token] = base
		pools[base] = append(pools[base], r.Token)
		part.Keys[base] = append(part.Keys[base], r.Key)
	}
	return pools, part, nil
}

func notLoaded(k Key, platformName, tag, reason, detail string) NotLoaded {
	return NotLoaded{
		OAuthID: k.OAuthID, KeyID: platform.KeyID(k.Token), Masked: platform.MaskToken(k.Token),
		Platform: platformName, Tag: tag, Source: k.Source, Reason: reason, Detail: detail,
	}
}

// InstanceForTag is the ONE mapping from a stored key's instance tag to the
// configured instance it loads into: "" is the main instance; any other tag
// is normalized and compared scheme-less, so a key stored for http://host
// belongs to the instance now configured as https://host. ok is false for a
// tag no configured instance has.
func InstanceForTag(instances []config.GitLabInstance, tag string) (webBase string, ok bool) {
	if tag == "" {
		for _, in := range instances {
			if in.Primary {
				return in.WebBase, true
			}
		}
		return "", false
	}
	nb, err := model.NormalizeInstanceWebBase(tag)
	if err != nil {
		return "", false
	}
	for _, in := range instances {
		if model.SchemelessWebBase(in.WebBase) == model.SchemelessWebBase(nb) {
			return in.WebBase, true
		}
	}
	return "", false
}

// StoredLoader reads one platform's stored keys.
type StoredLoader interface {
	Load(ctx context.Context, platform string) (Stored, error)
}

// Loader reads stored keys for a process. Whether Augur's keys are included
// is decided ONCE per platform — on the first SUCCESSFUL read of that
// platform's stored keys, by the pre-Phase C rule (use_augur_keys and no
// stored key) — and held for the life of the process. A live reload never
// re-decides, so adding the first key through the admin page cannot swap a
// running pool from Augur's keys to the new one, and removing the last
// cannot swap back. A read error decides nothing (SR-5): the next successful
// read decides (review of Phase C, finding 6). In practice the first read is
// buildForgeClients at startup.
type Loader struct {
	useAugur   bool
	logger     *slog.Logger
	readStored func(ctx context.Context, platform string) ([]db.StoredAPIKey, error)
	readAugur  func(ctx context.Context, platform string) ([]string, error)

	mu      sync.Mutex
	decided map[string]bool
	augur   map[string]bool
}

// NewLoader returns the stored-key loader for pool.
func NewLoader(pool *pgxpool.Pool, useAugurKeys bool, logger *slog.Logger) *Loader {
	return newLoaderWith(useAugurKeys, logger,
		func(ctx context.Context, p string) ([]db.StoredAPIKey, error) {
			return db.LoadStoredAPIKeys(ctx, pool, p)
		},
		func(ctx context.Context, p string) ([]string, error) { return db.LoadAugurAPIKeys(ctx, pool, p) })
}

func newLoaderWith(useAugur bool, logger *slog.Logger,
	readStored func(context.Context, string) ([]db.StoredAPIKey, error),
	readAugur func(context.Context, string) ([]string, error)) *Loader {
	return &Loader{useAugur: useAugur, logger: logger, readStored: readStored, readAugur: readAugur,
		decided: map[string]bool{}, augur: map[string]bool{}}
}

// AugurFallback reports whether this process includes Augur's keys for
// platformName (false until decided).
func (l *Loader) AugurFallback(platformName string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.augur[platformName]
}

// Load reads platformName's stored keys (and Augur's, per the decision). Any
// read error is returned (SR-5): a caller must not treat it as "no keys".
func (l *Loader) Load(ctx context.Context, platformName string) (Stored, error) {
	rows, err := l.readStored(ctx, platformName)
	if err != nil {
		return Stored{}, err
	}
	l.mu.Lock()
	if !l.decided[platformName] {
		l.decided[platformName] = true
		l.augur[platformName] = l.useAugur && len(rows) == 0
		if l.augur[platformName] {
			l.logger.Info("no stored API keys for this platform — Augur's keys are used for the life of this process", "platform", platformName)
		}
	}
	useAugur := l.augur[platformName]
	l.mu.Unlock()
	out := Stored{Database: rows}
	if useAugur {
		toks, err := l.readAugur(ctx, platformName)
		if err != nil {
			return Stored{}, err
		}
		out.Augur = toks
	}
	return out, nil
}

// errNoLoader guards a Maintainer built without a loader.
var errNoLoader = errors.New("forgekeys: no stored-key loader")
