// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"sort"
	"testing"
)

// D2 (0.29.122, summary/63 §8): the API's re-warm list survives a restart.
// Save applies a difference in one transaction — upserts (a repeated key
// takes the new cost) and deletes — and Load reads the set back.
func TestPageCacheReplayRoundTrip(t *testing.T) {
	ctx, store := caseConnect(t)
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.page_cache_replay WHERE uri LIKE '/_avreplay/%'`)
	})
	a := PageCacheReplayKey{RepoID: 1, URI: "/_avreplay/a"}
	b := PageCacheReplayKey{RepoID: 2, URI: "/_avreplay/b?x=1"}
	if err := store.SavePageCacheReplay(ctx, nil, nil); err != nil {
		t.Fatalf("an empty difference is a no-op, got %v", err)
	}
	if err := store.SavePageCacheReplay(ctx, []PageCacheReplayRow{{Key: a, CostMS: 5}, {Key: b, CostMS: 7}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.SavePageCacheReplay(ctx, []PageCacheReplayRow{{Key: b, CostMS: 70}}, []PageCacheReplayKey{a}); err != nil {
		t.Fatal(err)
	}
	rows, err := store.LoadPageCacheReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mine []PageCacheReplayRow
	for _, r := range rows {
		if len(r.Key.URI) > 11 && r.Key.URI[:11] == "/_avreplay/" {
			mine = append(mine, r)
		}
	}
	sort.Slice(mine, func(i, j int) bool { return mine[i].Key.URI < mine[j].Key.URI })
	if len(mine) != 1 || mine[0].Key != b || mine[0].CostMS != 70 {
		t.Errorf("loaded %+v, want only %+v at cost 70", mine, b)
	}
}
