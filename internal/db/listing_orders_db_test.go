// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// The three listings that are built from an allowlist of sort keys (the
// monitor's queue page, the repository listing of a group or a collection,
// the package leaderboard), each tested for EVERY key of its own allowlist
// and every variant of its statement (operator decision 2026-11-21, after
// review round 9: a queue sort key pointed at the wrong column passed the
// whole suite, and the list of statement texts never sent answered it with
// "delete its line". The list is gone; these tests stand where it stood).
//
// The rows are seeded so that every key gives a DIFFERENT order: a key
// that reads another key's column returns that key's order and fails.
// Each test iterates the real allowlist, so a key added to it is tested
// without an edit here, and fails until this file says what it orders by.

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// listingRows are six repositories with a queue row and two repo_info
// rows each. Rows 0-3 hold, for every attribute, a different arrangement
// of the ranks 1-4; rows 4 and 5 are row 0 again in every attribute (a tie
// on every key), one with a smaller repository id than row 0 and one with
// a larger, and their queue rows are stored out of id order.
type listingRows struct {
	tag  string
	ids  []int64          // by row
	rank map[string][]int // attribute -> rank by row
	// outside is a repository of another owner, in the queue, in no group
	// of this fixture.
	outside int64
}

// listingAttrs are the seeded attributes, in the order their arrangements
// are dealt.
var listingAttrs = []string{"owner", "status", "priority", "due", "last_run", "issues", "prs", "commits", "meta_issues", "meta_prs", "meta_commits", "activity"}

// listingNullAt is the rank at which a nullable attribute is NULL.
const listingNullAt = 4

var listingNullable = map[string]bool{"last_run": true, "activity": true}

func permutationsOf4() [][]int {
	var out [][]int
	var walk func(have []int)
	walk = func(have []int) {
		if len(have) == 4 {
			out = append(out, slices.Clone(have))
			return
		}
		for v := 1; v <= 4; v++ {
			if !slices.Contains(have, v) {
				walk(append(have, v))
			}
		}
	}
	walk(nil)
	return out
}

func seedListingRows(t *testing.T, store *PostgresStore) *listingRows {
	t.Helper()
	ctx := context.Background()
	f := &listingRows{tag: strings.ReplaceAll(pathsTag("listing"), "-", "x"), rank: map[string][]int{}}
	// Each attribute takes the first arrangement whose ascending order no
	// attribute before it has ascending, and likewise descending (with that
	// attribute's NULLs and classes): so every key of every listing orders
	// these rows differently from every other key in the same direction,
	// and differently in its two directions. Not the
	// arrangement 1,2,3,4, which is the order of the ids. The ids stand in
	// their final order here; the real ones are stored below.
	f.ids = []int64{2, 3, 4, 5, 1, 6}
	taken := map[string]bool{}
	for _, attr := range listingAttrs {
		for _, p := range permutationsOf4()[1:] {
			f.rank[attr] = []int{p[0], p[1], p[2], p[3], p[0], p[0]}
			up, down := "asc"+fmt.Sprint(f.order(attr, false)), "desc"+fmt.Sprint(f.order(attr, true))
			if !taken[up] && !taken[down] && up[3:] != down[4:] {
				taken[up], taken[down] = true, true
				break
			}
			delete(f.rank, attr)
		}
		if f.rank[attr] == nil {
			t.Fatalf("no arrangement is left for %s", attr)
		}
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var all []int64
	t.Cleanup(func() {
		c := context.Background()
		for _, table := range []string{"aveloxis_data.repo_info", "aveloxis_ops.user_repo_stars", "aveloxis_ops.user_repos", "aveloxis_ops.collection_queue", "aveloxis_data.repos"} {
			cleanupExecRetry(c, store, `DELETE FROM `+table+` WHERE repo_id = ANY($1)`, all)
		}
	})
	repo := func(owner, git string) int64 {
		var id int64
		if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id) VALUES ($1, $2, 'r', 1) RETURNING repo_id`,
			"https://github.com/"+git+"/r", owner).Scan(&id); err != nil {
			t.Fatalf("seed repository: %v", err)
		}
		all = append(all, id)
		return id
	}
	owner := func(row int) string { return fmt.Sprintf("%sowner%d", f.tag, f.rank["owner"][row]) }
	// Ids ascend in this order: a twin, rows 0-3, the other twin.
	f.ids = make([]int64, 6)
	for _, row := range []int{4, 0, 1, 2, 3, 5} {
		f.ids[row] = repo(owner(row), fmt.Sprintf("%sgit%d", f.tag, row))
	}
	f.outside = repo("zzoutside"+f.tag[len(f.tag)-6:]+"other", "zzoutside"+f.tag)
	status := []string{"", "collecting", "queued", "failed", "failed"}
	for _, row := range []int{3, 5, 2, 0, 1, 4} {
		r := func(attr string) int { return f.rank[attr][row] }
		var lastRun any
		if r("last_run") != listingNullAt {
			lastRun = base.Add(time.Duration(r("last_run")) * 24 * time.Hour)
		}
		var activity any
		if r("activity") != listingNullAt {
			activity = base.Add(time.Duration(r("activity")) * 7 * 24 * time.Hour)
		}
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_collected, last_issues, last_prs, last_commits)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			f.ids[row], r("priority")*10, status[r("status")], base.Add(time.Duration(r("due"))*time.Hour), lastRun, r("issues")*3, r("prs")*5, r("commits")*7)
		// The latest repo_info row is the one read; an older one holds the
		// arrangement reversed.
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_info (repo_id, issues_count, pr_count, commit_count, last_updated, data_collection_date) VALUES
			($1, $2, $3, $4, $5, NOW() - INTERVAL '1 day'), ($1, $6, $7, $8, $9, NOW() - INTERVAL '9 days')`,
			f.ids[row], r("meta_issues")*11, r("meta_prs")*13, r("meta_commits")*17, activity,
			(5-r("meta_issues"))*11, (5-r("meta_prs"))*13, (5-r("meta_commits"))*17, base.Add(time.Duration(5-r("activity"))*7*24*time.Hour))
	}
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at) VALUES ($1, 1, 'queued', $2)`, f.outside, base)
	return f
}

// order is the fixture's ids as a statement that orders by attr must return
// them: by the attribute's rank (status by its three classes), descending
// when asked, a NULL last in either direction, and equal rows by id.
func (f *listingRows) order(attr string, desc bool) []int64 {
	ranks, ok := f.rank[attr]
	if !ok {
		panic("no seeded attribute " + attr)
	}
	value := func(row int) (int, bool) {
		v := ranks[row]
		if attr == "status" && v > 3 {
			v = 3
		}
		return v, listingNullable[attr] && ranks[row] == listingNullAt
	}
	rows := []int{0, 1, 2, 3, 4, 5}
	sort.SliceStable(rows, func(i, k int) bool {
		a, aNull := value(rows[i])
		b, bNull := value(rows[k])
		switch {
		case aNull != bNull:
			return bNull
		case !aNull && a != b:
			return (a < b) != desc
		}
		return f.ids[rows[i]] < f.ids[rows[k]]
	})
	out := make([]int64, len(rows))
	for i, row := range rows {
		out[i] = f.ids[row]
	}
	return out
}

// mine keeps, in order, the ids that are the fixture's.
func (f *listingRows) mine(ids []int64) []int64 {
	var out []int64
	for _, id := range ids {
		if slices.Contains(f.ids, id) {
			out = append(out, id)
		}
	}
	return out
}

// distinctOrders fails the test unless every key's order differs from every
// other key's in the same direction (the keys are "name asc", "name
// desc"): the premise of "a key that reads another key's column fails".
func distinctOrders(t *testing.T, orders map[string][]int64) {
	t.Helper()
	seen := map[string]string{}
	for _, key := range slices.Sorted(maps.Keys(orders)) {
		text := key[strings.LastIndex(key, " "):] + fmt.Sprint(orders[key])
		if other, dup := seen[text]; dup {
			t.Fatalf("the fixture gives %s and %s the same order %s: a key reading the other's column would pass", other, key, text)
		}
		seen[text] = key
	}
}

// queueSortReads says which seeded attribute each key of queueSorts orders
// by. A key of queueSorts that is not here fails the test.
var queueSortReads = map[string]string{
	"repo": "owner", "status": "status", "priority": "priority", "due": "due", "last_run": "last_run",
	"issues": "issues", "prs": "prs", "commits": "commits",
	"meta_issues": "meta_issues", "meta_prs": "meta_prs", "meta_commits": "meta_commits",
}

func TestTheQueuePageIsOrderedByEveryKeyOfItsAllowlist(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	f := seedListingRows(t, store)
	if len(queueSortReads) != len(queueSorts) {
		t.Errorf("queueSorts has %d keys and this test knows %d", len(queueSorts), len(queueSortReads))
	}
	orders := map[string][]int64{}
	for key := range queueSorts {
		attr, known := queueSortReads[key]
		if !known {
			t.Fatalf("queueSorts has the key %q and this test does not say what it orders by", key)
		}
		orders[key+" asc"], orders[key+" desc"] = f.order(attr, false), f.order(attr, true)
	}
	distinctOrders(t, orders)

	page := func(limit, offset int, search, key, dir string) ([]int64, int) {
		t.Helper()
		jobs, total, err := store.ListQueuePage(ctx, limit, offset, search, key, dir)
		if err != nil {
			t.Fatalf("ListQueuePage(search %q, sort %q %s): %v", search, key, dir, err)
		}
		ids := make([]int64, len(jobs))
		for i, j := range jobs {
			ids[i] = j.RepoID
		}
		return ids, total
	}
	for _, search := range []string{"", strings.ToUpper(f.tag) + "OWNER"} {
		for key := range queueSorts {
			for _, dir := range []string{"asc", "desc"} {
				ids, total := page(100000, 0, search, key, dir)
				if want := orders[key+" "+dir]; !reflect.DeepEqual(f.mine(ids), want) {
					t.Errorf("search %q, sort %s %s: this test's rows came as %v, want %v", search, key, dir, f.mine(ids), want)
				}
				// The search keeps this fixture's six and leaves out the
				// repository of another owner; without it that one is listed.
				if outside := slices.Contains(ids, f.outside); (search != "") == outside || (search != "" && (len(ids) != 6 || total != 6)) || total < len(ids) {
					t.Errorf("search %q, sort %s %s: %d rows of a total %d, the other owner's repository listed: %v", search, key, dir, len(ids), total, outside)
				}
			}
		}
		// The default order, which a key outside the allowlist also gets,
		// whatever direction is asked: collecting first, then queued, then
		// the rest; by priority, then by the time due, then by id.
		byDefault := []int{0, 1, 2, 3, 4, 5}
		sort.SliceStable(byDefault, func(i, k int) bool {
			a, b := byDefault[i], byDefault[k]
			for _, attr := range []string{"status", "priority", "due"} {
				x, y := f.rank[attr][a], f.rank[attr][b]
				if attr == "status" {
					x, y = min(x, 3), min(y, 3)
				}
				if x != y {
					return x < y
				}
			}
			return f.ids[a] < f.ids[b]
		})
		want := make([]int64, 6)
		for i, row := range byDefault {
			want[i] = f.ids[row]
		}
		for _, key := range []string{"", "no such key", "repo; DROP TABLE x"} {
			for _, dir := range []string{"asc", "desc"} {
				if ids, _ := page(100000, 0, search, key, dir); !reflect.DeepEqual(f.mine(ids), want) {
					t.Errorf("search %q, sort %q %s (the default order): this test's rows came as %v, want %v", search, key, dir, f.mine(ids), want)
				}
			}
		}
	}
	// A page of the sorted list: two rows after the first three.
	search := f.tag + "owner"
	if ids, total := page(2, 3, search, "priority", "desc"); !reflect.DeepEqual(ids, orders["priority desc"][3:5]) || total != 6 {
		t.Errorf("two rows after three, by priority descending: %v of %d; want %v of 6", ids, total, orders["priority desc"][3:5])
	}
	// The default order's third key, the time due: two rows of one status
	// and one priority, the one with the larger id due first.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.collection_queue SET status = 'queued', priority = 55,
		due_at = CASE WHEN repo_id = $1 THEN TIMESTAMPTZ '2026-02-02' ELSE TIMESTAMPTZ '2026-02-01' END WHERE repo_id IN ($1, $2)`, f.ids[1], f.ids[2])
	if ids, _ := page(100000, 0, search, "", ""); slices.Index(ids, f.ids[2]) != slices.Index(ids, f.ids[1])-1 {
		t.Errorf("the default order with two rows of one status and priority: %v; want %d (due first) just before %d", ids, f.ids[2], f.ids[1])
	}
	// The search is for the text as typed: its wildcard characters stand
	// for themselves.
	if ids, total := page(100, 0, "%", "", ""); len(ids) != 0 || total != 0 {
		t.Errorf("a search for %%: %d rows of %d; want none (no repository has a percent sign in its name)", len(ids), total)
	}
}

// collectionRepoSortReads is queueSortReads for collectionRepoSorts.
var collectionRepoSortReads = map[string]string{
	"name": "owner", "issues": "issues", "prs": "prs", "commits": "commits", "collected": "last_run", "activity": "activity",
}

func TestTheRepositoryListingIsOrderedByEveryKeyOfItsAllowlist(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	f := seedListingRows(t, store)
	reader := tokenTestUser(t, store, ctx, f.tag+"-reader")
	group, err := store.CreateUserGroup(ctx, reader, f.tag+"-group")
	if err != nil {
		t.Fatal(err)
	}
	// A second group of the same collection that holds two of the same
	// repositories again (a collection lists a repository once), and a
	// group of ANOTHER collection that holds the outside repository.
	second, err := store.CreateUserGroup(ctx, reader, f.tag+"-second")
	if err != nil {
		t.Fatal(err)
	}
	elsewhere, err := store.CreateUserGroup(ctx, reader, f.tag+"-elsewhere")
	if err != nil {
		t.Fatal(err)
	}
	collection, err := store.CreateCollection(ctx, f.tag+"-collection", "", 1, reader)
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.CreateCollection(ctx, f.tag+"-other-collection", "", 2, reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.collection_groups WHERE collection_id = ANY($1)`, []int64{collection, other})
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.collections WHERE collection_id = ANY($1)`, []int64{collection, other})
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.user_repos WHERE group_id = ANY($1)`, []int64{group, second, elsewhere})
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.user_groups WHERE group_id = ANY($1)`, []int64{group, second, elsewhere})
	})
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) SELECT $1, unnest($2::bigint[])`, group, f.ids)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) SELECT $1, unnest($2::bigint[])`, second, f.ids[1:3])
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, elsewhere, f.outside)
	// (Stored directly: AddGroupToCollection takes an administrator's groups,
	// which is not what is tested here.)
	for g, c := range map[int64]int64{group: collection, second: collection, elsewhere: other} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_groups (collection_id, group_id) VALUES ($1, $2)`, c, g)
	}
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.user_repo_stars (user_id, repo_id) VALUES ($1, $2)`, reader, f.ids[2])
	// Another user's star, on another repository, is not the reader's.
	someoneElse := tokenTestUser(t, store, ctx, f.tag+"-someone-else")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.user_repo_stars (user_id, repo_id) VALUES ($1, $2)`, someoneElse, f.ids[3])

	if len(collectionRepoSortReads) != len(collectionRepoSorts) {
		t.Errorf("collectionRepoSorts has %d keys and this test knows %d", len(collectionRepoSorts), len(collectionRepoSortReads))
	}
	orders := map[string][]int64{}
	for key := range collectionRepoSorts {
		attr, known := collectionRepoSortReads[key]
		if !known {
			t.Fatalf("collectionRepoSorts has the key %q and this test does not say what it orders by", key)
		}
		orders[key+" asc"], orders[key+" desc"] = f.order(attr, false), f.order(attr, true)
	}
	distinctOrders(t, orders)

	listings := map[string]func(page, size int, key, dir string) ([]PortalGroupRepo, int, error){
		"the group": func(page, size int, key, dir string) ([]PortalGroupRepo, int, error) {
			return store.GetPortalGroupRepos(ctx, reader, group, page, size, key, dir)
		},
		"the collection": func(page, size int, key, dir string) ([]PortalGroupRepo, int, error) {
			return store.GetCollectionRepos(ctx, collection, reader, page, size, key, dir)
		},
	}
	for name, list := range listings {
		ids := func(page, size int, key, dir string) []int64 {
			t.Helper()
			rows, total, err := list(page, size, key, dir)
			if err != nil {
				t.Fatalf("%s, sort %q %s: %v", name, key, dir, err)
			}
			if total != 6 {
				t.Errorf("%s, sort %q %s: a total of %d, want the 6 repositories (each once, the outside one not among them)", name, key, dir, total)
			}
			out := make([]int64, len(rows))
			for i, r := range rows {
				out[i] = r.RepoID
				if r.Starred != (r.RepoID == f.ids[2]) {
					t.Errorf("%s: repository %d starred %v; the reader starred %d alone", name, r.RepoID, r.Starred, f.ids[2])
				}
			}
			return out
		}
		for key := range collectionRepoSorts {
			for _, dir := range []string{"asc", "desc"} {
				if got, want := ids(1, 50, key, dir), orders[key+" "+dir]; !reflect.DeepEqual(got, want) {
					t.Errorf("%s, sort %s %s: %v, want %v", name, key, dir, got, want)
				}
			}
		}
		// A key outside the allowlist is the name's order, and any direction
		// but "desc" is ascending.
		if got := ids(1, 50, "no such key", "sideways"); !reflect.DeepEqual(got, orders["name asc"]) {
			t.Errorf("%s, a sort outside the allowlist: %v, want the name's ascending order %v", name, got, orders["name asc"])
		}
		if got := ids(2, 2, "commits", "desc"); !reflect.DeepEqual(got, orders["commits desc"][2:4]) {
			t.Errorf("%s, the second page of two by commits descending: %v, want %v", name, got, orders["commits desc"][2:4])
		}
	}
	// What a row carries: the queue's last counts, and the LATEST repo_info
	// row's activity.
	rows, _, err := store.GetPortalGroupRepos(ctx, reader, group, 1, 50, "name", "asc")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		row := slices.Index(f.ids, r.RepoID)
		wantActivity := f.rank["activity"][row] != listingNullAt
		if r.Issues != int64(f.rank["issues"][row]*3) || r.PRs != int64(f.rank["prs"][row]*5) || r.Commits != int64(f.rank["commits"][row]*7) || (r.LastActivity != nil) != wantActivity {
			t.Errorf("row %d is listed as %+v; want issues %d, prs %d, commits %d, an activity time %v", row, r, f.rank["issues"][row]*3, f.rank["prs"][row]*5, f.rank["commits"][row]*7, wantActivity)
		}
	}
}

// exposureLess is the documented order of each key of PackageExposureSorts,
// over the rows the listing returned. A key that is not here fails the test.
var exposureLess = map[string]func(a, b *PackageExposure) int{
	"repos":      func(a, b *PackageExposure) int { return exposureThen(b.CohortRepos-a.CohortRepos, 0, a, b) },
	"unresolved": func(a, b *PackageExposure) int { return exposureThen(b.ReposUnresolved-a.ReposUnresolved, 0, a, b) },
	"cvss": func(a, b *PackageExposure) int {
		return exposureThen(exposureNullsLast(a.MaxCVSS, b.MaxCVSS, a.MaxCVSS == 0, b.MaxCVSS == 0), b.CohortRepos-a.CohortRepos, a, b)
	},
	"transitive": func(a, b *PackageExposure) int {
		return exposureThen(exposureNullsLast(a.PctTransitive, b.PctTransitive, false, false), b.CohortRepos-a.CohortRepos, a, b)
	},
	"versions": func(a, b *PackageExposure) int {
		return exposureThen(b.DistinctVersionsInUse-a.DistinctVersionsInUse, b.CohortRepos-a.CohortRepos, a, b)
	},
	"days": func(a, b *PackageExposure) int {
		var x, y float64
		if a.MedianDaysOpen != nil {
			x = *a.MedianDaysOpen
		}
		if b.MedianDaysOpen != nil {
			y = *b.MedianDaysOpen
		}
		return exposureThen(exposureNullsLast(x, y, a.MedianDaysOpen == nil, b.MedianDaysOpen == nil), b.CohortRepos-a.CohortRepos, a, b)
	},
	"name": func(a, b *PackageExposure) int {
		if c := strings.Compare(a.PackageName, b.PackageName); c != 0 {
			return c
		}
		return strings.Compare(a.Ecosystem, b.Ecosystem)
	},
}

// exposureNullsLast: the larger first, a NULL last.
func exposureNullsLast(a, b float64, aNull, bNull bool) int {
	switch {
	case aNull && bNull:
		return 0
	case aNull:
		return 1
	case bNull:
		return -1
	case a > b:
		return -1
	case a < b:
		return 1
	}
	return 0
}

func exposureThen(first, second int, a, b *PackageExposure) int {
	if first != 0 {
		return first
	}
	if second != 0 {
		return second
	}
	return strings.Compare(a.PackageName, b.PackageName)
}

func TestThePackageLeaderboardIsOrderedByEveryKeyOfItsAllowlist(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := strings.ReplaceAll(pathsTag("board"), "-", "x")
	var repos []int64
	for i := 0; i < 5; i++ {
		id, _, _ := pathsRepo(t, store, fmt.Sprintf("board-%d", i), "aveloxis_data.repo_deps_vulnerabilities")
		repos = append(repos, id)
	}
	cohort, outside := repos[:4], repos[4]
	// One finding. cvss 0 stores NULL; days is how long ago it was first
	// detected; version "" is a purl with no version.
	n := 0
	finding := func(repo int64, eco, pkg, kind, version string, cvss float64, days int, resolved bool) {
		n++
		purl := "pkg:" + eco + "/" + pkg
		if version != "" {
			purl += "@" + version
		}
		var score, at any
		if cvss != 0 {
			score = cvss
		}
		if resolved {
			at = time.Now()
		}
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_deps_vulnerabilities
			(repo_id, vuln_id, package_name, package_purl, ecosystem, severity, cvss_score, dependency_kind, first_detected_at, last_seen_at, resolved_at, tool_source, data_source)
			VALUES ($1, $2, $3, $4, $5, 'HIGH', $6, $7, NOW() - make_interval(days => $8), NOW(), $9, 'aveloxis', 'test')`,
			repo, fmt.Sprintf("GHSA-%s-%d", tag, n), pkg, purl, eco, score, kind, days, at)
	}
	a, b, c, d, e := tag+"-a", tag+"-b", tag+"-c", tag+"-d", tag+"-e"
	// a: in four repositories, one unresolved; never transitive; one version.
	finding(cohort[0], "npm", a, "direct", "1.0.0", 3.0, 1, false)
	for _, r := range cohort[1:] {
		finding(r, "npm", a, "direct", "1.0.0", 3.0, 1, true)
	}
	// b: in three, all unresolved, all transitive, three versions.
	for i, r := range cohort[:3] {
		finding(r, "npm", b, "transitive", fmt.Sprintf("2.%d.0", i), 5.0, 10, false)
	}
	// c: in two, both unresolved, half transitive, one version, the worst score.
	finding(cohort[0], "npm", c, "transitive", "3.0.0", 9.0, 30, false)
	finding(cohort[1], "npm", c, "direct", "3.0.0", 9.0, 30, false)
	// d: in one, unresolved, transitive, no version, no score, the oldest.
	finding(cohort[3], "npm", d, "transitive", "", 0, 50, false)
	// e: in two, both resolved (no days open), c's score and c's count.
	finding(cohort[2], "npm", e, "direct", "", 9.0, 7, true)
	finding(cohort[3], "npm", e, "direct", "", 9.0, 7, true)
	// a again in another ecosystem: the name's order ends on the ecosystem.
	finding(cohort[0], "pypi", a, "direct", "0.1.0", 1.0, 5, false)
	// A repository outside the cohort has a too: the fleet counts it, the
	// cohort does not.
	finding(outside, "npm", a, "direct", "1.0.0", 3.0, 1, true)

	if len(exposureLess) != len(PackageExposureSorts) {
		t.Errorf("PackageExposureSorts has %d keys and this test knows %d", len(PackageExposureSorts), len(exposureLess))
	}
	names := func(rows []*PackageExposure) []string {
		var out []string
		for _, r := range rows {
			if strings.HasPrefix(r.PackageName, tag) {
				out = append(out, strings.TrimPrefix(r.PackageName, tag+"-")+"/"+r.Ecosystem)
			}
		}
		return out
	}
	// The orders the seeded packages must come in, by hand, for the cohort
	// with no filter: what the comparison functions above must agree with,
	// and each different from every other.
	byHand := map[string][]string{
		"repos":      {"a/npm", "b/npm", "c/npm", "e/npm", "a/pypi", "d/npm"},
		"unresolved": {"b/npm", "c/npm", "a/npm", "a/pypi", "d/npm", "e/npm"},
		"cvss":       {"c/npm", "e/npm", "b/npm", "a/npm", "a/pypi", "d/npm"},
		"transitive": {"b/npm", "d/npm", "c/npm", "a/npm", "e/npm", "a/pypi"},
		"versions":   {"b/npm", "a/npm", "c/npm", "a/pypi", "e/npm", "d/npm"},
		"days":       {"d/npm", "c/npm", "b/npm", "a/pypi", "a/npm", "e/npm"},
		"name":       {"a/npm", "a/pypi", "b/npm", "c/npm", "d/npm", "e/npm"},
	}
	seen := map[string]string{}
	for key, order := range byHand {
		if other, dup := seen[fmt.Sprint(order)]; dup {
			t.Fatalf("the fixture gives %s and %s the same order", other, key)
		}
		seen[fmt.Sprint(order)] = key
	}

	check := func(what string, q PackageExposureQuery, wantLive bool, want map[string]bool, aRepos int) {
		t.Helper()
		for key := range PackageExposureSorts {
			less, known := exposureLess[key]
			if !known {
				t.Fatalf("PackageExposureSorts has the key %q and this test does not say what it orders by", key)
			}
			q.Sort, q.Limit, q.Offset = key, packageExposureMaxLimit, 0
			page, err := store.ListPackageExposure(ctx, q)
			if err != nil {
				t.Fatalf("%s, sort %s: %v", what, key, err)
			}
			if page.Live != wantLive {
				t.Errorf("%s, sort %s: computed live %v, want %v", what, key, page.Live, wantLive)
			}
			var mine []*PackageExposure
			for _, r := range page.Rows {
				if strings.HasPrefix(r.PackageName, tag) {
					mine = append(mine, r)
				}
			}
			got := names(mine)
			if listed := map[string]bool{}; true {
				for _, g := range got {
					listed[g] = true
				}
				if !reflect.DeepEqual(listed, want) {
					t.Errorf("%s, sort %s: lists %v, want exactly %v", what, key, got, want)
					continue
				}
			}
			if !slices.IsSortedFunc(mine, less) {
				t.Errorf("%s, sort %s: %v is not in that key's order", what, key, got)
			}
			if q.RepoIDs != nil && q.Ecosystem == "" && q.Search == "" && !reflect.DeepEqual(got, byHand[key]) {
				t.Errorf("%s, sort %s: %v, want %v", what, key, got, byHand[key])
			}
			for _, r := range mine {
				if r.PackageName == a && r.Ecosystem == "npm" && r.CohortRepos != aRepos {
					t.Errorf("%s, sort %s: package a is in %d repositories, want %d", what, key, r.CohortRepos, aRepos)
				}
			}
			if (q.RepoIDs != nil || q.Search != "") && (len(page.Rows) != len(want) || page.Total != len(want)) {
				t.Errorf("%s, sort %s: %d rows of a total %d, want %d of %d", what, key, len(page.Rows), page.Total, len(want), len(want))
			}
		}
	}
	all := map[string]bool{"a/npm": true, "a/pypi": true, "b/npm": true, "c/npm": true, "d/npm": true, "e/npm": true}
	npm := map[string]bool{"a/npm": true, "b/npm": true, "c/npm": true, "d/npm": true, "e/npm": true}
	variants := func(source string, repoIDs []int64, live bool, aRepos int) {
		t.Helper()
		check(source, PackageExposureQuery{RepoIDs: repoIDs}, live, all, aRepos)
		check(source+", one ecosystem", PackageExposureQuery{RepoIDs: repoIDs, Ecosystem: "npm"}, live, npm, aRepos)
		check(source+", a search", PackageExposureQuery{RepoIDs: repoIDs, Search: strings.ToUpper(tag)}, live, all, aRepos)
		check(source+", a search for one package", PackageExposureQuery{RepoIDs: repoIDs, Search: tag + "-B"}, live, map[string]bool{"b/npm": true}, aRepos)
		check(source+", one ecosystem and a search", PackageExposureQuery{RepoIDs: repoIDs, Ecosystem: "pypi", Search: tag + "-A"}, live, map[string]bool{"a/pypi": true}, aRepos)
	}
	variants("the cohort", cohort, true, 4)

	// A page of the cohort's list, and a page past its end (which counts
	// the set once more).
	page, err := store.ListPackageExposure(ctx, PackageExposureQuery{RepoIDs: cohort, Sort: "name", Limit: 2, Offset: 1})
	if err != nil || !reflect.DeepEqual(names(page.Rows), byHand["name"][1:3]) || page.Total != 6 {
		t.Errorf("two after the first by name: %v of %d (%v); want %v of 6", names(page.Rows), page.Total, err, byHand["name"][1:3])
	}
	// A page past the end counts the set once more, with the same filters:
	// exactly this fixture's packages for the cohort or a search for its
	// tag, and at least those for the fleet with neither.
	pastEnd := func(what string, repoIDs []int64) {
		t.Helper()
		for _, q := range []PackageExposureQuery{{}, {Ecosystem: "npm"}, {Search: tag}, {Ecosystem: "npm", Search: tag}} {
			q.RepoIDs, q.Sort, q.Limit, q.Offset = repoIDs, "repos", 2, 1_000_000
			want := 6
			if q.Ecosystem != "" {
				want = 5
			}
			exact := repoIDs != nil || q.Search != ""
			page, err := store.ListPackageExposure(ctx, q)
			if err != nil || len(page.Rows) != 0 || page.Total < want || (exact && page.Total != want) {
				t.Errorf("%s, a page past the end (ecosystem %q, search %q): %d rows of %d (%v); want none of %d (exactly: %v)", what, q.Ecosystem, q.Search, len(page.Rows), page.Total, err, want, exact)
			}
		}
	}
	pastEnd("the cohort", cohort)

	// The fleet, first with no view (computed live over every repository),
	// then from the view as a migrate builds it.
	const drop = `DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_exposure`
	mustExecRetry(ctx, t, store, drop)
	t.Cleanup(func() { cleanupExecRetry(context.Background(), store, drop) })
	variants("the fleet with no view", nil, true, 5)
	pastEnd("the fleet with no view", nil)
	mustExecRetry(ctx, t, store, `CREATE MATERIALIZED VIEW aveloxis_data.explorer_package_exposure AS `+PackageExposureMatviewSQL())
	variants("the fleet's view", nil, false, 5)
	pastEnd("the fleet's view", nil)
}
