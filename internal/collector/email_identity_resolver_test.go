// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"testing"
)

type fakeEmailDB struct {
	login string
	calls int
}

func (f *fakeEmailDB) FindLoginByEmail(_ context.Context, _ string) (string, error) {
	f.calls++
	return f.login, nil
}

type fakeSearchClient struct {
	searchLogin string
	searchID    int64
	commitLogin string
	commitID    int64
	searchCalls int
	commitCalls int
}

func (f *fakeSearchClient) SearchUserByEmail(_ context.Context, _ string) (string, int64, error) {
	f.searchCalls++
	return f.searchLogin, f.searchID, nil
}
func (f *fakeSearchClient) SearchCommitByAuthorEmail(_ context.Context, _ string) (string, int64, error) {
	f.commitCalls++
	return f.commitLogin, f.commitID, nil
}

func TestResolveEmailToIdentity_Noreply(t *testing.T) {
	db := &fakeEmailDB{login: "shouldNotBeUsed"}
	cl := &fakeSearchClient{}
	login, id, src, err := ResolveEmailToIdentity(context.Background(), db, cl, "12345+octocat@users.noreply.github.com")
	if err != nil || login != "octocat" || id != 12345 || src != EmailSourceNoreply {
		t.Fatalf("noreply: got (%q,%d,%q,%v), want (octocat,12345,noreply,nil)", login, id, src, err)
	}
	if db.calls != 0 || cl.searchCalls != 0 || cl.commitCalls != 0 {
		t.Error("noreply must short-circuit before DB/API calls")
	}
}

func TestResolveEmailToIdentity_BotShortCircuits(t *testing.T) {
	db := &fakeEmailDB{login: "x"}
	cl := &fakeSearchClient{searchLogin: "x"}
	login, _, src, _ := ResolveEmailToIdentity(context.Background(), db, cl, "notifications@github.com")
	if login != "" || src != "" {
		t.Errorf("bot email must resolve to nothing; got (%q,%q)", login, src)
	}
	if db.calls != 0 || cl.searchCalls != 0 || cl.commitCalls != 0 {
		t.Error("bot email must short-circuit before DB/API calls")
	}
}

func TestResolveEmailToIdentity_DBHit(t *testing.T) {
	db := &fakeEmailDB{login: "alicelogin"}
	cl := &fakeSearchClient{searchLogin: "shouldNotBeUsed"}
	login, _, src, _ := ResolveEmailToIdentity(context.Background(), db, cl, "alice@example.com")
	if login != "alicelogin" || src != EmailSourceDB {
		t.Errorf("DB hit: got (%q,%q), want (alicelogin,db)", login, src)
	}
	if cl.searchCalls != 0 || cl.commitCalls != 0 {
		t.Error("DB hit must not call the API tail")
	}
}

func TestResolveEmailToIdentity_SearchHit(t *testing.T) {
	db := &fakeEmailDB{login: ""}
	cl := &fakeSearchClient{searchLogin: "bob", searchID: 42}
	login, id, src, _ := ResolveEmailToIdentity(context.Background(), db, cl, "bob@example.com")
	if login != "bob" || id != 42 || src != EmailSourceSearch {
		t.Errorf("search hit: got (%q,%d,%q)", login, id, src)
	}
	if cl.commitCalls != 0 {
		t.Error("search hit must short-circuit before commit-search")
	}
}

// The load-bearing new path: DB + Search miss, global commit-search resolves.
func TestResolveEmailToIdentity_CommitSearchHit(t *testing.T) {
	db := &fakeEmailDB{login: ""}
	cl := &fakeSearchClient{searchLogin: "", commitLogin: "carol", commitID: 99}
	login, id, src, _ := ResolveEmailToIdentity(context.Background(), db, cl, "carol@example.com")
	if login != "carol" || id != 99 || src != EmailSourceCommitSearch {
		t.Errorf("commit-search hit: got (%q,%d,%q), want (carol,99,commit-search)", login, id, src)
	}
	if cl.searchCalls != 1 || cl.commitCalls != 1 {
		t.Errorf("expected exactly 1 search + 1 commit-search call; got %d/%d", cl.searchCalls, cl.commitCalls)
	}
}

func TestResolveEmailToIdentity_AllMiss(t *testing.T) {
	db := &fakeEmailDB{login: ""}
	cl := &fakeSearchClient{}
	login, _, src, err := ResolveEmailToIdentity(context.Background(), db, cl, "ghost@example.com")
	if err != nil || login != "" || src != "" {
		t.Errorf("all miss must be (\"\",0,\"\",nil); got (%q,%q,%v)", login, src, err)
	}
}

func TestResolveEmailToIdentity_NilClientIsDBOnly(t *testing.T) {
	db := &fakeEmailDB{login: "dbonly"}
	login, _, src, _ := ResolveEmailToIdentity(context.Background(), db, nil, "x@example.com")
	if login != "dbonly" || src != EmailSourceDB {
		t.Errorf("nil client must still do DB lookup; got (%q,%q)", login, src)
	}
}

func TestResolveEmailViaAPI_SearchBeforeCommit(t *testing.T) {
	cl := &fakeSearchClient{searchLogin: "s", searchID: 1, commitLogin: "c", commitID: 2}
	login, _, src, _ := ResolveEmailViaAPI(context.Background(), cl, "x@example.com")
	if login != "s" || src != EmailSourceSearch || cl.commitCalls != 0 {
		t.Errorf("API tail must try search first and short-circuit; got (%q,%q) commitCalls=%d", login, src, cl.commitCalls)
	}
}
