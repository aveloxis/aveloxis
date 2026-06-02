// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import "testing"

func loadOrFail(t *testing.T) map[string]*System {
	t.Helper()
	sys, err := LoadSystems()
	if err != nil {
		t.Fatalf("LoadSystems: %v", err)
	}
	return sys
}

func TestEmbeddedSystemsParse(t *testing.T) {
	sys := loadOrFail(t)
	for _, name := range []string{"apache_ponymail", "lore_public_inbox"} {
		if sys[name] == nil {
			t.Errorf("expected system %q in systems.yaml", name)
		}
	}
	if got := sys["apache_ponymail"].ArchiveBackend; got != "apache_ponymail" {
		t.Errorf("apache backend = %q", got)
	}
	if got := sys["lore_public_inbox"].ArchiveBackend; got != "public_inbox" {
		t.Errorf("kernel backend = %q", got)
	}
}

// TestApacheClassification — table-driven against the real subject/sender
// shapes captured in the 2026-06-02 scans (jira@kafka, github@arrow, dev@).
func TestApacheClassification(t *testing.T) {
	apache := loadOrFail(t)["apache_ponymail"]
	cases := []struct {
		name      string
		msg       Message
		wantClass string
		wantCap   map[string]string
	}{
		{
			name:      "jira created",
			msg:       Message{Subject: "[jira] [Created] (KAFKA-20652) Improve/Fix java docs", Sender: "Chia-Ping Tsai (Jira)"},
			wantClass: ClassIssueEvent,
			wantCap:   map[string]string{"action": "Created", "external_key": "KAFKA-20652"},
		},
		{
			name: "github mirror via body url",
			msg: Message{
				Subject: "Re: [PR] fix(parquet): bound capacity [arrow-rs]",
				Sender:  "tustvold (via GitHub) <git@apache.org>",
				Body:    "comment\nURL: https://github.com/apache/arrow-rs/pull/123#issuecomment-1\n",
			},
			wantClass: ClassGitHubMirror,
			wantCap:   map[string]string{"owner": "apache", "repo": "arrow-rs", "kind": "pull", "number": "123"},
		},
		{
			name:      "github mirror via subject tag only",
			msg:       Message{Subject: "[PR] Bump version to 23 [arrow-dotnet]", Sender: "curthagenlocher"},
			wantClass: ClassGitHubMirror,
			wantCap:   map[string]string{"repo": "arrow-dotnet"},
		},
		{
			name:      "issue mirror subject tag",
			msg:       Message{Subject: "Re: [I] [Python][C++] pyarrow.repeat returns invalid array [arrow]", Sender: "x"},
			wantClass: ClassGitHubMirror,
			wantCap:   map[string]string{"repo": "arrow"},
		},
		{
			name:      "vote",
			msg:       Message{Subject: "[VOTE][.NET] Release Apache Arrow .NET 23.0.0 RC0", ListID: "<dev.arrow.apache.org>"},
			wantClass: ClassVote,
		},
		{
			name:      "announce",
			msg:       Message{Subject: "[ANNOUNCE] Apache Arrow .NET 23.0.0 released"},
			wantClass: ClassAnnounce,
		},
		{
			name:      "commit notify by list-id",
			msg:       Message{Subject: "[arrow] branch main updated: ...", ListID: "<commits.arrow.apache.org>"},
			wantClass: ClassCommitNotify,
		},
		{
			name:      "user support by list-id",
			msg:       Message{Subject: "Help with pyarrow S3", ListID: "<users.arrow.apache.org>"},
			wantClass: ClassSupport,
		},
		{
			name:      "soft component tag is just discussion",
			msg:       Message{Subject: "[RUST] Planned patch releases this week", ListID: "<dev.arrow.apache.org>"},
			wantClass: ClassDiscuss,
		},
		{
			name:      "plain discuss fallthrough",
			msg:       Message{Subject: "[DISCUSS][Erlang] Erlang Apache Arrow Implementation", ListID: "<dev.arrow.apache.org>"},
			wantClass: ClassDiscuss,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := apache.Classify(tc.msg)
			if got.Class != tc.wantClass {
				t.Errorf("class = %q, want %q", got.Class, tc.wantClass)
			}
			for k, v := range tc.wantCap {
				if got.Captures[k] != v {
					t.Errorf("capture[%s] = %q, want %q", k, got.Captures[k], v)
				}
			}
		})
	}
}

// TestKernelClassification — the kernel ruleset has NO bots: patch-by-email
// and Reviewed-by trailers are the PR/review equivalents.
func TestKernelClassification(t *testing.T) {
	kernel := loadOrFail(t)["lore_public_inbox"]
	cases := []struct {
		msg       Message
		wantClass string
	}{
		{Message{Subject: "[PATCH v2 1/3] pci: fix foo"}, ClassPatchSubmission},
		{Message{Subject: "[GIT PULL] PCI changes for v6.x"}, ClassPatchSubmission},
		{Message{Subject: "Re: [PATCH v2 1/3] pci: fix foo", Body: "On Mon...\nReviewed-by: Bjorn Helgaas <bhelgaas@google.com>\n"}, ClassReview},
		{Message{Subject: "Re: random thread about policy"}, ClassDiscuss},
	}
	for _, tc := range cases {
		got := kernel.Classify(tc.msg)
		if got.Class != tc.wantClass {
			t.Errorf("subject %q → class %q, want %q", tc.msg.Subject, got.Class, tc.wantClass)
		}
	}
}

// TestRepoURLFromCaptures — body URL (explicit owner/repo) is authoritative;
// a bare [repo] tag uses the system template; soft tags resolve to nothing.
func TestRepoURLFromCaptures(t *testing.T) {
	apache := loadOrFail(t)["apache_ponymail"]

	bodyC := apache.Classify(Message{
		Subject: "Re: [PR] fix [arrow-rs]",
		Sender:  "x (via GitHub)",
		Body:    "URL: https://github.com/apache/arrow-rs/pull/9\n",
	})
	if got := apache.RepoURLFromCaptures(bodyC); got != "https://github.com/apache/arrow-rs" {
		t.Errorf("body-url repo = %q", got)
	}

	tagC := apache.Classify(Message{Subject: "[PR] Bump [arrow-dotnet]", Sender: "x"})
	if got := apache.RepoURLFromCaptures(tagC); got != "https://github.com/apache/arrow-dotnet" {
		t.Errorf("tag repo via template = %q", got)
	}

	softC := apache.Classify(Message{Subject: "[RUST] releases", ListID: "<dev.arrow.apache.org>"})
	if got := apache.RepoURLFromCaptures(softC); got != "" {
		t.Errorf("soft component tag must NOT synthesize a repo URL, got %q", got)
	}
}
