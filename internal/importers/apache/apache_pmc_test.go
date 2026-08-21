// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package apache

import "testing"

func TestParsePMCsKeepsSlugAndDerivesListDomain(t *testing.T) {
	data := []byte(`{
		"kafka": {"name": "Apache Kafka", "homepage": "https://kafka.apache.org",
		          "bug-database": "https://issues.apache.org/jira/browse/KAFKA", "pmc": "kafka"},
		"arrow": {"name": "Apache Arrow", "homepage": "https://arrow.apache.org",
		          "bug-database": "https://github.com/apache/arrow/issues", "pmc": "arrow"}
	}`)
	pmcs, err := ParsePMCs(data)
	if err != nil {
		t.Fatal(err)
	}
	bySlug := map[string]PMC{}
	for _, p := range pmcs {
		bySlug[p.Slug] = p
	}

	kafka, ok := bySlug["kafka"]
	if !ok {
		t.Fatal("expected kafka PMC")
	}
	if kafka.ListDomain() != "kafka.apache.org" {
		t.Errorf("kafka ListDomain = %q", kafka.ListDomain())
	}
	// Jira bug-database → repo falls back to github.com/apache/<slug>.
	if kafka.RepoURL != "https://github.com/apache/kafka" {
		t.Errorf("kafka RepoURL = %q", kafka.RepoURL)
	}

	// arrow's bug-database IS a github URL → that's the repo.
	if bySlug["arrow"].RepoURL != "https://github.com/apache/arrow" {
		t.Errorf("arrow RepoURL = %q", bySlug["arrow"].RepoURL)
	}
}

func TestParsePodlingPMCs(t *testing.T) {
	data := []byte(`{"amoro": {"name": "Apache Amoro", "homepage": "https://amoro.apache.org"}}`)
	pmcs, err := ParsePodlingPMCs(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(pmcs) != 1 || pmcs[0].Slug != "amoro" {
		t.Fatalf("expected one amoro podling, got %+v", pmcs)
	}
	if !pmcs[0].Incubating {
		t.Error("podling must be marked Incubating")
	}
	if pmcs[0].ListDomain() != "amoro.apache.org" {
		t.Errorf("amoro ListDomain = %q (current podlings live at <slug>.apache.org, not incubator.apache.org)", pmcs[0].ListDomain())
	}
	// v0.27.132: Apache INFRA names podling repos with the incubator-
	// prefix (github.com/apache/incubator-<slug>). The old derived URL
	// (apache/<slug>) matched nothing — which is exactly how the four
	// production PMC groups (pegasus/graphar/ponymail/seata) ended up
	// registered against phantom rows and then empty. This was the
	// UNPINNED gap that let it ship.
	if pmcs[0].RepoURL != "https://github.com/apache/incubator-amoro" {
		t.Errorf("podling RepoURL = %q, want the incubator-prefixed form", pmcs[0].RepoURL)
	}
}

// v0.27.132: the lookup variants that make load-apache-lists (and any
// future consumer) resilient to the incubator-prefix naming in BOTH
// directions — a graduated podling's repo may keep (or shed) the
// prefix out of step with podlings.json.
func TestRepoURLVariants(t *testing.T) {
	cases := []struct {
		name string
		pmc  PMC
		want []string
	}{
		{"podling-primary-incubator", PMC{Slug: "pegasus", Incubating: true,
			RepoURL: "https://github.com/apache/incubator-pegasus"},
			[]string{"https://github.com/apache/incubator-pegasus", "https://github.com/apache/pegasus"}},
		{"tlp-plain-gains-incubator-twin", PMC{Slug: "kafka",
			RepoURL: "https://github.com/apache/kafka"},
			[]string{"https://github.com/apache/kafka", "https://github.com/apache/incubator-kafka"}},
		{"custom-bugdb-url-stays-alone", PMC{Slug: "arrow",
			RepoURL: "https://github.com/apache/arrow-site"},
			[]string{"https://github.com/apache/arrow-site"}},
		{"empty-url", PMC{Slug: "x"}, nil},
	}
	for _, c := range cases {
		got := c.pmc.RepoURLVariants()
		if len(got) != len(c.want) {
			t.Errorf("%s: variants = %v, want %v", c.name, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: variant[%d] = %q, want %q", c.name, i, got[i], c.want[i])
			}
		}
	}
}
