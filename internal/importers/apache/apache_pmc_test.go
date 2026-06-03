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
}
