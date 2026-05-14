# Lineage

Aveloxis is a ground-up Go rewrite of [Augur](https://github.com/augurlabs/augur),
an open source software health analytics platform originally written in Python.
Both projects are maintained by Sean Goggins (University of Missouri) and are
products of the same research program on open source ecosystem health.

This document establishes the formal lineage between the two codebases so that
research, citation, and provenance tooling can traverse the relationship even
though the two repositories have independent Git histories on GitHub.

## Provenance

| | |
|---|---|
| **Predecessor** | [`augurlabs/augur`](https://github.com/augurlabs/augur) |
| **Successor**   | [`aveloxis/aveloxis`](https://github.com/aveloxis/aveloxis) |
| **Splice date** | 2026-03-31 (UTC) |
| **Splice commit (augur)** | AUGUR_TIP=`03b14ff19012912cfd4e4369f981bfae143156d1` — last Augur commit at or before the splice date |
| **First commit (aveloxis)** | AVELOXIS_ROOT=`15fdc87288181206b33f74e47e58b2704adfd6ea` — root commit of the Go rewrite |
| **Copyright holder** | Sean P. Goggins |
| **License continuity** | Same license terms carry forward from Augur to Aveloxis |
| **Contribution model** | Developer Certificate of Origin (DCO); no CLA |

## Relationship

Aveloxis is not a fork in the Git sense. It is a successor project that
re-implements Augur's architecture, schema concepts, and analytical model in
Go, with a redesigned scheduler, a staged JSONB ingestion pipeline, and a
deterministic UUID scheme for contributor identity. The Python codebase at
`augurlabs/augur` remains the historical reference implementation; ongoing
development continues in `aveloxis/aveloxis`.

The Augur commit history through 2026-03-31 should be treated as the
prehistory of Aveloxis for the purposes of:

- Authorship attribution and contributor credit
- Research citation and academic provenance
- Software health and lifecycle measurement, where the unified history
  reflects the true span of work
- DCO and copyright chain of custody

## Reconstructing the Unified History Locally

The two repositories have independent Git DAGs on GitHub. A unified view can
be reconstructed locally using Git's `replace` mechanism, which leaves both
upstream repositories untouched:

    # Clone aveloxis and add augur as an additional remote
    git clone https://github.com/aveloxis/aveloxis.git
    cd aveloxis
    git remote add augur https://github.com/augurlabs/augur.git
    git fetch augur

    # Option A: fetch published replace refs (see "Published Replace Refs" below)
    git config --add remote.origin.fetch '+refs/replace/*:refs/replace/*'
    git fetch origin

    # Option B: graft locally without published refs
    AUGUR_TIP=03b14ff19012912cfd4e4369f981bfae143156d1
    AVELOXIS_ROOT=15fdc87288181206b33f74e47e58b2704adfd6ea
    git replace --graft "$AVELOXIS_ROOT" "$AUGUR_TIP"

    # Verify
    git log --oneline | tail -5    # should show augur commits at the bottom

Once the graft is in place, `git log`, `git blame`, `git bisect`, and most
analytical tooling (including Augur/Aveloxis itself) traverse the combined
history transparently. Removing the graft is a single command:
`git replace -d <AVELOXIS_ROOT_SHA>`.

## Published Replace Refs

To make the unified history available without each user having to know the
splice SHAs, this repository publishes a `refs/replace/15fdc87288181206b33f74e47e58b2704adfd6ea`
ref pointing at `03b14ff19012912cfd4e4369f981bfae143156d1`. Clients that opt in to fetching replace
refs (see the snippet above) receive the graft automatically.

This is a non-destructive mechanism: the underlying commit objects are
unchanged, no SHAs are rewritten, no force pushes are required, and clients
that do not opt in see the repository exactly as it was before.

## Why Not Rewrite History?

A graft-and-`filter-repo` rewrite that physically prepends Augur's history
onto Aveloxis was considered and rejected. Rewriting would change every
Aveloxis commit SHA, invalidate external references (issue/PR comments, the
launch announcement, NSF grant documentation, any cached checkouts), and
require every collaborator to re-clone. The replace-ref approach preserves
the same logical lineage without any of those costs.

## Citation

When citing Aveloxis in academic work, cite the Aveloxis repository directly.
When the work specifically depends on the long-run history — for example,
contributor lifecycle analysis spanning the Python-to-Go transition — cite
both repositories and reference this document for the splice point.

## Maintainer

Sean P. Goggins
Department of Electrical Engineering and Computer Science
University of Missouri, Columbia

---

*Last updated: 2026-05-14*