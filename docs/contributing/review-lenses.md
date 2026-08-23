<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Review lenses — the adversarial pass every diff gets before it ships

Distilled from the PR #184 retrospective (~25 external review rounds,
~70 verified-real findings, most of them in code added days or hours
earlier in the same PR). The recurring defect classes below are
SEMANTIC — error shapes, key granularity, invariant placement — which
is why textual tripwires kept missing the next instance while a fresh
adversarial read of the diff kept finding it.

**The practice**: before a push, the NEW diff gets one adversarial
pass with these lenses as the explicit checklist — by a fresh-context
reviewer (human or agent) who did not write the code. Findings get the
same treatment as an external review round: verify against code first,
then fix or decline with a reason at the site. The cycle is
review→fix→push, not push→review→fix.

Each lens is one question to ask of the diff, with the standing rule
it enforces where one exists (`scripts/standing_rules.go`).

## L1 — Error arms on probes (SR-5 / SR-16, the most-recurrent class)

For every new probe, lookup, or check that answers yes/no:
**what happens on a TRANSPORT error, a rate limit, or a 5xx — can it
reach the "no" branch?** Only a typed, definitive negative (a 404
sentinel, `ErrNoRows`, an explicit not-found) may mean "no". A
`(value, bool)` signature with no error arm is the defect shape
itself. A warn-and-continue on a lookup error is the same defect in a
different costume.

## L2 — Key granularity (SR-17)

For every new map key, uniqueness constraint, dedup set, or index:
**is the key's granularity the REAL identity?** Ask what two
independent things could share this key — two lockfiles, two
platforms, two ecosystems, two repos — and what silently merges when
they do. The repo's history: platform-blind login matching, a chain
adjacency merged across lockfiles, a repo-wide direct set against
per-lockfile graphs, ecosystem-less SPDX IDs.

## L3 — One shared normalizer (SR-17)

For every string that crosses a subsystem boundary (ecosystem names,
package names, URLs, logins): **does BOTH the producer and the
consumer go through the same named normalizer function?** A second
inline spelling of an existing key — even a correct one — is a defect,
because the two copies WILL drift. Exemplar: `db.LockfileGraphKey`.

## L4 — Who enforces the invariant? (SR-18)

For every rule a change introduces ("callers must pass zero on
failure", "this must be called after that"): **if a caller does the
wrong thing, what breaks?** If the answer is "nothing — we trust the
callers", move the enforcement into the layer that owns the data so a
wrong caller cannot succeed. Watch for decorative gates: a check
reading a signal that nothing populates is enforcement theater.

## L5 — Convergence (SR-19)

For anything documented as "rerun until done", "self-draining",
"resume state": **is there a test that drives the loop TO done?** A
convergence contract without a convergence test is a promise. The
motivating incident: a healer whose candidate predicate never stopped
matching healed repos. Mechanized in v0.27.146: the marker phrases are
scanned by `scripts/convergence_contracts_test.go`, which requires a
registered driving test per contract site — write the test first, then
register the site.

## L6 — Check-then-act and lock lifecycles

For every load-then-store on shared state (caches, claims, locks):
**what do two concurrent cold callers do?** For every lock: **what
happens on the failure/cancel path — does the unlock run on a dead
context, does an unconfirmed release return a poisoned resource to a
pool?** The repo's history: a cache thundering herd, a blocking
advisory lock recreating a documented deadlock, an unlock on a
canceled context.

## L7 — Boundary equality

For every comparison against a boundary (a `since`, a threshold, a
cap): **which side does equality land on, is that stated in a comment,
and is there a test pinning it?** Sibling code paths (GraphQL vs REST)
must agree. History: a `<=` breakout that dropped the boundary item
AND terminated pagination behind it.

## L8 — External authority vs local convention

For every value derived from a naming convention, a documented
default, or an assumption about an external system: **is the external
system the authority, and if so, does the code ASK it rather than
guess?** History: the Apache `incubator-` prefix is a moving target by
design — the forge, not the convention, knows which repo exists.
Related: mock-only tests miss real-API drift; pair them with a
network-gated live canary.

## L9 — Diagnostics tell the truth (SR-10's logging half)

For every log line and counter a change touches: **does the label
still describe what is counted after this change?** Log the EFFECTIVE
value at the point of use; split counts when a set becomes
heterogeneous. History: "direct_resolutions" counting an entire
transitive closure.

## L10 — The fix itself is new code

The previous rounds' hardest lesson: **the fix for finding N is the
most likely source of finding N+1**, because it ships with less
scrutiny than the code it fixes. Apply L1–L9 to the fix diff itself,
and check it against the standing-rules registry
(`scripts/standing_rules.go`) before shipping — several findings were
the repo's own rules violated by fresh fixes.

## Running the pass

- Scope: the full diff of the push batch (not per-micro-release), so
  the pass covers exactly what an external reviewer will see.
- Reviewer: someone/something that did NOT write the diff — a fresh
  agent context works; the lens list above is its instruction set.
- Output discipline: findings are verified against the code before
  acting (the false-positive rate of un-verified review comments is
  what this step's credibility depends on); real ones are fixed with
  the error-path red-first rule (a behavioral test exercising the
  FAILING input, not just the happy path); declined ones get the
  reason recorded at the site.
