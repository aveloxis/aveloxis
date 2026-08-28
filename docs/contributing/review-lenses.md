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

## L11 — Class sweep

This fix targets a mechanism, not a symptom: **name the primitive**
(an ETag-cached `Get`, a clamp, a default, an FK column, a lookup by
login) **and grep for every sibling site that shares it.** Each sibling
is fixed or explicitly exempted at the site — "fixed one call site of a
class" is the finding an external reviewer will make next. History:
PR #191 — the GitLab MR batch fix (v0.28.15) left the REST waterfall,
then `GetJSON` itself, then three bare `Get` readers with the identical
304 hole (v0.28.17); the house config-clamp rule (SR-10) is the same
lens for defaults.

## L12 — Stale prose

Every comment and doc sentence inside a touched function or section:
**is it still true after this diff?** Refactors move behaviour and
leave the old description behind; take the words of the diff's REMOVED
lines and grep the surviving prose for them. History: PR #191 — a
"returns nil, nil when inaccessible" contract that described a branch
deleted in the same change; an upgrade guide claiming "each ledgered"
for steps that were not.

## L13 — Probe/tripwire parity

When a runtime probe (readiness, coverage, "is every X indexed") and a
build-time tripwire guard the same invariant: **do they iterate ONE
shared list?** Two hand-maintained lists drift the day one gains an
entry (SR-17 applied to test/runtime pairs). History: PR #191 —
`repoGroupFKIndexesReady` checked its own list while the tripwire
also covered `repoGroupFKCoveredElsewhere`; the gate could pass with an
unindexed child.

## L14 — Keep-going passes and exits tell the truth

*Anchor: L9 (diagnostics) + the v0.27.106 nonzero-exit convention.*

A loop that logs a failure and continues must accumulate what it
dropped and return it (bounded — the first N with the exact count in
the prefix), and the command driving it must exit nonzero when any item
was refused or failed; a canceled context is ONE exit, classified
before the failure arm and never logged as a failure or as "complete";
a summary line that says "complete" or "N remain because X" must be
true for the exit path that printed it (a `--limit` cap is not "mid-
collection"). Ask of every keep-going loop: *what does the caller see
when one item fails — and when all of them do?*

Incidents: v0.28.18 passes 26–29 — the dm_ aggregate and materialized
view refreshes returned nil over stale rows; `refresh-views --aggregates`
skipped the aggregate pass on a view failure; `reconcile-repos` exited 0
after refusing every consolidation; the weekly rebuild and seven tickers
logged `stop serve` as `… failed: context canceled`.

## L15 — The forge's cache and session semantics

*Anchor: L8 (external authority) + the ETag rules of v0.28.15/.17/.18.*

A 304 means "nothing new" ONLY for an incremental listing (a since-
bounded walk); a single-object read and a full-snapshot walk (zero
since: a set-diff's expected set, a force-full recollect) must bypass
the conditional cache, or the second read in one process returns an
empty truth set. A payload field whose value depends on the caller's
token (`feature_available?(current_user)` booleans, counts hidden from
a narrowed token, totals a forge omits above a threshold) is not the
feature's state — read the access level or mark the value unknown,
never persist the token's view as the fact. Ask of every forge read:
*what does this return to a token that cannot see the feature, and to
the second identical request in this process?*

Incidents: the GitLab MR batch 304 (v0.28.15), `GetJSON` 304 (v0.28.17),
full-snapshot listings answered from the ETag cache (Copilot round 5),
`*_enabled` booleans persisted as "disabled" for members-only features
and `pages` read as enabled when its level was absent (rounds 4 and 26).

## L16 — Widening a tripwire is a contract change

When a pin false-fires on code you believe is correct, the reflex is to
widen it: add the spelling, accept the shape, move on. That reflex is
how a pin starts recommending a defect.

The scancode shutdown pin rejected `context.WithTimeout` as a deadline
arm. It looked like a category error — a timeout is a timeout — so the
spelling was added, and the failure message began offering it as legal
guidance. But that code path is reached *because* the worker context
was canceled, so a child of it is born expired: following the pin's own
advice reduced a 70-second shutdown wait to 1.1µs. The widening was
never checked against the contract, only against the category.

Ask, before accepting a new shape: **does this shape satisfy the
contract HERE, in this code path, with these preconditions?** Not "is
it the same kind of thing".

Two structural answers, in order of preference:

1. **Make the question executable.** If the contract is about
   behaviour — bounded, terminates, does not deadlock, converges —
   a runtime test settles "does this actually do it" with no
   judgment call. A source pin can only ever approximate behaviour
   by naming shapes, and every accepted shape is a standing bet
   that the naming was right.
2. **Narrow the widening to the precondition that makes it true.**
   `context.WithTimeout` is a real bound when its parent is
   `Background()`/`TODO()`; the accepted shape should say so.

Corollary: when a pin has been escaped repeatedly and each fix adds
surface, that is evidence the property is being asserted at the wrong
level, not that the next predicate will be the one that holds. Passes
43–48 escaped the same shutdown pin fifteen times (`grep -ohE
'\b4[3-9][a-f]\b' internal/collector/*.go | sort -u`); the runtime test that replaced its semantic half
catches all nine bound-class escapes without naming any of them.

Note what that did NOT buy: the total surface for this one contract
grew, from 356 lines to about 1000. (Measured as the pin's doc comment
through end of file — `git show 49f25b2:internal/collector/scancode_worker_test.go
| awk '/^\/\/ The shutdown contract/,0' | wc -l` — plus the runtime
test file. State the boundary when quoting a figure like this: a
round-closing reviewer read a different one and reported the number as
inverted.) Moving a property to a runtime test is
not a code-size win, and claiming one here was itself an unchecked
claim — pass 50 measured it. What changes is which failures are
*reachable*: the escape-prone half (deadline spellings, goroutine-launch
derivation, synchronous-path analysis, select-arm taxonomy) is gone,
and with it all four of the round's false fires. The remaining pin
asserts only structural facts — where the wait lives, that Run
delegates to it, that nothing else blocks — which are the facts a
source pin can actually hold.

Incidents: the `context.WithTimeout` recommendation (pass 48); the
deadline arm that checked only the function name while both operands
appeared in the log line inside it (pass 48); the four legitimate
refactors rejected with misdiagnosing messages (passes 47–48).

## Running the pass

The brief, the verification protocol and the pre-launch checklist are
in [review-pass-brief.md](review-pass-brief.md).


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
