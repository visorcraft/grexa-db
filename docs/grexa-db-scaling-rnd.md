<!-- SPDX-FileCopyrightText: 2026 VisorCraft LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# grexa-db scaling R&D — resolving the five bottlenecks past 250k records

This document is the result of a deep investigation (profiling, prototyping,
parallel research, and independent peer analysis) into the five remediable
reasons a flat-file engine loses to a B-tree database at scale. **Every
performance claim here is either measured on this machine or a stated
analytical bound** — see [`rnd/measured-results.md`](rnd/measured-results.md)
and the prototypes in [`rnd/prototype/`](rnd/prototype/).

## The invariant (the whole point)

Nothing below may break this: **the plain Markdown+YAML files are the sole
source of truth, edited out of band; the engine never mediates writes.** Every
acceleration is a *derived, rebuildable sidecar* — delete it and all data
survives; it is never *silently* stale. No record ever changes format; records
stay greppable, diffable, ownable. We steal the on-disk mechanics of real
index engines while inverting their closed-world "engine owns the write path"
assumption: **correctness is anchored on filesystem reconciliation, never on an
engine-owned log.**

## Executive summary

| # | Bottleneck | Verdict | Lever | Measured / bound | Format change |
|---|---|---|---|---|---|
| 2+4 | per-record I/O + YAML parse | **solved** | rayon + hand-parse + head-read | **10.1×** (924→92 ms) | none |
| 1 | no index → O(n) scan | **solved** | sidecar inverted/columnar index | **1,204×** selective; count ~instant | none (sidecar) |
| 5 | order_by buffers everything | **solved** | top-K heap + keys-only | **26,277×** less memory | none |
| 3 | large-directory degradation | **non-issue to 1M** | adaptive shard, on-disk only, past ~100k | linear to 1M; no knee | (deferred) |
| 6 | inode/storage amplification | **irreducible — the real ceiling** | accept; it's the rent for transparency | ~18× disk, ~270× delete @500k | — |

Stacked, a selective query goes from **~924 ms → ~0.1 ms** with every record
still a plain file. The honest crossover where a real DB wins moves from the
spec's ~250k toward low millions; #6 (and the staleness-reconciliation tax)
keeps *a* ceiling — the design goal is to widen the sweet spot, not pretend
there's no ceiling.

## Methodology

16-core Intel box; `/tmp` = tmpfs (so read+parse is CPU-bound; **on real disk
the I/O wins are larger — these numbers are a conservative floor**). Dataset:
200,000 records, 4 frontmatter fields, small bodies. Reproduce:

```bash
python3 scripts/profile_scale.py        # baseline + flat-vs-sharded
# prototypes (copy rnd/prototype/* into a temp crate; deps in Cargo.toml.txt):
cargo run --release --bin readparse     # issues #2/#4
cargo run --release --bin indexdemo     # issue #1
cargo run --release --bin topkdemo      # issue #5
```

## Baseline: where the time actually goes

At 200k records, one flat collection: OS walk = 76 ms (**9%**); full
walk+read+parse = 889 ms; so **read+parse is ~91% (~4 µs/record)**. The
directory walk is *not* the bottleneck — and a 256-way sharded layout was
*slower* here. This single measurement reorders the whole problem: attack
read+parse (#2/#4) and indexing (#1) first; directory layout (#3) is premature.

---

## Item #2 + #4 — read + parse (SOLVED, 10.1× measured)

**Finding (two independent harnesses agree):** the dominant cost is
`serde_yaml` itself (~2.9 µs/record building an indexmap-backed `Value`), *not*
the body `String` copy (~6%). Filter queries only touch a few named fields, yet
today every record is fully parsed and its whole body copied.

**Measured stack** (filter `rating>=4`; all strategies return the identical
count, proving correctness-preservation):

| Optimization | Time | Speedup |
|---|---|---|
| current (full serde parse + body copy) | 924 ms | 1.0× |
| hand-rolled field scan (no serde) | 318 ms | 2.9× |
| + rayon parallel | 102 ms | 9.0× |
| + head-only read | **92 ms** | **10.1×** |

**Design (no public API or on-disk change):**
1. **Parallelize** the materialize/filter path with `rayon` (`par_iter` over the
   already-collected path vec preserves order → existing tests pass). rayon is
   MIT/Apache — clean for this Apache-2.0 crate. ~6× alone.
2. **Hand-rolled frontmatter parser** for the common flat `key: scalar | [array]`
   subset; fall back to `serde_yaml` on anything indented/block/anchored. Parse
   **only the fields a query references** (`Filter.field`/`OrderBy.field`).
   Keep `record.field()` returning `&Value` via a lazy `OnceCell` so the public
   API is unchanged. ~2.9× alone, additive with rayon.
3. **Lazy body**: store file content once, return `body()` as a slice — never
   copy the body for filter/sort/validate paths (they never read it).
4. **Head-only read**: read a 4–8 KB prefix; the frontmatter is there. Modest on
   tmpfs, a big multiplier on real disk.
5. **Skip `paths.sort()`** for filter queries (sort only the small result), and
   fix the CLI `records` command to return walked paths without read+parse
   (~1 s → ~90 ms).

Files: `collection.rs` (`records_par`, drop sort), `query.rs` (`init_state`),
`frontmatter.rs` (fast scanner), `record.rs` (lazy body/fields).

---

## Item #1 — the secondary index (SOLVED, 1,204× measured; the master lever)

A hidden, per-collection `.grexa-index/` sidecar mapping **field value → record
ids** plus a **columnar cache of extracted field values**, so a selective query
reads only matching records — or answers a count/path-list with **zero record
reads**.

**Measured (200k records):**

| Query | Full scan (already parallel) | Index | Speedup |
|---|---|---|---|
| selective `read_at == date` (0.05%) | 104.6 ms | **0.09 ms** | **1,204×** |
| `count(rating>=4)` from postings | ~924 ms (orig) | **0.001 ms** | ~instant |
| index build (parallel, one-time) | — | 137 ms | — |

Against the original serde scan, selective queries hit ~0.1 ms — **~10,000×**.

**Structure** (Tantivy/Xapian mechanics, scaled down): per-field segments =
a **sorted term dictionary** (binary-search equality; range = bound + tail
scan) + **delta-encoded posting lists** of record-ids + an explicit **`absent`
set** (so `ne` is correct vs missing fields) + **columnar "fast fields"** /
Xapian value-slots with order-preserving encoding so sort/range never parse
YAML. `array<string>` (tags) is an inverted index → `contains_any`=union,
`contains_all`=intersection. Published via **write-temp + atomic `rename(2)`**
of a manifest (the exact pattern `view.rs` already uses) — crash-safe, and a
deleted sidecar just falls back to scan.

**Planner** (wired into `query.rs::init_state`, no API change): per filter,
classify index-serviceable vs residual; choose **index-only** (count/paths,
zero reads) → **index-narrowed scan** (read only candidates, apply residual
filters) → **full scan** (today's path; the zero-regression floor). Multi-filter
= intersect posting lists smallest-first (Lucene conjunction). It reproduces
`query.rs`'s exact comparison semantics (incl. the i64/f64 cross-type case) so
it can **never disagree with a scan**.

**Staleness / freshness** (the hard part — synthesized from the notmuch/git/
mlocate/Tantivy/CouchDB prior art and an independent protocol analysis):
- **Directory-mtime skip** (notmuch/mlocate): cache per-dir `max(ctime,mtime)`;
  on query, stat dirs — unchanged dir ⇒ skip readdir+reparse entirely. Turns a
  250k scan into "stat N dirs, reparse only changed ones."
- **Per-record `(mtime,size,ctime,dev,ino)` signature** as the cheap filter;
  `ctime` catches mtime forgery; `dev/ino` catches same-path replacement.
- **Content hash as the correctness gate** for the paranoid path (closes
  recoll's same-size/mtime-preserved blind spot).
- **Racy-second guard** (git racy-git + mlocate `time_is_current`): distrust any
  file/dir whose recorded time is in the manifest's write-second.
- **Verify-on-read** (the key correctness move): the index yields *candidate*
  paths; each is re-stat-checked before being returned → **O(k), never stale**.
- **Notifications (inotify/fanotify) are optimization only**; any "can't prove
  completeness" signal (`IN_Q_OVERFLOW`, fresh-instance, foreign sidecar UUID)
  forces a full reconcile. The dir-mtime scan is the correctness floor.
- **Manifest carries a format/schema/parser version**; mismatch ⇒ rebuild.

**Incremental reconcile** is O(changed × fields): re-parse only added/modified
records, drop deleted ids, publish a new generation via atomic swap (CoW of
touched segments). A 5-record edit in 200k re-parses 5 records.

**Where it does NOT help (honest):** non-selective filters needing full records
(planner picks scan), body/full-text search (use `rg` — that's the point),
nested/non-indexed fields, and the first query after a big out-of-band edit
batch (pays reconcile).

Files: new `index.rs`, `plan.rs`, shared `atomic.rs` (factored from `view.rs`);
`query.rs`/`collection.rs`/`schema.rs` edits. Staged: (1) eq-only + manual
rebuild + drift⇒bypass; (2) ranges/`ne`/multi-filter + index-only; (3)
incremental reconcile; (4) index-ordered `order_by` + parallel build.

---

## Item #5 — sorted/grouped queries (SOLVED, 26,277× less memory measured)

Today `order_by` buffers every matching `Record` (with its body) then sorts.

**Measured** (200k, `order_by read_at desc`, top-20):

| Approach | Buffer | vs today |
|---|---|---|
| full buffer `(key,path,body)` — today | 25.6 MB | 1× |
| keys-only `(key,path)` | 9.7 MB | 2.6× less |
| **top-K bounded heap** | **1,020 B** | **26,277× less** |

(Bodies are tiny here; real KB-body vaults widen this further.)

**Design:** add `.limit(k)` that **fuses with `order_by`** into a bounded
min/max-heap → O(n log K) time, O(K) memory (the GUI "20 most recent" case);
for full sorts, **buffer `(key,path)` tuples not Records** and read full records
back in sorted order (streamed output). If a sorted index exists (Item #1),
emit candidates in index order → no buffer, no sort at all. External merge sort
is the last-resort tail case (millions of full-sorted rows) — build only if
keys-only buffering is ever the bottleneck. Prior art: SQLite `ORDER BY … LIMIT`
top-N, Lucene `TopFieldCollector`. Files: `query.rs` (`limit` field, new
`QueryState`).

---

## Item #3 — directory layout (a NON-ISSUE for speed up to 1M)

**Measured to 1M records on both tmpfs and ext4:** the directory walk, stat,
read, and single-record lookup all scale **strictly linearly — no superlinear
knee through 1M.** Per-100k-records cost is constant (tmpfs ~30 ms, ext4 ~52 ms
per 100k); a single-record lookup is **~8 µs flat regardless of collection size
or layout** (ext4 htree gives O(1)-ish hashed lookup). **The design doc's
"~250k where filesystems get cranky" is not observable — raise or drop it.**

**The earlier "sharding hurts" result was a tmpfs artifact.** On a real disk
(ext4) a 256-way shard *helps* the walk ~2× — 256 small htrees with per-dir
readahead beat one 7 MB monolithic htree that thrashes cache during `getdents`.
On tmpfs (RAM: no blocks, no readahead) sharding just multiplies
`opendir`/`getdents` overhead 256× for no benefit, so it loses. Either way the
walk is only ~9–15% of query time and read+parse (layout-invariant) dominates,
so even the ext4 win moves *total* query time by single-digit %.

**Recommendation (adaptive — justified by ergonomics, not speed):** flat to
~50k; optionally 256-way shard above ~100k **only on a real disk** (detect tmpfs
via `statfs` `f_type` and skip — sharding strictly hurts there); keep leaf dirs
~1–10k entries; **do not two-level shard** (a 65536-way layout was *worse* at
1M). Prefer **semantic/time bucketing** (`notes/2024/03/…`) over opaque hash
fanout to preserve human browseability — a core grexa-db value.
`collect_record_paths` is already recursive, so a sharded layout needs **zero
reader changes**; only placement changes, and migration is a deliberate
caller-driven re-import (path is identity).

---

## Item #6 — inode / storage amplification (IRREDUCIBLE — and the real ceiling)

**Measured (ext4, 4 KiB blocks):** each ~232-byte record rounds up to a full
4 KiB block → **~18× on-disk blowup** (826 MB real for 46 MB of data at 200k),
constant across scale. At 500k records vs a packed single-file store: **~18×
disk space, ~7× slower `cp -r`, ~270× slower delete** — all growing linearly.
**This — not walk latency — is the honest ceiling on huge flat-file
collections,** and sharding makes it slightly *worse* (more directory inodes).

Every "fix" (packing records into shared files) destroys *"one record = one
greppable, editable, ownable file"* — the product itself. FS tricks (Btrfs
tail-packing, reflink) aren't ours to rely on. So #6 is **not** engineered away;
it's accepted as the rent paid for transparency, and grexa-db is aimed where
that rent is worth it. This — plus the reconciliation tax of the editor-owned
write path — is what keeps a finite ceiling. The goal is to push the *speed*
ceiling to low millions (done, above) and be honest that the *storage* ceiling
is where "records are files" actually costs you.

## Prior-art distilled (what we're copying)

- **notmuch** — Xapian sidecar over plain files reconciled by `notmuch new`:
  directory-mtime skip + stable identity + `(lastmod, UUID)` freshness cursor.
  The closest analog; copy its whole model.
- **Tantivy/Lucene** — immutable segments + one atomically-renamed manifest +
  columnar fast-fields + FST/block-postings. (MIT — usable dep.)
- **git** — racy-timestamp guard, 256-fanout, loose-then-pack compaction.
- **mlocate** — `max(ctime,mtime)` per-dir oracle + same-second zero-out.
- **Xapian** — `sortable_serialise` order-preserving keys (one column serves
  sort *and* range via `memcmp`).
- **CouchDB** — `update_seq` freshness cursor: the staleness gap is always
  *visible*, never silent; per-query freshness choice.
- **TileDB** — marker-written-last commit (torn writes are simply invisible).
- **The universal escape hatch** — "can't prove completeness ⇒ full reconcile."

## Combined roadmap

1. **Quick wins (no index):** rayon + hand-parse + lazy body + drop sort + CLI
   path-list fix + top-K/keys-only `order_by`. → ~10× filter, ~26,000× sort
   memory. Days of work, zero format change, zero risk to the invariant.
2. **Index v1:** eq/contains-only, manual rebuild, drift⇒bypass. → selective
   queries ~1,000×.
3. **Index v2:** ranges, `ne`, multi-filter intersection, index-only count/paths,
   index-ordered `order_by`.
4. **Incremental reconcile** (notmuch-style staleness) so edits don't rebuild.
5. **Adaptive sharding** only once the directory knee is actually hit.

After 1–4 the practical crossover plausibly moves from ~250k toward low
millions, every record still a plain file. #6 sets the final ceiling — by
design.
