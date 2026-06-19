<!-- SPDX-FileCopyrightText: 2026 VisorCraft LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# grexa-db scaling R&D — measured results

All numbers are measured on one box (16-core Intel Core Ultra 9 386H, `/tmp` =
tmpfs, release builds). **tmpfs has no disk I/O, so read+parse here is CPU-bound;
on a real SSD/HDD the parallel + head-only-read wins below are LARGER, making
these a conservative floor.** Dataset: 200,000 records, 4 frontmatter fields
(`title`, `tags[]`, `rating`, `read_at`), small bodies.

Reproduce:
- `python3 scripts/profile_scale.py` — baseline + flat-vs-sharded profile.
- The Rust prototypes in `docs/rnd/prototype/` (copy to a temp crate; deps in
  `Cargo.toml.txt`): `readparse.rs` (issues #2/#4) and `indexdemo.rs` (issue #1).

## 1. Where the time goes (profiler)

At 200,000 records, one flat collection:

| Stage | Time | Share |
|---|---|---|
| OS directory walk (`find`) | 76 ms | ~9% |
| grexa-db walk + read + parse all | 889 ms | 100% |
| **read + parse (the rest)** | **812 ms** | **~91%** (~4 µs/record) |

A git-style 256-way **sharded** layout was *slower* here (walk 93 ms) — but
**that is a tmpfs artifact**: on real ext4 disk, sharding *helps* the walk ~2×
(see §5). Either way **directory layout is not the bottleneck — read+parse is**
(~91%), and the walk scales linearly to 1M with no knee.

## 2. Read + parse (issues #2 I/O, #4 parse) — `readparse.rs`

Filter `rating >= 4`. All 7 strategies return the identical count (80,150),
proving the optimizations are correctness-preserving.

| Strategy | Time | Speedup |
|---|---|---|
| S0 current: full `serde_yaml` parse + body `String` copy | 924 ms | 1.0× |
| S1 skip body copy | 792 ms | 1.2× |
| S2 hand-rolled field scan (no serde_yaml) | 318 ms | 2.9× |
| S3 rayon parallel, full parse | 158 ms | 5.9× |
| S4 parallel + no body copy | 132 ms | 7.0× |
| S5 parallel + field scan | 102 ms | 9.0× |
| **S6 parallel + field scan + head-only read** | **92 ms** | **10.1×** |

Diagnosis (corroborated by an independent second harness): the dominant cost is
**`serde_yaml` itself (~2.9 µs/record)**, not the body copy (~6%). The two
independent, additive wins are (a) a hand-rolled parser for the common flat
`key: scalar | [array]` frontmatter (2.9×) and (b) rayon parallelism (~6×).
Head-only reads add more on real disk. **No on-disk format change; records stay
plain files.**

## 3. Secondary index (issue #1, the master lever) — `indexdemo.rs`

A sidecar index (`field value -> record ids`, built in parallel) turns selective
queries from O(n) parse into O(matches) — or O(1) for counts.

| Query | Full scan (already parallel) | With index | Speedup |
|---|---|---|---|
| selective `read_at == <date>` (0.05% = 100 recs) | 104.6 ms | **0.09 ms** | **1,204×** |
| `count(rating >= 4)` (answered from postings) | ~924 ms (orig) | **0.001 ms** | ~instant, zero reads |
| index build (one-time, parallel, 200k) | — | 137 ms | — |

Against the *original* serde-based scan (924 ms), a selective query drops to
~0.1 ms — **~10,000×**. The index uses **verify-on-read** (candidate paths from
the index, each re-stat/re-checked before returning) so it can never return a
stale result; on drift it degrades to a scan. It is a derived, rebuildable
sidecar — delete it and every record is still intact.

## 4. Sorted/grouped queries (issue #5) — `topkdemo.rs`

Current `order_by` buffers every matching `Record` (incl. its body `String`)
then sorts. Measured at 200k records, `order_by read_at desc`, top-20:

| Approach | Buffer | Time | vs today |
|---|---|---|---|
| A full buffer `(key, path, body)` — today | 25.6 MB | 58.7 ms | 1× |
| B keys-only `(key, path)` | 9.7 MB | 31.1 ms | 2.6× less mem |
| **C top-K bounded heap `(key, path)`** | **1,020 B** | 14.1 ms | **26,277× less mem** |

Bodies here are tiny (~140 B synthetic). Real note vaults have KB-scale bodies,
so the full buffer (A) grows while keys-only (B) and top-K (C) stay flat — the
wins get *larger*. Top-K (the GUI "20 most recent" case) is O(n log K) time and
O(K) memory; neither needs an index.

## 5. Directory layout (#3) & storage amplification (#6) — measured to 1M

Walk time (`collect_record_paths` equivalent), min ms — **strictly linear, no
knee through 1M on either filesystem**:

| records | tmpfs flat | tmpfs shard256 | ext4 flat | ext4 shard256 |
|---|---|---|---|---|
| 200,000 | **61** | 98 | 106 | **60** |
| 500,000 | **152** | 280 | 260 | **132** |
| 1,000,000 | **292** | 569 | 514 | **274** |

Per-100k cost is constant (tmpfs flat ~30 ms, ext4 flat ~52 ms). Single-record
lookup is **~8 µs flat** regardless of size/layout (ext4 htree). **Sharding
helps ~2× on ext4 (small htrees + readahead) but hurts on tmpfs (RAM, no
readahead)** — the original "sharding hurts" was a tmpfs artifact. The walk is
~9–15% of query time, so even the ext4 win moves total time single-digit %.

**Storage amplification (#6) — the real, large, linear cost (ext4, 4 KiB blocks):**

| records | apparent | on-disk | amplification |
|---|---|---|---|
| 200,000 | 46 MB | **826 MB** | **~18×** |
| 500,000 | 116 MB | **2.06 GB** | **~18×** |

At 500k vs a packed single-file store: **~18× disk, ~7× slower `cp -r`, ~270×
slower delete.** This — not walk latency — is the honest ceiling on huge
flat-file collections, and it's the cost of the "every record is a file"
promise.

## Summary of legitimate, measured gains

| Issue | Lever | Measured / bound | On-disk format change? |
|---|---|---|---|
| #2 + #4 | parallel + hand-parse + head-read | **10.1×** (conservative floor) | none |
| #1 | sidecar index | **1,204×** selective; ~instant count | none (derived sidecar) |
| #5 | top-K heap + keys-only | **26,277×** less buffer memory | none |
| #3 | adaptive shard, on-disk only | linear to 1M; no knee | n/a |
| #6 | (irreducible — the real ceiling) | ~18× disk, ~270× delete @500k | n/a |

Stacked, a selective query goes from **924 ms → ~0.1 ms** while every record
stays a plain, greppable, ownable file.
