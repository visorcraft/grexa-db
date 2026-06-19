#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 VisorCraft LLC
# SPDX-License-Identifier: Apache-2.0
#
# Scaling profiler: where does grexa-db's time go as record count grows, and
# how much do flat vs sharded directory layouts cost? Generates datasets at
# several sizes and times: a pure OS walk (`find`), grexa-db `records`
# (walk+read+parse all), and a filter query. Establishes the baseline the
# scaling R&D is targeting.
#
#   python3 scripts/profile_scale.py            # default sizes
#   SIZES=50000,200000 python3 scripts/profile_scale.py

import datetime as dt
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CLI = os.environ.get("GREXA_DB_CLI") or str(REPO / "target/release/grexa-db-cli")
SIZES = [int(x) for x in os.environ.get("SIZES", "50000,200000").split(",")]
TAGS = ["rust", "ai", "ml", "db", "linux", "qt", "web", "sec", "perf", "docs"]
DEVNULL = subprocess.DEVNULL

SCHEMA = ("---\ncollection: notes\nfields:\n"
          "  - { name: title, type: string, required: true }\n"
          '  - { name: tags, type: "array<string>" }\n'
          "  - { name: rating, type: integer, range: [1, 5] }\n"
          "  - { name: read_at, type: date }\n---\n\n# Notes\n")


def record_text(i):
    rnd = random.Random(i)
    tags = sorted(rnd.sample(TAGS, rnd.randint(1, 3)))
    rating = rnd.randint(1, 5)
    day = dt.date(2020, 1, 1) + dt.timedelta(days=i % 2000)
    body = " ".join(rnd.choice(TAGS) for _ in range(20))
    return (f"---\ntitle: Note {i:07d}\ntags: [{', '.join(tags)}]\n"
            f"rating: {rating}\nread_at: {day.isoformat()}\n---\n\n{body}\n")


def gen_flat(root, n):
    notes = root / "flat" / "notes"
    notes.mkdir(parents=True)
    (notes / "schema.md").write_text(SCHEMA)
    for i in range(n):
        (notes / f"note-{i:07d}.md").write_text(record_text(i))
    return root / "flat"


def gen_sharded(root, n, fanout=256):
    # git-style: bucket records into 2-hex-char subdirs so no single dir holds
    # more than ~n/256 entries.
    notes = root / "sharded" / "notes"
    notes.mkdir(parents=True)
    (notes / "schema.md").write_text(SCHEMA)
    made = set()
    for i in range(n):
        h = f"{(i * 2654435761) & 0xff:02x}"  # cheap hash -> 256 buckets
        if h not in made:
            (notes / h).mkdir()
            made.add(h)
        (notes / h / f"note-{i:07d}.md").write_text(record_text(i))
    return root / "sharded"


def timed(cmd, runs=3, shell=False):
    ts = []
    for _ in range(runs):
        t0 = time.perf_counter()
        subprocess.run(cmd, stdout=DEVNULL, stderr=DEVNULL, shell=shell)
        ts.append(time.perf_counter() - t0)
    return statistics.median(ts) * 1000  # ms


def main():
    work = Path(tempfile.mkdtemp(prefix="grexa-db-prof-"))
    print(f"# grexa-db scaling profile  (CLI={CLI})")
    print(f"{'N':>9} {'layout':>8} {'find_ms':>9} {'records_ms':>11} "
          f"{'filter_ms':>10} {'read+parse_ms':>14} {'us/record':>10}")
    for n in SIZES:
        d = Path(tempfile.mkdtemp(prefix=f"n{n}-", dir=work))
        flat = gen_flat(d, n)
        fnotes = flat / "notes"
        find_ms = timed(f"find {fnotes} -name '*.md'", shell=True)
        records_ms = timed([CLI, str(flat), "records", "notes"])
        filter_ms = timed([CLI, str(flat), "query", "notes",
                           "--filter", "rating:ge:4"])
        readparse = records_ms - find_ms
        print(f"{n:>9} {'flat':>8} {find_ms:>9.1f} {records_ms:>11.1f} "
              f"{filter_ms:>10.1f} {readparse:>14.1f} {readparse*1000/n:>10.2f}")

        sharded = gen_sharded(d, n)
        snotes = sharded / "notes"
        sfind_ms = timed(f"find {snotes} -name '*.md'", shell=True)
        srecords_ms = timed([CLI, str(sharded), "records", "notes"])
        sfilter_ms = timed([CLI, str(sharded), "query", "notes",
                            "--filter", "rating:ge:4"])
        sreadparse = srecords_ms - sfind_ms
        print(f"{n:>9} {'sharded':>8} {sfind_ms:>9.1f} {srecords_ms:>11.1f} "
              f"{sfilter_ms:>10.1f} {sreadparse:>14.1f} {sreadparse*1000/n:>10.2f}")
        shutil.rmtree(d, ignore_errors=True)
    shutil.rmtree(work, ignore_errors=True)
    print("\nfind_ms = OS directory walk; records_ms = grexa walk+read+parse all;")
    print("read+parse_ms = records_ms - find_ms (grexa's own read+parse cost).")


if __name__ == "__main__":
    main()
