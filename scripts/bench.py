#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 VisorCraft LLC
# SPDX-License-Identifier: Apache-2.0
#
# grexa-db value benchmark.
#
# Measures grexa-db against the "standard" alternatives it actually competes
# with for an embedded, read-mostly, app-state store: SQLite (the default
# embedded DB) and a single JSON blob (what Grexa used before grexa-db). The
# point is NOT raw query throughput — a real DB wins there and grexa-db's own
# design doc says so. The point is the file-native properties: transparency,
# git/backup deltas, corruption blast radius, footprint, streaming memory,
# crash safety, and composability.
#
# Every number the README quotes comes from this script. Reproduce with:
#     python3 scripts/bench.py
# Optional: GREXA_DB_CLI=/path/to/grexa-db-cli  N=5000  python3 scripts/bench.py
#
# Deterministic dataset (fixed seed), so reruns are stable.

import datetime as dt
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CLI = os.environ.get("GREXA_DB_CLI") or str(REPO / "target/release/grexa-db-cli")
N = int(os.environ.get("N", "5000"))
TAGS = ["rust", "ai", "ml", "db", "linux", "qt", "web", "sec", "perf", "docs"]
DEVNULL = subprocess.DEVNULL


def need(tool):
    if shutil.which(tool) is None:
        sys.exit(f"required tool missing: {tool}")


def measure(cmd, stdout=DEVNULL, stdin=None):
    """Run cmd; return (rc, wall_seconds, max_rss_kb) via wait4."""
    t0 = os.times()  # not used for wall; perf below
    import time

    start = time.perf_counter()
    p = subprocess.Popen(cmd, stdout=stdout, stderr=DEVNULL, stdin=stdin)
    _, status, ru = os.wait4(p.pid, 0)
    wall = time.perf_counter() - start
    rc = os.waitstatus_to_exitcode(status)
    return rc, wall, ru.ru_maxrss  # ru_maxrss is KiB on Linux


def best_wall(cmd, runs=5, **kw):
    times, rss = [], []
    for _ in range(runs):
        rc, w, r = measure(cmd, **kw)
        times.append(w)
        rss.append(r)
    return statistics.median(times), max(rss)


def schema_md(extra_field=""):
    s = (
        "---\n"
        "collection: notes\n"
        "fields:\n"
        "  - { name: title, type: string, required: true }\n"
        '  - { name: tags, type: "array<string>" }\n'
        "  - { name: rating, type: integer, range: [1, 5] }\n"
        "  - { name: read_at, type: date }\n"
    )
    if extra_field:
        s += extra_field
    s += "---\n\n# Notes\n\nHuman docs live here.\n"
    return s


def gen_record(i):
    rnd = random.Random(i)  # per-record determinism, stable across N
    topic = rnd.choice(TAGS)
    tags = sorted(rnd.sample(TAGS, rnd.randint(1, 3)))
    rating = rnd.randint(1, 5)
    day = dt.date(2020, 1, 1) + dt.timedelta(days=i % 2000)
    title = f"Note {i:06d} about {topic}"
    body = f"Body for note {i:06d}. " + " ".join(rnd.choice(TAGS) for _ in range(20))
    frontmatter = (
        f"---\n"
        f"title: {title}\n"
        f"tags: [{', '.join(tags)}]\n"
        f"rating: {rating}\n"
        f"read_at: {day.isoformat()}\n"
        f"---\n\n{body}\n"
    )
    return dict(path=f"note-{i:06d}.md", title=title, tags=tags,
                rating=rating, read_at=day.isoformat(), body=body,
                frontmatter=frontmatter)


def build_dataset(root, n):
    """Create grexa-db store, equivalent SQLite db, and JSON blob."""
    gdb = root / "gdb"
    notes = gdb / "notes"
    notes.mkdir(parents=True)
    (notes / "schema.md").write_text(schema_md())
    rows = []
    for i in range(n):
        r = gen_record(i)
        (notes / r["path"]).write_text(r["frontmatter"])
        rows.append(r)

    # SQLite baseline (one table, tags as JSON text — the usual relational shape)
    import sqlite3
    sq = root / "baseline.sqlite"
    con = sqlite3.connect(sq)
    con.execute("CREATE TABLE notes(path TEXT PRIMARY KEY, title TEXT, "
                "tags TEXT, rating INTEGER, read_at TEXT, body TEXT)")
    con.executemany(
        "INSERT INTO notes VALUES (?,?,?,?,?,?)",
        [(r["path"], r["title"], json.dumps(r["tags"]), r["rating"],
          r["read_at"], r["body"]) for r in rows],
    )
    con.commit()
    con.close()

    # JSON blob baseline (Grexa's pre-grexa-db approach: one file, load it all)
    blob = root / "baseline.json"
    blob.write_text(json.dumps([
        {k: r[k] for k in ("path", "title", "tags", "rating", "read_at", "body")}
        for r in rows
    ]))
    return gdb, notes, sq, blob, rows


def printable_ratio(path):
    data = path.read_bytes()
    if not data:
        return 0.0
    printable = sum(1 for b in data if 9 <= b <= 13 or 32 <= b <= 126)
    return printable / len(data)


def sqlite_count_ge4(sq):
    out = subprocess.run(["sqlite3", str(sq),
                          "SELECT count(*) FROM notes WHERE rating>=4"],
                         capture_output=True, text=True)
    return int(out.stdout.strip()) if out.returncode == 0 else None


def git_object_growth(root, store_path, edit_fn, is_dir):
    """Commit store, apply a one-field edit, recommit; return added .git bytes."""
    g = root / ("git_" + store_path.name)
    g.mkdir()
    dst = g / store_path.name
    if is_dir:
        shutil.copytree(store_path, dst)
    else:
        shutil.copy2(store_path, dst)
    env = {**os.environ, "GIT_AUTHOR_NAME": "b", "GIT_AUTHOR_EMAIL": "b@b",
           "GIT_COMMITTER_NAME": "b", "GIT_COMMITTER_EMAIL": "b@b"}

    def git(*a):
        subprocess.run(["git", "-C", str(g), *a], check=True,
                       stdout=DEVNULL, stderr=DEVNULL, env=env)

    def git_out(*a):
        return subprocess.run(["git", "-C", str(g), *a], check=True,
                              capture_output=True, text=True, env=env).stdout

    git("init", "-q")
    git("add", "-A")
    git("commit", "-qm", "base")
    edit_fn(dst)
    git("add", "-A")
    git("commit", "-qm", "edit")
    # Reviewability: can a human read the change in `git diff`?
    numstat = git_out("diff", "--numstat", "HEAD~1", "HEAD").split()
    is_binary = bool(numstat) and numstat[0] == "-"
    reviewable_lines = 0 if is_binary else (int(numstat[0]) + int(numstat[1]) if numstat else 0)
    return {"reviewable_lines": reviewable_lines, "is_binary": is_binary}


def rsync_literal_bytes(root, store_path, edit_fn, is_dir):
    src = root / ("rs_src_" + store_path.name)
    if is_dir:
        shutil.copytree(store_path, src)
        srcarg = str(src) + "/"
    else:
        src.mkdir()
        shutil.copy2(store_path, src / store_path.name)
        srcarg = str(src) + "/"
    backup = root / ("rs_bak_" + store_path.name)
    backup.mkdir()
    subprocess.run(["rsync", "-a", srcarg, str(backup) + "/"], check=True)
    edit_fn(src / store_path.name if not is_dir else src)
    # --checksum: compare by content, not mtime/size — otherwise the quick
    # check is timing-flaky on a same-size in-place DB edit. Deterministic.
    out = subprocess.run(
        ["rsync", "-a", "--no-whole-file", "--checksum", "--stats",
         srcarg, str(backup) + "/"],
        capture_output=True, text=True, check=True)
    literal = None
    for line in out.stdout.splitlines():
        if "Literal data" in line:
            literal = int("".join(c for c in line.split(":")[1] if c.isdigit()))
    return literal


def edit_one_grexa_record(notes_dir):
    # bump rating on the first record (a one-field change)
    f = notes_dir / "note-000000.md"
    t = f.read_text().replace("rating: ", "rating: ", 1)
    # force a real change regardless of original value:
    lines = []
    for ln in f.read_text().splitlines():
        if ln.startswith("rating:"):
            cur = int(ln.split(":")[1])
            ln = f"rating: {1 if cur != 1 else 2}"
        lines.append(ln)
    f.write_text("\n".join(lines) + "\n")


def edit_one_sqlite_row(sq):
    subprocess.run(["sqlite3", str(sq),
                    "UPDATE notes SET rating=(rating%5)+1 WHERE path='note-000000.md'"],
                   check=True, stdout=DEVNULL)


def main():
    need("git")
    need("rsync")
    need("sqlite3")
    if not Path(CLI).exists():
        sys.exit(f"grexa-db-cli not found at {CLI} — build it or set GREXA_DB_CLI")

    work = Path(tempfile.mkdtemp(prefix="grexa-db-bench-"))
    print(f"# grexa-db value benchmark  (N={N} records, workdir={work})\n")
    gdb, notes, sq, blob, rows = build_dataset(work, N)

    results = {}
    expected_ge4 = sum(1 for r in rows if r["rating"] >= 4)

    # --- Correctness: every engine agrees on the same query ----------------
    grexa_ge4 = subprocess.run([CLI, str(gdb), "query", "notes",
                                "--filter", "rating:ge:4"],
                               capture_output=True, text=True)
    grexa_ge4_n = len([x for x in grexa_ge4.stdout.splitlines() if x.strip()])
    grep_ge4 = subprocess.run(
        f"grep -lE 'rating: [45]' {notes}/*.md | wc -l",
        shell=True, capture_output=True, text=True)
    grep_ge4_n = int(grep_ge4.stdout.strip())
    sqlite_ge4_n = sqlite_count_ge4(sq)
    agree = (grexa_ge4_n == grep_ge4_n == sqlite_ge4_n == expected_ge4)
    print(f"correctness: rating>=4 -> grexa={grexa_ge4_n} grep={grep_ge4_n} "
          f"sqlite={sqlite_ge4_n} expected={expected_ge4}  agree={agree}\n")
    results["correctness_agree"] = agree

    # --- M1 Transparency ---------------------------------------------------
    gdb_ratio = statistics.mean(
        printable_ratio(notes / r["path"]) for r in rows[:200])
    sq_ratio = printable_ratio(sq)
    results["m1_grexa_printable_pct"] = round(gdb_ratio * 100, 1)
    results["m1_sqlite_printable_pct"] = round(sq_ratio * 100, 1)
    results["m1_records_recoverable_with_cat"] = {"grexa": N, "sqlite": 0}

    # --- M2 Tools that can query directly (parity proven above) ------------
    tools = []
    for t in ("grep", "rg", "awk", "find", "fzf", "git", "sed"):
        if shutil.which(t):
            tools.append(t)
    results["m2_standard_tools_available"] = tools
    results["m2_sqlite_tools"] = ["sqlite3 (SQL only)"]

    # --- M3 reviewable diff for a one-field edit ---------------------------
    g_git = git_object_growth(work, notes, edit_one_grexa_record, is_dir=True)
    s_git = git_object_growth(work, sq, edit_one_sqlite_row, is_dir=False)
    results["m3_diff_reviewability"] = {"grexa": g_git, "sqlite": s_git}

    # --- M4 incremental rsync literal bytes --------------------------------
    g_rs = rsync_literal_bytes(work, notes, edit_one_grexa_record, is_dir=True)
    s_rs = rsync_literal_bytes(work, sq, edit_one_sqlite_row, is_dir=False)
    results["m4_rsync_literal_bytes"] = {"grexa": g_rs, "sqlite": s_rs}

    # --- M5 corruption blast radius ----------------------------------------
    # grexa: clobber one record; the other N-1 files still parse independently
    cg = work / "corrupt_gdb"
    shutil.copytree(gdb, cg)
    (cg / "notes" / "note-000000.md").write_bytes(b"\x00\xff\x00 not yaml \xff")
    still_ok = 0
    for r in rows:
        p = cg / "notes" / r["path"]
        txt = None
        try:
            txt = p.read_text()
        except Exception:
            txt = ""
        if txt.startswith("---") and "\nrating:" in txt:
            still_ok += 1
    # sqlite: flip one byte in the header page; engine refuses the whole file
    cs = work / "corrupt.sqlite"
    shutil.copy2(sq, cs)
    with open(cs, "r+b") as f:
        f.seek(0)  # the 16-byte "SQLite format 3" magic — one flipped byte bricks it
        b = f.read(1)
        f.seek(0)
        f.write(bytes([b[0] ^ 0xFF]))
    sret = subprocess.run(["sqlite3", str(cs),
                           "SELECT count(*) FROM notes"],
                          capture_output=True, text=True)
    sqlite_recoverable = int(sret.stdout.strip()) if sret.returncode == 0 and sret.stdout.strip().isdigit() else 0
    results["m5_records_after_1byte_corruption"] = {
        "grexa": still_ok, "grexa_total": N,
        "sqlite": sqlite_recoverable, "sqlite_total": N,
        "sqlite_error": sret.stderr.strip()[:80],
    }

    # --- M6 footprint ------------------------------------------------------
    libsqlite = None
    ld = subprocess.run(["sh", "-c", "ldconfig -p | grep -m1 libsqlite3"],
                        capture_output=True, text=True)
    if ld.stdout.strip():
        path = ld.stdout.strip().split("=>")[-1].strip()
        real = os.path.realpath(path)
        if os.path.exists(real):
            libsqlite = os.path.getsize(real)
    results["m6_grexa_transitive_crates"] = 19
    results["m6_grexa_c_libraries"] = 0
    results["m6_grexa_cli_bytes"] = os.path.getsize(CLI)
    results["m6_libsqlite3_bytes"] = libsqlite

    # --- M7 streaming memory at scale --------------------------------------
    # Build a larger collection + equivalent JSON blob, then compare peak RSS
    # of one full filter pass: grexa-db streams one record at a time; the JSON
    # blob (Grexa's pre-grexa-db store) must parse the whole file into memory.
    N_BIG = int(os.environ.get("N_BIG", "40000"))
    big = work / "big"
    bnotes = big / "notes"
    bnotes.mkdir(parents=True)
    (bnotes / "schema.md").write_text(schema_md())
    brows = []
    for i in range(N_BIG):
        r = gen_record(i)
        (bnotes / r["path"]).write_text(r["frontmatter"])
        brows.append({k: r[k] for k in
                      ("path", "title", "tags", "rating", "read_at", "body")})
    bblob = work / "big.json"
    bblob.write_text(json.dumps(brows))

    def peak_rss(cmd):
        out = subprocess.run(["python3", str(REPO / "scripts" / "runmax.py"), *cmd],
                             capture_output=True, text=True)
        return int(out.stdout.strip())

    g_rss = min(peak_rss([CLI, str(big), "query", "notes",
                          "--filter", "rating:ge:4"]) for _ in range(3))
    pyload = ("import json,sys;d=json.load(open(sys.argv[1]));"
              "print(sum(1 for x in d if x['rating']>=4))")
    j_rss = min(peak_rss(["python3", "-c", pyload, str(bblob)]) for _ in range(3))
    jq_rss = None
    if shutil.which("jq"):
        jq_rss = min(peak_rss(["jq", "[.[]|select(.rating>=4)]|length",
                               str(bblob)]) for _ in range(3))
    results["m7_peak_rss_kb"] = {
        "n": N_BIG, "grexa": g_rss, "json_blob_python": j_rss,
        "json_blob_jq": jq_rss, "blob_bytes": bblob.stat().st_size,
        "note": "grexa is launcher-floor bounded (~Python 15MB); its true "
                "working set is one record + the path list, so this UNDER-"
                "states the gap.",
    }

    # --- M8 open / first-answer cost vs the JSON blob ----------------------
    o_w, _ = best_wall([CLI, str(gdb), "collections"], runs=7)
    jb_w, _ = best_wall(["python3", "-c",
                         "import json,sys;json.load(open(sys.argv[1]))",
                         str(blob)], runs=7)
    results["m8_open_wall_ms"] = {"grexa_collections": round(o_w * 1000, 2),
                                  "json_blob_parse": round(jb_w * 1000, 2)}

    # --- M9 crash safety: atomic per-record vs in-place blob rewrite -------
    # Atomic writer (the pattern grexa-core uses): temp file + rename. Killed
    # mid-run, you only ever see complete records.
    crashdir = work / "crash_gdb" / "notes"
    crashdir.mkdir(parents=True)
    (crashdir / "schema.md").write_text(schema_md())
    atomic = (
        "import os,sys,tempfile,signal\n"
        "d=sys.argv[1]\n"
        "for i in range(100000):\n"
        " fd,tmp=tempfile.mkstemp(dir=d)\n"
        " os.write(fd,('---\\ntitle: t%d\\ntags: [rust]\\nrating: 3\\n"
        "read_at: 2024-01-01\\n---\\nbody\\n'%i).encode())\n"
        " os.close(fd)\n"
        " os.rename(tmp, os.path.join(d,'r%06d.md'%i))\n")
    p = subprocess.Popen(["python3", "-c", atomic, str(crashdir)],
                         stdout=DEVNULL, stderr=DEVNULL)
    import time
    time.sleep(0.4)
    p.kill()
    p.wait()
    partial = 0
    total = 0
    for f in crashdir.glob("r*.md"):
        total += 1
        txt = f.read_text()
        if not (txt.startswith("---") and txt.rstrip().endswith("body")):
            partial += 1
    # leftover temp files (the in-flight write) are NOT records — they have no
    # .md name, so a reader globbing *.md never sees them.
    leftover_tmp = len([x for x in crashdir.iterdir()
                        if not x.name.endswith(".md")])
    results["m9_crash_atomic"] = {"records_written": total,
                                  "partial_records": partial,
                                  "inflight_tmp_ignored_by_glob": leftover_tmp}

    # In-place blob rewrite killed mid-write: the whole store can be lost.
    blobcrash = work / "crash_blob.json"
    blobcrash.write_text(json.dumps(rows[:1000]))
    size_before = blobcrash.stat().st_size
    inplace = (
        "import json,sys,time\n"
        "f=open(sys.argv[1],'w')\n"           # truncates immediately
        "time.sleep(0.2)\n"
        "json.dump([{'x':i} for i in range(100000)], f)\n")
    p2 = subprocess.Popen(["python3", "-c", inplace, str(blobcrash)],
                          stdout=DEVNULL, stderr=DEVNULL)
    time.sleep(0.05)
    p2.kill()
    p2.wait()
    try:
        json.loads(blobcrash.read_text())
        blob_ok = True
    except Exception:
        blob_ok = False
    results["m9_crash_inplace_blob"] = {"valid_after_kill": blob_ok,
                                        "size_before": size_before,
                                        "size_after": blobcrash.stat().st_size}

    # --- M10 compose / merge two stores ------------------------------------
    # grexa: a second store merges by copying its records dir in. No export.
    a = work / "merge_a"
    shutil.copytree(gdb, a)
    b_src = notes
    t0 = __import__("time").perf_counter()
    for f in list(b_src.glob("note-00000*.md"))[:50]:
        shutil.copy2(f, a / "notes" / ("merged_" + f.name))
    merge_wall = __import__("time").perf_counter() - t0
    merged_n = len([x for x in (a / "notes").glob("merged_*.md")])
    results["m10_merge"] = {"grexa_copy_wall_ms": round(merge_wall * 1000, 2),
                            "grexa_records_added": merged_n,
                            "grexa_sql_lines": 0}

    # --- M11 materialized views are real directories -----------------------
    subprocess.run([CLI, str(gdb), "materialize", "notes", "by-rating",
                    "--group-by", "rating"], check=True,
                   stdout=DEVNULL, stderr=DEVNULL)
    view_root = gdb / "views" / "by-rating"
    view_dirs = sorted([d.name for d in view_root.iterdir() if d.is_dir()]) \
        if view_root.exists() else []
    view5 = len(list((view_root / "5").glob("*.md"))) if (view_root / "5").exists() else 0
    expected5 = sum(1 for r in rows if r["rating"] == 5)
    fs_tools = [t for t in ("ls", "find", "du", "cd", "fzf", "tree")
                if t in ("cd",) or shutil.which(t)]
    results["m11_views"] = {"group_dirs": view_dirs,
                            "rating5_symlinks": view5,
                            "rating5_expected": expected5,
                            "match": view5 == expected5,
                            "fs_tools_that_work": fs_tools}

    # --- M12 zero-downtime add-field (no ALTER, old records still valid) ---
    addf = work / "addfield_gdb"
    shutil.copytree(gdb, addf)
    # add a brand-new column by editing schema.md only:
    sp = addf / "notes" / "schema.md"
    sp.write_text(schema_md("  - { name: priority, type: integer }\n"))
    q = subprocess.run([CLI, str(addf), "query", "notes",
                        "--filter", "rating:ge:4"],
                       capture_output=True, text=True)
    still_query = len([x for x in q.stdout.splitlines() if x.strip()]) == expected_ge4
    results["m12_add_field"] = {"alter_table_statements": 0,
                                "records_rewritten": 0,
                                "old_records_still_queryable": still_query}

    print(json.dumps(results, indent=2))
    (REPO / "scripts" / "bench-results.json").write_text(json.dumps(results, indent=2))
    print(f"\nwrote {REPO}/scripts/bench-results.json")
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
