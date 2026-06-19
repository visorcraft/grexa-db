<!-- SPDX-FileCopyrightText: 2026 VisorCraft LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Scaling R&D prototypes

Standalone micro-benchmarks that mirror grexa-db's read+parse+filter hot path to
**measure** the gains discussed in [`../../grexa-db-scaling-rnd.md`](../../grexa-db-scaling-rnd.md).
They are *not* wired into the workspace (deliberately — they pull `rayon`); run
them as a throwaway crate:

```bash
mkdir /tmp/gdbproto && cd /tmp/gdbproto
cargo init --name gdbproto -q
cp /path/to/docs/rnd/prototype/Cargo.toml.txt Cargo.toml
mkdir -p src/bin
cp /path/to/docs/rnd/prototype/readparse.rs  src/main.rs
cp /path/to/docs/rnd/prototype/indexdemo.rs  src/bin/indexdemo.rs
cp /path/to/docs/rnd/prototype/topkdemo.rs   src/bin/topkdemo.rs

# generate a dataset (any grexa-db collection works); e.g. reuse the profiler's:
#   python3 ../../scripts/profile_scale.py   leaves none behind, so generate one:
python3 - <<'PY'
import random, datetime as dt, pathlib
TAGS=["rust","ai","ml","db","linux","qt","web","sec","perf","docs"]
root=pathlib.Path("/tmp/gdb_bench_data/notes"); root.mkdir(parents=True, exist_ok=True)
(root/"schema.md").write_text("---\ncollection: notes\nfields: []\n---\n")
for i in range(200000):
    r=random.Random(i); tags=sorted(r.sample(TAGS,r.randint(1,3))); rating=r.randint(1,5)
    day=dt.date(2020,1,1)+dt.timedelta(days=i%2000)
    (root/f"note-{i:07d}.md").write_text(
      f"---\ntitle: Note {i:07d}\ntags: [{', '.join(tags)}]\nrating: {rating}\nread_at: {day.isoformat()}\n---\n\nbody\n")
PY

cargo run --release --bin gdbproto    /tmp/gdb_bench_data/notes   # issues #2/#4
cargo run --release --bin indexdemo   /tmp/gdb_bench_data/notes   # issue  #1
cargo run --release --bin topkdemo    /tmp/gdb_bench_data/notes   # issue  #5
```

- **`readparse.rs`** (the `gdbproto` bin) — issues #2/#4: serial full-serde vs
  hand-parse vs rayon parallel vs head-only read. Mirrors `Record::from_content`.
- **`indexdemo.rs`** — issue #1: builds a sidecar index and answers a selective
  query reading only candidates, plus an index-only count.
- **`topkdemo.rs`** — issue #5: full-buffer vs keys-only vs top-K-heap memory.

All measured numbers from these are recorded in
[`../measured-results.md`](../measured-results.md). They mirror the engine's
logic but are not the engine — they exist to size the wins before implementing
them in `crates/grexa-db`.
