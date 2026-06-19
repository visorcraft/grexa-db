// Prototype: measure issue #5. Current order_by buffers every matching Record
// (incl. its body String) then sorts. Compare buffer memory + time for:
//   A) full buffer of (key, path, body)  -- today's behavior
//   B) keys-only buffer of (key, path)    -- cheap win, no index
//   C) top-K bounded heap of (key, path)  -- the GUI "20 most recent" case
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

fn collect_paths(root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else { continue };
        for e in entries.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.starts_with('.') || name == "schema.md" { continue }
            let Ok(ft) = e.file_type() else { continue };
            if ft.is_symlink() { continue }
            if ft.is_dir() { stack.push(e.path()); continue }
            paths.push(e.path());
        }
    }
    paths
}
fn split<'a>(c: &'a str) -> (Option<&'a str>, &'a str) {
    let s = c.strip_prefix('\u{feff}').unwrap_or(c);
    let Some(rest) = s.strip_prefix("---\r\n").or_else(|| s.strip_prefix("---\n"))
        else { return (None, c) };
    let mut off = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end() == "---" {
            return (Some(&rest[..off]), &rest[off + line.len()..]);
        }
        off += line.len();
    }
    (None, c)
}
fn field<'a>(head: &'a str, key: &str) -> Option<&'a str> {
    head.lines().find_map(|l| l.strip_prefix(key)?.strip_prefix(':').map(|v| v.trim()))
}

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1)
        .unwrap_or_else(|| "/tmp/gdb_bench_data/notes".into()));
    let k = 20usize;
    let paths = collect_paths(&dir);
    let n = paths.len();

    // Read+extract once: (key=read_at, path, body) for every record.
    let rows: Vec<(String, String, String)> = paths.iter().map(|p| {
        let c = fs::read_to_string(p).unwrap_or_default();
        let (h, body) = split(&c);
        let key = h.and_then(|h| field(h, "read_at")).unwrap_or("").to_string();
        (key, p.to_string_lossy().into_owned(), body.to_string())
    }).collect();

    // --- A) FULL BUFFER (today): hold (key, path, body) for all, sort, take k.
    let a_bytes: usize = rows.iter().map(|(k, p, b)| k.len() + p.len() + b.len()).sum();
    let t = Instant::now();
    let mut full = rows.clone();
    full.sort_by(|a, b| b.0.cmp(&a.0)); // desc
    let _topA: Vec<_> = full.into_iter().take(k).collect();
    let a_ms = t.elapsed().as_secs_f64() * 1000.0;

    // --- B) KEYS-ONLY BUFFER: hold (key, path), sort, take k.
    let b_bytes: usize = rows.iter().map(|(k, p, _)| k.len() + p.len()).sum();
    let t = Instant::now();
    let mut keys: Vec<(String, String)> =
        rows.iter().map(|(k, p, _)| (k.clone(), p.clone())).collect();
    keys.sort_by(|a, b| b.0.cmp(&a.0));
    let _topB: Vec<_> = keys.into_iter().take(k).collect();
    let b_ms = t.elapsed().as_secs_f64() * 1000.0;

    // --- C) TOP-K HEAP: one pass, hold at most k (key, path).
    let t = Instant::now();
    let mut heap: BinaryHeap<Reverse<(String, String)>> = BinaryHeap::new();
    for (key, path, _) in &rows {
        heap.push(Reverse((key.clone(), path.clone())));
        if heap.len() > k { heap.pop(); } // evict the smallest -> keeps k largest
    }
    let c_bytes: usize = heap.iter().map(|Reverse((k, p))| k.len() + p.len()).sum();
    let mut topC: Vec<_> = heap.into_iter().map(|Reverse(x)| x).collect();
    topC.sort_by(|a, b| b.0.cmp(&a.0));
    let c_ms = t.elapsed().as_secs_f64() * 1000.0;

    let mb = |b: usize| b as f64 / 1_048_576.0;
    println!("records: {n}   top-K: {k}   (sort key = read_at, desc)\n");
    println!("{:<28} {:>12} {:>12}", "approach", "buffer", "time");
    println!("{:<28} {:>10.1} MB {:>9.1} ms   (today)", "A full buffer (key,path,body)", mb(a_bytes), a_ms);
    println!("{:<28} {:>10.1} MB {:>9.1} ms   {:.1}x less mem", "B keys-only (key,path)", mb(b_bytes), b_ms, a_bytes as f64 / b_bytes as f64);
    println!("{:<28} {:>10} B  {:>9.1} ms   {:.0}x less mem", "C top-K heap (key,path)", c_bytes, c_ms, a_bytes as f64 / c_bytes as f64);
    println!("\nnote: bodies here are ~140 B (synthetic). Real note vaults have");
    println!("KB-scale bodies, so A grows and the keys-only / top-K wins get larger.");
}
