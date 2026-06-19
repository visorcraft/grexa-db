// Prototype: measure the index win (#1). Build a sidecar index (field value ->
// record ids), then answer (a) a count entirely from the index with ZERO record
// reads, and (b) a selective query by reading only the matching records — vs the
// current full-scan baseline. Proves the "master lever" empirically.
use rayon::prelude::*;
use std::collections::BTreeMap;
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
            if name.starts_with('.') { continue }
            let Ok(ft) = e.file_type() else { continue };
            if ft.is_symlink() { continue }
            if ft.is_dir() { stack.push(e.path()); continue }
            if name == "schema.md" { continue }
            paths.push(e.path());
        }
    }
    paths.sort();
    paths
}

fn head_slice(content: &str) -> Option<&str> {
    let s = content.strip_prefix('\u{feff}').unwrap_or(content);
    let rest = s.strip_prefix("---\r\n").or_else(|| s.strip_prefix("---\n"))?;
    let mut off = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end() == "---" { return Some(&rest[..off]); }
        off += line.len();
    }
    None
}
fn field<'a>(head: &'a str, key: &str) -> Option<&'a str> {
    for line in head.lines() {
        if let Some(r) = line.strip_prefix(key) {
            if let Some(v) = r.strip_prefix(':') { return Some(v.trim()); }
        }
    }
    None
}
// read only the frontmatter prefix from disk (no body)
fn read_head(p: &Path) -> Option<String> {
    use std::io::Read;
    let mut f = fs::File::open(p).ok()?;
    let mut buf = vec![0u8; 4096];
    let n = f.read(&mut buf).ok()?;
    String::from_utf8(buf[..n].to_vec()).ok()
}

struct Index {
    paths: Vec<PathBuf>,
    rating: BTreeMap<i64, Vec<u32>>,   // sorted -> range queries
    read_at: BTreeMap<String, Vec<u32>>,
}

fn build_index(paths: Vec<PathBuf>) -> Index {
    // parallel extract (id, rating, read_at)
    let rows: Vec<(u32, Option<i64>, Option<String>)> = paths
        .par_iter().enumerate()
        .map(|(i, p)| {
            let c = read_head(p).unwrap_or_default();
            let h = head_slice(&c).unwrap_or("");
            (i as u32, field(h, "rating").and_then(|v| v.parse().ok()),
             field(h, "read_at").map(|s| s.to_string()))
        }).collect();
    let mut rating: BTreeMap<i64, Vec<u32>> = BTreeMap::new();
    let mut read_at: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    for (id, r, d) in rows {
        if let Some(r) = r { rating.entry(r).or_default().push(id); }
        if let Some(d) = d { read_at.entry(d).or_default().push(id); }
    }
    Index { paths, rating, read_at }
}

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1)
        .unwrap_or_else(|| "/tmp/gdb_bench_data/notes".into()));
    let paths = collect_paths(&dir);
    let n = paths.len();
    println!("records: {n}   threads: {}", rayon::current_num_threads());

    let t = Instant::now();
    let idx = build_index(paths.clone());
    let build_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!("index build (parallel, one-time): {build_ms:.1} ms   \
              ({} distinct read_at, {} distinct rating)",
             idx.read_at.len(), idx.rating.len());

    // pick the most selective read_at value to demo a selective query
    let (date, ids) = idx.read_at.iter()
        .min_by_key(|(_, v)| v.len()).map(|(k, v)| (k.clone(), v.clone())).unwrap();
    let sel_pct = 100.0 * ids.len() as f64 / n as f64;

    // --- BASELINE: full scan to answer `read_at == date` (read+parse every record)
    let t = Instant::now();
    let base_hits: usize = paths.par_iter().filter(|p| {
        let c = fs::read_to_string(p).unwrap_or_default();
        head_slice(&c).and_then(|h| field(h, "read_at").map(|d| d == date)).unwrap_or(false)
    }).count();
    let base_ms = t.elapsed().as_secs_f64() * 1000.0;

    // --- INDEX: read_at == date -> candidate ids -> read ONLY those (verify-on-read)
    let t = Instant::now();
    let hits: usize = ids.par_iter().filter(|&&id| {
        let c = read_head(&idx.paths[id as usize]).unwrap_or_default();
        head_slice(&c).and_then(|h| field(h, "read_at").map(|d| d == date)).unwrap_or(false)
    }).count();
    let idx_ms = t.elapsed().as_secs_f64() * 1000.0;

    // --- INDEX-ONLY: count(rating>=4) answered from postings, ZERO record reads
    let t = Instant::now();
    let cnt: usize = idx.rating.range(4..).map(|(_, v)| v.len()).sum();
    let cnt_ms = t.elapsed().as_secs_f64() * 1000.0;

    println!();
    println!("selective query: read_at == {date}  ({} matches, {:.3}% selective)",
             ids.len(), sel_pct);
    println!("  full scan (read+parse all {n}):   {base_ms:8.2} ms   hits={base_hits}");
    println!("  index (read only {} candidates):  {idx_ms:8.2} ms   hits={hits}   speedup={:.0}x",
             ids.len(), base_ms / idx_ms);
    println!();
    println!("index-only count(rating>=4): {cnt_ms:8.3} ms   count={cnt}   (zero record reads)");
}
