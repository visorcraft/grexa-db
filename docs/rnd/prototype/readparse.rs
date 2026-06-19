// Prototype micro-benchmark mirroring grexa-db's read+parse+filter hot path,
// isolating the contribution of (a) skipping the body copy, (b) a hand-rolled
// field scan vs full serde_yaml, and (c) rayon parallelism.
//
// usage: gdbproto <collection-dir> [field] [threshold]
use rayon::prelude::*;
use serde_yaml_ng as serde_yaml;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

// Mirror of collection.rs::collect_record_paths (DFS, skip schema.md/hidden/symlink/noise).
fn collect_paths(root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.starts_with('.') { continue }
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_symlink() { continue }
            if ft.is_dir() {
                if matches!(name, "node_modules" | "target" | "__pycache__") { continue }
                stack.push(entry.path());
                continue;
            }
            if name == "schema.md" { continue }
            paths.push(entry.path());
        }
    }
    paths.sort();
    paths
}

// Mirror frontmatter::split — return the YAML head slice (between --- delimiters).
fn head_slice(content: &str) -> Option<&str> {
    let stripped = content.strip_prefix('\u{feff}').unwrap_or(content);
    let rest = stripped.strip_prefix("---\r\n").or_else(|| stripped.strip_prefix("---\n"))?;
    let mut offset = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end() == "---" {
            return Some(&rest[..offset]);
        }
        offset += line.len();
    }
    None
}

// Hand-rolled scalar field scan: find `field:` at line start, parse i64. No serde, no body.
fn scan_int_field(head: &str, field: &str) -> Option<i64> {
    for line in head.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            if let Some(v) = rest.strip_prefix(':') {
                return v.trim().parse::<i64>().ok();
            }
        }
    }
    None
}

// Strategy A: faithful mirror of Record::from_content — full serde_yaml parse + body copy.
fn parse_full(content: &str, field: &str) -> Option<i64> {
    let head = head_slice(content)?;
    let fm: serde_yaml::Value = serde_yaml::from_str(head).ok()?;
    let _body: String = { // mirror `split.body.to_string()` — the wasted copy
        let rest = content.split_inclusive('\n');
        let mut s = String::new();
        let mut seen_close = false;
        let mut started = false;
        for line in rest {
            if !started { started = true; if line.starts_with("---") { continue } }
            if !seen_close && line.trim_end() == "---" { seen_close = true; continue }
            if seen_close { s.push_str(line); }
        }
        s
    };
    fm.get(field).and_then(|v| v.as_i64())
}

// Strategy B: full serde_yaml parse, NO body copy.
fn parse_no_body(content: &str, field: &str) -> Option<i64> {
    let head = head_slice(content)?;
    let fm: serde_yaml::Value = serde_yaml::from_str(head).ok()?;
    fm.get(field).and_then(|v| v.as_i64())
}

fn read_whole(p: &Path) -> Option<String> { fs::read_to_string(p).ok() }

// Read only the first chunk (frontmatter is tiny) instead of the whole file+body.
fn read_head_bytes(p: &Path) -> Option<String> {
    let mut f = fs::File::open(p).ok()?;
    let mut buf = vec![0u8; 4096];
    let n = f.read(&mut buf).ok()?;
    String::from_utf8(buf[..n].to_vec()).ok()
}

fn count_ge<F>(paths: &[PathBuf], field: &str, thr: i64, f: F) -> usize
where F: Fn(&Path) -> Option<i64> + Sync {
    paths.iter().filter(|p| f(p).map(|v| v >= thr).unwrap_or(false)).count()
}
fn count_ge_par<F>(paths: &[PathBuf], field: &str, thr: i64, f: F) -> usize
where F: Fn(&Path) -> Option<i64> + Sync {
    paths.par_iter().filter(|p| f(p).map(|v| v >= thr).unwrap_or(false)).count()
}

fn bench<R>(name: &str, runs: usize, base_ms: f64, mut f: impl FnMut() -> (usize, R)) -> f64 {
    let mut best = f64::MAX;
    let mut count = 0;
    for _ in 0..runs {
        let t = Instant::now();
        let (c, _r) = f();
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        best = best.min(ms);
        count = c;
    }
    let speed = if base_ms > 0.0 { base_ms / best } else { 1.0 };
    println!("{:<34} {:>9.1} ms   {:>5.1}x   count={}", name, best, speed, count);
    best
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(args.get(1).cloned().unwrap_or_else(|| "/tmp/gdb_bench_data/notes".into()));
    let field = args.get(2).cloned().unwrap_or_else(|| "rating".into());
    let thr: i64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
    let runs = 3;

    let t = Instant::now();
    let paths = collect_paths(&dir);
    let walk_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!("collection: {}  ({} records)  walk={:.1} ms  threads={}",
             dir.display(), paths.len(), walk_ms, rayon::current_num_threads());
    println!("{:<34} {:>9}      {:>5}", "strategy", "time", "speedup");

    // Baseline = faithful mirror of current grexa-db (sequential, full parse, body copy).
    let base = bench("S0 seq  full-parse + body copy", runs, 0.0,
        || (count_ge(&paths, &field, thr, |p| read_whole(p).and_then(|c| parse_full(&c, &field))), ()));
    bench("S1 seq  full-parse, no body", runs, base,
        || (count_ge(&paths, &field, thr, |p| read_whole(p).and_then(|c| parse_no_body(&c, &field))), ()));
    bench("S2 seq  field-scan (no serde)", runs, base,
        || (count_ge(&paths, &field, thr, |p| read_whole(p).and_then(|c| head_slice(&c).and_then(|h| scan_int_field(h, &field)))), ()));
    bench("S3 par  full-parse + body copy", runs, base,
        || (count_ge_par(&paths, &field, thr, |p| read_whole(p).and_then(|c| parse_full(&c, &field))), ()));
    bench("S4 par  full-parse, no body", runs, base,
        || (count_ge_par(&paths, &field, thr, |p| read_whole(p).and_then(|c| parse_no_body(&c, &field))), ()));
    bench("S5 par  field-scan (no serde)", runs, base,
        || (count_ge_par(&paths, &field, thr, |p| read_whole(p).and_then(|c| head_slice(&c).and_then(|h| scan_int_field(h, &field)))), ()));
    bench("S6 par  field-scan + head-only read", runs, base,
        || (count_ge_par(&paths, &field, thr, |p| read_head_bytes(p).and_then(|c| head_slice(&c).and_then(|h| scan_int_field(h, &field)))), ()));
}
