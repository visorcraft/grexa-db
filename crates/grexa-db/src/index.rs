// SPDX-FileCopyrightText: 2026 VisorCraft LLC
// SPDX-License-Identifier: Apache-2.0

//! Optional secondary index — a *derived, rebuildable* sidecar that maps field
//! values to record paths so selective `eq` / `contains` queries read only the
//! matching records instead of scanning the whole collection.
//!
//! # Correctness (it can never return a wrong result)
//!
//! The plain record files are the source of truth; this index is a disposable
//! cache. Delete `.grexa-index/` and every record is still intact and every
//! query still works (it falls back to a scan). A query uses the index **only**
//! when [`Index::is_fresh`] proves it current (matching per-record
//! `(mtime, size)` signatures, with added/removed files detected), and even
//! then the candidate records it points at are re-read and re-checked against
//! *all* the query's filters (verify-on-read). So:
//!
//! - **No false positives:** a candidate whose content changed to no longer
//!   match is dropped by verify-on-read.
//! - **No false negatives:** if *any* file was added, removed, or modified
//!   since the index was built, `is_fresh` returns false and the planner falls
//!   back to a full scan — so a record edited to newly match is never missed.
//!
//! # On disk
//!
//! `<collection>/.grexa-index/index.json` (hidden, so the record walk skips it),
//! published by write-temp + atomic `rename(2)`. JSON keyed deterministically
//! (BTreeMaps) so it diffs cleanly and stays inspectable, like the records.

use crate::collection::Collection;
use serde_yaml::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::UNIX_EPOCH;
use thiserror::Error;

const INDEX_VERSION: u64 = 2;
const INDEX_DIR: &str = ".grexa-index";
const INDEX_FILE: &str = "index.json";

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("record error: {0}")]
    Record(#[from] crate::record::RecordError),
    #[error("serialize error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// File-change signature: `(mtime_nanos, size_bytes)`.
type Sig = (u64, u64);

fn stat_sig(path: &Path) -> Option<Sig> {
    let md = std::fs::metadata(path).ok()?;
    let mtime = md
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    Some((mtime, md.len()))
}

/// A canonical, **order-preserving** string key for a value: comparing two keys
/// of the same type byte-wise reproduces the scan's `cmp` ordering, so a
/// `BTreeMap` range scan over the keys answers `lt/le/gt/ge`. It also collapses
/// exactly the equivalence classes `values_equal` treats as equal (e.g. 4 and
/// 4.0 → the same key), so the index never splits scan-equal values (a false
/// negative). Type-prefixed (`n`/`s`/`b`) so cross-type keys never interleave;
/// a collision can only ever be a false *positive*, which verify-on-read drops.
/// Returns `None` for unindexable values (null / nested mappings).
pub(crate) fn index_key(v: &Value) -> Option<String> {
    // Numbers (i64 and f64 unified through f64, matching `values_equal`): encode
    // as the IEEE total-order transform of the bits, hex zero-padded, so byte
    // order == numeric order. `4` and `4.0` collapse to the same key.
    if let Some(f) = crate::query::as_f64(v) {
        let bits = f.to_bits();
        let ordered = if bits & (1 << 63) == 0 {
            bits | (1 << 63)
        } else {
            !bits
        };
        return Some(format!("n{ordered:016x}"));
    }
    if let Some(b) = v.as_bool() {
        return Some(format!("b{}", b as u8));
    }
    // Strings sort lexicographically already — the raw bytes after the prefix
    // reproduce `str::cmp`, so string range queries (e.g. ISO dates) work too.
    if let Some(s) = v.as_str() {
        return Some(format!("s{s}"));
    }
    None
}

/// The `[lo, hi)` key range covering exactly the keys that share `key`'s type
/// prefix byte — used to bound range scans to one type (cross-type `cmp` is
/// undefined, so other-typed keys must not be returned as range candidates).
pub(crate) fn key_prefix_bounds(key: &str) -> (String, String) {
    let first = key.as_bytes()[0]; // ascii 'b' / 'n' / 's'
    ((first as char).to_string(), ((first + 1) as char).to_string())
}

/// field name -> value key -> list of record relative paths.
type Fields = BTreeMap<String, BTreeMap<String, Vec<String>>>;

/// A secondary index over one collection — a caller-held, in-memory handle. See
/// the module docs for the correctness contract. Build or load it once, reuse it
/// across queries via [`crate::query::Query::using_index`], and keep it current
/// with [`Index::reconcile`].
pub struct Index {
    /// record relative path -> `(mtime_nanos, size)` at build time
    records: BTreeMap<String, Sig>,
    fields: Fields,
}

fn rel_path(root: &Path, full: &Path) -> String {
    full.strip_prefix(root)
        .unwrap_or(full)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Add one record's field values to the inverted index. Arrays index each
/// element (so `contains` works); scalars index their single key.
fn index_record(fields: &mut Fields, rel: &str, record: &crate::record::Record) {
    let Some(map) = record.frontmatter().as_mapping() else {
        return;
    };
    for (k, v) in map {
        let Some(field) = k.as_str() else { continue };
        let entry = fields.entry(field.to_string()).or_default();
        match v {
            Value::Sequence(items) => {
                for item in items {
                    if let Some(key) = index_key(item) {
                        entry.entry(key).or_default().push(rel.to_string());
                    }
                }
            }
            scalar => {
                if let Some(key) = index_key(scalar) {
                    entry.entry(key).or_default().push(rel.to_string());
                }
            }
        }
    }
}

impl Index {
    /// Build an in-memory index by reading and parsing every record once. The
    /// caller holds it and reuses it across queries.
    pub fn build(coll: &Collection) -> Result<Self, IndexError> {
        let root = coll.root();
        let mut records = BTreeMap::new();
        let mut fields: Fields = BTreeMap::new();
        for full in coll.collect_paths_full() {
            let rel = rel_path(root, &full);
            let Some(sig) = stat_sig(&full) else { continue };
            let record = coll.read_record_at(&full)?;
            records.insert(rel.clone(), sig);
            index_record(&mut fields, &rel, &record);
        }
        Ok(Self { records, fields })
    }

    /// Incrementally update the index for a set of changed record paths
    /// (collection-relative, forward-slash). Added/modified records are re-read
    /// and re-indexed; deleted records are dropped. The query path stays correct
    /// via verify-on-read even between reconciles — this just restores the index
    /// as a fast pre-filter. A persistent caller reconciles on the changes it
    /// observes (it owns the writes, or watches via `inotify`).
    pub fn reconcile(&mut self, coll: &Collection, changed: &[String]) -> Result<(), IndexError> {
        let root = coll.root();
        let set: std::collections::HashSet<&str> = changed.iter().map(String::as_str).collect();
        // Drop every changed path from all postings; re-add survivors below.
        for buckets in self.fields.values_mut() {
            for paths in buckets.values_mut() {
                paths.retain(|p| !set.contains(p.as_str()));
            }
        }
        for rel in changed {
            self.records.remove(rel);
            let full = root.join(rel);
            let Some(sig) = stat_sig(&full) else { continue }; // deleted → stays gone
            let record = coll.read_record_at(&full)?;
            self.records.insert(rel.clone(), sig);
            index_record(&mut self.fields, rel, &record);
        }
        Ok(())
    }

    /// Number of indexed records.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Atomically write the index under `<root>/.grexa-index/index.json`.
    pub fn save(&self, root: &Path) -> Result<(), IndexError> {
        let dir = root.join(INDEX_DIR);
        std::fs::create_dir_all(&dir)?;
        let doc = serde_json::json!({
            "version": INDEX_VERSION,
            "records": serde_json::to_value(&self.records)?,
            "fields": serde_json::to_value(&self.fields)?,
        });
        let bytes = serde_json::to_vec(&doc)?;
        let tmp = dir.join(format!("{INDEX_FILE}.tmp.{}", std::process::id()));
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, dir.join(INDEX_FILE))?; // atomic publish
        Ok(())
    }

    /// Load the index for a collection root, or `None` if absent / unreadable /
    /// a version we don't understand (the caller then scans or rebuilds).
    pub fn load(root: &Path) -> Option<Self> {
        let path = root.join(INDEX_DIR).join(INDEX_FILE);
        let content = std::fs::read_to_string(path).ok()?;
        let v: serde_json::Value = serde_json::from_str(&content).ok()?;
        if v.get("version")?.as_u64()? != INDEX_VERSION {
            return None;
        }
        let records = serde_json::from_value(v.get("records")?.clone()).ok()?;
        let fields = serde_json::from_value(v.get("fields")?.clone()).ok()?;
        Some(Self { records, fields })
    }

    /// True iff the on-disk records exactly match what the index was built from
    /// — same set of paths, each with an unchanged `(mtime, size)` signature.
    /// Any addition, removal, or content change makes this false. O(n) `stat`s
    /// but no record reads/parses; a persistent caller can use this as an
    /// occasional safety check rather than per query.
    pub fn is_fresh(&self, coll: &Collection) -> bool {
        let root = coll.root();
        let current = coll.collect_paths_full();
        if current.len() != self.records.len() {
            return false;
        }
        for full in &current {
            let rel = rel_path(root, full);
            match (self.records.get(&rel), stat_sig(full)) {
                (Some(stored), Some(now)) if *stored == now => continue,
                _ => return false,
            }
        }
        true
    }

    /// Whether `field` has an index (any record carried it).
    pub(crate) fn has_field(&self, field: &str) -> bool {
        self.fields.contains_key(field)
    }

    /// The posting list (sorted relative paths) for `field == key`, if any.
    pub(crate) fn posting(&self, field: &str, key: &str) -> Option<&Vec<String>> {
        self.fields.get(field)?.get(key)
    }

    /// Union of posting lists for every indexed key of `field` within the key
    /// range `[lower, upper)` (order-preserving keys → numeric/lexicographic
    /// ranges). Used for `lt/le/gt/ge`.
    pub(crate) fn range_postings(
        &self,
        field: &str,
        lower: std::ops::Bound<String>,
        upper: std::ops::Bound<String>,
    ) -> Vec<String> {
        let Some(buckets) = self.fields.get(field) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for paths in buckets.range((lower, upper)).map(|(_, v)| v) {
            out.extend(paths.iter().cloned());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    #[test]
    fn index_key_collapses_int_and_float_equal() {
        // 4 and 4.0 are `values_equal` in query.rs, so they must share a key.
        assert_eq!(index_key(&Value::from(4i64)), index_key(&Value::from(4.0f64)));
    }

    #[test]
    fn index_key_separates_number_and_string() {
        // number 4 vs string "4": not equal in a scan, so different keys
        // (a collision would only be a harmless false positive anyway).
        assert_ne!(index_key(&Value::from(4i64)), index_key(&Value::from("4")));
    }

    #[test]
    fn index_key_skips_null() {
        assert_eq!(index_key(&Value::Null), None);
    }
}
