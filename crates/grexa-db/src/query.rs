// SPDX-FileCopyrightText: 2026 VisorCraft LLC
// SPDX-License-Identifier: Apache-2.0

//! Typed query builder with streaming filters and buffering `order_by`.
//!
//! Filters chain fluently. Filter-only queries read record *content*
//! lazily (O(1) per record body); file paths are collected eagerly during
//! the directory walk (O(n) in path count). `order_by` forces full
//! materialization of matching records before yielding.
//!
//! # Edge cases
//!
//! - Calling `order_by` twice keeps only the last sort key.
//! - `contains_any(&[])` matches nothing; `contains_all(&[])` matches
//!   everything (vacuous truth).
//! - Records missing a filtered field never match that filter.
//! - Records missing an `order_by` field always sort last, regardless of
//!   ascending or descending direction.
//!
//! ```
//! use grexa_db::Collection;
//! # use std::fs;
//! # let dir = tempfile::TempDir::new().unwrap();
//! # fs::write(dir.path().join("schema.md"),
//! #     "---\ncollection: notes\nfields:\n  - { name: rating, type: integer }\n---\n").unwrap();
//! # fs::write(dir.path().join("a.md"), "---\nrating: 5\n---\n").unwrap();
//! # fs::write(dir.path().join("b.md"), "---\nrating: 2\n---\n").unwrap();
//! let notes = Collection::open(dir.path()).unwrap();
//!
//! // Streaming filter — O(1) memory
//! let top: Vec<_> = notes.query()
//!     .filter("rating").ge(4)
//!     .collect::<Result<_, _>>()
//!     .unwrap();
//! assert_eq!(top.len(), 1);
//! ```

use crate::collection::Collection;
use crate::index::{Index, index_key, key_prefix_bounds};
use crate::record::Record;
use crate::record::RecordError;
use serde_yaml::Value;
use std::cmp::Ordering;
use std::iter::FusedIterator;
use std::ops::Bound;
use std::path::{Path, PathBuf};

/// A trait for Rust values that can be converted into a YAML [`Value`] for
/// filter comparisons.
pub trait IntoValue {
    fn to_value(&self) -> Value;
}

impl IntoValue for i32 {
    fn to_value(&self) -> Value {
        Value::from(*self as i64)
    }
}

impl IntoValue for i64 {
    fn to_value(&self) -> Value {
        Value::from(*self)
    }
}

impl IntoValue for f64 {
    fn to_value(&self) -> Value {
        Value::from(*self)
    }
}

impl IntoValue for bool {
    fn to_value(&self) -> Value {
        Value::from(*self)
    }
}

impl IntoValue for &str {
    fn to_value(&self) -> Value {
        Value::from(*self)
    }
}

impl IntoValue for String {
    fn to_value(&self) -> Value {
        Value::from(self.as_str())
    }
}

#[derive(Debug, Clone)]
enum FilterOp {
    Eq(Value),
    Ne(Value),
    Lt(Value),
    Le(Value),
    Gt(Value),
    Ge(Value),
    Contains(Value),
    ContainsAny(Vec<Value>),
    ContainsAll(Vec<Value>),
}

impl FilterOp {
    fn matches(&self, field_value: &Value) -> bool {
        match self {
            FilterOp::Eq(t) => values_equal(field_value, t),
            FilterOp::Ne(t) => !values_equal(field_value, t),
            FilterOp::Lt(t) => cmp(field_value, t).is_some_and(|o| o == Ordering::Less),
            FilterOp::Le(t) => cmp(field_value, t).is_some_and(|o| o != Ordering::Greater),
            FilterOp::Gt(t) => cmp(field_value, t).is_some_and(|o| o == Ordering::Greater),
            FilterOp::Ge(t) => cmp(field_value, t).is_some_and(|o| o != Ordering::Less),
            FilterOp::Contains(t) => value_in_collection(field_value, t),
            FilterOp::ContainsAny(ts) => ts.iter().any(|t| value_in_collection(field_value, t)),
            FilterOp::ContainsAll(ts) => ts.iter().all(|t| value_in_collection(field_value, t)),
        }
    }
}

#[derive(Debug, Clone)]
struct Filter {
    field: String,
    op: FilterOp,
}

impl Filter {
    fn matches(&self, record: &Record) -> bool {
        match record.field(&self.field) {
            Some(v) => self.op.matches(v),
            None => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
struct OrderBy {
    field: String,
    direction: SortDir,
}

/// A query builder over a [`Collection`].
///
/// Implements [`Iterator`] directly: filter-only queries read record
/// content lazily; `order_by` forces full buffering before the first
/// yield.
///
/// # Edge cases
///
/// - Records missing a filtered field never match.
/// - Records missing the `order_by` field always sort last.
/// - `order_by` called twice keeps only the last key.
/// - `contains_any(&[])` matches nothing; `contains_all(&[])` matches
///   everything.
pub struct Query<'a> {
    collection: &'a Collection,
    filters: Vec<Filter>,
    order_by: Option<OrderBy>,
    /// A caller-held, in-memory index to accelerate this query. `None` means a
    /// full scan. The index is never auto-loaded per query (that was slower than
    /// scanning); a persistent caller loads it once and reuses it here.
    index: Option<&'a Index>,
    /// Cap the result at this many records. With `order_by` it fuses into a
    /// bounded top-K heap (O(n log k) time, O(k) memory); without, it stops the
    /// scan/stream after k matches.
    limit: Option<usize>,
    state: QueryState<'a>,
}

enum QueryState<'a> {
    NotStarted,
    Streaming(crate::collection::RecordIter<'a>),
    Buffered(std::vec::IntoIter<Record>),
    Errored(Option<RecordError>),
    Exhausted,
}

/// A run of matched records tagged with the chunk's start index in the path
/// list, so worker results can be re-sorted back into directory-walk order.
type WorkerChunks = Vec<(usize, Vec<Record>)>;

impl<'a> Query<'a> {
    pub(crate) fn new(collection: &'a Collection) -> Self {
        Self {
            collection,
            filters: Vec::new(),
            order_by: None,
            index: None,
            limit: None,
            state: QueryState::NotStarted,
        }
    }

    /// Cap the result at `k` records. With `order_by` this fuses into a bounded
    /// top-K heap: O(n log k) time and **O(k)** memory instead of buffering and
    /// sorting every match. Without `order_by`, it just stops after k matches.
    pub fn limit(mut self, k: usize) -> Query<'a> {
        self.limit = Some(k);
        self
    }

    /// Use a caller-held, in-memory [`Index`] to accelerate this query: selective
    /// `eq` / `contains` filters read only the matching records instead of
    /// scanning the whole collection. The caller is responsible for keeping the
    /// index current (via [`Index::reconcile`] when records change); every
    /// candidate is still re-read and re-checked against all filters
    /// (verify-on-read), so a stale index can never yield a wrong-valued match —
    /// at worst it misses a record the caller hasn't reconciled yet.
    pub fn using_index(mut self, index: &'a Index) -> Query<'a> {
        self.index = Some(index);
        self
    }

    /// Begin a filter clause on `field`. Returns a [`FilterBuilder`] whose
    /// methods pick the comparison operator and return the [`Query`] for
    /// further chaining.
    pub fn filter(self, field: impl Into<String>) -> FilterBuilder<'a> {
        FilterBuilder {
            query: self,
            field: field.into(),
        }
    }

    /// Begin an `order_by` clause on `field`. Returns an [`OrderBuilder`]
    /// for choosing ascending or descending. This switches the query from
    /// streaming to buffering.
    pub fn order_by(self, field: impl Into<String>) -> OrderBuilder<'a> {
        OrderBuilder {
            query: self,
            field: field.into(),
        }
    }

    /// The on-disk root directory of the collection this query runs against.
    pub(crate) fn collection_root(&self) -> &Path {
        self.collection.root()
    }

    fn init_state(&mut self) {
        // `order_by` must materialize the whole result set anyway, so read +
        // parse it in parallel. Filter-only queries stay lazy/streaming below
        // (preserving O(1) memory and early-exit for `.next()` callers).
        if self.order_by.is_some() {
            match self.materialize_par() {
                Ok(records) => self.state = QueryState::Buffered(records.into_iter()),
                Err(e) => self.state = QueryState::Errored(Some(e)),
            }
        } else {
            self.state = QueryState::Streaming(self.collection.records());
        }
    }

    /// Drain the query into a `Vec`, reading and parsing records **in parallel**
    /// across the available CPUs (serial fallback for small collections). The
    /// result set and ordering are identical to draining the streaming
    /// [`Iterator`]. Use this for "read everything" callers (CLI, batch jobs);
    /// the `Iterator` impl stays lazy for early-exit / streaming callers.
    pub fn collect_par(self) -> Result<Vec<Record>, RecordError> {
        self.materialize_par()
    }

    fn materialize_par(&self) -> Result<Vec<Record>, RecordError> {
        // A caller-held index can answer selective eq/contains queries by reading
        // only the matching records, not the whole tree. Only used when the
        // caller explicitly attached one (see `using_index`); never auto-loaded.
        if let Some(out) = self.try_index()? {
            return Ok(out);
        }

        // `order_by` + `limit` fuses into a bounded top-K: workers keep only
        // their k best, so memory is O(k) instead of buffering every match.
        if let (Some(ob), Some(k)) = (&self.order_by, self.limit) {
            return self.topk_par(ob, k);
        }

        // Below this many records, thread setup costs more than it saves — and
        // grexa-db's own dogfooded stores hold a handful of records.
        const MIN_PER_WORKER: usize = 512;

        let paths = self.collection.collect_paths_full();
        let n = paths.len();
        let cores = std::thread::available_parallelism()
            .map(|c| c.get())
            .unwrap_or(1);
        let workers = (n / MIN_PER_WORKER).clamp(1, cores);

        let mut out = if workers <= 1 {
            filter_paths(self.collection, &self.filters, &paths)?
        } else {
            use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
            // Dynamic work-stealing: each worker pulls the next small chunk via
            // a shared cursor, so uneven per-record cost (a big body, a slow
            // read) can't leave a core idle the way a fixed even split would.
            // Chunks are tagged with their start index and re-sorted at the end,
            // so output order still matches the directory walk.
            const STEAL_CHUNK: usize = 128;
            let cursor = AtomicUsize::new(0);
            let cursor = &cursor;
            let filters = &self.filters;
            let coll = self.collection;
            let paths_ref: &[PathBuf] = &paths;
            // Scoped threads borrow `coll`/`filters`/`paths` directly — no
            // clones, no 'static bound, joined before the borrows end.
            let worker_out: Vec<Result<WorkerChunks, RecordError>> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..workers)
                    .map(|_| {
                        s.spawn(move || {
                            let mut local: WorkerChunks = Vec::new();
                            loop {
                                let start = cursor.fetch_add(STEAL_CHUNK, AtomicOrdering::Relaxed);
                                if start >= n {
                                    break;
                                }
                                let end = (start + STEAL_CHUNK).min(n);
                                let recs = filter_paths(coll, filters, &paths_ref[start..end])?;
                                local.push((start, recs));
                            }
                            Ok(local)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("record worker thread panicked"))
                    .collect()
            });
            let mut chunks: WorkerChunks = Vec::new();
            for w in worker_out {
                chunks.extend(w?);
            }
            chunks.sort_by_key(|(start, _)| *start);
            let mut merged = Vec::new();
            for (_, recs) in chunks {
                merged.extend(recs);
            }
            merged
        };

        if let Some(ob) = &self.order_by {
            out.sort_by(|a, b| order_cmp(a, b, ob));
        }
        if let Some(k) = self.limit {
            out.truncate(k);
        }
        Ok(out)
    }

    /// Parallel bounded top-K for `order_by` + `limit`. Each worker keeps only
    /// its k best (trimming at 2k), so per-worker memory is O(k); merging the
    /// workers' top-k and trimming again yields the global top-k — identical to
    /// a full sort then truncate, but without ever holding every match.
    fn topk_par(&self, ob: &OrderBy, k: usize) -> Result<Vec<Record>, RecordError> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let paths = self.collection.collect_paths_full();
        let n = paths.len();
        const MIN_PER_WORKER: usize = 512;
        let cores = std::thread::available_parallelism()
            .map(|c| c.get())
            .unwrap_or(1);
        let workers = (n / MIN_PER_WORKER).clamp(1, cores);

        let mut merged = if workers <= 1 {
            worker_topk(self.collection, &self.filters, ob, k, &paths)?
        } else {
            use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
            const STEAL_CHUNK: usize = 128;
            let cursor = AtomicUsize::new(0);
            let cursor = &cursor;
            let filters = &self.filters;
            let coll = self.collection;
            let paths_ref: &[PathBuf] = &paths;
            let outs: Vec<Result<Vec<Record>, RecordError>> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..workers)
                    .map(|_| {
                        s.spawn(move || {
                            let mut buf: Vec<Record> = Vec::new();
                            loop {
                                let start = cursor.fetch_add(STEAL_CHUNK, AtomicOrdering::Relaxed);
                                if start >= n {
                                    break;
                                }
                                let end = (start + STEAL_CHUNK).min(n);
                                for p in &paths_ref[start..end] {
                                    let rec = coll.read_record_at(p)?;
                                    if filters.iter().all(|f| f.matches(&rec)) {
                                        buf.push(rec);
                                        if buf.len() >= 2 * k {
                                            topk_trim(&mut buf, ob, k);
                                        }
                                    }
                                }
                            }
                            topk_trim(&mut buf, ob, k);
                            Ok(buf)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("topk worker thread panicked"))
                    .collect()
            });
            let mut global = Vec::new();
            for r in outs {
                global.extend(r?);
            }
            global
        };
        topk_trim(&mut merged, ob, k);
        Ok(merged)
    }

    /// Answer the query from the caller-held index, if one is attached and at
    /// least one filter is index-serviceable. Returns `Ok(None)` so the caller
    /// scans otherwise. Candidate paths from the index are re-read and
    /// re-checked against *every* filter (verify-on-read) — so a candidate whose
    /// content changed, or that was deleted, can never produce a wrong match —
    /// then ordered exactly as the scan path orders them.
    fn try_index(&self) -> Result<Option<Vec<Record>>, RecordError> {
        let Some(index) = self.index else {
            return Ok(None);
        };
        let Some(rels) = plan_candidates(index, &self.filters) else {
            return Ok(None);
        };
        // Selectivity guard: candidates are read one-by-one here, while a scan
        // reads in parallel across all cores. Past roughly 1/16 of the
        // collection, the parallel scan wins — so hand low-selectivity queries
        // back to it rather than risk a slowdown. (The index's whole point is
        // the highly-selective case, where this never triggers.)
        if rels.len().saturating_mul(16) > index.record_count() {
            return Ok(None);
        }
        let root = self.collection.root();
        let mut out = Vec::new();
        for rel in &rels {
            // Tolerate a candidate that has since been deleted (a change the
            // caller hasn't reconciled): it simply no longer matches.
            let record = match self.collection.read_record_at(&root.join(rel)) {
                Ok(r) => r,
                Err(RecordError::ReadFile { .. }) => continue,
                Err(e) => return Err(e),
            };
            if self.filters.iter().all(|f| f.matches(&record)) {
                out.push(record);
            }
        }
        // The scan path emits filter-only results in path order, and order_by
        // results with ties broken by path (sorted input + stable sort); match
        // both so index and scan are byte-identical.
        match &self.order_by {
            Some(ob) => {
                out.sort_by(|a, b| order_cmp(a, b, ob).then_with(|| a.path().cmp(b.path())))
            }
            None => out.sort_by(|a, b| a.path().cmp(b.path())),
        }
        if let Some(k) = self.limit {
            out.truncate(k);
        }
        Ok(Some(out))
    }
}

/// Build a candidate path set from the index for the `eq` / `contains` filters
/// (the v1 index-serviceable ops). Returns `None` when no filter is serviceable,
/// so the caller scans. The set is a superset of the true matches; verify-on-read
/// makes the final result exact regardless.
fn plan_candidates(index: &Index, filters: &[Filter]) -> Option<Vec<String>> {
    let mut sets: Vec<Vec<String>> = Vec::new();
    for f in filters {
        if !index.has_field(&f.field) {
            continue;
        }
        let set = match &f.op {
            FilterOp::Eq(v) | FilterOp::Contains(v) => {
                let Some(key) = index_key(v) else { continue };
                index.posting(&f.field, &key).cloned().unwrap_or_default()
            }
            FilterOp::Ge(v) | FilterOp::Gt(v) | FilterOp::Le(v) | FilterOp::Lt(v) => {
                let Some(key) = index_key(v) else { continue };
                let (lo, hi) = key_prefix_bounds(&key);
                let (lower, upper) = match &f.op {
                    FilterOp::Ge(_) => (Bound::Included(key), Bound::Excluded(hi)),
                    FilterOp::Gt(_) => (Bound::Excluded(key), Bound::Excluded(hi)),
                    FilterOp::Le(_) => (Bound::Included(lo), Bound::Included(key)),
                    FilterOp::Lt(_) => (Bound::Included(lo), Bound::Excluded(key)),
                    _ => unreachable!(),
                };
                index.range_postings(&f.field, lower, upper)
            }
            _ => continue, // ne / contains_any|all → residual, applied at verify
        };
        sets.push(set);
    }
    if sets.is_empty() {
        return None;
    }
    // AND the serviceable filters: intersect posting lists, smallest first.
    sets.sort_by_key(|s| s.len());
    let mut iter = sets.into_iter();
    let mut acc = iter.next().unwrap();
    for s in iter {
        let keep: std::collections::HashSet<&str> = s.iter().map(String::as_str).collect();
        acc.retain(|p| keep.contains(p.as_str()));
        if acc.is_empty() {
            break;
        }
    }
    // Postings may carry a path more than once (e.g. a record listing a tag
    // twice); dedup so verify-on-read never reads — and emits — it twice.
    acc.sort();
    acc.dedup();
    Some(acc)
}

/// Read + parse each path and keep the records matching every filter. Shared by
/// the serial and per-worker paths so both apply identical predicate logic.
fn filter_paths(
    coll: &Collection,
    filters: &[Filter],
    paths: &[PathBuf],
) -> Result<Vec<Record>, RecordError> {
    let mut kept = Vec::new();
    for p in paths {
        let record = coll.read_record_at(p)?;
        if filters.iter().all(|f| f.matches(&record)) {
            kept.push(record);
        }
    }
    Ok(kept)
}

/// Sort by the order key (ties broken by path) and keep only the first k.
fn topk_trim(buf: &mut Vec<Record>, ob: &OrderBy, k: usize) {
    buf.sort_by(|a, b| order_cmp(a, b, ob).then_with(|| a.path().cmp(b.path())));
    buf.truncate(k);
}

/// Read + filter a path slice, keeping only the k best by `ob` (bounded memory).
fn worker_topk(
    coll: &Collection,
    filters: &[Filter],
    ob: &OrderBy,
    k: usize,
    paths: &[PathBuf],
) -> Result<Vec<Record>, RecordError> {
    let mut buf = Vec::new();
    for p in paths {
        let record = coll.read_record_at(p)?;
        if filters.iter().all(|f| f.matches(&record)) {
            buf.push(record);
            if buf.len() >= 2 * k {
                topk_trim(&mut buf, ob, k);
            }
        }
    }
    topk_trim(&mut buf, ob, k);
    Ok(buf)
}

/// The `order_by` comparator: records missing the sort field always sort last,
/// regardless of direction (matches the streaming-sort semantics).
fn order_cmp(a: &Record, b: &Record, ob: &OrderBy) -> Ordering {
    match (a.field(&ob.field), b.field(&ob.field)) {
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some(av), Some(bv)) => {
            let ord = cmp(av, bv).unwrap_or(Ordering::Equal);
            match ob.direction {
                SortDir::Asc => ord,
                SortDir::Desc => ord.reverse(),
            }
        }
        (None, None) => Ordering::Equal,
    }
}

impl<'a> Iterator for Query<'a> {
    type Item = Result<Record, RecordError>;

    fn next(&mut self) -> Option<Self::Item> {
        if matches!(self.state, QueryState::NotStarted) {
            self.init_state();
        }
        // Streaming filter-only `limit`: stop after k matches. (order_by/index
        // paths are already truncated in `materialize_par`/`try_index`.)
        if self.limit == Some(0) {
            self.state = QueryState::Exhausted;
            return None;
        }

        let filters = &self.filters;
        match &mut self.state {
            QueryState::Streaming(source) => loop {
                match source.next()? {
                    Ok(record) => {
                        if filters.iter().all(|f| f.matches(&record)) {
                            if let Some(rem) = self.limit.as_mut() {
                                *rem -= 1;
                            }
                            return Some(Ok(record));
                        }
                    }
                    Err(e) => {
                        self.state = QueryState::Exhausted;
                        return Some(Err(e));
                    }
                }
            },
            QueryState::Buffered(iter) => iter.next().map(Ok),
            QueryState::Errored(e) => {
                let err = e.take();
                self.state = QueryState::Exhausted;
                err.map(Err)
            }
            QueryState::Exhausted | QueryState::NotStarted => None,
        }
    }
}

impl FusedIterator for Query<'_> {}

/// Intermediate builder returned by [`Query::filter`].
pub struct FilterBuilder<'a> {
    query: Query<'a>,
    field: String,
}

impl<'a> FilterBuilder<'a> {
    /// Field equals `value`.
    pub fn eq<V: IntoValue>(mut self, value: V) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::Eq(value.to_value()),
        });
        self.query
    }

    /// Field does not equal `value`.
    pub fn ne<V: IntoValue>(mut self, value: V) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::Ne(value.to_value()),
        });
        self.query
    }

    /// Field is less than `value`.
    pub fn lt<V: IntoValue>(mut self, value: V) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::Lt(value.to_value()),
        });
        self.query
    }

    /// Field is less than or equal to `value`.
    pub fn le<V: IntoValue>(mut self, value: V) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::Le(value.to_value()),
        });
        self.query
    }

    /// Field is greater than `value`.
    pub fn gt<V: IntoValue>(mut self, value: V) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::Gt(value.to_value()),
        });
        self.query
    }

    /// Field is greater than or equal to `value`.
    pub fn ge<V: IntoValue>(mut self, value: V) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::Ge(value.to_value()),
        });
        self.query
    }

    /// Field (an array) contains `value`, or field (a scalar) equals `value`.
    pub fn contains<V: IntoValue>(mut self, value: V) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::Contains(value.to_value()),
        });
        self.query
    }

    /// Field contains any of `values`.
    pub fn contains_any<V: IntoValue>(mut self, values: &[V]) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::ContainsAny(values.iter().map(|v| IntoValue::to_value(v)).collect()),
        });
        self.query
    }

    /// Field contains all of `values`.
    pub fn contains_all<V: IntoValue>(mut self, values: &[V]) -> Query<'a> {
        self.query.filters.push(Filter {
            field: self.field,
            op: FilterOp::ContainsAll(values.iter().map(|v| IntoValue::to_value(v)).collect()),
        });
        self.query
    }
}

/// Intermediate builder returned by [`Query::order_by`].
pub struct OrderBuilder<'a> {
    query: Query<'a>,
    field: String,
}

impl<'a> OrderBuilder<'a> {
    /// Sort ascending (records missing the field sort last).
    pub fn asc(mut self) -> Query<'a> {
        self.query.order_by = Some(OrderBy {
            field: self.field,
            direction: SortDir::Asc,
        });
        self.query
    }

    /// Sort descending (records missing the field sort last).
    pub fn desc(mut self) -> Query<'a> {
        self.query.order_by = Some(OrderBy {
            field: self.field,
            direction: SortDir::Desc,
        });
        self.query
    }
}

pub(crate) fn as_f64(v: &Value) -> Option<f64> {
    if let Some(i) = v.as_i64() {
        return Some(i as f64);
    }
    v.as_f64()
}

fn values_equal(a: &Value, b: &Value) -> bool {
    if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
        return ai == bi;
    }
    if let (Some(an), Some(bn)) = (as_f64(a), as_f64(b)) {
        return an == bn;
    }
    if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
        return as_ == bs;
    }
    if let (Some(ab), Some(bb)) = (a.as_bool(), b.as_bool()) {
        return ab == bb;
    }
    a == b
}

fn cmp(a: &Value, b: &Value) -> Option<Ordering> {
    if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
        return Some(ai.cmp(&bi));
    }
    if let (Some(an), Some(bn)) = (as_f64(a), as_f64(b)) {
        return an.partial_cmp(&bn);
    }
    if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
        return Some(as_.cmp(bs));
    }
    if let (Some(ab), Some(bb)) = (a.as_bool(), b.as_bool()) {
        return Some(ab.cmp(&bb));
    }
    None
}

fn value_in_collection(value: &Value, target: &Value) -> bool {
    if let Some(seq) = value.as_sequence() {
        return seq.iter().any(|item| values_equal(item, target));
    }
    values_equal(value, target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    const SCHEMA: &str = "---\ncollection: notes\nfields:\n  - { name: title, type: string }\n  - { name: tags, type: \"array<string>\" }\n  - { name: rating, type: integer }\n  - { name: read_at, type: date }\n---\n";

    fn make_collection() -> TempDir {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("schema.md"), SCHEMA).unwrap();
        fs::write(
            dir.path().join("alpha.md"),
            "---\ntitle: Alpha\ntags: [rust, db]\nrating: 5\nread_at: 2024-03-01\n---\nAlpha body.\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("beta.md"),
            "---\ntitle: Beta\ntags: [python, db]\nrating: 3\nread_at: 2024-02-01\n---\nBeta body.\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("gamma.md"),
            "---\ntitle: Gamma\ntags: [rust, ai]\nrating: 4\nread_at: 2024-04-01\n---\nGamma body.\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("delta.md"),
            "---\ntitle: Delta\nrating: 1\n---\nDelta body (no tags, no read_at).\n",
        )
        .unwrap();
        dir
    }

    fn paths(records: Vec<Record>) -> Vec<String> {
        records.into_iter().map(|r| r.path().to_string()).collect()
    }

    #[test]
    fn no_filter_returns_all() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let all: Vec<_> = coll.query().collect::<Result<_, _>>().unwrap();
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn filter_ge_integer() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .filter("rating")
            .ge(4)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(result), vec!["alpha.md", "gamma.md"]);
    }

    #[test]
    fn filter_eq_string() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .filter("title")
            .eq("Beta")
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(result), vec!["beta.md"]);
    }

    #[test]
    fn filter_ne_string() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .filter("title")
            .ne("Alpha")
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn filter_lt_and_gt() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let mid: Vec<_> = coll
            .query()
            .filter("rating")
            .gt(2)
            .filter("rating")
            .lt(5)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(mid), vec!["beta.md", "gamma.md"]);
    }

    #[test]
    fn filter_contains_any_tags() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .filter("tags")
            .contains_any(&["python", "ai"])
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(result), vec!["beta.md", "gamma.md"]);
    }

    #[test]
    fn filter_contains_all_tags() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .filter("tags")
            .contains_all(&["rust", "db"])
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(result), vec!["alpha.md"]);
    }

    #[test]
    fn filter_contains_single_value() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .filter("tags")
            .contains("db")
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(result), vec!["alpha.md", "beta.md"]);
    }

    #[test]
    fn missing_field_excludes_record() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let tagged: Vec<_> = coll
            .query()
            .filter("tags")
            .contains_any(&["rust"])
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(!tagged.iter().any(|r| r.path() == "delta.md"));
    }

    #[test]
    fn empty_result_set() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .filter("rating")
            .ge(100)
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn order_by_asc() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .order_by("rating")
            .asc()
            .collect::<Result<_, _>>()
            .unwrap();
        let ratings: Vec<_> = result
            .iter()
            .map(|r| r.field("rating").unwrap().as_i64().unwrap())
            .collect();
        assert_eq!(ratings, vec![1, 3, 4, 5]);
    }

    #[test]
    fn order_by_desc() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .order_by("rating")
            .desc()
            .collect::<Result<_, _>>()
            .unwrap();
        let ratings: Vec<_> = result
            .iter()
            .map(|r| r.field("rating").unwrap().as_i64().unwrap())
            .collect();
        assert_eq!(ratings, vec![5, 4, 3, 1]);
    }

    #[test]
    fn order_by_string_field() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .order_by("title")
            .asc()
            .collect::<Result<_, _>>()
            .unwrap();
        let titles: Vec<_> = result
            .iter()
            .map(|r| r.field("title").unwrap().as_str().unwrap().to_string())
            .collect();
        assert_eq!(titles, vec!["Alpha", "Beta", "Delta", "Gamma"]);
    }

    #[test]
    fn order_by_missing_field_sorts_last() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .order_by("read_at")
            .asc()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(result.last().unwrap().path(), "delta.md");
    }

    #[test]
    fn order_by_desc_missing_field_still_sorts_last() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .order_by("read_at")
            .desc()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            result.last().unwrap().path(),
            "delta.md",
            "missing-field records must sort last even in descending order"
        );
    }

    #[test]
    fn i64_precision_beyond_2pow53() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("schema.md"), SCHEMA).unwrap();
        let big = 9_007_199_254_740_993_i64;
        let big_minus_1 = 9_007_199_254_740_992_i64;
        fs::write(dir.path().join("big.md"), format!("---\nrating: {big}\n---\nbody\n")).unwrap();
        fs::write(dir.path().join("small.md"), format!("---\nrating: {big_minus_1}\n---\nbody\n"))
            .unwrap();
        let coll = Collection::open(dir.path()).unwrap();

        let exact: Vec<_> = coll
            .query()
            .filter("rating")
            .eq(big)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(exact), vec!["big.md"]);

        let gt: Vec<_> = coll
            .query()
            .filter("rating")
            .gt(big_minus_1)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(gt), vec!["big.md"]);
    }

    #[test]
    fn combined_filter_and_order() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let result: Vec<_> = coll
            .query()
            .filter("tags")
            .contains_any(&["rust", "db"])
            .order_by("rating")
            .desc()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths(result), vec!["alpha.md", "gamma.md", "beta.md"]);
    }

    #[test]
    fn streaming_is_lazy() {
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let mut query = coll.query().filter("rating").ge(4);
        let first = query.next().unwrap().unwrap();
        assert!(first.field("rating").unwrap().as_i64().unwrap() >= 4);
    }

    #[test]
    fn collect_par_matches_serial_small() {
        // Small collection takes the serial fallback inside materialize_par.
        let dir = make_collection();
        let coll = Collection::open(dir.path()).unwrap();
        let serial: Vec<String> = paths(
            coll.query()
                .filter("rating")
                .ge(4)
                .collect::<Result<_, _>>()
                .unwrap(),
        );
        let par: Vec<String> = paths(coll.query().filter("rating").ge(4).collect_par().unwrap());
        assert_eq!(serial, par);
    }

    #[test]
    fn collect_par_matches_serial_large() {
        // > MIN_PER_WORKER records so the multi-threaded path actually runs;
        // results (and order) must be byte-identical to the serial iterator.
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("schema.md"), SCHEMA).unwrap();
        for i in 0..3000 {
            let rating = (i % 5) + 1;
            fs::write(
                dir.path().join(format!("n{i:05}.md")),
                format!(
                    "---\ntitle: N{i}\ntags: [t{}, db]\nrating: {rating}\nread_at: 2024-01-{:02}\n---\nbody {i}\n",
                    i % 7,
                    (i % 28) + 1
                ),
            )
            .unwrap();
        }
        let coll = Collection::open(dir.path()).unwrap();

        // filter-only
        let serial: Vec<String> = paths(
            coll.query()
                .filter("rating")
                .ge(4)
                .collect::<Result<_, _>>()
                .unwrap(),
        );
        let par: Vec<String> = paths(coll.query().filter("rating").ge(4).collect_par().unwrap());
        assert_eq!(serial, par, "parallel filter must equal serial");
        assert!(!par.is_empty());

        // order_by (the path init_state now parallelizes)
        let serial_sorted: Vec<String> = paths(
            coll.query()
                .order_by("read_at")
                .desc()
                .collect::<Result<_, _>>()
                .unwrap(),
        );
        let par_sorted: Vec<String> = paths(
            coll.query()
                .order_by("read_at")
                .desc()
                .collect_par()
                .unwrap(),
        );
        assert_eq!(serial_sorted, par_sorted, "parallel sort must equal serial");
    }

    fn mix(x: u64) -> u64 {
        let mut z = x.wrapping_add(0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn rand_collection(n: usize) -> TempDir {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("schema.md"), SCHEMA).unwrap();
        let tags = ["rust", "ai", "ml", "db", "linux", "qt"];
        for i in 0..n {
            let h = |s: u64| mix(((i as u64) << 8) | s) as usize;
            let rating = h(0) % 5 + 1;
            let mut chosen: Vec<&str> = (0..(h(1) % 3 + 1))
                .map(|k| tags[h(10 + k as u64) % tags.len()])
                .collect();
            chosen.sort();
            chosen.dedup();
            let day = h(2) % 28 + 1;
            fs::write(
                dir.path().join(format!("r{i:04}.md")),
                format!(
                    "---\ntitle: R{i}\ntags: [{}]\nrating: {rating}\nread_at: 2024-02-{day:02}\n---\nbody {i}\n",
                    chosen.join(", ")
                ),
            )
            .unwrap();
        }
        dir
    }

    #[test]
    fn index_matches_scan() {
        // Each builder is run as a plain scan and via a held index; the two must
        // be byte-identical, including order, across serviceable and
        // non-serviceable filter shapes.
        type BuildQ = Box<dyn for<'a> Fn(Query<'a>) -> Query<'a>>;
        let dir = rand_collection(800);
        let coll = Collection::open(dir.path()).unwrap();
        let idx = Index::build(&coll).unwrap();
        let cases: Vec<BuildQ> = vec![
            Box::new(|q| q.filter("rating").eq(5)),
            Box::new(|q| q.filter("rating").eq(1)),
            Box::new(|q| q.filter("tags").contains("rust")),
            Box::new(|q| q.filter("tags").contains("qt")),
            Box::new(|q| q.filter("rating").eq(4).filter("tags").contains("db")),
            Box::new(|q| q.filter("read_at").eq("2024-02-15")),
            Box::new(|q| q.filter("tags").contains("ai").order_by("rating").desc()),
            Box::new(|q| q.filter("rating").eq(3).order_by("read_at").asc()),
            // Not index-serviceable (range / ne / none): scanned either way.
            Box::new(|q| q.filter("rating").ge(4)),
            Box::new(|q| q.filter("rating").ne(5)),
            Box::new(|q| q),
        ];
        let mut any = false;
        for (i, bq) in cases.iter().enumerate() {
            let scan = paths(bq(coll.query()).collect_par().unwrap());
            let indexed = paths(bq(coll.query()).using_index(&idx).collect_par().unwrap());
            assert_eq!(scan, indexed, "case {i}: index differs from scan");
            any |= !scan.is_empty();
        }
        assert!(any, "no case matched anything");
    }

    #[test]
    fn reconcile_reflects_modified() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("schema.md"), SCHEMA).unwrap();
        fs::write(dir.path().join("a.md"), "---\nrating: 3\n---\nbody\n").unwrap();
        fs::write(dir.path().join("b.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        let coll = Collection::open(dir.path()).unwrap();
        let mut idx = Index::build(&coll).unwrap();
        assert_eq!(
            paths(
                coll.query()
                    .filter("rating")
                    .eq(5)
                    .using_index(&idx)
                    .collect_par()
                    .unwrap()
            ),
            vec!["b.md".to_string()]
        );

        fs::write(dir.path().join("a.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        idx.reconcile(&coll, &["a.md".to_string()]).unwrap();

        let mut fives = paths(
            coll.query()
                .filter("rating")
                .eq(5)
                .using_index(&idx)
                .collect_par()
                .unwrap(),
        );
        fives.sort();
        assert_eq!(fives, vec!["a.md".to_string(), "b.md".to_string()]);
        assert!(
            paths(
                coll.query()
                    .filter("rating")
                    .eq(3)
                    .using_index(&idx)
                    .collect_par()
                    .unwrap()
            )
            .is_empty()
        );
    }

    #[test]
    fn reconcile_reflects_added_and_removed() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("schema.md"), SCHEMA).unwrap();
        fs::write(dir.path().join("a.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        fs::write(dir.path().join("b.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        let coll = Collection::open(dir.path()).unwrap();
        let mut idx = Index::build(&coll).unwrap();

        fs::write(dir.path().join("c.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        fs::remove_file(dir.path().join("a.md")).unwrap();
        idx.reconcile(&coll, &["c.md".to_string(), "a.md".to_string()])
            .unwrap();

        let mut fives = paths(
            coll.query()
                .filter("rating")
                .eq(5)
                .using_index(&idx)
                .collect_par()
                .unwrap(),
        );
        fives.sort();
        assert_eq!(fives, vec!["b.md".to_string(), "c.md".to_string()]);
    }

    #[test]
    fn stale_index_verify_on_read_drops_false_positive() {
        // Without a reconcile, a record edited to no longer match is still in the
        // stale index — but verify-on-read re-reads it and drops it. (A record
        // edited to *newly* match would be missed until reconcile; that's the
        // documented caller-owns-freshness contract.)
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("schema.md"), SCHEMA).unwrap();
        fs::write(dir.path().join("a.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        fs::write(dir.path().join("b.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        let coll = Collection::open(dir.path()).unwrap();
        let idx = Index::build(&coll).unwrap();

        fs::write(dir.path().join("a.md"), "---\nrating: 1\n---\nbody\n").unwrap(); // no reconcile
        let fives = paths(
            coll.query()
                .filter("rating")
                .eq(5)
                .using_index(&idx)
                .collect_par()
                .unwrap(),
        );
        assert_eq!(fives, vec!["b.md".to_string()], "stale false positive not dropped");
    }

    #[test]
    fn stale_deleted_candidate_is_tolerated() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("schema.md"), SCHEMA).unwrap();
        fs::write(dir.path().join("a.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        fs::write(dir.path().join("b.md"), "---\nrating: 5\n---\nbody\n").unwrap();
        let coll = Collection::open(dir.path()).unwrap();
        let idx = Index::build(&coll).unwrap();

        fs::remove_file(dir.path().join("a.md")).unwrap(); // candidate vanishes, no reconcile
        let fives = paths(
            coll.query()
                .filter("rating")
                .eq(5)
                .using_index(&idx)
                .collect_par()
                .unwrap(),
        );
        assert_eq!(fives, vec!["b.md".to_string()], "deleted candidate not tolerated");
    }

    #[test]
    fn limit_topk_matches_full_sort() {
        let dir = rand_collection(800);
        let coll = Collection::open(dir.path()).unwrap();
        // Bounded top-K must equal a full sort then truncate, for every k —
        // including ties (read_at/rating repeat heavily across 800 records).
        for field in ["rating", "read_at"] {
            let full = paths(coll.query().order_by(field).desc().collect_par().unwrap());
            for k in [0usize, 1, 5, 20, 100, 800, 1000] {
                let topk = paths(
                    coll.query()
                        .order_by(field)
                        .desc()
                        .limit(k)
                        .collect_par()
                        .unwrap(),
                );
                let want: Vec<String> = full.iter().take(k).cloned().collect();
                assert_eq!(topk, want, "top-{k} by {field} != full sort truncated");
            }
        }
        // filter + order_by + limit
        let full = paths(
            coll.query()
                .filter("tags")
                .contains("rust")
                .order_by("read_at")
                .asc()
                .collect_par()
                .unwrap(),
        );
        let top = paths(
            coll.query()
                .filter("tags")
                .contains("rust")
                .order_by("read_at")
                .asc()
                .limit(10)
                .collect_par()
                .unwrap(),
        );
        assert_eq!(top, full.into_iter().take(10).collect::<Vec<_>>());
    }

    #[test]
    fn limit_filter_only_stops_early() {
        let dir = rand_collection(800);
        let coll = Collection::open(dir.path()).unwrap();
        // Streaming iterator and collect_par both honor the limit, identically.
        let streamed = paths(
            coll.query()
                .filter("rating")
                .ge(1)
                .limit(7)
                .collect::<Result<_, _>>()
                .unwrap(),
        );
        let par = paths(
            coll.query()
                .filter("rating")
                .ge(1)
                .limit(7)
                .collect_par()
                .unwrap(),
        );
        assert_eq!(streamed.len(), 7);
        assert_eq!(streamed, par);
        // limit(0) yields nothing on either path.
        assert!(
            coll.query()
                .filter("rating")
                .ge(1)
                .limit(0)
                .collect_par()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            coll.query()
                .filter("rating")
                .ge(1)
                .limit(0)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn index_ranges_match_scan() {
        // Range queries (lt/le/gt/ge), on numeric and string fields, alone and
        // intersected with eq/contains/order_by, must match a scan exactly.
        type BuildQ = Box<dyn for<'a> Fn(Query<'a>) -> Query<'a>>;
        let dir = rand_collection(800);
        let coll = Collection::open(dir.path()).unwrap();
        let idx = Index::build(&coll).unwrap();
        let cases: Vec<BuildQ> = vec![
            Box::new(|q| q.filter("rating").ge(4)),
            Box::new(|q| q.filter("rating").gt(2)),
            Box::new(|q| q.filter("rating").le(2)),
            Box::new(|q| q.filter("rating").lt(3)),
            Box::new(|q| q.filter("read_at").ge("2024-02-20")), // string range
            Box::new(|q| q.filter("read_at").lt("2024-02-02")), // selective → index read
            Box::new(|q| q.filter("rating").ge(4).filter("tags").contains("rust")), // range ∩ contains
            Box::new(|q| {
                q.filter("read_at")
                    .lt("2024-02-02")
                    .order_by("rating")
                    .desc()
            }), // range + order
        ];
        let mut any = false;
        for (i, bq) in cases.iter().enumerate() {
            let scan = paths(bq(coll.query()).collect_par().unwrap());
            let indexed = paths(bq(coll.query()).using_index(&idx).collect_par().unwrap());
            assert_eq!(scan, indexed, "range case {i}: index != scan");
            any |= !scan.is_empty();
        }
        assert!(any);
    }
}
