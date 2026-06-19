// SPDX-FileCopyrightText: 2026 VisorCraft LLC
// SPDX-License-Identifier: Apache-2.0

//! A single record — one file's parsed frontmatter and body.

use crate::frontmatter;
use serde_yaml::Value;
use std::cell::OnceCell;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("frontmatter error: {0}")]
    Frontmatter(String),
    #[error("failed to read record file `{path}`: {reason}")]
    ReadFile { path: String, reason: String },
    #[error("invalid record path `{0}`: absolute paths, `..`, and backslashes are not allowed")]
    InvalidPath(String),
}

/// A single record within a collection.
///
/// A record has:
/// - a **path** relative to its collection root (forward-slash normalized),
/// - optional **frontmatter** parsed from YAML,
/// - a **body** (everything after the frontmatter block).
pub struct Record {
    path: String,
    frontmatter: Frontmatter,
    body: String,
}

/// How a record's frontmatter is held. A *flat* head (the common case) keeps
/// its raw text and resolves fields on demand — a query that touches one field
/// never builds the whole `Value`. Anything else is parsed eagerly, so invalid
/// YAML still errors at read time exactly as before.
enum Frontmatter {
    Flat {
        head: Box<str>,
        full: OnceCell<Value>,
    },
    Eager(Value),
}

impl Record {
    /// Parse a record from its collection-relative path and raw file content.
    pub fn from_content(path: impl Into<String>, content: &str) -> Result<Self, RecordError> {
        let raw =
            frontmatter::split_raw(content).map_err(|e| RecordError::Frontmatter(e.to_string()))?;
        let frontmatter = match raw.head {
            None => Frontmatter::Eager(Value::Null),
            // A flat head is valid YAML by construction, so deferring its parse
            // can't hide an error — only complex heads can fail, and those parse
            // eagerly below.
            Some(h) if frontmatter::is_flat(h) => Frontmatter::Flat {
                head: h.into(),
                full: OnceCell::new(),
            },
            Some(h) => Frontmatter::Eager(
                frontmatter::parse_head(h).map_err(|e| RecordError::Frontmatter(e.to_string()))?,
            ),
        };
        Ok(Self {
            path: path.into(),
            frontmatter,
            body: raw.body.to_string(),
        })
    }

    /// The record's path relative to its collection root.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The body content — everything after the frontmatter block.
    pub fn body(&self) -> &str {
        &self.body
    }

    /// Whether this record has a (non-null) frontmatter block.
    pub fn has_frontmatter(&self) -> bool {
        match &self.frontmatter {
            Frontmatter::Flat { .. } => true, // flat heads are non-empty mappings
            Frontmatter::Eager(v) => !v.is_null(),
        }
    }

    /// The whole frontmatter `Value` — parsing a flat head lazily on first call.
    fn full(&self) -> &Value {
        match &self.frontmatter {
            Frontmatter::Eager(v) => v,
            Frontmatter::Flat { head, full } => {
                full.get_or_init(|| frontmatter::parse_head(head).unwrap_or(Value::Null))
            }
        }
    }

    /// Raw YAML value of a frontmatter field, or `None` if absent.
    pub fn field(&self, name: &str) -> Option<&Value> {
        self.full().get(name)
    }

    /// An owned single field, resolved directly from a flat head without
    /// building the whole `Value` — the hot path for filtering and ordering.
    /// Falls back to the full parse for values the fast resolver doesn't handle.
    pub(crate) fn field_scalar(&self, name: &str) -> Option<Value> {
        match &self.frontmatter {
            Frontmatter::Eager(v) => v.get(name).cloned(),
            Frontmatter::Flat { head, full } => match frontmatter::scan_one(head, name) {
                frontmatter::ScanOne::Found(v) => Some(v),
                frontmatter::ScanOne::Missing => None,
                frontmatter::ScanOne::Unresolvable => full
                    .get_or_init(|| frontmatter::parse_head(head).unwrap_or(Value::Null))
                    .get(name)
                    .cloned(),
            },
        }
    }

    /// The whole parsed frontmatter value (a mapping, or `Null` if none). Used
    /// by the secondary index to enumerate every field.
    pub(crate) fn frontmatter(&self) -> &Value {
        self.full()
    }

    /// Serialize the entire frontmatter to a JSON string.
    pub fn frontmatter_json(&self) -> String {
        serde_json::to_string(self.full()).unwrap_or_else(|_| "{}".into())
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("path", &self.path)
            .field("frontmatter", self.full())
            .field("body_len", &self.body.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_with_frontmatter_and_body() {
        let content = "---\ntitle: Hello\nrating: 4\n---\nBody text.\n";
        let r = Record::from_content("note.md", content).unwrap();
        assert_eq!(r.path(), "note.md");
        assert!(r.has_frontmatter());
        assert_eq!(r.field("title").unwrap().as_str(), Some("Hello"));
        assert_eq!(r.field("rating").unwrap().as_i64(), Some(4));
        assert_eq!(r.body(), "Body text.\n");
    }

    #[test]
    fn record_without_frontmatter() {
        let content = "Just markdown, no frontmatter.\n";
        let r = Record::from_content("note.md", content).unwrap();
        assert!(!r.has_frontmatter());
        assert!(r.field("title").is_none());
        assert_eq!(r.body(), "Just markdown, no frontmatter.\n");
    }

    #[test]
    fn missing_field_returns_none() {
        let content = "---\ntitle: Hello\n---\nbody\n";
        let r = Record::from_content("note.md", content).unwrap();
        assert!(r.field("nonexistent").is_none());
    }

    #[test]
    fn array_field_access() {
        let content = "---\ntags: [rust, db, flat-file]\n---\nbody\n";
        let r = Record::from_content("note.md", content).unwrap();
        let tags = r.field("tags").unwrap();
        let seq = tags.as_sequence().unwrap();
        assert_eq!(seq.len(), 3);
        assert_eq!(seq[0].as_str(), Some("rust"));
        assert_eq!(seq[2].as_str(), Some("flat-file"));
    }

    #[test]
    fn debug_does_not_dump_full_body() {
        let content = "---\nk: v\n---\nvery long body ".repeat(100);
        let r = Record::from_content("note.md", &content).unwrap();
        let debug = format!("{:?}", r);
        assert!(debug.contains("body_len"));
        assert!(!debug.contains("very long body very long body"));
    }

    /// The fast single-field path must return exactly what the full parse would.
    /// Covers flat heads (lazy scan) and complex/eager heads (full fallback),
    /// plus values the scanner punts on (must agree via the eager fallback).
    #[test]
    fn field_scalar_equals_field_over_corpus() {
        let heads = [
            // flat, fast-resolvable
            "title: Hello\nrating: 4\nactive: true\nscore: 4.0\n",
            "name: thing\ntags: [a, b, c]\nn: -17\nf: 1.5e3\n",
            "a: 0x1F\nb: 0o17\nc: .inf\nd: .nan\ne: null\n",
            "iso: 2026-06-18\nflag: false\nempty_seq: []\n",
            // strings that look tricky but are plain
            "url: https://example.com/path\nv: v1.2.3\n",
            // space before the colon — key is `author`/`year`, not `author `
            "author : Jane\nyear : 2026\n",
            // numeric / bool / null keys — eager path keys them like serde
            "4: x\ntrue: y\nnull: z\nnormal: ok\n",
            // complex → eager fallback (nested mapping, block seq)
            "meta:\n  author: x\n  year: 2026\nitems:\n  - one\n  - two\n",
            // quoted / flow map → scanner punts, eager resolves
            "q: \"has: colon\"\nm: {x: 1, y: 2}\n",
        ];
        let names = [
            "title",
            "rating",
            "active",
            "score",
            "name",
            "tags",
            "n",
            "f",
            "a",
            "b",
            "c",
            "d",
            "e",
            "iso",
            "flag",
            "empty_seq",
            "url",
            "v",
            "meta",
            "items",
            "q",
            "m",
            "author",
            "year",
            "4",
            "true",
            "null",
            "normal",
            "absent",
        ];
        for head in heads {
            let content = format!("---\n{head}---\nbody\n");
            let r = Record::from_content("note.md", &content).unwrap();
            for name in names {
                assert_eq!(
                    r.field_scalar(name),
                    r.field(name).cloned(),
                    "field_scalar disagreed with field for {name:?} in head:\n{head}"
                );
            }
        }
    }

    /// Exhaustive single-field cartesian: every key token paired with every
    /// value token. If the record parses, `field_scalar` must equal `field` for
    /// the key and a few absent names — the core lazy-vs-full invariant.
    #[test]
    fn field_scalar_equals_field_cartesian() {
        let keys = [
            "k",
            "title",
            "a b",
            "with-dash",
            "under_score",
            "dot.dot",
            "x1",
            // tricky keys that must route through the eager path:
            "4",
            "true",
            "false",
            "null",
            "1abc",
        ];
        let vals = [
            "",
            "hello",
            "two words",
            "true",
            "false",
            "null",
            "~",
            "0",
            "4",
            "-7",
            "+3",
            "04",
            "0x1F",
            "0o17",
            "1_000",
            "4.5",
            ".5",
            "1e5",
            ".inf",
            ".nan",
            "2024-01-01",
            "2024-12-31T23:59:59Z",
            "v1.2.3",
            "[a, b]",
            "[]",
            "[1, 2, 3]",
            "mid:colon",
            "https://x/y",
            "yes",
            "no",
        ];
        let probes = ["k", "title", "a b", "4", "true", "null", "absent", "x1"];
        for key in keys {
            for val in vals {
                let head = if val.is_empty() {
                    format!("{key}:\n")
                } else {
                    format!("{key}: {val}\n")
                };
                let content = format!("---\n{head}---\nb\n");
                let Ok(r) = Record::from_content("n.md", &content) else {
                    continue; // serde-invalid head — both paths would error
                };
                for name in probes {
                    assert_eq!(
                        r.field_scalar(name),
                        r.field(name).cloned(),
                        "field_scalar != field for {name:?} in head {head:?}"
                    );
                }
            }
        }
    }

    /// A duplicate key that only differs by spacing (`a ` vs `a`) must still be
    /// rejected — the flat fast-path must not silently accept what serde errors
    /// on, or it would change which records are even readable.
    #[test]
    fn spacing_duplicate_key_still_errors() {
        let content = "---\na : 1\na: 2\n---\nbody\n";
        assert!(
            Record::from_content("n.md", content).is_err(),
            "duplicate key differing only by spacing must error like serde"
        );
    }

    /// Randomized multi-line differential fuzz: a finite corpus can't prove a
    /// hand-rolled parser, so generate thousands of small heads and assert both
    /// invariants — (1) the lazy `field_scalar` matches the full `field`, and
    /// (2) the full parse matches serde byte-for-byte. Deterministic LCG so a
    /// failure reproduces from the printed seed.
    #[test]
    fn fuzz_field_scalar_and_full_match_serde() {
        // tokens chosen to stress key trimming, numeric/bool/null typing,
        // fallback triggers (hex/float/quote/comment), and flow seqs.
        let keys = ["k", "title", "a b", "4", "true", "null", "x_1", "with-dash"];
        let vals = [
            "",
            "hi",
            "two words",
            "true",
            "false",
            "null",
            "~",
            "0",
            "-7",
            "04",
            "0x1F",
            "1_000",
            "4.5",
            "1e5",
            ".inf",
            "2024-01-01",
            "[a, b]",
            "[]",
            "mid:colon",
            "v1.2.3",
            "yes",
        ];
        let mut state: u64 = 0x9e3779b97f4a7c15;
        let mut next = |n: usize| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) as usize) % n
        };
        let probes = [
            "k",
            "title",
            "a b",
            "4",
            "true",
            "null",
            "x_1",
            "with-dash",
            "absent",
        ];
        for _ in 0..8000 {
            let lines = 1 + next(4);
            let mut head = String::new();
            for _ in 0..lines {
                let k = keys[next(keys.len())];
                let v = vals[next(vals.len())];
                if v.is_empty() {
                    head.push_str(&format!("{k}:\n"));
                } else {
                    head.push_str(&format!("{k}: {v}\n"));
                }
            }
            let content = format!("---\n{head}---\nb\n");
            let Ok(r) = Record::from_content("n.md", &content) else {
                // serde-invalid (e.g. duplicate key) — both paths error alike.
                assert!(
                    serde_yaml::from_str::<Value>(&head).is_err(),
                    "we errored but serde accepts head {head:?}"
                );
                continue;
            };
            // (1) lazy single-field == full field
            for name in probes {
                assert_eq!(
                    r.field_scalar(name),
                    r.field(name).cloned(),
                    "field_scalar != field for {name:?} in head {head:?}"
                );
            }
            // (2) full parse == serde, exactly
            let serde_v: Value = serde_yaml::from_str(&head).expect("flat head is valid YAML");
            assert_eq!(
                r.frontmatter(),
                &serde_v,
                "full parse diverged from serde for head {head:?}"
            );
        }
    }
}
