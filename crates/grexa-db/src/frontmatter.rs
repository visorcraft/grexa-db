// SPDX-FileCopyrightText: 2026 VisorCraft LLC
// SPDX-License-Identifier: Apache-2.0

//! YAML frontmatter extraction and parsing.
//!
//! Splits a file's content into an optional YAML frontmatter block and the
//! remaining body. The frontmatter format follows the Jekyll / Hugo /
//! Obsidian convention: an opening `---` on the first line, YAML content,
//! and a closing `---` on its own line.
//!
//! ```text
//! ---
//! title: Hello
//! tags: [a, b]
//! ---
//! Body content here.
//! ```
//!
//! The opening `---` must be the very first line (an optional UTF-8 BOM is
//! tolerated). A line that is exactly `---` (after trimming trailing
//! whitespace) closes the block. Files without frontmatter pass through
//! unchanged.

use serde_yaml::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FrontmatterError {
    #[error("frontmatter opened with `---` but never closed")]
    Unclosed,
    #[error("invalid YAML in frontmatter: {0}")]
    Yaml(String),
}

/// The result of splitting a file into frontmatter and body.
#[derive(Debug)]
pub struct Split<'a> {
    /// Parsed YAML frontmatter, or `None` if the file had no frontmatter
    /// block (or the block was empty).
    pub frontmatter: Option<Value>,
    /// The body content — everything after the frontmatter block, verbatim.
    pub body: &'a str,
}

/// Split a file's content into optional YAML frontmatter and body.
///
/// See the [module docs](self) for the format. Returns
/// `Split { frontmatter: None, body: <input> }` for files without an
/// opening `---` delimiter.
pub fn split(content: &str) -> Result<Split<'_>, FrontmatterError> {
    let raw = split_raw(content)?;
    let frontmatter = match raw.head {
        None => None,
        Some(h) => Some(parse_head(h)?),
    };
    Ok(Split {
        frontmatter,
        body: raw.body,
    })
}

/// The lexical split — the raw frontmatter text and the body, without parsing
/// the YAML. Lets [`crate::record::Record`] defer the parse (it only resolves
/// the fields a query touches; see `Record::field_scalar`).
pub(crate) struct RawSplit<'a> {
    pub head: Option<&'a str>,
    pub body: &'a str,
}

pub(crate) fn split_raw(content: &str) -> Result<RawSplit<'_>, FrontmatterError> {
    let stripped = content.strip_prefix('\u{feff}').unwrap_or(content);
    let rest = stripped
        .strip_prefix("---\r\n")
        .or_else(|| stripped.strip_prefix("---\n"));
    let Some(rest) = rest else {
        return Ok(RawSplit {
            head: None,
            body: content,
        });
    };
    let mut offset = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end() == "---" {
            let yaml_str = &rest[..offset];
            let body = &rest[offset + line.len()..];
            let head = if yaml_str.trim().is_empty() {
                None
            } else {
                Some(yaml_str)
            };
            return Ok(RawSplit { head, body });
        }
        offset += line.len();
    }
    Err(FrontmatterError::Unclosed)
}

/// Parse a non-empty frontmatter head — fast path, else serde (errors as serde).
pub(crate) fn parse_head(head: &str) -> Result<Value, FrontmatterError> {
    if let Some(v) = fast_parse(head) {
        Ok(v)
    } else {
        serde_yaml::from_str(head).map_err(|e| FrontmatterError::Yaml(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Fast path for the common flat `key: value` frontmatter, avoiding the
// serde_yaml / libyaml machinery. It resolves scalars to *exactly* the same
// `Value` serde_yaml_ng produces (verified empirically and locked by the
// differential test), and returns `None` — falling back to serde — for ANY
// input it isn't certain about (indentation, block/flow collections beyond a
// simple `[a, b]`, quotes, comments, anchors, hex/octal/float/leading-zero
// numbers, duplicate keys, …). So a stale heuristic can only ever be *slower*,
// never wrong.
// ---------------------------------------------------------------------------

/// Parse a flat frontmatter block, or `None` to fall back to serde_yaml.
fn fast_parse(yaml: &str) -> Option<Value> {
    let mut map = serde_yaml::Mapping::new();
    for line in yaml.lines() {
        if line.trim().is_empty() {
            continue;
        }
        // Any leading whitespace means a nested/multi-line structure → fall back.
        if line.starts_with([' ', '\t']) {
            return None;
        }
        let (key, val) = if let Some(i) = line.find(": ") {
            (&line[..i], &line[i + 2..])
        } else if let Some(k) = line.strip_suffix(':') {
            (k, "")
        } else {
            return None;
        };
        if key.is_empty() {
            return None; // `: value` — serde rejects a bare null key; let it.
        }
        let key_v = resolve_scalar(key)?;
        let val_v = resolve_scalar(val)?;
        if map.contains_key(&key_v) {
            return None; // duplicate key — let serde decide
        }
        map.insert(key_v, val_v);
    }
    if map.is_empty() {
        return None;
    }
    Some(Value::Mapping(map))
}

/// Resolve one YAML scalar to a `Value`, or `None` to fall back to serde.
fn resolve_scalar(raw: &str) -> Option<Value> {
    let s = raw.trim();
    if s.is_empty() || matches!(s, "null" | "Null" | "NULL" | "~") {
        return Some(Value::Null);
    }
    match s {
        "true" | "True" | "TRUE" => return Some(Value::Bool(true)),
        "false" | "False" | "FALSE" => return Some(Value::Bool(false)),
        _ => {}
    }
    if s.starts_with('[') {
        return if s.ends_with(']') {
            parse_flow_seq(s)
        } else {
            None // multi-line flow → fall back
        };
    }
    if is_number_shaped(s) {
        // Only resolve plain decimal ints we're sure of; hex/octal/float/
        // leading-zero/overflow fall back to serde for exact resolution.
        return safe_decimal_int(s).map(Value::from);
    }
    if is_plain_string(s) {
        return Some(Value::String(s.to_string()));
    }
    None
}

/// A single-line `[a, b, c]` flow sequence of simple scalars (no nesting,
/// quotes, or embedded commas), or `None` to fall back.
fn parse_flow_seq(s: &str) -> Option<Value> {
    let inner = s.strip_prefix('[')?.strip_suffix(']')?.trim();
    if inner.is_empty() {
        return Some(Value::Sequence(Vec::new()));
    }
    if inner.contains(['[', ']', '{', '}', '"', '\'']) {
        return None;
    }
    let mut seq = Vec::new();
    for part in inner.split(',') {
        seq.push(resolve_scalar(part.trim())?);
    }
    Some(Value::Sequence(seq))
}

/// True if serde might resolve `s` as a number (decimal/hex/octal int, float,
/// or `.inf`/`.nan`). A *superset* of serde's numbers, so any real number is
/// caught and resolved-or-fallen-back — never mis-typed as a string.
fn is_number_shaped(s: &str) -> bool {
    is_core_int_shape(s) || is_core_float_shape(s)
}

fn is_core_int_shape(s: &str) -> bool {
    // decimal: [+-]?(0 | [1-9][0-9]*)   — leading zeros are NOT ints (they're strings)
    let dec = {
        let b = s.strip_prefix(['+', '-']).unwrap_or(s);
        b == "0"
            || (!b.is_empty() && b.as_bytes()[0] != b'0' && b.bytes().all(|c| c.is_ascii_digit()))
    };
    let hex = s
        .strip_prefix("0x")
        .is_some_and(|h| !h.is_empty() && h.bytes().all(|c| c.is_ascii_hexdigit()));
    let oct = s
        .strip_prefix("0o")
        .is_some_and(|o| !o.is_empty() && o.bytes().all(|c| (b'0'..=b'7').contains(&c)));
    dec || hex || oct
}

fn is_core_float_shape(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    if matches!(lower.as_str(), ".inf" | "+.inf" | "-.inf" | ".nan") {
        return true;
    }
    let body = s.strip_prefix(['+', '-']).unwrap_or(s);
    if body.is_empty() {
        return false;
    }
    let (mantissa, exp) = match body.split_once(['e', 'E']) {
        Some((m, e)) => (m, Some(e)),
        None => (body, None),
    };
    if let Some(e) = exp {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        if e.is_empty() || !e.bytes().all(|c| c.is_ascii_digit()) {
            return false;
        }
    }
    if mantissa.bytes().filter(|&c| c == b'.').count() > 1 {
        return false;
    }
    if !mantissa.bytes().all(|c| c.is_ascii_digit() || c == b'.') {
        return false;
    }
    if !mantissa.bytes().any(|c| c.is_ascii_digit()) {
        return false; // bare "." is not a float
    }
    // A float must have a '.' or an exponent (else it's an int).
    mantissa.contains('.') || exp.is_some()
}

/// Parse a plain decimal integer we're certain serde agrees on, else `None`.
fn safe_decimal_int(s: &str) -> Option<i64> {
    let b = s.strip_prefix(['+', '-']).unwrap_or(s);
    let plain_decimal = b == "0"
        || (!b.is_empty() && b.as_bytes()[0] != b'0' && b.bytes().all(|c| c.is_ascii_digit()));
    if !plain_decimal {
        return None; // hex/octal/leading-zero → fall back
    }
    s.parse::<i64>().ok() // overflow → fall back
}

/// True if `s` is a plain unquoted string with no YAML-special meaning, so
/// serde would also resolve it as a `String`. Conservative: any indicator
/// character, comment, or `: ` falls back.
fn is_plain_string(s: &str) -> bool {
    let first = s.as_bytes()[0];
    if br#"[]{}&*!|>%@`"'#,?:-"#.contains(&first) {
        return false;
    }
    if s.contains(" #") || s.contains(": ") {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Lazy single-field resolution. `Record` keeps the raw head text for a
// "flat" frontmatter (every line a simple `key: scalar`/`[a, b]` with a plain
// key, no duplicates) and resolves only the fields a query touches, via
// `scan_one` — never building the whole `Value`. Because a flat head is, by
// construction, valid YAML, deferring its parse can't change error behavior;
// anything not flat is parsed eagerly by serde (errors preserved).
// ---------------------------------------------------------------------------

/// Result of looking up one field in a flat head.
pub(crate) enum ScanOne {
    Found(Value),
    Missing,
    /// The value exists but isn't one the fast resolver handles (float, hex,
    /// quoted, …) — the caller should fall back to a full serde parse.
    Unresolvable,
}

/// A line of a flat head, as `(key, value)` raw text, or `None` if the line
/// isn't a simple top-level `key: value` (indented, complex value, no key, …).
fn parse_flat_line(line: &str) -> Option<(&str, &str)> {
    if line.starts_with([' ', '\t']) {
        return None;
    }
    let (key, val) = if let Some(i) = line.find(": ") {
        (&line[..i], &line[i + 2..])
    } else if let Some(k) = line.strip_suffix(':') {
        (k, "")
    } else {
        return None;
    };
    // Trim the key (`foo : bar` keys on `foo`, not `foo `) and require a plain
    // *string* key, so a flat head's key matches a query name by exact string
    // equality — identical to `Value::get(name)` on the fully-parsed mapping.
    let key = key.trim();
    if !is_plain_string_key(key) {
        return None;
    }
    if !value_self_contained(val) {
        return None;
    }
    Some((key, val))
}

/// True if `key` is a plain unquoted string that serde resolves to *itself* —
/// so it keys the mapping by `String(key)` and matches a query name by string
/// equality. Excludes keys serde would type as a number / bool / null (which
/// would key by a non-string value, making a string-name lookup miss). Such
/// heads fall back to the eager parse, which keys them exactly like serde.
fn is_plain_string_key(key: &str) -> bool {
    !key.is_empty()
        && is_plain_string(key)
        && !is_number_shaped(key)
        && !matches!(
            key,
            "null" | "Null" | "NULL" | "~" | "true" | "True" | "TRUE" | "false" | "False" | "FALSE"
        )
}

/// True if `val` is a single-line scalar or simple `[a, b]` flow sequence with
/// no YAML-special meaning — so the line can't hide structure (a nested
/// mapping, a continued block/flow) that would make an "absent" answer wrong.
fn value_self_contained(val: &str) -> bool {
    let v = val.trim();
    if v.is_empty() {
        return true;
    }
    let first = v.as_bytes()[0];
    match first {
        b'[' => v.ends_with(']') && !v[1..v.len() - 1].contains(['[', '{', '"', '\'']),
        b'{' | b'|' | b'>' | b'"' | b'\'' | b'&' | b'*' | b'!' | b'%' | b'@' | b'`' | b'#' => false,
        _ => !v.contains(" #") && !v.contains(": "),
    }
}

/// True if `head` is flat (every line a simple `key: value`, plain keys, no
/// duplicate keys) — so it's valid YAML and a single field can be resolved
/// without parsing the whole thing.
pub(crate) fn is_flat(head: &str) -> bool {
    let mut keys: Vec<&str> = Vec::new();
    for line in head.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Some((key, _)) = parse_flat_line(line) else {
            return false;
        };
        if keys.contains(&key) {
            return false; // duplicate key — let serde decide
        }
        keys.push(key);
    }
    !keys.is_empty()
}

/// Resolve a single field from a head already known to be flat
/// ([`is_flat`] returned true).
pub(crate) fn scan_one(head: &str, name: &str) -> ScanOne {
    let mut found = None;
    for line in head.lines() {
        if line.trim().is_empty() {
            continue;
        }
        // `head` is flat, so every line parses; ignore any that somehow don't.
        let Some((key, val)) = parse_flat_line(line) else {
            return ScanOne::Unresolvable;
        };
        if key == name {
            if found.is_some() {
                return ScanOne::Unresolvable; // duplicate (shouldn't happen if flat)
            }
            match resolve_scalar(val) {
                Some(v) => found = Some(v),
                None => return ScanOne::Unresolvable,
            }
        }
    }
    match found {
        Some(v) => ScanOne::Found(v),
        None => ScanOne::Missing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE safety net: wherever the fast path produces a value, it must equal
    /// what serde_yaml produces — byte-for-byte. Covers every resolution branch
    /// and every fallback trigger. (The whole record-parse test suite also
    /// exercises this indirectly, since all parsing now goes through `split`.)
    #[test]
    fn fast_parse_equals_serde_over_corpus() {
        let values = [
            // bools / nulls (and 1.1-isms that are STRINGS in 1.2 core)
            "true",
            "false",
            "True",
            "TRUE",
            "False",
            "FALSE",
            "yes",
            "no",
            "on",
            "off",
            "y",
            "n",
            "null",
            "Null",
            "NULL",
            "~",
            "",
            // ints + fallbacks (leading zero, hex, octal, underscore, overflow)
            "0",
            "-0",
            "4",
            "-4",
            "+4",
            "42",
            "123456789",
            "04",
            "0123",
            "0x1F",
            "0o17",
            "1_000",
            "99999999999999999999",
            "-99999999999999999999",
            // floats / special (all must fall back to serde)
            "4.5",
            "-4.5",
            ".5",
            "5.",
            "1e5",
            "1E5",
            "1.5e3",
            "+1.0",
            ".inf",
            "-.inf",
            ".nan",
            "inf",
            "nan",
            // strings incl dates, words, punctuation
            "rust",
            "ai",
            "2024-01-01",
            "2024-12-31T23:59:59Z",
            "a b c",
            "true story",
            "mid:colon",
            "with-dash",
            "under_score",
            "dot.dot",
            "1abc",
            "abc1",
            "3d",
            "v1.2.3",
            // flow seqs
            "[1, 2, 3]",
            "[a, b]",
            "[]",
            "[rust, ai, ml]",
            "[true, false]",
            "[1, a, 2024-01-01]",
            "[nested, [x]]",
            "[a, \"b\"]",
            // must fall back: quotes, flow map, indicators, comments
            "\"quoted\"",
            "'single'",
            "{a: 1}",
            "#comment",
            "value # tail",
            "&anchor",
            "*alias",
            "| block",
            "> folded",
            "%TAG",
            "@at",
            "- item",
            "? key",
            ": colon",
        ];
        let mut fast_hits = 0;
        for v in values {
            let head = format!("k: {v}\n");
            let serde_v: Option<Value> = serde_yaml::from_str(&head).ok();
            if let Some(fast) = fast_parse(&head) {
                assert_eq!(Some(&fast), serde_v.as_ref(), "fast_parse({head:?}) != serde");
                fast_hits += 1;
            }
        }
        assert!(fast_hits > 25, "fast path engaged too rarely ({fast_hits})");

        // Realistic multi-field heads + structural fallbacks.
        let heads = [
            "title: Note 1\ntags: [rust, db]\nrating: 4\nread_at: 2024-03-15\n",
            "a: 1\nb: two words\nc: [x, y]\nd: true\ne: null\nf: -7\ng: 2024-01-01\n",
            "nested:\n  a: 1\n", // indentation → fallback
            "a: 1\na: 2\n",      // duplicate key → fallback
            "blk: |\n  text\n",  // block scalar → fallback
            "n: 1.5\n",          // float → fallback
            "h: 0xFF\n",         // hex → fallback
            "c: red # nope\n",   // comment → fallback
            "4: x\nfive: 5\n",   // numeric key (fast path handles)
            // serde-error / odd shapes the fast path must NOT accept:
            ": invalid\n",
            "key:value\n", // no space → a plain scalar, not a mapping
            "a: 1\n: b\n",
        ];
        for head in heads {
            let serde_v: Option<Value> = serde_yaml::from_str(head).ok();
            if let Some(fast) = fast_parse(head) {
                assert_eq!(Some(&fast), serde_v.as_ref(), "fast_parse({head:?}) != serde");
            }
        }
    }

    #[test]
    fn no_frontmatter_returns_body_unchanged() {
        let result = split("just body\n").unwrap();
        assert!(result.frontmatter.is_none());
        assert_eq!(result.body, "just body\n");
    }

    #[test]
    fn empty_file() {
        let result = split("").unwrap();
        assert!(result.frontmatter.is_none());
        assert_eq!(result.body, "");
    }

    #[test]
    fn dashes_not_at_start_are_body() {
        let result = split("--- not frontmatter\n").unwrap();
        assert!(result.frontmatter.is_none());
        assert_eq!(result.body, "--- not frontmatter\n");
    }

    #[test]
    fn simple_frontmatter_and_body() {
        let content = "---\ntitle: Hello\ntags: [a, b]\n---\nBody text.\n";
        let result = split(content).unwrap();
        let fm = result.frontmatter.expect("frontmatter should parse");
        assert_eq!(fm["title"].as_str(), Some("Hello"));
        assert_eq!(fm["tags"][0].as_str(), Some("a"));
        assert_eq!(fm["tags"][1].as_str(), Some("b"));
        assert_eq!(result.body, "Body text.\n");
    }

    #[test]
    fn frontmatter_preserves_body_verbatim() {
        let content = "---\nk: v\n---\n# Heading\n\nParagraph with --- in it.\n";
        let result = split(content).unwrap();
        assert_eq!(result.body, "# Heading\n\nParagraph with --- in it.\n");
    }

    #[test]
    fn empty_frontmatter_block() {
        let result = split("---\n---\nbody\n").unwrap();
        assert!(result.frontmatter.is_none());
        assert_eq!(result.body, "body\n");
    }

    #[test]
    fn whitespace_only_frontmatter_is_none() {
        let result = split("---\n   \n  \n---\nbody\n").unwrap();
        assert!(result.frontmatter.is_none());
        assert_eq!(result.body, "body\n");
    }

    #[test]
    fn unclosed_frontmatter_errors() {
        let result = split("---\ntitle: Hello\nbody without closer\n");
        assert!(matches!(result, Err(FrontmatterError::Unclosed)));
    }

    #[test]
    fn closing_delimiter_at_eof_without_newline() {
        let content = "---\ntitle: Hello\n---";
        let result = split(content).unwrap();
        let fm = result.frontmatter.expect("frontmatter");
        assert_eq!(fm["title"].as_str(), Some("Hello"));
        assert_eq!(result.body, "");
    }

    #[test]
    fn body_containing_dashes_is_preserved() {
        let content = "---\nk: v\n---\n---\n---\n";
        let result = split(content).unwrap();
        assert_eq!(result.body, "---\n---\n");
    }

    #[test]
    fn crlf_line_endings() {
        let content = "---\r\ntitle: Hello\r\ntags: [a, b]\r\n---\r\nBody.\r\n";
        let result = split(content).unwrap();
        let fm = result.frontmatter.expect("frontmatter");
        assert_eq!(fm["title"].as_str(), Some("Hello"));
        assert_eq!(fm["tags"][0].as_str(), Some("a"));
        assert_eq!(result.body, "Body.\r\n");
    }

    #[test]
    fn trailing_whitespace_after_closing_delimiter() {
        let result = split("---\nk: v\n---   \nbody\n").unwrap();
        assert_eq!(result.body, "body\n");
    }

    #[test]
    fn bom_at_start_is_stripped() {
        let content = "\u{feff}---\nk: v\n---\nbody\n";
        let result = split(content).unwrap();
        let fm = result.frontmatter.expect("frontmatter");
        assert_eq!(fm["k"].as_str(), Some("v"));
        assert_eq!(result.body, "body\n");
    }

    #[test]
    fn invalid_yaml_errors() {
        let result = split("---\n: invalid\n---\nbody\n");
        assert!(matches!(result, Err(FrontmatterError::Yaml(_))));
    }

    #[test]
    fn nested_and_typed_values() {
        let content = "---\nmetadata:\n  author: Jane\n  year: 2024\nrating: 4.5\npublished: true\n---\nbody\n";
        let result = split(content).unwrap();
        let fm = result.frontmatter.expect("frontmatter");
        assert_eq!(fm["metadata"]["author"].as_str(), Some("Jane"));
        assert_eq!(fm["metadata"]["year"].as_i64(), Some(2024));
        assert_eq!(fm["rating"].as_f64(), Some(4.5));
        assert_eq!(fm["published"].as_bool(), Some(true));
    }

    #[test]
    fn empty_array_value() {
        let content = "---\ntags: []\n---\nbody\n";
        let result = split(content).unwrap();
        let fm = result.frontmatter.expect("frontmatter");
        let tags = fm["tags"].as_sequence().expect("should be a sequence");
        assert!(tags.is_empty());
    }

    #[test]
    fn bom_preserved_when_no_frontmatter() {
        let content = "\u{feff}just body with bom\n";
        let result = split(content).unwrap();
        assert!(result.frontmatter.is_none());
        assert_eq!(result.body, "\u{feff}just body with bom\n");
    }
}
