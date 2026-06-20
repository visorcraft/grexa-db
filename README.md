# grexa-db

A flat-file database engine where records are plain files in a directory tree
and relational joins materialize as directories of symlinks. The filesystem is
the interface: any tool that reads files (`rg`, `grep`, editors, file managers)
is a client without knowing the database exists.

> Extracted from the [Grexa](https://github.com/visorcraft/grexa) workspace into
> its own repository so it can be embedded standalone. `Apache-2.0`, so it stays
> usable in proprietary applications.

## Workspace layout

| Crate | What |
|-------|------|
| [`crates/grexa-db`](crates/grexa-db) | The engine (library). See its [README](crates/grexa-db/README.md) for the quick start. |
| [`crates/grexa-db-cli`](crates/grexa-db-cli) | `grexa-db-cli` — standalone CLI over a database directory. |

## Use it as a dependency

```toml
# Git dependency, pinned to a tag:
grexa-db = { git = "https://github.com/visorcraft/grexa-db", tag = "v1.9.1" }
```

## Docs

- [`docs/grexa-db-design.md`](docs/grexa-db-design.md) — full design spec:
  storage layout, schema format, field types, query API, view materialization,
  concurrency model, reference-path safety.
- [`docs/grexa-db-implementation-plan.md`](docs/grexa-db-implementation-plan.md)
  — phase status, what's done, what's deferred.
- [`docs/grexa-db-scaling-rnd.md`](docs/grexa-db-scaling-rnd.md) — scaling R&D:
  the parallel scan, the held secondary index, and the measured numbers.

## Build

```bash
cargo build
cargo test
```

## License

`Apache-2.0` — see [LICENSE](LICENSE).
