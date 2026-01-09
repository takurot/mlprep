# Repository Guidelines

## Project Structure & Modules
- `src/` — Rust crate: core engine in `lib.rs`, CLI in `main.rs`, modules like `dsl.rs`, `runner.rs`, `features.rs`, `validate.rs`.
- `python/` — Python package `mlprep` (CLI wrapper in `mlprep/cli.py`, entry via `python -m mlprep`). Tests live in `python/tests/`.
- `tests/` — Rust integration/E2E tests (`*_test.rs`, `*_integration.rs`).
- `examples/`, `docs/`, `scripts/benchmark.py` — runnable samples, docs, and perf tooling.

## Build, Test, and Dev Commands
- Rust build/run: `cargo build` • `cargo run -- run examples/01_basic_etl/pipeline.yaml`
- Rust tests: `cargo test`
- Python dev install (maturin): `pip install maturin` then `maturin develop` (builds and installs the extension into the active venv)
- Python tests: `pytest -q python/tests`
- Lint/format: `cargo fmt --all` • `cargo clippy --all-targets -- -D warnings` • `ruff check .` • `ruff format .`
- Pre-commit: `pre-commit install && pre-commit run -a`

## Coding Style & Naming
- Indentation: 4 spaces, LF endings (`.editorconfig`).
- Rust: Edition 2021. snake_case for functions/modules, CamelCase for types. Prefer `miette` for CLI-facing errors and `tracing` for logs.
- Python: Target 3.10+, Ruff line length 88. snake_case for modules/functions, CapWords for classes. Add type hints where practical.

## Testing Guidelines
- Rust: Place integration tests in `tests/`. Use `tempfile` and avoid non-temporary filesystem writes. Keep tests deterministic and parallel-safe.
- Python: Name tests `python/tests/test_*.py`. Use temporary dirs/files; avoid network calls. Run with `pytest -q`.
- Coverage is not enforced; add meaningful tests alongside new features and bug fixes.

## Commit & PR Guidelines
- Use Conventional Commits: `feat(scope): summary`, `fix: ...`, `docs: ...`, `chore: ...`. Reference issues/PRs (e.g., `(#23)`).
- PRs should include: what/why, key changes, how to test (commands), and any perf/safety notes. Add screenshots/log snippets when relevant.

## Security & Runtime Tips
- I/O sandboxing and masking: prefer `--allowed-paths` and `--mask-columns` when invoking the CLI.
- Tuning knobs: `--streaming`, `--memory-limit`, `--threads`, `--cache`; set log level via `MLPREP_LOG` or `--log-format json` for structured logs.

