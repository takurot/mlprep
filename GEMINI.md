# mlprep Project Context

## Project Overview
`mlprep` is a high-performance, no-code data preprocessing engine for Machine Learning, powered by **Rust** and **Polars**. It provides both a CLI tool and a Python library to handle complex ETL, feature engineering, and data validation tasks with significant speed and memory improvements over standard Pandas-based workflows.

**Key Features:**
- **Blazing Speed:** Multi-threaded execution via Rust and Polars.
- **Zero-Code:** Pipelines defined in YAML (`pipeline.yaml`).
- **Data Quality:** "Quarantine" mode for bad data, schema validation.
- **Hybrid Architecture:** Rust core for performance, Python bindings for ease of use.

## Architecture & Tech Stack
- **Core Engine (Rust):** Implements the heavy lifting (IO, compute, validation). Located in `src/`.
- **Python Bindings:** Uses `PyO3` and `Maturin` to expose Rust functionality to Python.
- **CLI:** Rust-based CLI (`clap`) for running pipelines directly from the terminal.
- **Data Interchange:** Apache Arrow (zero-copy between Rust and Python/Polars).

## Building and Running

### Rust (Core & CLI)
*   **Build:** `cargo build`
*   **Run CLI:** `cargo run --bin mlprep -- <args>`
    *   *Example:* `cargo run --bin mlprep -- run examples/01_basic_etl/pipeline.yaml`
*   **Test:** `cargo test --no-default-features`
    *   *Note:* The `--no-default-features` flag is used in CI, likely to handle `pyo3` linking features correctly during pure Rust testing.
*   **Format:** `cargo fmt --all`
*   **Lint:** `cargo clippy --all-targets --no-default-features -- -D warnings`

### Python (Bindings)
*   **Prerequisite:** `pip install maturin`
*   **Build & Install (Dev):** `maturin develop` (Builds Rust extension and installs it into current venv)
*   **Build Wheels:** `maturin build --release`
*   **Test:** `pytest -q python/tests`
*   **Format:** `ruff format python/`
*   **Lint:** `ruff check python/`

## Directory Structure
*   `src/`: Rust source code.
    *   `lib.rs`: Library entry point (Python bindings `#[pymodule]`).
    *   `main.rs`: CLI binary entry point.
    *   `engine.rs`, `dsl.rs`, `features.rs`: Core logic modules.
*   `python/`: Python package source (`mlprep`).
    *   `mlprep/__init__.py`: Imports extension module.
*   `tests/`: Rust integration and E2E tests.
*   `examples/`: Sample `pipeline.yaml` files and data for various use cases.
*   `docs/`: Documentation (SPEC, PLAN, architecture).
*   `.github/workflows/`: CI/CD definitions.

## Development Conventions
*   **Error Handling:** Use `miette` for user-facing CLI errors and `thiserror` for library errors.
*   **Logging:** Use `tracing` for structured logging.
*   **Style:**
    *   **Rust:** Follow standard Rustfmt and Clippy advice.
    *   **Python:** Follow PEP 8 via `ruff`.
*   **Commits:** Use [Conventional Commits](https://www.conventionalcommits.org/) (e.g., `feat:`, `fix:`, `docs:`).
*   **Testing:**
    *   Rust integration tests in `tests/` should use `tempfile` for I/O.
    *   Python tests in `python/tests/` should use `pytest` fixtures.

## Key Files
*   `pipeline.yaml`: The central configuration file for defining ML pipelines.
*   `Cargo.toml`: Rust workspace and dependency definitions.
*   `pyproject.toml`: Python build configuration and dependencies.
*   `src/lib.rs`: The bridge between Rust core and Python (PyO3 module definition).
