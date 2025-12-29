# Benchmark Results

Target: CSV 1GB Read < 5s (Release build, 8 cores)

## Core Scenarios

| Date | Commit | Scenario | Time (s) | Notes |
|------|--------|----------|----------|-------|
| 2025-12-23 | PR-10 | Read CSV (mlprep) | 0.0535 | 0.5GB, Jemalloc enabled |
| 2025-12-23 | PR-10 | Read CSV (Polars) | 1.4099 | Baseline (Python) |
| 2025-12-23 | PR-10 | GroupBy (Polars) | 0.2153 | Baseline |
| 2025-12-23 | PR-10 | Join (Polars) | 0.2455 | Baseline |

## Showcase (Validation + Features)

`scripts/benchmark.py` now runs the showcase workload by default (`--showcase`), combining validation (unique + range) and feature engineering (standard scale + one-hot) with throughput (`Rows/s`) output. CLI overhead is measured both with and without streaming enabled.

| Date | Commit | Scenario | Time (s) | Notes |
|------|--------|----------|----------|-------|
| 2025-12-28 | PR-21 | Showcase (mlprep streaming) | — | validate + features, streaming enabled |
| 2025-12-28 | PR-21 | Showcase (mlprep) | — | validate + features, non-streaming |
| 2025-12-28 | PR-21 | Showcase (Pandas) | — | baseline validation + z-score |

> Re-run `env PATH="target/debug:$PATH" python3 scripts/benchmark.py --size 1 --format showcase` after installing Python deps (`polars`, optional `pandas`) to populate times for 1GB datasets.
