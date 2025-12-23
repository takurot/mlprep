# Benchmark Results

Target: CSV 1GB Read < 5s (Release build, 8 cores)

## Current Results

| Date | Commit | Scenario | Time (s) | Notes |
|------|--------|----------|----------|-------|
| 2025-12-23 | PR-10 | Read CSV (mlprep) | 0.0535 | 0.5GB, Jemalloc enabled |
| 2025-12-23 | PR-10 | Read CSV (Polars) | 1.4099 | Baseline (Python) |
| 2025-12-23 | PR-10 | GroupBy (Polars) | 0.2153 | Baseline |
| 2025-12-23 | PR-10 | Join (Polars) | 0.2455 | Baseline |
