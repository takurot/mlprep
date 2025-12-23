# Performance

mlprep is built on **Polars** and **Rust** for optimal performance.

## Benchmarks

| Operation | mlprep | pandas | Speedup |
|-----------|--------|--------|---------|
| CSV Read (1GB) | ~3s | ~15s | 5x |
| Pipeline Execution | ~2s | ~8s | 4x |
| Memory Usage | 1.5GB | 4GB+ | 2.5x less |

*Benchmarks on Apple M1 Pro, 16GB RAM, SSD*

## Performance Tips

### 1. Use Parquet Format

Parquet is significantly faster than CSV for both reading and writing:

```yaml
outputs:
  - path: output.parquet
    format: parquet
```

### 2. Filter Early

Apply filters as early as possible to reduce data volume:

```yaml
steps:
  - filter:        # Do this first
      column: active
      op: "=="
      value: true
  - groupby:       # Then aggregate
      by: [category]
      agg: [...]
```

### 3. Select Only Needed Columns

Reduce memory usage by selecting only required columns:

```yaml
steps:
  - select:
      columns: [id, name, value]  # Only what you need
```

### 4. Use Streaming for Large Files

mlprep uses Polars' lazy evaluation, which enables streaming for files larger than memory.

## Running Benchmarks

```bash
python scripts/benchmark.py --size 1.0 --compare-pandas
```

Options:
- `--size`: Data size in GB (default: 0.1)
- `--compare-pandas`: Include pandas comparison
- `--output`: Output format (markdown/json)

## Memory Optimization

mlprep uses these optimizations:
- **jemalloc**: More efficient memory allocation (on Linux/macOS)
- **Lazy evaluation**: Operations are optimized before execution
- **Column pruning**: Unused columns are not loaded
- **Predicate pushdown**: Filters are applied during read
