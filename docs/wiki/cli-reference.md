# CLI Reference

Complete reference for the mlprep command-line interface.

## Synopsis

```bash
mlprep [OPTIONS] <COMMAND>
```

## Commands

### `mlprep run`

Run a data processing pipeline from a YAML configuration file.

```bash
mlprep run <PIPELINE_FILE>... [OPTIONS]
```

#### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `PIPELINE_FILE` | Yes | One or more pipeline YAML files to execute sequentially |

#### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--verbose` | `-v` | Enable debug logging (INFO → DEBUG) | off |
| `--quiet` | `-q` | Silence all logs except errors | off |
| `--log-format` | | Log format: `text` or `json` | `text` |
| `--allowed-paths` | | Sandboxed I/O paths (can specify multiple) | none |
| `--mask-columns` | | Columns to mask in log output | none |
| `--streaming` | | Enable streaming mode (low memory) | off |
| `--memory-limit` | | Set memory limit (e.g., `4GB`, `500MB`) | none |
| `--threads` | | Override `POLARS_MAX_THREADS` | env default |
| `--cache` | | Toggle Polars plan cache (`POLARS_CACHE`) | none |

#### Examples

```bash
# Basic usage
mlprep run pipeline.yaml

# With verbose logging
mlprep run pipeline.yaml --verbose

# Silent mode
mlprep run pipeline.yaml --quiet

# JSON logging for CI/CD
mlprep run pipeline.yaml --log-format json

# Security sandboxing
mlprep run pipeline.yaml --allowed-paths ./data --allowed-paths ./output

# Mask sensitive columns in logs
mlprep run pipeline.yaml --mask-columns ssn --mask-columns password

# Streaming mode for large files
mlprep run pipeline.yaml --streaming

# With memory limit
mlprep run pipeline.yaml --memory-limit 8GB

# Multi-run in one process (reduces CLI startup overhead)
mlprep run pipeline.yaml pipeline_eval.yaml --threads 8 --streaming

# Combined options
mlprep run pipeline.yaml --verbose --streaming --memory-limit 4GB
```

---

## Global Options

These options work with all commands:

| Option | Description |
|--------|-------------|
| `--help`, `-h` | Print help information |
| `--version`, `-V` | Print version information |

---

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `MLPREP_LOG` | Set log level (overrides CLI flags) | `MLPREP_LOG=debug` |

### Log Levels

| Level | Description |
|-------|-------------|
| `trace` | Most verbose, includes all internal details |
| `debug` | Detailed debugging information |
| `info` | General progress information (default) |
| `warn` | Warning messages only |
| `error` | Error messages only |

```bash
# Run with debug logging via environment
MLPREP_LOG=debug mlprep run pipeline.yaml

# Run with trace logging
MLPREP_LOG=trace mlprep run pipeline.yaml
```

---

## Exit Codes

| Code | Meaning | Description |
|------|---------|-------------|
| `0` | Success | Pipeline completed successfully |
| `1` | General error | Unspecified error |
| `2` | Validation failure | Data validation checks failed |
| `3` | I/O error | File read/write error |
| `4` | Out of memory | Memory limit exceeded |
| `5` | Configuration error | Invalid YAML or configuration |

---

## Python API

mlprep can also be used as a Python library:

```python
import mlprep

# Read data
df = mlprep.read_csv("data.csv")
print(df)

# Read parquet
df = mlprep.read_parquet("data.parquet")

# Write parquet
mlprep.write_parquet(df, "output.parquet")

# Convert to Polars for further analysis
pl_df = df.to_polars()
```

### Available Functions

| Function | Description |
|----------|-------------|
| `read_csv(path)` | Read a CSV file |
| `read_parquet(path)` | Read a Parquet file |
| `write_parquet(df, path)` | Write DataFrame to Parquet |
| `PyDataFrame.to_polars()` | Convert to Polars DataFrame |

---

## See Also

- [Quick Start Guide](quick-start.md)
- [Pipeline YAML Reference](pipeline-reference.md)
- [Troubleshooting](troubleshooting.md)
