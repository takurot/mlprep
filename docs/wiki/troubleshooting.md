# Troubleshooting Guide

Common issues and solutions when using mlprep.

## Common Errors

### YAML Syntax Errors

**Error:**
```
Error: YAML parse error at line 5, column 3
```

**Cause:** Invalid YAML syntax (incorrect indentation, missing quotes, etc.)

**Solution:**
1. Check indentation (use spaces, not tabs)
2. Quote strings with special characters
3. Use a YAML validator

**Example fix:**
```yaml
# ✗ Wrong
steps:
- filter:
column: age  # Bad indentation

# ✓ Correct
steps:
  - filter:
      column: age
```

---

### File Not Found

**Error:**
```
MLPREP-E003: File not found: data/input.csv
```

**Cause:** Input file path doesn't exist or is incorrect

**Solution:**
1. Check the file path is relative to where you run `mlprep`
2. Use absolute paths if needed
3. Verify file permissions

```bash
# Check if file exists
ls -la data/input.csv
```

---

### Column Not Found

**Error:**
```
MLPREP-E005: Column 'user_id' not found in dataframe
```

**Cause:** Referenced column doesn't exist in the data

**Solution:**
1. Check column names in your data (case-sensitive)
2. Ensure previous steps didn't drop the column

```bash
# Preview your CSV headers
head -1 data/input.csv
```

---

### Type Mismatch

**Error:**
```
MLPREP-E004: Cannot cast column 'price' from Utf8 to Float64
```

**Cause:** Column contains non-numeric values

**Solution:**
1. Clean data before casting
2. Use fillna to handle missing values first
3. Check for non-numeric strings ("N/A", "-", etc.)

```yaml
steps:
  - fillna:
      column: price
      strategy: zero
  - cast:
      column: price
      dtype: Float64
```

---

### Path Outside Allowed Paths

**Error:**
```
MLPREP-E010: Path '/etc/passwd' is outside allowed paths
```

**Cause:** Security sandbox prevents access to unauthorized paths

**Solution:**
1. Use `--allowed-paths` to specify allowed directories
2. Move files to allowed directory

```bash
mlprep run pipeline.yaml --allowed-paths ./data --allowed-paths ./output
```

---

### Memory Limit Exceeded

**Error:**
```
MLPREP-E020: Memory limit (4GB) exceeded
```

**Cause:** Data too large for configured memory limit

**Solution:**
1. Increase memory limit
2. Enable streaming mode
3. Process data in chunks

```bash
# Increase limit
mlprep run pipeline.yaml --memory-limit 8GB

# Or enable streaming
mlprep run pipeline.yaml --streaming
```

---

## Debugging Tips

### Enable Verbose Logging

```bash
mlprep run pipeline.yaml --verbose
```

### Use JSON Log Format

For CI/CD pipelines or log parsing:

```bash
mlprep run pipeline.yaml --log-format json
```

### Environment Variable Logging

```bash
# Set log level via environment
MLPREP_LOG=debug mlprep run pipeline.yaml

# Available levels: trace, debug, info, warn, error
```

---

## CLI Flags Reference

| Flag | Description | Example |
|------|-------------|---------|
| `--verbose`, `-v` | Enable debug logging | `mlprep run -v pipeline.yaml` |
| `--quiet`, `-q` | Silence all output | `mlprep run -q pipeline.yaml` |
| `--log-format` | Set log format | `--log-format json` |
| `--allowed-paths` | Sandbox I/O paths | `--allowed-paths ./data` |
| `--mask-columns` | Mask column data in logs | `--mask-columns ssn,password` |
| `--streaming` | Low memory mode | `--streaming` |
| `--memory-limit` | Set memory limit | `--memory-limit 4GB` |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | General error |
| `2` | Validation failure |
| `3` | I/O error |
| `4` | Out of memory |
| `5` | Configuration error |

---

## Getting Help

1. **Check documentation:**
   - [Pipeline Reference](pipeline-reference.md)
   - [CLI Reference](cli-reference.md)

2. **Run with verbose:**
   ```bash
   mlprep run pipeline.yaml --verbose
   ```

3. **Report issues:**
   - GitHub Issues: https://github.com/takurot/mlprep/issues
   - Include: error message, pipeline.yaml, mlprep version
