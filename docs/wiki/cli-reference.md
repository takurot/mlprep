# CLI Reference

## Commands

### `mlprep run`

Run a pipeline from a YAML configuration file.

```bash
mlprep run <pipeline.yaml>
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `pipeline.yaml` | Path to the pipeline YAML file |

#### Example

```bash
mlprep run pipelines/etl.yaml
```

### `mlprep --help`

Show help message.

```bash
mlprep --help
```

Output:
```
usage: mlprep [-h] {run} ...

mlprep CLI

positional arguments:
  {run}
    run       Run a pipeline

options:
  -h, --help  show this help message and exit
```

### `mlprep --version`

Show version information.

```bash
mlprep --version
```

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

# Run pipeline programmatically
mlprep.run_pipeline("pipeline.yaml")
```
