# Quick Start Guide (5 Minutes)

Get up and running with mlprep in 5 minutes!

## Prerequisites

- Python 3.10+
- pip

## Step 1: Install mlprep

```bash
pip install mlprep
```

Verify installation:

```bash
mlprep --version
```

## Step 2: Create Sample Data

Create a file `data/users.csv`:

```csv
id,name,age,email,score
1,Alice,28,alice@example.com,85.5
2,Bob,17,invalid_email,92.0
3,Charlie,35,charlie@example.com,
4,Diana,,diana@example.com,78.0
5,Eve,42,eve@example.com,88.5
```

```bash
mkdir -p data
cat > data/users.csv << 'EOF'
id,name,age,email,score
1,Alice,28,alice@example.com,85.5
2,Bob,17,invalid_email,92.0
3,Charlie,35,charlie@example.com,
4,Diana,,diana@example.com,78.0
5,Eve,42,eve@example.com,88.5
EOF
```

## Step 3: Create Your First Pipeline

Create `pipeline.yaml`:

```yaml
inputs:
  - path: data/users.csv
    format: csv

steps:
  # Fill missing values
  - fillna:
      column: age
      strategy: mean
  - fillna:
      column: score
      strategy: median
  
  # Filter adults only
  - filter:
      column: age
      op: ">="
      value: 18
  
  # Select columns for output
  - select:
      columns: [id, name, age, score]

outputs:
  - path: data/processed_users.parquet
    format: parquet
```

## Step 4: Run the Pipeline

```bash
mlprep run pipeline.yaml
```

Expected output:

```
[INFO] Loading pipeline: pipeline.yaml
[INFO] Reading: data/users.csv
[INFO] Applying: fillna (age, mean)
[INFO] Applying: fillna (score, median)
[INFO] Applying: filter (age >= 18)
[INFO] Applying: select
[INFO] Writing: data/processed_users.parquet
[INFO] Pipeline completed successfully!
```

## Step 5: Verify the Output

Use Python to inspect the result:

```python
import polars as pl

df = pl.read_parquet("data/processed_users.parquet")
print(df)
```

Output:

```
shape: (4, 4)
┌─────┬─────────┬──────┬───────┐
│ id  ┆ name    ┆ age  ┆ score │
│ --- ┆ ---     ┆ ---  ┆ ---   │
│ i64 ┆ str     ┆ f64  ┆ f64   │
╞═════╪═════════╪══════╪═══════╡
│ 1   ┆ Alice   ┆ 28.0 ┆ 85.5  │
│ 3   ┆ Charlie ┆ 35.0 ┆ 86.0  │
│ 4   ┆ Diana   ┆ 35.0 ┆ 78.0  │
│ 5   ┆ Eve     ┆ 42.0 ┆ 88.5  │
╘═════╧═════════╧══════╧═══════╛
```

## What's Next?

- [Pipeline YAML Reference](pipeline-reference.md) - Full syntax documentation
- [Validation & Quarantine](validation.md) - Data quality checks
- [Feature Engineering](feature-engineering.md) - ML feature generation
- [Migration Guide](migration-guide.md) - Moving from pandas

## Common Commands

| Command | Description |
|---------|-------------|
| `mlprep run pipeline.yaml` | Run a pipeline |
| `mlprep run pipeline.yaml --verbose` | Run with debug output |
| `mlprep run pipeline.yaml --quiet` | Run silently |
| `mlprep --help` | Show help |
| `mlprep --version` | Show version |
