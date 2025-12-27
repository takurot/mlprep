# Migration Guide: pandas → mlprep

This guide helps you migrate from pandas to mlprep for faster, more reproducible data pipelines.

## Why Migrate?

| Aspect | pandas | mlprep |
|--------|--------|--------|
| **Speed** | Single-threaded | Multi-threaded (3-10x faster) |
| **Memory** | High memory usage | Arrow-based (1/4 memory) |
| **Reproducibility** | Requires manual scripting | Built-in via YAML |
| **Data Validation** | Manual checks | Built-in quarantine mode |
| **Production Ready** | Notebooks → scripts | Single YAML file |

## Operation Mapping

### Reading Data

**pandas:**
```python
import pandas as pd
df = pd.read_csv("data.csv")
```

**mlprep (YAML):**
```yaml
inputs:
  - path: data.csv
    format: csv
```

**mlprep (Python):**
```python
import mlprep
df = mlprep.read_csv("data.csv")
```

---

### Filtering Rows

**pandas:**
```python
df = df[df["age"] >= 18]
```

**mlprep (YAML):**
```yaml
steps:
  - filter:
      column: age
      op: ">="
      value: 18
```

---

### Selecting Columns

**pandas:**
```python
df = df[["id", "name", "age"]]
```

**mlprep (YAML):**
```yaml
steps:
  - select:
      columns: [id, name, age]
```

---

### Type Casting

**pandas:**
```python
df["price"] = df["price"].astype(float)
```

**mlprep (YAML):**
```yaml
steps:
  - cast:
      column: price
      dtype: Float64
```

---

### Filling Missing Values

**pandas:**
```python
df["age"].fillna(df["age"].mean(), inplace=True)
```

**mlprep (YAML):**
```yaml
steps:
  - fillna:
      column: age
      strategy: mean
```

---

### Dropping NA

**pandas:**
```python
df = df.dropna(subset=["email", "phone"])
```

**mlprep (YAML):**
```yaml
steps:
  - dropna:
      columns: [email, phone]
```

---

### Sorting

**pandas:**
```python
df = df.sort_values(["date", "id"], ascending=[False, True])
```

**mlprep (YAML):**
```yaml
steps:
  - sort:
      by: [date, id]
      descending: [true, false]
```

---

### GroupBy Aggregation

**pandas:**
```python
result = df.groupby("category").agg(
    total_sales=("amount", "sum"),
    avg_price=("price", "mean")
).reset_index()
```

**mlprep (YAML):**
```yaml
steps:
  - groupby:
      by: [category]
      agg:
        - column: amount
          op: sum
          alias: total_sales
        - column: price
          op: mean
          alias: avg_price
```

---

### Join

**pandas:**
```python
result = df.merge(other_df, on="user_id", how="left")
```

**mlprep (YAML):**
```yaml
steps:
  - join:
      right_path: other.csv
      on: [user_id]
      how: left
```

---

### Writing Output

**pandas:**
```python
df.to_parquet("output.parquet", compression="zstd")
```

**mlprep (YAML):**
```yaml
outputs:
  - path: output.parquet
    format: parquet
```

---

## Complete Migration Example

### Before (pandas script)

```python
import pandas as pd

# Read
df = pd.read_csv("raw_data.csv")

# Clean
df["age"] = df["age"].fillna(df["age"].mean())
df = df[df["age"] >= 18]

# Transform  
df["income"] = df["income"].astype(float)

# Aggregate
summary = df.groupby("region").agg(
    avg_income=("income", "mean"),
    count=("id", "count")
).reset_index()

# Write
summary.to_parquet("summary.parquet")
```

### After (mlprep YAML)

```yaml
inputs:
  - path: raw_data.csv
    format: csv

steps:
  - fillna:
      column: age
      strategy: mean
  - filter:
      column: age
      op: ">="
      value: 18
  - cast:
      column: income
      dtype: Float64
  - groupby:
      by: [region]
      agg:
        - column: income
          op: mean
          alias: avg_income
        - column: id
          op: count
          alias: count

outputs:
  - path: summary.parquet
    format: parquet
```

Run with:
```bash
mlprep run pipeline.yaml
```

## Benefits After Migration

1. **No Python environment needed** - Run in CI/CD with single binary
2. **Reproducible** - Same YAML = same results
3. **Version controlled** - Track pipeline changes in Git
4. **3-10x faster** - Rust + Polars engine
5. **Built-in validation** - Add data quality checks easily

## Next Steps

- [Quick Start Guide](quick-start.md) - Get started quickly
- [Pipeline Reference](pipeline-reference.md) - Full YAML syntax
- [Feature Engineering](feature-engineering.md) - ML transformations
