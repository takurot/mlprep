# Pipeline YAML Reference

Complete reference for mlprep pipeline configuration files.

## Structure

```yaml
inputs:
  - path: <input_file_path>
    format: csv | parquet

steps:
  - <transformation>

outputs:
  - path: <output_file_path>
    format: csv | parquet

# Optional runtime configuration
runtime:
  streaming: true | false
  memory_limit: "4GB"
```

---

## Inputs

### Basic Input

```yaml
inputs:
  - path: data/input.csv
    format: csv
```

### Multiple Inputs

```yaml
inputs:
  - path: data/users.csv
    format: csv
  - path: data/orders.parquet
    format: parquet
```

### Input Options

| Option | Description | Default |
|--------|-------------|---------|
| `path` | File path (relative or absolute) | required |
| `format` | File format: `csv`, `parquet` | auto-detect |

---

## Transformations

### `filter`

Filter rows based on a condition.

```yaml
- filter:
    column: age
    op: ">=" | ">" | "<=" | "<" | "==" | "!="
    value: 18
```

**Operators:**
| Op | Description |
|----|-------------|
| `>=` | Greater than or equal |
| `>` | Greater than |
| `<=` | Less than or equal |
| `<` | Less than |
| `==` | Equal |
| `!=` | Not equal |

---

### `select`

Select specific columns.

```yaml
- select:
    columns: [col1, col2, col3]
```

---

### `cast`

Change column data types.

```yaml
- cast:
    column: price
    dtype: Float64 | Int64 | Utf8 | Boolean
```

**Available Types:**
| Type | Description |
|------|-------------|
| `Int32`, `Int64` | Integer types |
| `Float32`, `Float64` | Floating point types |
| `Utf8` | String type |
| `Boolean` | Boolean type |

---

### `sort`

Sort by columns.

```yaml
- sort:
    by: [timestamp, id]
    descending: [false, true]
```

---

### `join`

Join with another dataset.

```yaml
- join:
    right_path: other.csv
    on: [id]
    how: inner | left | outer
```

**Join Types:**
| Type | Description |
|------|-------------|
| `inner` | Only matching rows |
| `left` | All left rows, matching right |
| `outer` | All rows from both |

---

### `groupby`

Aggregate data.

```yaml
- groupby:
    by: [category]
    agg:
      - column: amount
        op: sum | mean | min | max | count
        alias: total_amount
```

**Aggregation Operators:**
| Op | Description |
|----|-------------|
| `sum` | Sum of values |
| `mean` | Average |
| `min` | Minimum |
| `max` | Maximum |
| `count` | Count of rows |

---

### `fillna`

Fill missing values.

```yaml
- fillna:
    column: value
    strategy: mean | median | zero | forward | backward
```

**Strategies:**
| Strategy | Description |
|----------|-------------|
| `mean` | Fill with column mean |
| `median` | Fill with column median |
| `zero` | Fill with 0 |
| `forward` | Forward fill (ffill) |
| `backward` | Backward fill (bfill) |

---

### `dropna`

Drop rows with missing values.

```yaml
- dropna:
    columns: [col1, col2]
```

---

### `validate`

Validate data quality with optional quarantine mode.

```yaml
- validate:
    checks:
      - column: email
        rule: not_null
      - column: age
        rule: range
        min: 0
        max: 150
      - column: status
        rule: enum
        values: [active, inactive]
    quarantine_path: invalid_rows.parquet
```

**Validation Rules:**
| Rule | Parameters | Description |
|------|------------|-------------|
| `not_null` | — | Column must not contain nulls |
| `unique` | — | Column values must be unique |
| `range` | `min`, `max` | Values within range |
| `regex` | `pattern` | Values match pattern |
| `enum` | `values` | Values in allowed list |

See [Validation & Quarantine](validation.md) for details.

---

### `features`

Apply feature engineering transformations.

```yaml
- features:
    - column: price
      transform: minmax
    - column: category
      transform: onehot
    state_path: feature_state.json
```

**Transform Types:**
| Transform | Description |
|-----------|-------------|
| `minmax` | Min-Max scaling [0, 1] |
| `standard` | Z-score standardization |
| `onehot` | One-hot encoding |
| `count` | Count encoding |

See [Feature Engineering](feature-engineering.md) for details.

---

## Outputs

### Basic Output

```yaml
outputs:
  - path: data/output.parquet
    format: parquet
```

### Output Options

| Option | Description | Default |
|--------|-------------|---------|
| `path` | Output file path | required |
| `format` | `csv` or `parquet` | `parquet` |

---

## Runtime Configuration

Optional runtime settings:

```yaml
runtime:
  streaming: true
  threads: "8"
  cache: true
  memory_limit: "4GB"
```

| Option | Description | Default |
|--------|-------------|---------|
| `streaming` | Enable streaming mode | `false` |
| `threads` | Override `POLARS_MAX_THREADS` | env default |
| `cache` | Enable Polars plan cache (`POLARS_CACHE`) | none |
| `memory_limit` | Memory limit (e.g., "4GB") | none |

> **Note:** Runtime options can be overridden via CLI flags.

---

## Complete Examples

### ETL Pipeline

```yaml
inputs:
  - path: raw_data.csv
    format: csv

steps:
  - cast:
      column: price
      dtype: Float64
  - filter:
      column: price
      op: ">"
      value: 0
  - fillna:
      column: discount
      strategy: zero
  - sort:
      by: [date]
      descending: [true]

outputs:
  - path: cleaned_data.parquet
    format: parquet
```

### Aggregation Pipeline

```yaml
inputs:
  - path: sales.csv

steps:
  - groupby:
      by: [category, region]
      agg:
        - column: amount
          op: sum
          alias: total_sales
        - column: amount
          op: count
          alias: num_orders
  - sort:
      by: [total_sales]
      descending: [true]

outputs:
  - path: sales_summary.parquet
```

### Validation Pipeline

```yaml
inputs:
  - path: user_data.csv

steps:
  - validate:
      checks:
        - column: user_id
          rule: not_null
        - column: user_id
          rule: unique
        - column: age
          rule: range
          min: 0
          max: 150
        - column: email
          rule: regex
          pattern: "^[^@]+@[^@]+\\.[^@]+$"
      quarantine_path: invalid_users.parquet

outputs:
  - path: valid_users.parquet
```

### Feature Engineering Pipeline

```yaml
inputs:
  - path: train.csv

steps:
  - fillna:
      column: income
      strategy: median
  - features:
      - column: age
        transform: minmax
      - column: income
        transform: standard
      - column: occupation
        transform: onehot
      state_path: models/feature_state.json

outputs:
  - path: train_features.parquet
```

---

## See Also

- [Quick Start Guide](quick-start.md)
- [Validation & Quarantine](validation.md)
- [Feature Engineering](feature-engineering.md)
- [CLI Reference](cli-reference.md)
