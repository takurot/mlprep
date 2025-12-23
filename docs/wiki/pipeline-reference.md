# Pipeline YAML Reference

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
```

## Available Transformations

### `filter`

Filter rows based on a condition.

```yaml
- filter:
    column: age
    op: ">=" | ">" | "<=" | "<" | "==" | "!="
    value: 18
```

### `select`

Select specific columns.

```yaml
- select:
    columns: [col1, col2, col3]
```

### `cast`

Change column data types.

```yaml
- cast:
    column: price
    dtype: Float64 | Int64 | Utf8 | Boolean
```

### `sort`

Sort by columns.

```yaml
- sort:
    by: [timestamp, id]
    descending: [false, true]
```

### `join`

Join with another dataset.

```yaml
- join:
    right_path: other.csv
    on: [id]
    how: inner | left | outer
```

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

### `fillna`

Fill missing values.

```yaml
- fillna:
    column: value
    strategy: mean | median | zero | forward | backward
```

### `dropna`

Drop rows with missing values.

```yaml
- dropna:
    columns: [col1, col2]
```

## Complete Example

```yaml
inputs:
  - path: sales.csv
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
  - groupby:
      by: [category]
      agg:
        - column: price
          op: sum
          alias: total_sales
  - sort:
      by: [total_sales]
      descending: [true]

outputs:
  - path: sales_summary.parquet
    format: parquet
```
