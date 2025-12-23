# Validation & Quarantine Mode

mlprep supports data validation with **quarantine mode** — invalid rows are separated into a dedicated file rather than being silently dropped.

## Basic Validation

```yaml
inputs:
  - path: data/raw.csv

steps:
  - validate:
      checks:
        - column: email
          rule: not_null
        - column: age
          rule: range
          min: 0
          max: 150
      quarantine_path: data/quarantine.csv

outputs:
  - path: data/clean.parquet
```

## Available Validation Rules

| Rule | Description | Parameters |
|------|-------------|------------|
| `not_null` | Column must not contain nulls | — |
| `unique` | Column values must be unique | — |
| `range` | Values must be within range | `min`, `max` |
| `regex` | Values must match pattern | `pattern` |
| `enum` | Values must be in allowed list | `values` |

## Rule Examples

### not_null

```yaml
- column: user_id
  rule: not_null
```

### unique

```yaml
- column: email
  rule: unique
```

### range

```yaml
- column: age
  rule: range
  min: 0
  max: 150
```

### regex

```yaml
- column: email
  rule: regex
  pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
```

### enum

```yaml
- column: status
  rule: enum
  values: [active, inactive, pending]
```

## Quarantine Mode

When `quarantine_path` is specified, rows that fail validation are written to a separate file instead of being dropped:

```yaml
- validate:
    checks:
      - column: price
        rule: range
        min: 0
    quarantine_path: invalid_rows.csv
```

This allows you to:
- Keep a record of invalid data for review
- Debug data quality issues
- Reprocess invalid rows after fixing
