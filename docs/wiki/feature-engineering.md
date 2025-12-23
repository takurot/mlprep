# Feature Engineering

mlprep provides declarative feature transformations with **fit/transform** semantics for train/test consistency.

## Basic Usage

```yaml
steps:
  - features:
      - column: price
        transform: minmax
      - column: category
        transform: onehot
      state_path: feature_state.json
```

## Available Transforms

| Transform | Description | Output |
|-----------|-------------|--------|
| `minmax` | Min-Max scaling to [0, 1] | Single column |
| `standard` | Standardization (z-score) | Single column |
| `onehot` | One-hot encoding | Multiple columns |
| `count` | Count encoding | Single column |

## Transform Details

### minmax

Scales values to range [0, 1].

```yaml
- column: price
  transform: minmax
```

Formula: `(x - min) / (max - min)`

### standard

Standardizes values using z-score.

```yaml
- column: score
  transform: standard
```

Formula: `(x - mean) / std`

### onehot

Creates binary columns for each unique category.

```yaml
- column: color
  transform: onehot
```

Input: `[red, blue, red, green]`
Output columns: `color_red`, `color_blue`, `color_green`

### count

Replaces categories with their frequency count.

```yaml
- column: user_id
  transform: count
```

## State Persistence

Feature transformers save their fitted state (min/max values, category mappings) to a JSON file:

```yaml
features:
  - column: price
    transform: minmax
  state_path: feature_state.json
```

### Training Phase

```yaml
# Fits and transforms, saves state
state_path: feature_state.json
```

### Inference Phase

Use the same state file to ensure consistent transformations:

```yaml
# Loads state and transforms (no fitting)
state_path: feature_state.json
```

## Complete Example

```yaml
inputs:
  - path: train.csv

steps:
  - features:
      - column: age
        transform: minmax
      - column: income
        transform: standard
      - column: occupation
        transform: onehot
      - column: zip_code
        transform: count
      state_path: models/feature_state.json

outputs:
  - path: train_features.parquet
```
