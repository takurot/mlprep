# Feature Engineering Example

This example demonstrates how to fit feature transformers (Scaler, OneHot) on training data
and apply them to test data without data leakage.

## The Problem: Data Leakage

If you run the test pipeline without referencing the training statistics, `mlprep` will
refit the scalers and encoders on the test data. This means the test data is scaled using
its own mean and standard deviation rather than the training set's — causing **data leakage**
and inconsistent feature representations between training and inference.

## The Solution: `state_path`

Both pipelines specify the same `state_path: feature_state.json` inside the `features` step:

- **Train pipeline** (`pipeline_train.yaml`): fits transformers on `train_data.csv` and
  **saves** the learned parameters (mean, std, categories) to `feature_state.json`.
- **Test pipeline** (`pipeline_test.yaml`): **loads** the saved parameters from
  `feature_state.json` and applies them to `test_data.csv`.

The `state_path` field must be a sibling of `config` inside the step — not nested under it:

```yaml
steps:
  - type: features
    config:
      features:
        - column: age
          transform: standard_scale
        - column: income
          transform: standard_scale
        - column: city
          transform: one_hot_encode
    state_path: feature_state.json   # <-- sibling of config, not inside it
```

## Steps

1. **Generate Data**:
   ```bash
   python generate_train_test.py
   ```

2. **Run Train Pipeline** (fits transformers and saves state):
   ```bash
   mlprep run pipeline_train.yaml
   ```
   This writes `feature_state.json` and `train_features.parquet`.

3. **Run Test Pipeline** (loads state from training and transforms test data):
   ```bash
   mlprep run pipeline_test.yaml
   ```
   This reads `feature_state.json` and writes `test_features.parquet`.

Both output files will use the same scaling parameters and one-hot encoding categories,
ensuring consistency between training and inference.
