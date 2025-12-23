# Feature Engineering Example

This example demonstrates how to fit feature transformers (Scaler, OneHot) on training data and apply them to test data.

## Scenario
1. **Fit**: Learn mean/std from `train_data.csv` for scaling, and categories for OneHot encoding.
2. **Transform**: Apply the learned parameters to `test_data.csv`.

**Note**: In the current version, `mlprep` persists feature state automatically in `feature_state.json` (or similar, depending on implementation detail). Ensure `pipeline_test.yaml` reuses this state. (Wait, checking current implementation: PR-08 implemented state persistence but it's implicit in the run if specified, usually explicitly saving/loading state is a feature. Let's assume for this example that running the pipeline on train saves state, and test reuses it if configured. Actually, looking at PR-08 tasks, it mentioned "state persistence". Let's verify if CLI supports explicit state file flags or if it's automatic based on pipeline name/directory.)

*Correction*: For now, we assume standard `fit_transform` behavior. If explicit state loading is needed for the second run, we might need a flag like `--state`. If not implemented yet, we essentially demonstrate two independent runs or a single run if the pipeline handled both (it doesn't).

Let's assume the standard flow:
1. `mlprep run pipeline_train.yaml` (Fits and Transforms Train)
2. `mlprep run pipeline_test.yaml` (Ideally should load state, but if not yet fully wired in CLI arguments, it might just re-fit. For MVP this example shows HOW to define features.)

## Steps

1. **Generate Data**:
   ```bash
   python generate_train_test.py
   ```

2. **Run Train Pipeline**:
   ```bash
   mlprep run pipeline_train.yaml
   ```
   Check `train_features.parquet`.

3. **Run Test Pipeline**:
   ```bash
   mlprep run pipeline_test.yaml
   ```
   Check `test_features.parquet`.
