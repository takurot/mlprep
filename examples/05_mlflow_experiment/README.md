# MLflow Integration Example

This example demonstrates how to track `mlprep` pipeline executions using MLflow.

## Workflow
1. Start an MLflow run.
2. Log the `pipeline.yaml` as a configuration artifact.
3. Execute `mlprep`.
4. Log the resulting output file (or metrics) to MLflow.

## Prerequisites
- `pip install mlflow pandas numpy`

## Steps

1. **Run MLflow Script**:
   ```bash
   python mlflow_run.py
   ```

2. **View UI (Optional)**:
   ```bash
   mlflow ui
   ```
   Open `http://127.0.0.1:5000` to see the experiment run and artifacts.
