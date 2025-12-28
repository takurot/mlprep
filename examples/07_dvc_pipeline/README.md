# DVC Pipeline Example

This example shows how to run an `mlprep` pipeline from a DVC stage so processed data is reproducible and versioned.

## Scenario
- Input: `raw_data.csv` (small demo dataset)
- Pipeline: `pipeline.yaml` cleans the data (`cast` → `fill_null` → `drop_null` → `filter` → `sort`)
- Output: `outputs/clean_users.parquet` tracked by DVC

## Steps

1. Install tools (inside your virtualenv):
   ```bash
   pip install mlprep dvc
   ```
2. Move into the example:
   ```bash
   cd examples/07_dvc_pipeline
   ```
3. (Optional) Run once without DVC:
   ```bash
   mlprep run pipeline.yaml
   ```
4. Initialize and run via DVC:
   ```bash
   dvc init
   dvc repro
   ```
   This executes `mlprep run pipeline.yaml`, produces `outputs/clean_users.parquet`, and records it in `dvc.lock`.

5. Verify the tracked artifact:
   ```bash
   dvc status
   ls outputs/clean_users.parquet
   ```

> Tip: Configure a remote (e.g., S3, Azure Blob) with `dvc remote add` to push the processed dataset for your team.
