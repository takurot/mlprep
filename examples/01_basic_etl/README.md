# Basic ETL Example

This example demonstrates a simple ETL pipeline using `mlprep`.

## Scenario
We have a CSV file `data.csv` containing user data. We want to:
1. Select specific columns (`id`, `name`, `age`, `city`).
2. Filter users who are 18 years or older.
3. Save the result as a Parquet file `output.parquet`.

## Steps

1. **Generate Data**:
   ```bash
   python generate_data.py
   ```

2. **Run Pipeline**:
   ```bash
   mlprep run pipeline.yaml
   ```

3. **Verify Output**:
   Check that `output.parquet` is created.
