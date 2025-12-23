# Data Validation Example

This example demonstrates how to validate data and handle errors using `quarantine` mode.

## Scenario
We check for:
1. **Unique Email**: Duplicate emails are quarantined.
2. **Valid Age**: Age must be between 0 and 120. Outliers are quarantined.
3. **User Type**: Must not be null.

## Steps

1. **Generate Dirty Data**:
   ```bash
   python generate_dirty_data.py
   ```
   This creates `dirty_data.csv` with duplicates, negative ages, and null values.

2. **Run Pipeline**:
   ```bash
   mlprep run pipeline.yaml
   ```

3. **Verify Output**:
   - `clean_output.parquet`: Contains only valid rows.
   - `quarantine/`: Contains isolated bad rows (e.g., `dirty_data_quarantine.csv`).
