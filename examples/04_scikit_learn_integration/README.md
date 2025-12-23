# Scikit-Learn Integration Example

This example demonstrates how to integrate `mlprep` into a Python-based machine learning workflow.

## Workflow
1. **Feature Engineering with mlprep**: Scalable, consistent preprocessing using `mlprep` binary.
2. **Training with Scikit-Learn**: Loading the processed Parquet file for model training.

## Steps

1. **Run Integration Script**:
   ```bash
   python train_model.py
   ```
   
   This script will:
   1. Generate `raw_data.csv`.
   2. Execute `mlprep run pipeline.yaml`.
   3. Load `processed_train.parquet`.
   4. Train a Logistic Regression model and print accuracy.
