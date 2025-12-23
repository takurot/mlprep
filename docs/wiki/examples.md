# Examples

The repository includes several examples in the `examples/` directory.

## 01 - Basic ETL

**Location:** `examples/01_basic_etl/`

Simple CSV → filter → Parquet pipeline demonstrating:
- Reading CSV files
- Filtering rows
- Selecting columns
- Writing to Parquet

```bash
cd examples/01_basic_etl
mlprep run pipeline.yaml
```

## 02 - Data Validation

**Location:** `examples/02_data_validation/`

Schema validation with quarantine mode:
- Not-null checks
- Range validation
- Enum validation
- Quarantine file output

```bash
cd examples/02_data_validation
mlprep run pipeline.yaml
```

## 03 - Feature Engineering

**Location:** `examples/03_feature_engineering/`

Fit/transform feature pipelines:
- MinMax scaling
- Standard scaling
- One-hot encoding
- State persistence

```bash
cd examples/03_feature_engineering
mlprep run pipeline.yaml
```

## 04 - Scikit-learn Integration

**Location:** `examples/04_scikit_learn_integration/`

mlprep → scikit-learn workflow:
- Preprocessing with mlprep
- Model training with scikit-learn
- Feature state management

```bash
cd examples/04_scikit_learn_integration
python train.py
```

## 05 - MLflow Experiment

**Location:** `examples/05_mlflow_experiment/`

MLflow experiment tracking integration:
- Data preprocessing with mlprep
- Experiment logging with MLflow
- Metric tracking

```bash
cd examples/05_mlflow_experiment
python mlflow_run.py
```

## Running All Examples

```bash
# From repository root
for dir in examples/*/; do
  echo "Running $dir..."
  cd "$dir"
  if [ -f "pipeline.yaml" ]; then
    mlprep run pipeline.yaml
  fi
  cd ../..
done
```
