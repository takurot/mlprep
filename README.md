# mlprep 🚀

**The fastest no-code data preprocessing engine for Machine Learning.**  
*Powered by Rust & Polars.*

[![CI](https://github.com/takurot/mlprep/actions/workflows/ci.yml/badge.svg)](https://github.com/takurot/mlprep/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://pypi.org/project/mlprep/)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange)](https://www.rust-lang.org/)

---

**Stop writing slow, fragile pandas boilerplate.**  
**Start defining robust, reproducible pipelines.**

`mlprep` is a high-performance CLI tool and Python library that handles the dirty work of ML engineers: type inference, missing value imputation, complex joins, and feature engineering—all defined in a simple YAML config.

## 🔥 Why mlprep?

### 🚀 Blazing Speed
Built on **Rust** and **Polars**, `mlprep` processes gigabytes of data in seconds, not minutes. It leverages multi-threading and SIMD vectorization out of the box.

### ✨ Zero-Code Pipelines
Define your entire preprocessing workflow in `pipeline.yaml`. No more "spaghetti code" notebooks that no one can read.

### 🛡️ Quarantine Mode
Don't let dirty data crash your training. `mlprep` isolates invalid rows (schema mismatch, outliers) into a separate "quarantine" file, so your pipeline stays green and your models stay clean.

### 🔄 Build Once, Run Anywhere
`fit` your feature engineering steps (scaling, encoding) on training data and `transform` production data with **exact reproducibility**. No more training-serving skew.

---

## ⚡️ Quick Start

### 1. Install
```bash
pip install mlprep
```

### 2. Define your pipeline (`pipeline.yaml`)
```yaml
inputs:
  - path: "data/raw_users.csv"
    format: csv

steps:
  # ETL
  - fillna:
      strategy: mean
      columns: [age, income]
  - filter: "age >= 18"
  
  # Data Quality Check
  - validate:
      mode: quarantine # Bad rows go to 'quarantine.parquet'
      checks:
        - name: email
          regex: "^.+@.+\\..+$"

  # Feature Engineering
  - features:
      config: features.yaml

outputs:
  - path: "data/processed_users.parquet"
    format: parquet
    compression: zstd
```

### 3. Run it
```bash
mlprep run pipeline.yaml
```

> **Result**: A clean, highly-compressed Parquet file ready for training. 🚀

---

## 🆚 Comparison

| Feature | **Pandas** | **mlprep** |
| :--- | :--- | :--- |
| **Speed** | 🐢 Single-threaded | 🐆 **Multi-threaded (Rust)** |
| **Pipeline** | Python Script | **YAML Config** |
| **Validation** | Manual `.loc[]` checks | **Built-in Quality Engine** |
| **Bad Data** | Crash or Silent Fail | **Quarantine Execution** |
| **Memory** | Bloated Objects | **Zero-Copy Arrow** |

---

## ⚡️ Performance

mlprep is designed for speed, leveraging Rust's ownership model and Polars' query engine.

| Operation | vs Pandas | Note |
|:--- | :--- | :--- |
| **CSV Read** | **~3-5x Faster** | Multi-threaded parsing |
| **Pipeline** | **~10x Faster** | Lazy evaluation & query optimization |
| **Memory** | **~1/4 Usage** | Zero-copy Arrow memory format |

*Benchmarks run on 1GB generated dataset. To run your own benchmarks:*

```bash
python scripts/benchmark.py --size 1.0 --compare-pandas
```

---

## 🗺️ Roadmap

We are actively building MVP (Phase 1). Check out our documentation:

* [**Implementation Plan & Roadmap**](docs/PLAN.md)
* [**Technical Specification**](docs/SPEC.md)

---

## 📚 Use Cases & Examples

Explore full examples in the [`examples/`](examples/) directory:

### 1. [Basic ETL Pipeline](examples/01_basic_etl/)
* **Scenario**: Filter, select columns, and convert CSV to Parquet.
* **Key Features**: `filter`, `select`, `write_parquet`.

### 2. [Data Validation](examples/02_data_validation/)
* **Scenario**: Ensure data quality before training.
* **Key Features**: Schema validation, `quarantine` mode for invalid rows.

### 3. [Feature Engineering](examples/03_feature_engineering/)
* **Scenario**: Generate features for ML training.
* **Key Features**: `fit` (train) / `transform` (prod) pattern, `standard_scaler`, `one_hot_encoding`.

### 4. [Scikit-Learn Integration](examples/04_scikit_learn_integration/)
* **Scenario**: Use mlprep as a preprocessing step in a Scikit-Learn pipeline.
* **Key Features**: Seamless integration with Python ML ecosystem.

### 5. [MLflow Experiment Tracking](examples/05_mlflow_experiment/)
* **Scenario**: Track preprocessing parameters and artifacts in MLflow.
* **Key Features**: Reproducibility and experiment management.

---

## 🤝 Contributing

We welcome contributions! Please see the issue tracker for good first issues.

## 📄 License

MIT
