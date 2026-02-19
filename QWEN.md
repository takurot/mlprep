# mlprep - High-Performance Data Preprocessing Engine

## Project Overview

mlprep is a high-performance, no-code data preprocessing engine for Machine Learning, powered by Rust and Polars. It provides both a CLI tool and Python library that handles common ML preprocessing tasks like type inference, missing value imputation, complex joins, and feature engineering through simple YAML configurations.

### Key Features

- **Blazing Speed**: Built on Rust and Polars with multi-threading and SIMD vectorization
- **Zero-Code Pipelines**: Define entire preprocessing workflows in YAML configuration files
- **Quarantine Mode**: Isolates invalid rows into separate files to prevent pipeline failures
- **Reproducible Pipelines**: `fit` on training data and `transform` on production data with exact reproducibility
- **Memory Efficient**: Uses zero-copy Arrow memory format and supports streaming mode for low-memory environments

### Architecture

The project is a hybrid Rust/Python implementation:
- **Rust Backend**: Core processing engine using Polars for high-performance data operations
- **Python Bindings**: Using PyO3 to expose Rust functionality to Python
- **CLI Interface**: Both Rust (native) and Python (wrapper) implementations
- **Build System**: Uses Maturin to build Python wheels from Rust code

## Building and Running

### Prerequisites
- Rust 1.75+
- Python 3.10+
- Cargo

### Installation

```bash
# From PyPI
pip install mlprep-rust

# From source (development)
pip install maturin
maturin develop
```

### Building from Source

```bash
# Install dependencies
pip install maturin

# Build and install in development mode
maturin develop

# Build release wheel
maturin build --release
```

### Running Pipelines

```bash
# Run a pipeline from YAML config
mlprep run pipeline.yaml

# With streaming mode for low memory usage
mlprep run pipeline.yaml --streaming

# With memory limit
mlprep run pipeline.yaml --memory-limit "4GB"

# CLI options
mlprep --help
mlprep run --help
```

### Python Usage

```python
import mlprep

# Run pipeline from Python
mlprep.run_pipeline('pipeline.yaml')

# Read/write data
df = mlprep.read_csv('input.csv')
mlprep.write_parquet(df, 'output.parquet')
```

## Development Conventions

### Code Structure
- `/src/` - Rust source code with modules for compute, DSL, engine, IO, etc.
- `/python/mlprep/` - Python wrapper code
- `/examples/` - Example pipeline configurations
- `/docs/` - Documentation files
- `/tests/` - Test files

### Configuration Format
Pipelines are defined in YAML format with sections:
- `inputs`: Data sources with path and format
- `steps`: Processing operations (filter, select, fillna, validate, features, etc.)
- `outputs`: Destination for processed data

### Testing
- Rust unit tests in the respective modules
- Python tests in `/python/tests/`
- Use `pytest` for Python tests
- Use `cargo test` for Rust tests

### Logging
- Uses Rust `tracing` crate with configurable levels
- Supports both text and JSON log formats
- Environment variable: `MLPREP_LOG`
- CLI flags: `--verbose`, `--quiet`, `--log-format`

### Security
- I/O sandboxing with allowed paths restriction
- Column masking for sensitive data in logs
- Runtime configuration overrides

## Common Operations

### Basic ETL Pipeline
```yaml
inputs:
  - path: "data/input.csv"
    format: csv

steps:
  - select:
      columns: [id, name, age, city]
  - filter: "age >= 18"

outputs:
  - path: "data/output.parquet"
    format: parquet
```

### Data Validation with Quarantine
```yaml
steps:
  - validate:
      mode: quarantine
      checks:
        - name: email
          regex: "^.+@.+\\..+$"
```

### Feature Engineering
```yaml
steps:
  - features:
      config: features.yaml
```

## Performance Features

- Multi-threaded CSV parsing
- Lazy evaluation and query optimization
- Streaming mode for large datasets
- Memory limits to prevent out-of-memory errors
- Zero-copy Arrow memory format