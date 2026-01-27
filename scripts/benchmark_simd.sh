#!/bin/bash
set -e

# Activate venv
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# Generate data if not exists
mkdir -p data
DATA_FILE="data/benchmark_scaling.csv"
if [ ! -f "$DATA_FILE" ]; then
    echo "Generating scaling benchmark data..."
    # Reuse python script to generate data. 
    # Or just use python to generate a simple CSV with 4 columns
    python3 -c "
import pandas as pd
import numpy as np
size = 10_000_000 # 10M rows
df = pd.DataFrame({
    'col_a': np.random.rand(size) * 100,
    'col_b': np.random.randn(size) * 50 + 20,
    'col_c': np.random.rand(size),
    'col_d': np.random.randn(size),
    'email': ['user{}@example.com'.format(i) if i % 10 != 0 else 'invalid-email' for i in range(size)],
    'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], size)
})
df.to_csv('$DATA_FILE', index=False)
"
fi

echo "Building WITHOUT SIMD..."
cargo build --release --bin mlprep

echo "Running Benchmark (No SIMD)..."
time target/release/mlprep run examples/benchmark_scaling.yaml

echo "Building WITH SIMD..."
cargo build --release --bin mlprep --features simd

echo "Running Benchmark (SIMD)..."
time target/release/mlprep run examples/benchmark_scaling.yaml
