import time
import os
import argparse
import polars as pl
import pandas as pd

def generate_data(path, size_gb=1):
    print(f"Generating {size_gb}GB of data...")
    num_rows = size_gb * 10_000_000 # Roughly 1GB for simple data
    df = pl.DataFrame({
        "a": range(num_rows),
        "b": [1.5] * num_rows,
        "c": ["test"] * num_rows
    })
    df.write_csv(path)
    print(f"Data generated at {path}")

def benchmark_polars(path):
    print("Benchmarking native Polars (Python)...")
    start = time.time()
    df = pl.scan_csv(path).collect()
    end = time.time()
    print(f"Polars read {path} in {end - start:.2f}s")
    return end - start

def benchmark_pandas(path):
    print("Benchmarking Pandas (Python)...")
    start = time.time()
    df = pd.read_csv(path)
    end = time.time()
    print(f"Pandas read {path} in {end - start:.2f}s")
    return end - start

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=1)
    parser.add_argument("--path", type=str, default="bench_data.csv")
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()

    if args.generate or not os.path.exists(args.path):
        generate_data(args.path, args.size)

    pl_time = benchmark_polars(args.path)
    # pd_time = benchmark_pandas(args.path) # Optional, can be slow

    print("\nBenchmark Results:")
    print(f"| Method | Time (s) |")
    print(f"|--------|----------|")
    print(f"| Polars | {pl_time:.4f} |")
