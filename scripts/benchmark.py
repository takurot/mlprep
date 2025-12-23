import time
import os
import argparse
import polars as pl
import pandas as pd
import subprocess
import json
import mlprep

def generate_data(path, size_gb=1):
    if os.path.exists(path):
        print(f"Data already exists at {path}")
        return

    print(f"Generating {size_gb}GB of data...")
    # Approx: 1GB ~ 10M rows for this schema
    num_rows = int(size_gb * 10_000_000)
    
    # Create chunks to avoid OOM during generation
    chunk_size = 1_000_000
    
    # Write header
    with open(path, "w") as f:
        f.write("a,b,c,group_key\n")

    print(f"Generating {num_rows} rows in chunks...")
    for i in range(0, num_rows, chunk_size):
        current_chunk = min(chunk_size, num_rows - i)
        df = pl.DataFrame({
            "a": range(i, i + current_chunk),
            "b": [1.5] * current_chunk,
            "c": ["test_string_value"] * current_chunk,
            "group_key": [f"key_{x % 100}" for x in range(i, i + current_chunk)]
        })
        with open(path, "a") as f:
            df.write_csv(f, include_header=False)
            
    print(f"Data generated at {path}")

def benchmark_polars_read(path):
    start = time.time()
    df = pl.scan_csv(path).collect()
    end = time.time()
    return end - start, df.height

def benchmark_mlprep_lib_read(path):
    start = time.time()
    df = mlprep.read_csv(path)
    end = time.time()
    return end - start, len(df)

def benchmark_groupby(df):
    start = time.time()
    _ = df.group_by("group_key").agg(pl.col("b").sum()).collect()
    end = time.time()
    return end - start

def benchmark_join(df):
    # Self join for simplicity
    other = df.select(["group_key"]).unique().with_columns(pl.lit(1).alias("other_val"))
    start = time.time()
    _ = df.join(other, on="group_key", how="inner").collect()
    end = time.time()
    return end - start

def run_benchmarks(args):
    results = []
    
    # 1. Read CSV
    print("Benchmarking Read CSV (Polars)...")
    pl_time, rows = benchmark_polars_read(args.path)
    results.append({"task": "Read CSV", "tool": "Polars (Python)", "time": pl_time, "rows": rows})
    
    print("Benchmarking Read CSV (mlprep lib)...")
    try:
        ml_time, ml_rows = benchmark_mlprep_lib_read(args.path)
        results.append({"task": "Read CSV", "tool": "mlprep (Lib)", "time": ml_time, "rows": ml_rows})
    except Exception as e:
        print(f"mlprep lib failed: {e}")
        results.append({"task": "Read CSV", "tool": "mlprep (Lib)", "time": -1, "rows": 0, "error": str(e)})

    # Pandas Baseline (Optional)
    if args.compare_pandas:
        print("Benchmarking Read CSV (Pandas)...")
        start = time.time()
        _ = pd.read_csv(args.path)
        end = time.time()
        results.append({"task": "Read CSV", "tool": "Pandas", "time": end - start, "rows": rows})

    # 2. Ops (using Polars for consistent baseline of "what is possible")
    # This benchmarks the underlying engine capability
    print("Benchmarking GroupBy (Polars)...")
    lf = pl.scan_csv(args.path)
    gb_time = benchmark_groupby(lf)
    results.append({"task": "GroupBy", "tool": "Polars (Native)", "time": gb_time, "rows": rows})

    print("Benchmarking Join (Polars)...")
    join_time = benchmark_join(lf)
    results.append({"task": "Join", "tool": "Polars (Native)", "time": join_time, "rows": rows})

    return results

def print_results(results, fmt):
    if fmt == "json":
        print(json.dumps(results, indent=2))
    else:
        print("| Task | Tool | Time (s) | Speedup vs Polars |")
        print("|------|------|----------|-------------------|")
        
        # Find baselines
        baselines = {r["task"]: r["time"] for r in results if "Polars" in r["tool"]}
        
        for r in results:
            baseline = baselines.get(r["task"], r["time"])
            if r["time"] > 0:
                speedup = f"{baseline / r['time']:.2f}x" if baseline > 0 else "-"
                print(f"| {r['task']} | {r['tool']} | {r['time']:.4f} | {speedup} |")
            else:
                print(f"| {r['task']} | {r['tool']} | FAILED | - |")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=float, default=0.1, help="Size in GB")
    parser.add_argument("--path", type=str, default="bench_data.csv")
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--format", type=str, default="markdown", choices=["markdown", "json"])
    parser.add_argument("--compare-pandas", action="store_true")
    args = parser.parse_args()

    if args.generate or not os.path.exists(args.path):
        generate_data(args.path, size_gb=args.size)

    results = run_benchmarks(args)
    print_results(results, args.format)
