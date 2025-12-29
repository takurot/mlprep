import os
import tempfile
import time
import argparse
import polars as pl
import subprocess
import json
import mlprep

CORE_COLUMNS = ["a", "b", "c", "group_key"]
SHOWCASE_COLUMNS = ["email", "age", "income", "city"]

def generate_data(path, size_gb=1, rows=None, profile="core"):
    if os.path.exists(path):
        print(f"Data already exists at {path}")
        return

    if profile not in ("core", "showcase"):
        raise ValueError(f"Unknown profile: {profile}")

    num_rows = rows if rows is not None else int(size_gb * 10_000_000)
    if num_rows <= 0:
        raise ValueError("rows must be positive")

    if rows is None:
        print(f"Generating {size_gb}GB of data...")
    else:
        print(f"Generating {num_rows} rows of data...")

    chunk_size = 1_000_000
    header = CORE_COLUMNS + (SHOWCASE_COLUMNS if profile == "showcase" else [])

    with open(path, "w") as f:
        f.write(",".join(header) + "\n")

    print(f"Generating {num_rows} rows in chunks...")
    invalid_every = max(1, int(1 / 0.01))

    for i in range(0, num_rows, chunk_size):
        current_chunk = min(chunk_size, num_rows - i)
        indexes = range(i, i + current_chunk)
        data = {
            "a": range(i, i + current_chunk),
            "b": [1.5] * current_chunk,
            "c": ["test_string_value"] * current_chunk,
            "group_key": [f"key_{x % 100}" for x in indexes],
        }

        if profile == "showcase":
            data.update(
                {
                    "email": [
                        "dup@example.com" if x % invalid_every == 0 else f"user_{x}@example.com"
                        for x in indexes
                    ],
                    "age": [
                        -1 if x % invalid_every == 0 else (x % 100) + 18
                        for x in indexes
                    ],
                    "income": [30000.0 + (x % 1000) for x in indexes],
                    "city": [f"city_{x % 10}" for x in indexes],
                }
            )

        df = pl.DataFrame(data)
        with open(path, "a") as f:
            df.write_csv(f, include_header=False)

    print(f"Data generated at {path}")


def ensure_showcase_schema(path):
    with open(path, "r") as f:
        header = f.readline().strip()
    columns = header.split(",") if header else []
    missing = [col for col in SHOWCASE_COLUMNS if col not in columns]
    if missing:
        raise ValueError(
            "Showcase benchmarks require columns: "
            + ", ".join(missing)
            + ". Re-run with --generate --schema showcase."
        )


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

def build_groupby_pipeline(input_path, output_path):
    return f"""
inputs:
  - path: "{os.path.abspath(input_path)}"
steps:
  - type: group_by
    by: ["group_key"]
    aggs:
      b:
        func: "sum"
        alias: "sum_b"
outputs:
  - path: "{output_path}"
    format: parquet
"""


def build_validation_pipeline(input_path, output_path):
    return f"""
inputs:
  - path: "{os.path.abspath(input_path)}"
steps:
  - type: validate
    checks:
      columns:
        - name: email
          unique: true
        - name: age
          range: [0, 120]
    mode: quarantine
outputs:
  - path: "{output_path}"
    format: parquet
"""


def build_features_pipeline(input_path, output_path, state_path):
    return f"""
inputs:
  - path: "{os.path.abspath(input_path)}"
steps:
  - type: features
    config:
      features:
        - column: age
          transform: standard_scale
        - column: income
          transform: standard_scale
        - column: city
          transform: one_hot_encode
    state_path: "{state_path}"
outputs:
  - path: "{output_path}"
    format: parquet
"""


def build_showcase_pipeline(input_path, output_path, state_path, streaming=True):
    runtime = "runtime:\n  streaming: true\n" if streaming else ""
    return f"""
{runtime}inputs:
  - path: "{os.path.abspath(input_path)}"
steps:
  - type: validate
    checks:
      columns:
        - name: email
          unique: true
        - name: age
          range: [0, 120]
    mode: quarantine
  - type: features
    config:
      features:
        - column: age
          transform: standard_scale
        - column: income
          transform: standard_scale
        - column: city
          transform: one_hot_encode
    state_path: "{state_path}"
outputs:
  - path: "{output_path}"
    format: parquet
"""


def benchmark_mlprep_cli_run(pipeline_yaml, streaming=False):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        tmp.write(pipeline_yaml)
        config_path = tmp.name

    cmd = ["mlprep", "run", config_path]
    if streaming:
        cmd.append("--streaming")

    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    end = time.time()

    os.remove(config_path)

    if result.returncode != 0:
        raise Exception(f"CLI failed: {result.stderr}")

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
        import pandas as pd

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

    # 3. CLI (Streaming vs Non-Streaming)
    print("Benchmarking CLI Run (Standard)...")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "groupby.parquet")
            pipeline_yaml = build_groupby_pipeline(args.path, output_path)
            cli_time = benchmark_mlprep_cli_run(pipeline_yaml, streaming=False)
        results.append({"task": "CLI Run", "tool": "mlprep (CLI)", "time": cli_time, "rows": rows})
    except Exception as e:
        print(f"CLI Standard failed: {e}")
        results.append({"task": "CLI Run", "tool": "mlprep (CLI)", "time": -1, "rows": 0, "error": str(e)})

    print("Benchmarking CLI Run (Streaming)...")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "groupby_stream.parquet")
            pipeline_yaml = build_groupby_pipeline(args.path, output_path)
            stream_time = benchmark_mlprep_cli_run(pipeline_yaml, streaming=True)
        results.append({"task": "CLI Run", "tool": "mlprep (Streaming)", "time": stream_time, "rows": rows})
    except Exception as e:
        print(f"CLI Streaming failed: {e}")
        results.append({"task": "CLI Run", "tool": "mlprep (Streaming)", "time": -1, "rows": 0, "error": str(e)})

    if args.showcase:
        results.extend(run_showcase_benchmarks(args, rows))

    return results


def run_showcase_benchmarks(args, rows):
    results = []

    print("Benchmarking Showcase Pipeline (validate + features, streaming)...")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "showcase.parquet")
            state_path = os.path.join(tmpdir, "feature_state.json")
            pipeline_yaml = build_showcase_pipeline(
                args.path, output_path, state_path, streaming=True
            )
            showcase_time = benchmark_mlprep_cli_run(pipeline_yaml, streaming=True)
        results.append(
            {
                "task": "Pipeline (Validation+Features)",
                "tool": "mlprep (CLI streaming)",
                "time": showcase_time,
                "rows": rows,
                "note": "quarantine + standard/onehot",
            }
        )
    except Exception as e:
        print(f"Showcase pipeline failed: {e}")
        results.append(
            {
                "task": "Pipeline (Validation+Features)",
                "tool": "mlprep (CLI streaming)",
                "time": -1,
                "rows": 0,
                "error": str(e),
                "note": "quarantine + standard/onehot",
            }
        )

    print("Benchmarking Showcase Pipeline (non-streaming)...")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "showcase_ns.parquet")
            state_path = os.path.join(tmpdir, "feature_state.json")
            pipeline_yaml = build_showcase_pipeline(
                args.path, output_path, state_path, streaming=False
            )
            showcase_time = benchmark_mlprep_cli_run(pipeline_yaml, streaming=False)
        results.append(
            {
                "task": "Pipeline (Validation+Features)",
                "tool": "mlprep (CLI)",
                "time": showcase_time,
                "rows": rows,
                "note": "quarantine + standard/onehot",
            }
        )
    except Exception as e:
        print(f"Showcase (non-streaming) pipeline failed: {e}")
        results.append(
            {
                "task": "Pipeline (Validation+Features)",
                "tool": "mlprep (CLI)",
                "time": -1,
                "rows": 0,
                "error": str(e),
                "note": "quarantine + standard/onehot",
            }
        )

    if args.compare_pandas:
        print("Benchmarking Pandas baseline (validate + z-score)...")
        try:
            import pandas as pd

            start = time.time()
            df = pd.read_csv(args.path)
            df = df.drop_duplicates(subset=["email"])
            df = df[(df["age"] >= 0) & (df["age"] <= 120)]
            df["age_z"] = (df["age"] - df["age"].mean()) / df["age"].std(ddof=1)
            pd_time = time.time() - start
            results.append(
                {
                    "task": "Pipeline (Validation+Features)",
                    "tool": "Pandas",
                    "time": pd_time,
                    "rows": len(df),
                    "note": "drop dup + zscore",
                }
            )
        except Exception as e:
            print(f"Pandas baseline failed: {e}")
            results.append(
                {
                    "task": "Pipeline (Validation+Features)",
                    "tool": "Pandas",
                    "time": -1,
                    "rows": 0,
                    "error": str(e),
                    "note": "drop dup + zscore",
                }
            )

    return results


def render_markdown(results):
    lines = ["| Task | Tool | Time (s) | Speedup vs Polars |", "|------|------|----------|-------------------|"]

    baselines = {r["task"]: r["time"] for r in results if "Polars" in r["tool"]}

    for r in results:
        baseline = baselines.get(r["task"], r["time"])
        if r["time"] > 0:
            speedup = f"{baseline / r['time']:.2f}x" if baseline > 0 else "-"
            lines.append(f"| {r['task']} | {r['tool']} | {r['time']:.4f} | {speedup} |")
        else:
            lines.append(f"| {r['task']} | {r['tool']} | FAILED | - |")

    return "\n".join(lines)


def render_showcase_markdown(results):
    lines = [
        "| Task | Tool | Time (s) | Rows | Rows/s | Highlights |",
        "|------|------|----------|------|--------|------------|",
    ]

    for r in results:
        rows = r.get("rows", 0)
        note = r.get("note", "-")
        if r["time"] > 0:
            rows_per_sec = rows / r["time"] if rows > 0 else 0
            lines.append(
                f"| {r['task']} | {r['tool']} | {r['time']:.4f} | {rows} | {rows_per_sec:.2f} | {note} |"
            )
        else:
            lines.append(
                f"| {r['task']} | {r['tool']} | FAILED | {rows} | - | {note} |"
            )

    return "\n".join(lines)


def print_results(results, fmt):
    if fmt == "json":
        print(json.dumps(results, indent=2))
    elif fmt == "showcase":
        print(render_showcase_markdown(results))
    else:
        print(render_markdown(results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=float, default=0.1, help="Size in GB")
    parser.add_argument("--rows", type=int, help="Number of rows to generate (overrides --size)")
    parser.add_argument("--path", type=str, default="bench_data.csv")
    parser.add_argument("--generate", action="store_true")
    parser.add_argument(
        "--format",
        type=str,
        default="markdown",
        choices=["markdown", "json", "showcase"],
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="showcase",
        choices=["core", "showcase"],
    )
    showcase_group = parser.add_mutually_exclusive_group()
    showcase_group.add_argument(
        "--showcase",
        dest="showcase",
        action="store_true",
        help="Include validation + feature pipelines for mlprep",
        default=True,
    )
    showcase_group.add_argument(
        "--no-showcase",
        dest="showcase",
        action="store_false",
        help="Skip showcase workloads",
    )
    pandas_group = parser.add_mutually_exclusive_group()
    pandas_group.add_argument(
        "--compare-pandas",
        dest="compare_pandas",
        action="store_true",
        default=True,
        help="Include Pandas baseline runs",
    )
    pandas_group.add_argument(
        "--no-compare-pandas",
        dest="compare_pandas",
        action="store_false",
        help="Skip Pandas baseline runs",
    )
    args = parser.parse_args()

    if args.showcase and args.schema == "core" and (args.generate or not os.path.exists(args.path)):
        args.schema = "showcase"

    if args.generate or not os.path.exists(args.path):
        generate_data(args.path, size_gb=args.size, rows=args.rows, profile=args.schema)

    if args.showcase:
        ensure_showcase_schema(args.path)

    results = run_benchmarks(args)
    print_results(results, args.format)
