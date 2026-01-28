import subprocess
import time
import os
import sys

def build(features):
    print(f"Building with features: {features} ...")
    cmd = ["cargo", "build", "--release", "--bin", "mlprep"]
    if features == "none":
        cmd.append("--no-default-features")
    else:
        cmd.extend(["--features", features])
    
    subprocess.run(cmd, check=True, capture_output=True)

def run_pipeline():
    cmd = ["target/release/mlprep", "run", "examples/bench_scaling/pipeline.yaml"]
    start = time.time()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time()
    return end - start

def main():
    # 1. Base (No SIMD)
    build("none")
    # Run once to warm up FS cache?
    run_pipeline()
    times = []
    for _ in range(3):
        times.append(run_pipeline())
    t_base = sum(times) / len(times)
    
    # 2. SIMD (Current)
    build("simd")
    # Warm up
    run_pipeline()
    times = []
    for _ in range(3):
        times.append(run_pipeline())
    t_simd = sum(times) / len(times)
    
    print(f"\nResults (Average of 3 runs):")
    print(f"Base (No SIMD): {t_base:.4f}s")
    print(f"SIMD (Current): {t_simd:.4f}s")
    if t_simd > 0:
        print(f"Ratio (Base/SIMD): {t_base/t_simd:.2f}x")

if __name__ == "__main__":
    main()
