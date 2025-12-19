use mlprep::io;
use std::time::Instant;

fn main() -> anyhow::Result<()> {
    let path = "bench_data.csv";
    if !std::path::Path::new(path).exists() {
        println!("Please generate data first using: python scripts/benchmark.py --generate");
        return Ok(());
    }

    println!("Benchmarking mlprep (Rust) read_csv...");
    let start = Instant::now();
    let lf = io::read_csv(path)?;
    let df = lf.collect()?;
    let duration = start.elapsed();

    println!("mlprep read {} rows in {:?}", df.height(), duration);
    Ok(())
}
