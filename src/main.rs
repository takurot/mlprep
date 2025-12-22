use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::SerWriter;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "mlprep")]
#[command(version = "0.1.0")]
#[command(about = "High-performance no-code data preprocessing engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a pipeline from a YAML configuration file
    Run {
        /// Path to the pipeline YAML file
        #[arg(value_name = "PIPELINE_FILE")]
        pipeline: PathBuf,
    },
}

fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let cli = Cli::parse();

    match &cli.command {
        Commands::Run { pipeline } => {
            run_pipeline(pipeline)?;
        }
    }

    Ok(())
}

fn run_pipeline(path: &PathBuf) -> Result<()> {
    info!("Loading pipeline from {:?}", path);

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let pipeline: mlprep::dsl::Pipeline = serde_yaml::from_reader(reader)?;

    // 1. Inputs
    if pipeline.inputs.is_empty() {
        return Err(anyhow!("No inputs specified in pipeline"));
    }
    
    // For MVP, handle first input
    let input_conf = &pipeline.inputs[0];
    info!("Reading input: {:?}", input_conf.path);
    
    let lf = if input_conf.path.ends_with(".parquet") {
        mlprep::io::read_parquet(&input_conf.path)?
    } else {
        mlprep::io::read_csv(&input_conf.path)?
    };

    // Apply Schema (infer_rows, null_values, etc. - todo: implement in io layer)
    // For now we assume defaults or basic read.
    
    let dp = mlprep::engine::DataPipeline::new(lf);
    
    // 2. Steps
    info!("Executing {} steps...", pipeline.steps.len());
    let pb = ProgressBar::new(1); // steps loop is inside engine
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] {msg}")?
        .progress_chars("#>-"));
    
    pb.set_message("Building execution graph...");
    
    // Create a clone of the pipeline logic part if needed, or just pass it since we already extracted input/output info (but inputs/outputs are still in pipeline struct)
    // engine::DataPipeline::apply_transforms takes Pipeline struct.
    // We already used `pipeline.inputs` and will use `pipeline.outputs`.
    // Since `Pipeline` is Clone, we can clone it.
    
    let processed_dp = dp.apply_transforms(pipeline.clone())?;
    pb.finish_with_message("Execution graph built.");
    
    // 3. Execution & Output
    if pipeline.outputs.is_empty() {
        info!("No outputs specified, executing pipeline without output...");
        let _ = processed_dp.collect()?;
        info!("Done.");
        return Ok(());
    }
    
    // We only support single output flow efficiently for now as discussed.
    // If multiple outputs are needed, we would need to clone the LazyFrame before writing each.
    // But `processed_dp` owns the LazyFrame.
    // We'll collect once for the first output, and if there are more, we'd be in trouble if we consumed it.
    // Actually `collect` consumes `DataPipeline`.
    // So for MVP effectively supports 1 output (or we implement sinks).
    
    let output_conf = &pipeline.outputs[0];
    info!("Executing pipeline and writing output to: {:?}", output_conf.path);
    
    // We collect into DataFrame then write. 
    // Ideally we should use `sink_parquet` for streaming/lazy writing if supported by Polars + our wrapper.
    // But `io::write_parquet` takes `DataFrame`.
    
    let mut final_df = processed_dp.collect()?;
    
    if output_conf.path.ends_with(".parquet") {
        mlprep::io::write_parquet(final_df.clone(), &output_conf.path)?;
    } else {
        // Fallback for CSV or others? io module currently only has write_parquet.
        // If user asks for CSV, we fail or implement write_csv.
        if output_conf.path.ends_with(".csv") {
             let mut file = std::fs::File::create(&output_conf.path)?;
             polars::prelude::CsvWriter::new(&mut file).finish(&mut final_df)?;
        } else {
             return Err(anyhow!("Unsupported output format for file: {}", output_conf.path));
        }
    }
    
    info!("Pipeline completed successfully.");
    Ok(())
}
