use crate::dsl::Pipeline;
use crate::engine::DataPipeline;
use crate::io;
use anyhow::{anyhow, Result};
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use tracing::info;

pub fn execution_pipeline(path: &PathBuf) -> Result<()> {
    info!("Loading pipeline from {:?}", path);

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let pipeline: Pipeline = serde_yaml::from_reader(reader)?;

    // 1. Inputs
    if pipeline.inputs.is_empty() {
        return Err(anyhow!("No inputs specified in pipeline"));
    }

    // For MVP, handle first input
    let input_conf = &pipeline.inputs[0];
    info!("Reading input: {:?}", input_conf.path);

    let lf = if input_conf.path.ends_with(".parquet") {
        io::read_parquet(&input_conf.path)?
    } else {
        io::read_csv(&input_conf.path)?
    };

    let dp = DataPipeline::new(lf);

    // 2. Steps
    info!("Executing {} steps...", pipeline.steps.len());
    let pb = ProgressBar::new(1);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")?
            .progress_chars("#>-"),
    );

    pb.set_message("Building execution graph...");

    let processed_dp = dp.apply_transforms(pipeline.clone())?;
    pb.finish_with_message("Execution graph built.");

    // 3. Execution & Output
    if pipeline.outputs.is_empty() {
        info!("No outputs specified, executing pipeline without output...");
        let _ = processed_dp.collect()?;
        info!("Done.");
        return Ok(());
    }

    let output_conf = &pipeline.outputs[0];
    info!(
        "Executing pipeline and writing output to: {:?}",
        output_conf.path
    );

    let mut final_df = processed_dp.collect()?;

    if output_conf.path.ends_with(".parquet") {
        io::write_parquet(final_df.clone(), &output_conf.path)?;
    } else {
        // Fallback for CSV
        if output_conf.path.ends_with(".csv") {
            let mut file = std::fs::File::create(&output_conf.path)?;
            CsvWriter::new(&mut file).finish(&mut final_df)?;
        } else {
            return Err(anyhow!(
                "Unsupported output format for file: {}",
                output_conf.path
            ));
        }
    }

    info!("Pipeline completed successfully.");
    Ok(())
}
