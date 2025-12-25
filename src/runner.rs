use crate::dsl::Pipeline;
use crate::engine::DataPipeline;
use crate::errors::{MlPrepError, MlPrepResult};
use crate::io;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use serde::de::Error;
use std::path::PathBuf;
use tracing::info;

pub fn execution_pipeline(path: &PathBuf) -> MlPrepResult<()> {
    info!("Loading pipeline from {:?}", path);

    let pipeline = Pipeline::from_path(path)?;

    // 1. Inputs
    if pipeline.inputs.is_empty() {
        return Err(MlPrepError::ConfigError(
            serde_yaml::Error::custom("No inputs specified in pipeline"),
            None,
        ));
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
            .template("{spinner:.green} [{elapsed_precise}] {msg}")
            .map_err(|e| MlPrepError::Unknown(e.into()))? // Template error is rare/internal
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
            let mut file = std::fs::File::create(&output_conf.path).map_err(MlPrepError::IoError)?;
            CsvWriter::new(&mut file)
                .finish(&mut final_df)
                .map_err(MlPrepError::PolarsError)?;
        } else {
            return Err(MlPrepError::ConfigError(
                serde_yaml::Error::custom(format!(
                    "Unsupported output format for file: {}",
                    output_conf.path
                )),
                None,
            ));
        }
    }

    info!("Pipeline completed successfully.");
    Ok(())
}
