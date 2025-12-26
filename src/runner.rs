use crate::dsl::Pipeline;
use crate::engine::DataPipeline;
use crate::errors::{MlPrepError, MlPrepResult};
use crate::io;
use crate::observability::{self, InputFileStats, Lineage, Metrics};
use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use serde::de::Error;
use std::path::PathBuf;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

pub fn execution_pipeline(path: &PathBuf, run_id: Uuid) -> MlPrepResult<()> {
    let mut metrics = Metrics::new();
    info!("Loading pipeline from {:?}", path);

    let pipeline = Pipeline::from_path(path)?;

    // 1. Inputs
    if pipeline.inputs.is_empty() {
        return Err(MlPrepError::ConfigError(
            serde_yaml::Error::custom("No inputs specified in pipeline"),
            None,
        ));
    }

    // Capture Input Stats
    let mut input_stats = Vec::new();
    for input in &pipeline.inputs {
        let metadata = std::fs::metadata(&input.path).map_err(MlPrepError::IoError)?;
        let hash = observability::compute_file_hash(&input.path).map_err(MlPrepError::IoError)?;
        input_stats.push(InputFileStats {
            path: input.path.clone(),
            size_bytes: metadata.len(),
            hash,
        });
    }

    // For MVP, handle first input
    let input_conf = &pipeline.inputs[0];
    info!("Reading input: {:?}", input_conf.path);
    let start_read = Instant::now();

    let lf = if input_conf.path.ends_with(".parquet") {
        io::read_parquet(&input_conf.path)?
    } else {
        io::read_csv(&input_conf.path)?
    };
    metrics.record_step("read_input", start_read.elapsed());

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
    let start_build = Instant::now();
    let processed_dp = dp.apply_transforms(pipeline.clone())?;
    metrics.record_step("build_graph", start_build.elapsed());
    pb.finish_with_message("Execution graph built.");

    // 3. Execution & Output
    let start_exec = Instant::now();
    if pipeline.outputs.is_empty() {
        info!("No outputs specified, executing pipeline without output...");
        let df = processed_dp.collect()?;
        metrics.record_step("execution", start_exec.elapsed());
        metrics.rows_read = df.height(); // Approx since we executed
        metrics.rows_written = 0;
        info!("Done.");
        return Ok(()); // Should we write lineage here too? Probably yes.
    }

    let output_conf = &pipeline.outputs[0];
    info!(
        "Executing pipeline and writing output to: {:?}",
        output_conf.path
    );

    let mut final_df = processed_dp.collect()?;
    metrics.record_step("execution", start_exec.elapsed());
    metrics.rows_written = final_df.height();
    // In lazy exec, we might not verify rows_read easily without scanning input separately
    // metrics.rows_read = ???

    let start_write = Instant::now();
    if output_conf.path.ends_with(".parquet") {
        io::write_parquet(final_df.clone(), &output_conf.path)?;
    } else {
        // Fallback for CSV
        if output_conf.path.ends_with(".csv") {
            let mut file =
                std::fs::File::create(&output_conf.path).map_err(MlPrepError::IoError)?;
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
    metrics.record_step("write_output", start_write.elapsed());

    // Generate Lineage
    let lineage = Lineage {
        run_id: run_id.to_string(),
        timestamp: Utc::now(),
        inputs: input_stats,
        outputs: pipeline.outputs.iter().map(|o| o.path.clone()).collect(),
    };

    // Write lineage.json
    let lineage_file = std::fs::File::create("lineage.json").map_err(MlPrepError::IoError)?;
    serde_json::to_writer_pretty(lineage_file, &lineage)
        .map_err(|e| MlPrepError::Unknown(e.into()))?;

    info!("Lineage written to lineage.json");
    if let Ok(m_json) = serde_json::to_string(&metrics) {
        info!("Metrics: {}", m_json);
    }

    info!("Pipeline completed successfully.");
    Ok(())
}
