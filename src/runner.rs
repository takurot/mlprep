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

pub fn execution_pipeline(path: &PathBuf, run_id: Uuid, security_config: crate::security::SecurityConfig) -> MlPrepResult<()> {
    let mut metrics = Metrics::new();
    info!("Loading pipeline from {:?}", path);

    // 0. Security Context
    let security_context = crate::security::SecurityContext::new(security_config)
        .map_err(|e| MlPrepError::ConfigError(serde_yaml::Error::custom(format!("Security context init failed: {}", e)), None))?;
    
    // Validate pipeline file path
    security_context.validate_path(path).map_err(|e| MlPrepError::IoError(std::io::Error::new(std::io::ErrorKind::PermissionDenied, e.to_string())))?;

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
        // Validate input path
        security_context.validate_path(&input.path).map_err(|e| MlPrepError::IoError(std::io::Error::new(std::io::ErrorKind::PermissionDenied, e.to_string())))?;

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
    let processed_dp = dp.apply_transforms(pipeline.clone(), &security_context)?;
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
    security_context.validate_path(&output_conf.path).map_err(|e| MlPrepError::IoError(std::io::Error::new(std::io::ErrorKind::PermissionDenied, e.to_string())))?;

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

#[cfg(test)]
mod tests {

    use crate::security::{SecurityConfig, SecurityContext};
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_sandboxing() {
        let dir = tempdir().unwrap();
        let allowed_dir = dir.path().join("allowed");
        std::fs::create_dir(&allowed_dir).unwrap();

        let restricted_dir = dir.path().join("restricted");
        std::fs::create_dir(&restricted_dir).unwrap();

        let allowed_file = allowed_dir.join("input.csv");
        File::create(&allowed_file).unwrap().write_all(b"a,b\n1,2").unwrap();

        let restricted_file = restricted_dir.join("secret.csv");
        File::create(&restricted_file).unwrap().write_all(b"x,y\n8,9").unwrap();

        let config = SecurityConfig {
            allowed_paths: Some(vec![allowed_dir.clone()]),
            mask_columns: None,
        };

        let context = SecurityContext::new(config).unwrap();

        assert!(context.validate_path(&allowed_file).is_ok());
        assert!(context.validate_path(&restricted_file).is_err());

        // Test non-existent allowed path
        let non_existent_out = allowed_dir.join("output.parquet");
        assert!(context.validate_path(&non_existent_out).is_ok());

        let non_existent_restricted = restricted_dir.join("output.parquet");
        assert!(context.validate_path(&non_existent_restricted).is_err());
    }
}
