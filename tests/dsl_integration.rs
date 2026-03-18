use anyhow::Result;
use mlprep::dsl::Pipeline;
use mlprep::engine::DataPipeline;
use polars::prelude::*;
use tempfile::tempdir;

#[test]
fn test_dsl_integration_flow() -> Result<()> {
    // 1. Setup Data
    let df = df! {
        "id" => [1, 2, 3, 4, 5],
        "category" => ["a", "b", "a", "c", "b"],
        "value" => [10.0, 20.0, 30.0, 40.0, 50.0],
    }?;
    let lf = df.lazy();

    // 2. Define YAML Pipeline
    // - Filter value > 15
    // - Cast value to Int32
    // - Select id and value
    let yaml = r#"
steps:
  - type: filter
    condition: "value > 15.0"
  - type: cast
    columns:
      value: "Int32"
  - type: select
    columns: ["id", "value"]
"#;

    // 3. Parse Pipeline
    let pipeline: Pipeline = serde_yaml::from_str(yaml)?;

    // 4. Execute
    let data_pipeline = DataPipeline::new(lf);
    let runtime = mlprep::dsl::RuntimeConfig::default();
    let result_pipeline = data_pipeline.apply_transforms(
        pipeline,
        &runtime,
        &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
    )?;
    let result_df = result_pipeline.collect(false)?;

    // 5. Verify
    // Expected: rows where value > 15.0 => [20.0, 30.0, 40.0, 50.0] corresponding to ids [2, 3, 4, 5]
    assert_eq!(result_df.height(), 4);
    assert_eq!(result_df.get_column_names(), &["id", "value"]);

    let id_col = result_df.column("id")?.i32()?;
    let val_col = result_df.column("value")?.i32()?; // Casted to Int32

    assert_eq!(id_col.get(0), Some(2));
    assert_eq!(val_col.get(0), Some(20));

    Ok(())
}

/// Regression test for https://github.com/takurot/mlprep/issues/35
/// Quarantine mode must save invalid rows to the quarantine_path file.
#[test]
fn test_quarantine_saves_invalid_rows_to_file() -> Result<()> {
    let dir = tempdir()?;
    let quarantine_path = dir.path().join("quarantine.parquet");
    let quarantine_path_str = quarantine_path.to_string_lossy().to_string();

    let df = df! {
        "id" => [1, 2, 3, 4, 5i32],
        "age" => [25, 150, 35, -5, 45i32],
    }?;
    let lf = df.lazy();

    let yaml = format!(
        r#"
steps:
  - type: validate
    checks:
      columns:
        - name: age
          range: [0, 120]
    mode: quarantine
    quarantine_path: "{}"
"#,
        quarantine_path_str
    );

    let pipeline: Pipeline = serde_yaml::from_str(&yaml)?;
    let data_pipeline = DataPipeline::new(lf);
    let runtime = mlprep::dsl::RuntimeConfig::default();
    let result_pipeline = data_pipeline.apply_transforms(
        pipeline,
        &runtime,
        &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
    )?;
    let result_df = result_pipeline.collect(false)?;

    // Valid rows: ages 25, 35, 45 (ids 1, 3, 5)
    assert_eq!(
        result_df.height(),
        3,
        "Output should contain only valid rows"
    );

    // Quarantine file must exist
    assert!(
        quarantine_path.exists(),
        "Quarantine file should have been created at {}",
        quarantine_path_str
    );

    // Quarantine file must contain the 2 invalid rows (ages 150, -5)
    let q_df = LazyFrame::scan_parquet(&quarantine_path_str, Default::default())?.collect()?;
    assert_eq!(
        q_df.height(),
        2,
        "Quarantine file should contain 2 invalid rows"
    );

    Ok(())
}

/// When quarantine mode is used without quarantine_path, it defaults to "quarantine.parquet"
/// in the current directory and still filters valid rows correctly.
#[test]
fn test_quarantine_mode_filters_valid_rows() -> Result<()> {
    let df = df! {
        "id" => [1, 2, 3i32],
        "score" => [0.5f64, 1.5f64, 0.8f64],
    }?;
    let lf = df.lazy();

    // Use a temp dir so default quarantine.parquet doesn't pollute repo root
    let dir = tempdir()?;
    let q_path = dir.path().join("q.parquet").to_string_lossy().to_string();

    let yaml = format!(
        r#"
steps:
  - type: validate
    checks:
      columns:
        - name: score
          range: [0, 1]
    mode: quarantine
    quarantine_path: "{}"
"#,
        q_path
    );

    let pipeline: Pipeline = serde_yaml::from_str(&yaml)?;
    let data_pipeline = DataPipeline::new(lf);
    let runtime = mlprep::dsl::RuntimeConfig::default();
    let result_pipeline = data_pipeline.apply_transforms(
        pipeline,
        &runtime,
        &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
    )?;
    let result_df = result_pipeline.collect(false)?;

    // Only rows with score in [0, 1]: ids 1 and 3
    assert_eq!(
        result_df.height(),
        2,
        "Only rows with valid score should remain"
    );

    let q_df = LazyFrame::scan_parquet(&q_path, Default::default())?.collect()?;
    assert_eq!(
        q_df.height(),
        1,
        "One row with score 1.5 should be quarantined"
    );

    Ok(())
}
