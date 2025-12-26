use anyhow::Result;
use mlprep::dsl::Pipeline;
use mlprep::engine::DataPipeline;
use polars::prelude::*;

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
    let result_pipeline = data_pipeline.apply_transforms(
        pipeline,
        &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
    )?;
    let result_df = result_pipeline.collect()?;

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
