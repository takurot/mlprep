use anyhow::Result;
use mlprep::dsl::Pipeline;
use mlprep::engine::DataPipeline;
use polars::prelude::*;
use std::io::Write;
use tempfile::NamedTempFile;

/// Test Sort with multiple columns and mixed ascending/descending
#[test]
fn test_sort_integration() -> Result<()> {
    let df = df! {
        "category" => ["a", "b", "a", "b", "a"],
        "value" => [30, 10, 20, 40, 10],
    }?;
    let lf = df.lazy();

    let yaml = r#"
steps:
  - type: sort
    by: ["category", "value"]
    descending: [false, true]
"#;

    let pipeline: Pipeline = serde_yaml::from_str(yaml)?;
    let data_pipeline = DataPipeline::new(lf);
    let result_df = data_pipeline
        .apply_transforms(
            pipeline,
            &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
        )?
        .collect()?;

    assert_eq!(result_df.height(), 5);
    let cat = result_df.column("category")?.str()?;
    let val = result_df.column("value")?.i32()?;

    // Expected order: a(30), a(20), a(10), b(40), b(10)
    assert_eq!(cat.get(0), Some("a"));
    assert_eq!(val.get(0), Some(30));
    assert_eq!(cat.get(1), Some("a"));
    assert_eq!(val.get(1), Some(20));
    assert_eq!(cat.get(2), Some("a"));
    assert_eq!(val.get(2), Some(10));
    assert_eq!(cat.get(3), Some("b"));
    assert_eq!(val.get(3), Some(40));

    Ok(())
}

/// Test GroupBy with sum aggregation
#[test]
fn test_groupby_integration() -> Result<()> {
    let df = df! {
        "region" => ["east", "west", "east", "west", "east"],
        "product" => ["A", "A", "B", "B", "A"],
        "sales" => [100, 200, 150, 300, 250],
    }?;
    let lf = df.lazy();

    let yaml = r#"
steps:
  - type: group_by
    by: ["region"]
    aggs:
      sales:
        func: "sum"
        alias: "total_sales"
"#;

    let pipeline: Pipeline = serde_yaml::from_str(yaml)?;
    let data_pipeline = DataPipeline::new(lf);
    let result_df = data_pipeline
        .apply_transforms(
            pipeline,
            &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
        )?
        .collect()?
        .sort(["region"], Default::default())?;

    assert_eq!(result_df.height(), 2);
    let region = result_df.column("region")?.str()?;
    let total = result_df.column("total_sales")?.i32()?;

    // east: 100 + 150 + 250 = 500
    // west: 200 + 300 = 500
    assert_eq!(region.get(0), Some("east"));
    assert_eq!(total.get(0), Some(500));
    assert_eq!(region.get(1), Some("west"));
    assert_eq!(total.get(1), Some(500));

    Ok(())
}

/// Test Window function with partition
#[test]
fn test_window_integration() -> Result<()> {
    let df = df! {
        "department" => ["eng", "eng", "sales", "sales"],
        "employee" => ["alice", "bob", "carol", "dave"],
        "salary" => [100, 120, 80, 90],
    }?;
    let lf = df.lazy();

    let yaml = r#"
steps:
  - type: window
    partition_by: ["department"]
    ops:
      - column: "salary"
        func: "sum"
        alias: "dept_total"
      - column: "salary"
        func: "mean"
        alias: "dept_avg"
"#;

    let pipeline: Pipeline = serde_yaml::from_str(yaml)?;
    let data_pipeline = DataPipeline::new(lf);
    let result_df = data_pipeline
        .apply_transforms(
            pipeline,
            &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
        )?
        .collect()?;

    assert_eq!(result_df.height(), 4);
    let dept_total = result_df.column("dept_total")?.i32()?;
    let dept_avg = result_df.column("dept_avg")?.f64()?;

    // eng: 100 + 120 = 220, avg = 110
    // sales: 80 + 90 = 170, avg = 85
    assert_eq!(dept_total.get(0), Some(220)); // eng
    assert_eq!(dept_total.get(1), Some(220)); // eng
    assert_eq!(dept_total.get(2), Some(170)); // sales
    assert_eq!(dept_total.get(3), Some(170)); // sales

    assert!((dept_avg.get(0).unwrap() - 110.0).abs() < 0.01);

    Ok(())
}

/// Test Join with CSV file
#[test]
fn test_join_integration() -> Result<()> {
    // Create a temporary lookup CSV file
    let mut lookup_file = NamedTempFile::new()?;
    writeln!(lookup_file, "user_id,name")?;
    writeln!(lookup_file, "1,Alice")?;
    writeln!(lookup_file, "2,Bob")?;
    writeln!(lookup_file, "3,Carol")?;
    lookup_file.flush()?;

    let df = df! {
        "id" => [1, 2, 1, 3],
        "action" => ["buy", "sell", "buy", "view"],
    }?;
    let lf = df.lazy();

    let yaml = format!(
        r#"
steps:
  - type: join
    right_path: "{}"
    left_on: ["id"]
    right_on: ["user_id"]
    how: "left"
"#,
        lookup_file.path().display()
    );

    let pipeline: Pipeline = serde_yaml::from_str(&yaml)?;
    let data_pipeline = DataPipeline::new(lf);
    let result_df = data_pipeline
        .apply_transforms(
            pipeline,
            &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
        )?
        .collect()?;

    assert_eq!(result_df.height(), 4);
    let column_names: Vec<String> = result_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(column_names.contains(&"name".to_string()));

    let names = result_df.column("name")?.str()?;
    assert_eq!(names.get(0), Some("Alice"));
    assert_eq!(names.get(1), Some("Bob"));

    Ok(())
}

/// Test complex pipeline combining filter, groupby, and sort
#[test]
fn test_complex_pipeline_integration() -> Result<()> {
    let df = df! {
        "date" => ["2024-01", "2024-01", "2024-02", "2024-02", "2024-01"],
        "category" => ["A", "B", "A", "B", "A"],
        "amount" => [100.0, 200.0, 150.0, 250.0, 50.0],
    }?;
    let lf = df.lazy();

    let yaml = r#"
steps:
  - type: filter
    condition: "amount >= 100"
  - type: group_by
    by: ["date", "category"]
    aggs:
      amount:
        func: "sum"
        alias: "total"
  - type: sort
    by: ["date", "total"]
    descending: [false, true]
"#;

    let pipeline: Pipeline = serde_yaml::from_str(yaml)?;
    let data_pipeline = DataPipeline::new(lf);
    let result_df = data_pipeline
        .apply_transforms(
            pipeline,
            &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
        )?
        .collect()?;

    // After filter: removes (2024-01, A, 50) since 50 < 100
    // After groupby:
    //   (2024-01, A) -> 100
    //   (2024-01, B) -> 200
    //   (2024-02, A) -> 150
    //   (2024-02, B) -> 250
    // After sort by date asc, total desc:
    //   (2024-01, B, 200)
    //   (2024-01, A, 100)
    //   (2024-02, B, 250)
    //   (2024-02, A, 150)

    assert_eq!(result_df.height(), 4);
    let total = result_df.column("total")?.f64()?;
    assert!((total.get(0).unwrap() - 200.0).abs() < 0.01);
    assert!((total.get(1).unwrap() - 100.0).abs() < 0.01);

    Ok(())
}

/// Test edge case: empty groupby result
#[test]
fn test_groupby_empty_filter() -> Result<()> {
    let df = df! {
        "category" => ["a", "b", "c"],
        "value" => [10, 20, 30],
    }?;
    let lf = df.lazy();

    let yaml = r#"
steps:
  - type: filter
    condition: "value > 100"
  - type: group_by
    by: ["category"]
    aggs:
      value:
        func: "sum"
        alias: "total"
"#;

    let pipeline: Pipeline = serde_yaml::from_str(yaml)?;
    let data_pipeline = DataPipeline::new(lf);
    let result_df = data_pipeline
        .apply_transforms(
            pipeline,
            &mlprep::security::SecurityContext::new(Default::default()).unwrap(),
        )?
        .collect()?;

    // All rows filtered out -> empty result
    assert_eq!(result_df.height(), 0);

    Ok(())
}
