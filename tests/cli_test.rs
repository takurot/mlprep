use std::fs;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_cli_run_pipeline() {
    // 1. Setup temp dir
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.csv");
    let output_path = dir.path().join("output.parquet");
    let config_path = dir.path().join("pipeline.yaml");

    // 2. Create input data
    fs::write(&input_path, "a,b\n1,10\n2,20\n3,30").unwrap();

    // 3. Create pipeline config
    // Note: paths in YAML must be absolute or relative to run location. 
    // Absolute paths are safer for test.
    let yaml = format!(
        r#"
inputs:
  - path: "{input}"
steps:
  - type: filter
    condition: "a >= 2"
outputs:
  - path: "{output}"
"#,
        input = input_path.to_str().unwrap(),
        output = output_path.to_str().unwrap()
    );
    fs::write(&config_path, yaml).unwrap();

    // 4. Run CLI
    // cargo run -- run <config>
    // In tests, we can use CARGO_BIN_EXE_<name> env var provided by cargo.
    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(&["run", config_path.to_str().unwrap()])
        .status()
        .expect("Failed to run mlprep");

    assert!(status.success());

    // 5. Verify output
    assert!(output_path.exists());
    let metadata = fs::metadata(&output_path).unwrap();
    assert!(metadata.len() > 0);
}
