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
        .args(["run", config_path.to_str().unwrap()])
        .status()
        .expect("Failed to run mlprep");

    assert!(status.success());

    // 5. Verify output
    assert!(output_path.exists());
    let metadata = fs::metadata(&output_path).unwrap();
    assert!(metadata.len() > 0);
}

#[test]
fn test_cli_init_template() {
    let dir = tempdir().unwrap();
    let output_path = dir.path().join("pipeline.yaml");

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["init", output_path.to_str().unwrap()])
        .status()
        .expect("Failed to run mlprep init");

    assert!(status.success());
    assert!(output_path.exists());

    let content = fs::read_to_string(&output_path).unwrap();
    // Template should have inputs, steps, and outputs sections
    assert!(content.contains("inputs:"));
    assert!(content.contains("steps:"));
    assert!(content.contains("outputs:"));
}

#[test]
fn test_cli_init_from_csv() {
    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("data.csv");
    let output_path = dir.path().join("pipeline.yaml");

    fs::write(&csv_path, "name,age,score\nalice,30,95.5\nbob,25,80.0").unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args([
            "init",
            "--from",
            csv_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run mlprep init --from");

    assert!(status.success());
    assert!(output_path.exists());

    let content = fs::read_to_string(&output_path).unwrap();
    // Should reference the source CSV
    assert!(content.contains("inputs:"));
    assert!(content.contains("steps:"));
    assert!(content.contains("outputs:"));
}

#[test]
fn test_cli_lint_valid_pipeline() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.csv");
    let output_path = dir.path().join("output.parquet");
    let config_path = dir.path().join("pipeline.yaml");

    fs::write(&input_path, "a,b\n1,2").unwrap();

    let yaml = format!(
        r#"
inputs:
  - path: "{input}"
steps:
  - type: select
    columns: ["a", "b"]
outputs:
  - path: "{output}"
"#,
        input = input_path.to_str().unwrap(),
        output = output_path.to_str().unwrap()
    );
    fs::write(&config_path, yaml).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["lint", config_path.to_str().unwrap()])
        .status()
        .expect("Failed to run mlprep lint");

    assert!(status.success());
}

#[test]
fn test_cli_lint_invalid_yaml() {
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("bad.yaml");

    // Write invalid YAML
    fs::write(
        &config_path,
        "steps:\n  - type: select\n    columns: [UNCLOSED",
    )
    .unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["lint", config_path.to_str().unwrap()])
        .status()
        .expect("Failed to run mlprep lint");

    assert!(!status.success());
}

#[test]
fn test_cli_lint_empty_steps_warns_without_failing() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.csv");
    let output_path = dir.path().join("output.parquet");
    let config_path = dir.path().join("pipeline.yaml");

    fs::write(&input_path, "a,b\n1,2").unwrap();

    let yaml = format!(
        r#"
inputs:
  - path: "{input}"
steps: []
outputs:
  - path: "{output}"
"#,
        input = input_path.to_str().unwrap(),
        output = output_path.to_str().unwrap()
    );
    fs::write(&config_path, yaml).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["lint", config_path.to_str().unwrap()])
        .output()
        .expect("Failed to run mlprep lint");

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("warning: pipeline has no steps"));
}

#[test]
fn test_cli_plan_pipeline() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.csv");
    let output_path = dir.path().join("output.parquet");
    let config_path = dir.path().join("pipeline.yaml");

    fs::write(&input_path, "a,b\n1,2").unwrap();

    let yaml = format!(
        r#"
inputs:
  - path: "{input}"
steps:
  - type: select
    columns: ["a"]
  - type: filter
    condition: "a > 0"
outputs:
  - path: "{output}"
"#,
        input = input_path.to_str().unwrap(),
        output = output_path.to_str().unwrap()
    );
    fs::write(&config_path, yaml).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["plan", config_path.to_str().unwrap()])
        .output()
        .expect("Failed to run mlprep plan");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should show step count
    assert!(stdout.contains("2") || stdout.contains("step"));
}

#[test]
fn test_cli_run_dry_run() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.csv");
    let output_path = dir.path().join("output.parquet");
    let config_path = dir.path().join("pipeline.yaml");

    fs::write(&input_path, "a,b\n1,2").unwrap();

    let yaml = format!(
        r#"
inputs:
  - path: "{input}"
steps:
  - type: select
    columns: ["a"]
outputs:
  - path: "{output}"
"#,
        input = input_path.to_str().unwrap(),
        output = output_path.to_str().unwrap()
    );
    fs::write(&config_path, yaml).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["run", "--dry-run", config_path.to_str().unwrap()])
        .status()
        .expect("Failed to run mlprep run --dry-run");

    assert!(status.success());
    // Output file should NOT be created in dry-run mode
    assert!(!output_path.exists());
}

#[test]
fn test_cli_profile_csv() {
    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("data.csv");
    let report_path = dir.path().join("report.json");

    fs::write(
        &csv_path,
        "name,age,score\nalice,30,95.5\nbob,25,80.0\ncharlie,35,70.0",
    )
    .unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args([
            "profile",
            csv_path.to_str().unwrap(),
            "--out",
            report_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run mlprep profile");

    assert!(status.success());
    assert!(report_path.exists());

    let content = fs::read_to_string(&report_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(json["row_count"].as_u64().unwrap() == 3);
    assert!(json["column_count"].as_u64().unwrap() == 3);
    assert!(json["columns"].is_array());
}

#[test]
fn test_cli_validate_pass() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data.csv");
    let checks_path = dir.path().join("checks.yaml");

    fs::write(&data_path, "age,score\n25,80\n30,90").unwrap();

    let checks_yaml = r#"
columns:
  - name: age
    not_null: true
  - name: score
    not_null: true
    range: [0, 100]
"#;
    fs::write(&checks_path, checks_yaml).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args([
            "validate",
            checks_path.to_str().unwrap(),
            data_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run mlprep validate");

    assert!(status.success());
}

#[test]
fn test_cli_validate_fail() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data.csv");
    let checks_path = dir.path().join("checks.yaml");

    // age column has a null value
    fs::write(&data_path, "age,score\n25,80\n,90").unwrap();

    let checks_yaml = r#"
columns:
  - name: age
    not_null: true
"#;
    fs::write(&checks_path, checks_yaml).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args([
            "validate",
            checks_path.to_str().unwrap(),
            data_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run mlprep validate");

    assert!(!status.success());
}

#[test]
fn test_cli_features_fit() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("train.csv");
    let config_path = dir.path().join("features.yaml");
    let state_path = dir.path().join("feature_state.json");

    fs::write(&data_path, "value,label\n1.0,a\n2.0,b\n3.0,a").unwrap();

    let config_yaml = r#"
features:
  - column: value
    transform: min_max_scale
"#;
    fs::write(&config_path, config_yaml).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args([
            "features",
            "fit",
            config_path.to_str().unwrap(),
            "--in",
            data_path.to_str().unwrap(),
            "--out",
            state_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run mlprep features fit");

    assert!(status.success());
    assert!(state_path.exists());

    let content = fs::read_to_string(&state_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(json["entries"].is_array());
    assert!(!json["entries"].as_array().unwrap().is_empty());
}

#[test]
fn test_cli_features_transform() {
    let dir = tempdir().unwrap();
    let train_path = dir.path().join("train.csv");
    let test_path = dir.path().join("test.csv");
    let config_path = dir.path().join("features.yaml");
    let state_path = dir.path().join("feature_state.json");
    let out_path = dir.path().join("output.parquet");

    fs::write(&train_path, "value\n1.0\n2.0\n3.0").unwrap();
    fs::write(&test_path, "value\n1.5\n2.5").unwrap();

    let config_yaml = r#"
features:
  - column: value
    transform: min_max_scale
"#;
    fs::write(&config_path, config_yaml).unwrap();

    // First fit
    let fit_status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args([
            "features",
            "fit",
            config_path.to_str().unwrap(),
            "--in",
            train_path.to_str().unwrap(),
            "--out",
            state_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run mlprep features fit");
    assert!(fit_status.success());

    // Then transform
    let transform_status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args([
            "features",
            "transform",
            "--state",
            state_path.to_str().unwrap(),
            "--in",
            test_path.to_str().unwrap(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run mlprep features transform");

    assert!(transform_status.success());
    assert!(out_path.exists());
}

#[test]
fn test_cli_completions_bash_outputs_script() {
    let output = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["completions", "bash"])
        .output()
        .expect("Failed to run mlprep completions bash");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.trim().is_empty());
    assert!(stdout.contains("mlprep"));
    assert!(!stdout.contains("coming soon"));
}

#[test]
fn test_cli_completions_powershell_outputs_script() {
    let output = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["completions", "powershell"])
        .output()
        .expect("Failed to run mlprep completions powershell");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Register-ArgumentCompleter"));
}
