use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::tempdir;

fn feature_eng_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/03_feature_engineering")
}

#[test]
fn pipeline_train_has_state_path() {
    let path = feature_eng_dir().join("pipeline_train.yaml");
    let content = fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "pipeline_train.yaml should be readable at {}",
            path.display()
        )
    });

    assert!(
        content.contains("state_path: feature_state.json"),
        "pipeline_train.yaml must contain 'state_path: feature_state.json' to save \
         fitted transformer state and prevent data leakage.\nActual content:\n{}",
        content
    );
}

#[test]
fn pipeline_test_has_state_path() {
    let path = feature_eng_dir().join("pipeline_test.yaml");
    let content = fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "pipeline_test.yaml should be readable at {}",
            path.display()
        )
    });

    assert!(
        content.contains("state_path: feature_state.json"),
        "pipeline_test.yaml must contain 'state_path: feature_state.json' to load \
         training statistics and prevent data leakage.\nActual content:\n{}",
        content
    );
}

#[test]
fn pipeline_train_and_test_share_same_state_path() {
    let dir = feature_eng_dir();

    let train_content = fs::read_to_string(dir.join("pipeline_train.yaml"))
        .expect("pipeline_train.yaml should be readable");
    let test_content = fs::read_to_string(dir.join("pipeline_test.yaml"))
        .expect("pipeline_test.yaml should be readable");

    let train_state_path = train_content
        .lines()
        .find(|l| l.trim().starts_with("state_path:"))
        .map(|l| {
            l.trim()
                .split_once(':')
                .map(|(_, v)| v.trim())
                .unwrap_or("")
        })
        .map(|v| v.to_string());
    let test_state_path = test_content
        .lines()
        .find(|l| l.trim().starts_with("state_path:"))
        .map(|l| {
            l.trim()
                .split_once(':')
                .map(|(_, v)| v.trim())
                .unwrap_or("")
        })
        .map(|v| v.to_string());

    assert!(
        train_state_path.is_some(),
        "pipeline_train.yaml must have a state_path field"
    );
    assert!(
        test_state_path.is_some(),
        "pipeline_test.yaml must have a state_path field"
    );
    assert_eq!(
        train_state_path, test_state_path,
        "pipeline_train.yaml and pipeline_test.yaml must share the same state_path \
         so test data is transformed using training statistics"
    );
}

#[test]
fn state_path_is_sibling_of_config_in_train_pipeline() {
    let path = feature_eng_dir().join("pipeline_train.yaml");
    let content = fs::read_to_string(&path).expect("pipeline_train.yaml should be readable");

    let lines: Vec<&str> = content.lines().collect();

    let config_indent = lines
        .iter()
        .find(|l| l.trim() == "config:")
        .map(|l| l.len() - l.trim_start().len());

    let state_indent = lines
        .iter()
        .find(|l| l.trim().starts_with("state_path:"))
        .map(|l| l.len() - l.trim_start().len());

    assert!(
        config_indent.is_some(),
        "pipeline_train.yaml should have a config: field"
    );
    assert!(
        state_indent.is_some(),
        "pipeline_train.yaml should have a state_path: field"
    );
    assert_eq!(
        config_indent, state_indent,
        "state_path must be at the same indentation level as config (sibling, not child)"
    );
}

#[test]
fn state_path_is_sibling_of_config_in_test_pipeline() {
    let path = feature_eng_dir().join("pipeline_test.yaml");
    let content = fs::read_to_string(&path).expect("pipeline_test.yaml should be readable");

    let lines: Vec<&str> = content.lines().collect();

    let config_indent = lines
        .iter()
        .find(|l| l.trim() == "config:")
        .map(|l| l.len() - l.trim_start().len());

    let state_indent = lines
        .iter()
        .find(|l| l.trim().starts_with("state_path:"))
        .map(|l| l.len() - l.trim_start().len());

    assert!(
        config_indent.is_some(),
        "pipeline_test.yaml should have a config: field"
    );
    assert!(
        state_indent.is_some(),
        "pipeline_test.yaml should have a state_path: field"
    );
    assert_eq!(
        config_indent, state_indent,
        "state_path must be at the same indentation level as config (sibling, not child)"
    );
}

/// Integration test: verify that running the train pipeline creates a state file,
/// and running the test pipeline with the same state_path reuses it (no refit).
#[test]
fn feature_engineering_pipelines_create_and_reuse_state() {
    let dir = tempdir().unwrap();
    let train_csv = dir.path().join("train.csv");
    let test_csv = dir.path().join("test.csv");
    let state_path = dir.path().join("feature_state.json");
    let train_out = dir.path().join("train_out.parquet");
    let test_out = dir.path().join("test_out.parquet");
    let train_yaml = dir.path().join("pipeline_train.yaml");
    let test_yaml = dir.path().join("pipeline_test.yaml");

    fs::write(
        &train_csv,
        "age,income,city\n25,500000,Tokyo\n35,700000,Osaka\n45,600000,Tokyo\n30,550000,Nagoya",
    )
    .unwrap();
    fs::write(
        &test_csv,
        "age,income,city\n28,520000,Tokyo\n40,650000,Osaka",
    )
    .unwrap();

    let train_pipeline = format!(
        r#"inputs:
  - path: "{train}"
    format: csv

steps:
  - type: features
    config:
      features:
        - column: age
          transform: standard_scale
        - column: income
          transform: standard_scale
        - column: city
          transform: one_hot_encode
    state_path: "{state}"

outputs:
  - path: "{out}"
    format: parquet
"#,
        train = train_csv.display(),
        state = state_path.display(),
        out = train_out.display(),
    );

    let test_pipeline = format!(
        r#"inputs:
  - path: "{test}"
    format: csv

steps:
  - type: features
    config:
      features:
        - column: age
          transform: standard_scale
        - column: income
          transform: standard_scale
        - column: city
          transform: one_hot_encode
    state_path: "{state}"

outputs:
  - path: "{out}"
    format: parquet
"#,
        test = test_csv.display(),
        state = state_path.display(),
        out = test_out.display(),
    );

    fs::write(&train_yaml, &train_pipeline).unwrap();
    fs::write(&test_yaml, &test_pipeline).unwrap();

    // Run train pipeline: should fit and save state
    let train_status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["run", train_yaml.to_str().unwrap()])
        .status()
        .expect("Failed to run train pipeline");
    assert!(train_status.success(), "Train pipeline should succeed");
    assert!(
        state_path.exists(),
        "state_path should be created after train pipeline"
    );
    assert!(train_out.exists(), "Train output parquet should be created");

    // Record state file mtime to detect if it's rewritten
    let state_mtime = fs::metadata(&state_path).unwrap().modified().unwrap();

    // Run test pipeline: should LOAD existing state (not refit)
    let test_status = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["run", test_yaml.to_str().unwrap()])
        .status()
        .expect("Failed to run test pipeline");
    assert!(test_status.success(), "Test pipeline should succeed");
    assert!(test_out.exists(), "Test output parquet should be created");

    // State file should NOT be modified by the test pipeline (it loaded, not refitted)
    let state_mtime_after = fs::metadata(&state_path).unwrap().modified().unwrap();
    assert_eq!(
        state_mtime, state_mtime_after,
        "State file should not be modified when test pipeline loads existing state"
    );

    // Verify state file contains valid JSON with feature entries
    let state_json: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&state_path).unwrap())
            .expect("feature_state.json should be valid JSON");
    assert!(
        state_json["entries"].is_array(),
        "feature_state.json should have an 'entries' array"
    );
    assert!(
        !state_json["entries"].as_array().unwrap().is_empty(),
        "feature_state.json entries should not be empty"
    );
}
