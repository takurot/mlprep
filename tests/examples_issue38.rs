use std::fs;
use std::path::Path;

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

    // Extract state_path value from each pipeline
    let train_state_path = train_content
        .lines()
        .find(|l| l.trim().starts_with("state_path:"))
        .map(|l| l.trim().to_string());
    let test_state_path = test_content
        .lines()
        .find(|l| l.trim().starts_with("state_path:"))
        .map(|l| l.trim().to_string());

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

    // state_path should appear at the step level (same indentation as config:)
    // It must NOT be nested inside the config block
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
