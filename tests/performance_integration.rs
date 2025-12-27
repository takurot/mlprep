use std::fs;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_streaming_mode_yaml() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.csv");
    fs::write(&input_path, "a,b\n1,2\n3,4").unwrap();

    let output_path = dir.path().join("output.csv");

    let config_path = dir.path().join("pipeline.yaml");
    let yaml = format!(
        r#"
inputs:
  - path: "{input}"
runtime:
  streaming: true
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

    let output = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args(["run", config_path.to_str().unwrap()])
        .env("MLPREP_LOG", "info")
        .output()
        .expect("Failed to run mlprep");

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Execution mode: Streaming enabled"),
        "Should log streaming enabled. log: {}",
        stderr
    );
}

#[test]
fn test_streaming_mode_cli_override() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.csv");
    fs::write(&input_path, "a,b\n1,2").unwrap();

    let config_path = dir.path().join("pipeline.yaml");
    // Default runtime (streaming: false)
    let yaml = format!(
        r#"
inputs:
  - path: "{input}"
steps:
  - type: select
    columns: ["a"]
outputs: []
"#,
        input = input_path.to_str().unwrap()
    );
    fs::write(&config_path, yaml).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_mlprep"))
        .args([
            "run",
            config_path.to_str().unwrap(),
            "--streaming",
            "--memory-limit",
            "1GB",
        ])
        .env("MLPREP_LOG", "info")
        .output()
        .expect("Failed to run mlprep");

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Execution mode: Streaming enabled"),
        "Should log streaming enabled due to CLI flag. log: {}",
        stderr
    );
    assert!(
        stderr.contains("Memory limit: 1GB"),
        "Should log memory limit. log: {}",
        stderr
    );
}
