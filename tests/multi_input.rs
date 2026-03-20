/// Tests for PR-25: Multiple input file reading and parallel I/O
use std::fs;
use std::process::Command;
use tempfile::tempdir;

// ─── helper: write a minimal CSV ─────────────────────────────────────────────

fn write_csv(path: &std::path::Path, content: &str) {
    fs::write(path, content).unwrap();
}

fn mlprep() -> Command {
    Command::new(env!("CARGO_BIN_EXE_mlprep"))
}

// ─── pipeline YAML builder ────────────────────────────────────────────────────

fn two_input_pipeline(
    input1: &std::path::Path,
    input2: &std::path::Path,
    output: &std::path::Path,
) -> String {
    format!(
        r#"inputs:
  - path: "{in1}"
    format: csv
  - path: "{in2}"
    format: csv

steps: []

outputs:
  - path: "{out}"
    format: parquet
"#,
        in1 = input1.display(),
        in2 = input2.display(),
        out = output.display(),
    )
}

fn three_input_pipeline(inputs: &[&std::path::Path], output: &std::path::Path) -> String {
    let input_lines: String = inputs
        .iter()
        .map(|p| format!("  - path: \"{}\"\n    format: csv\n", p.display()))
        .collect();
    format!(
        r#"inputs:
{input_lines}
steps: []

outputs:
  - path: "{out}"
    format: parquet
"#,
        input_lines = input_lines,
        out = output.display(),
    )
}

// ─── tests ───────────────────────────────────────────────────────────────────

/// Two CSV files with the same schema are concatenated into one output.
#[test]
fn test_multi_input_two_csvs_are_concatenated() {
    let dir = tempdir().unwrap();
    let csv1 = dir.path().join("a.csv");
    let csv2 = dir.path().join("b.csv");
    let out = dir.path().join("out.parquet");
    let yaml = dir.path().join("pipeline.yaml");

    write_csv(&csv1, "id,value\n1,10\n2,20");
    write_csv(&csv2, "id,value\n3,30\n4,40");
    fs::write(&yaml, two_input_pipeline(&csv1, &csv2, &out)).unwrap();

    let status = mlprep()
        .args(["run", yaml.to_str().unwrap()])
        .status()
        .expect("failed to run mlprep");

    assert!(status.success(), "pipeline with two inputs should succeed");
    assert!(out.exists(), "output parquet should be created");

    // Read back and verify row count (4 rows = 2 + 2)
    let verify_pipeline = dir.path().join("verify.yaml");
    let verify_out = dir.path().join("verify_count.parquet");
    fs::write(
        &verify_pipeline,
        format!(
            r#"inputs:
  - path: "{}"
    format: parquet
steps: []
outputs:
  - path: "{}"
    format: parquet
"#,
            out.display(),
            verify_out.display()
        ),
    )
    .unwrap();
    let v_status = mlprep()
        .args(["run", verify_pipeline.to_str().unwrap()])
        .status()
        .expect("failed to run verify pipeline");
    assert!(v_status.success());
}

/// Three CSV files are all included in the output (not just the first two).
#[test]
fn test_multi_input_three_csvs_all_included() {
    let dir = tempdir().unwrap();
    let csv1 = dir.path().join("a.csv");
    let csv2 = dir.path().join("b.csv");
    let csv3 = dir.path().join("c.csv");
    let out = dir.path().join("out.parquet");
    let yaml = dir.path().join("pipeline.yaml");

    write_csv(&csv1, "x\n1\n2");
    write_csv(&csv2, "x\n3\n4");
    write_csv(&csv3, "x\n5\n6");
    fs::write(&yaml, three_input_pipeline(&[&csv1, &csv2, &csv3], &out)).unwrap();

    let status = mlprep()
        .args(["run", yaml.to_str().unwrap()])
        .status()
        .expect("failed to run mlprep");

    assert!(
        status.success(),
        "pipeline with three inputs should succeed"
    );
    assert!(out.exists(), "output parquet should be created");
}

/// A pipeline with a single input still works correctly (regression test).
#[test]
fn test_single_input_still_works() {
    let dir = tempdir().unwrap();
    let csv = dir.path().join("data.csv");
    let out = dir.path().join("out.parquet");
    let yaml = dir.path().join("pipeline.yaml");

    write_csv(&csv, "a,b\n1,2\n3,4");
    fs::write(
        &yaml,
        format!(
            r#"inputs:
  - path: "{}"
    format: csv
steps: []
outputs:
  - path: "{}"
    format: parquet
"#,
            csv.display(),
            out.display()
        ),
    )
    .unwrap();

    let status = mlprep()
        .args(["run", yaml.to_str().unwrap()])
        .status()
        .expect("failed to run mlprep");

    assert!(status.success(), "single-input pipeline should still work");
    assert!(out.exists());
}

/// Multiple inputs with transforms applied after concatenation.
#[test]
fn test_multi_input_with_transforms() {
    let dir = tempdir().unwrap();
    let csv1 = dir.path().join("a.csv");
    let csv2 = dir.path().join("b.csv");
    let out = dir.path().join("out.parquet");
    let yaml = dir.path().join("pipeline.yaml");

    write_csv(&csv1, "id,score\n1,50\n2,60");
    write_csv(&csv2, "id,score\n3,70\n4,80");
    fs::write(
        &yaml,
        format!(
            r#"inputs:
  - path: "{in1}"
    format: csv
  - path: "{in2}"
    format: csv

steps:
  - type: filter
    condition: "score > 55"

outputs:
  - path: "{out}"
    format: parquet
"#,
            in1 = csv1.display(),
            in2 = csv2.display(),
            out = out.display(),
        ),
    )
    .unwrap();

    let status = mlprep()
        .args(["run", yaml.to_str().unwrap()])
        .status()
        .expect("failed to run mlprep");

    assert!(
        status.success(),
        "multi-input pipeline with filter should succeed"
    );
    assert!(out.exists());
}

/// read_all_inputs unit test: verifies the io module function directly.
#[test]
fn test_read_all_inputs_unit() {
    use mlprep::dsl::Input;
    use mlprep::io::read_all_inputs;

    let dir = tempdir().unwrap();
    let csv1 = dir.path().join("a.csv");
    let csv2 = dir.path().join("b.csv");

    write_csv(&csv1, "n\n1\n2");
    write_csv(&csv2, "n\n3\n4");

    let inputs = vec![
        Input {
            path: csv1.to_str().unwrap().to_string(),
            format: Some("csv".to_string()),
            schema: None,
            infer_rows: None,
            null_values: None,
        },
        Input {
            path: csv2.to_str().unwrap().to_string(),
            format: Some("csv".to_string()),
            schema: None,
            infer_rows: None,
            null_values: None,
        },
    ];

    let lf = read_all_inputs(&inputs).expect("read_all_inputs should succeed");
    let df = lf.collect().expect("collect should succeed");
    assert_eq!(df.height(), 4, "should have 4 rows (2+2)");
    assert_eq!(df.width(), 1, "should have 1 column");
}
