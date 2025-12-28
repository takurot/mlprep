use std::fs;
use std::path::Path;

#[test]
fn dvc_example_has_required_artifacts() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/07_dvc_pipeline");
    assert!(
        base.is_dir(),
        "DVC example directory should exist at {}",
        base.display()
    );

    for file in ["README.md", "dvc.yaml", "pipeline.yaml", "raw_data.csv"] {
        let path = base.join(file);
        assert!(
            path.exists(),
            "Expected required file to exist: {}",
            path.display()
        );
    }

    let dvc_yaml = fs::read_to_string(base.join("dvc.yaml")).expect("dvc.yaml should be readable");
    assert!(
        dvc_yaml.contains("mlprep run pipeline.yaml"),
        "dvc.yaml should run mlprep pipeline"
    );
    assert!(
        dvc_yaml.contains("outs:"),
        "dvc.yaml should declare tracked outputs"
    );
    assert!(
        dvc_yaml.contains("deps:"),
        "dvc.yaml should declare dependencies for reproducibility"
    );
}

#[test]
fn readme_lists_dvc_example() {
    let readme = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"))
        .expect("README.md should be readable");

    assert!(
        readme.to_lowercase().contains("dvc"),
        "Top-level README should mention the DVC pipeline example"
    );
}
