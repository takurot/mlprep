use miette::{Diagnostic, SourceSpan};
use thiserror::Error;

#[derive(Error, Diagnostic, Debug)]
pub enum MlPrepError {
    #[error("Configuration error: {0}")]
    #[diagnostic(
        code("MLPREP-001"),
        help("Please check your pipeline.yaml syntax and structure.")
    )]
    ConfigError(#[source] serde_yaml::Error, #[label("here")] Option<SourceSpan>),

    #[error("I/O error: {0}")]
    #[diagnostic(
        code("MLPREP-002"),
        help("Check file paths and permissions.")
    )]
    IoError(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    #[diagnostic(
        code("MLPREP-003"),
        help("An error occurred within the data processing engine.")
    )]
    PolarsError(#[from] polars::error::PolarsError),

    #[error("Validation failed: {0}")]
    #[diagnostic(
        code("MLPREP-004"),
        help("Data validation rules were violated.")
    )]
    ValidationError(String),
    
    #[error("Transformation error: {0}")]
    #[diagnostic(
        code("MLPREP-005"),
        help("Failed to apply transformation.")
    )]
    TransformError(String),

    #[error("Feature engineering error: {0}")]
    #[diagnostic(
        code("MLPREP-006"),
        help("Failed during feature fitting or transformation.")
    )]
    FeatureError(String),

    #[error(transparent)]
    #[diagnostic(code("MLPREP-000"))]
    Unknown(#[from] anyhow::Error),
}

pub type MlPrepResult<T> = Result<T, MlPrepError>;
