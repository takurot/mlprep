pub mod compute;
pub mod dsl;
pub mod engine;
pub mod errors;
pub mod features;
pub mod io;
pub mod observability;
pub mod runner;
pub mod security;
pub mod validate;

use polars::prelude::*;
use pyo3::exceptions::{PyIOError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use std::path::PathBuf;
use uuid::Uuid;

/// Wrapper for DataFrame that exposes it to Python
#[pyclass(name = "DataFrame")]
#[derive(Clone)]
pub struct MlPrepDataFrame {
    inner: DataFrame,
}

#[pymethods]
impl MlPrepDataFrame {
    /// Convert to a Polars DataFrame (Python)
    fn to_polars(&self, py: Python<'_>) -> PyResult<PyObject> {
        let py_df = PyDataFrame(self.inner.clone());
        Ok(py_df.into_pyobject(py)?.into_any().unbind())
    }

    fn __len__(&self) -> usize {
        self.inner.height()
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }
}

/// Read a CSV file and return a DataFrame
#[pyfunction]
fn read_csv(path: &str) -> PyResult<MlPrepDataFrame> {
    let lf =
        io::read_csv(path).map_err(|e| PyIOError::new_err(format!("Failed to read CSV: {}", e)))?;
    let df = lf
        .collect()
        .map_err(|e| PyIOError::new_err(format!("Failed to collect DataFrame: {}", e)))?;
    Ok(MlPrepDataFrame { inner: df })
}

/// Read a Parquet file and return a DataFrame
#[pyfunction]
fn read_parquet(path: &str) -> PyResult<MlPrepDataFrame> {
    let lf = io::read_parquet(path)
        .map_err(|e| PyIOError::new_err(format!("Failed to read Parquet: {}", e)))?;
    let df = lf
        .collect()
        .map_err(|e| PyIOError::new_err(format!("Failed to collect DataFrame: {}", e)))?;
    Ok(MlPrepDataFrame { inner: df })
}

/// Write a DataFrame to a Parquet file
#[pyfunction]
fn write_parquet(df: &MlPrepDataFrame, path: &str) -> PyResult<()> {
    io::write_parquet(df.inner.clone(), path)
        .map_err(|e| PyIOError::new_err(format!("Failed to write Parquet: {}", e)))?;
    Ok(())
}

/// Run a pipeline from a YAML configuration file path
#[pyfunction(signature = (path, streaming=None, memory_limit=None))]
fn run_pipeline(
    path: String,
    streaming: Option<bool>,
    memory_limit: Option<String>,
) -> PyResult<()> {
    let path_buf = PathBuf::from(path);
    let run_id = Uuid::new_v4();
    // Default security config for Python usage (no restrictions for now)
    let security_config = crate::security::SecurityConfig {
        allowed_paths: None,
        mask_columns: None,
    };
    let runtime_override = if streaming.unwrap_or(false) || memory_limit.is_some() {
        Some(crate::dsl::RuntimeConfig {
            streaming: streaming.unwrap_or(false),
            memory_limit,
            ..Default::default()
        })
    } else {
        None
    };
    runner::execution_pipeline(&path_buf, run_id, security_config, runtime_override)
        .map_err(|e| PyRuntimeError::new_err(format!("Pipeline execution failed: {}", e)))?;
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn mlprep(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.3.0")?;
    m.add_class::<MlPrepDataFrame>()?;
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(write_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(run_pipeline, m)?)?;
    Ok(())
}
