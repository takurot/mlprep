pub mod engine;
pub mod io;

use polars::prelude::*;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

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

/// A Python module implemented in Rust.
#[pymodule]
fn mlprep(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.1.0")?;
    m.add_class::<MlPrepDataFrame>()?;
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(write_parquet, m)?)?;
    Ok(())
}
