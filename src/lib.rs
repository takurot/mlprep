use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn mlprep(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", "0.1.0")?;
    Ok(())
}
