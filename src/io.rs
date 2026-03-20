use crate::dsl::Input;
use crate::errors::{MlPrepError, MlPrepResult};
use polars::prelude::*;
use rayon::prelude::*;
use serde::de::Error as _;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputFormat {
    Csv,
    Parquet,
}

impl InputFormat {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "csv" => Some(Self::Csv),
            "parquet" => Some(Self::Parquet),
            _ => None,
        }
    }
}

fn inferred_input_format(path: &str) -> Option<InputFormat> {
    if path.ends_with(".parquet") {
        Some(InputFormat::Parquet)
    } else if path.ends_with(".csv") {
        Some(InputFormat::Csv)
    } else {
        None
    }
}

fn resolved_input_format(input: &Input) -> MlPrepResult<InputFormat> {
    if let Some(format) = input.format.as_deref() {
        return InputFormat::parse(format).ok_or_else(|| {
            MlPrepError::ConfigError(
                serde_yaml::Error::custom(format!("Unsupported input format: {}", format)),
                None,
            )
        });
    }

    inferred_input_format(&input.path).ok_or_else(|| {
        MlPrepError::ConfigError(
            serde_yaml::Error::custom(format!(
                "Unable to infer input format for '{}'; set inputs[].format explicitly",
                input.path
            )),
            None,
        )
    })
}

pub(crate) fn read_parallelism(input_count: usize, max_threads: Option<usize>) -> Option<usize> {
    if input_count <= 1 {
        return None;
    }

    let max_threads = max_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
    });
    let effective_threads = max_threads.min(input_count);
    (effective_threads > 1).then_some(effective_threads)
}

/// Read a single [`Input`] descriptor as a [`LazyFrame`].
pub fn read_input(input: &Input) -> MlPrepResult<LazyFrame> {
    match resolved_input_format(input)? {
        InputFormat::Parquet => read_parquet(&input.path),
        InputFormat::Csv => read_csv(&input.path),
    }
}

/// Read all inputs in parallel (using rayon) and concatenate them into one
/// [`LazyFrame`].  Falls back to sequential reading when only one input is
/// given, avoiding rayon overhead for the common case.
pub fn read_all_inputs(inputs: &[Input], max_threads: Option<usize>) -> MlPrepResult<LazyFrame> {
    match inputs {
        [] => Err(MlPrepError::ConfigError(
            serde_yaml::Error::custom("No inputs specified"),
            None,
        )),
        [single] => read_input(single),
        _ => {
            let frames = if let Some(parallelism) = read_parallelism(inputs.len(), max_threads) {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(parallelism)
                    .build()
                    .map_err(|err| MlPrepError::Unknown(err.into()))?;
                let results: Vec<MlPrepResult<LazyFrame>> =
                    pool.install(|| inputs.par_iter().map(read_input).collect());
                results.into_iter().collect::<MlPrepResult<Vec<_>>>()?
            } else {
                inputs
                    .iter()
                    .map(read_input)
                    .collect::<MlPrepResult<Vec<_>>>()?
            };
            concat(frames, UnionArgs::default()).map_err(MlPrepError::PolarsError)
        }
    }
}

pub fn read_csv<P: AsRef<Path>>(path: P) -> MlPrepResult<LazyFrame> {
    LazyCsvReader::new(path)
        .finish()
        .map_err(MlPrepError::PolarsError)
}

pub fn read_parquet<P: AsRef<Path>>(path: P) -> MlPrepResult<LazyFrame> {
    LazyFrame::scan_parquet(path, Default::default()).map_err(MlPrepError::PolarsError)
}

pub fn write_parquet<P: AsRef<Path>>(df: DataFrame, path: P) -> MlPrepResult<()> {
    let file = std::fs::File::create(path).map_err(MlPrepError::IoError)?;
    ParquetWriter::new(file)
        .finish(&mut df.clone())
        .map_err(MlPrepError::PolarsError)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::Input;
    use polars::df;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_csv_io() -> MlPrepResult<()> {
        let csv_path = "test.csv";
        let csv_content = "a,b,c\n1,2,3\n4,5,6";
        fs::write(csv_path, csv_content)?;

        let lf = read_csv(csv_path)?;
        let df = lf.collect().map_err(MlPrepError::PolarsError)?;

        assert_eq!(df.shape(), (2, 3));
        assert_eq!(df.get_column_names(), vec!["a", "b", "c"]);

        fs::remove_file(csv_path)?;
        Ok(())
    }

    #[test]
    fn test_parquet_io() -> MlPrepResult<()> {
        let csv_path = "test_p.csv";
        let parquet_path = "test.parquet";
        let csv_content = "a,b,c\n1,2,3\n4,5,6";
        fs::write(csv_path, csv_content)?;

        let df = read_csv(csv_path)?
            .collect()
            .map_err(MlPrepError::PolarsError)?;
        write_parquet(df, parquet_path)?;

        let lf = read_parquet(parquet_path)?;
        let df_read = lf.collect().map_err(MlPrepError::PolarsError)?;

        assert_eq!(df_read.shape(), (2, 3));

        fs::remove_file(csv_path).map_err(MlPrepError::IoError)?;
        fs::remove_file(parquet_path).map_err(MlPrepError::IoError)?;
        Ok(())
    }

    #[test]
    fn test_read_input_respects_explicit_format() -> MlPrepResult<()> {
        let dir = tempdir().map_err(MlPrepError::IoError)?;
        let parquet_path = dir.path().join("dataset.bin");
        let df = df! { "value" => &[1_i64, 2] }.map_err(MlPrepError::PolarsError)?;
        write_parquet(df, &parquet_path)?;

        let input = Input {
            path: parquet_path.display().to_string(),
            format: Some("parquet".to_string()),
            schema: None,
            infer_rows: None,
            null_values: None,
        };

        let df = read_input(&input)?
            .collect()
            .map_err(MlPrepError::PolarsError)?;
        assert_eq!(df.height(), 2);
        Ok(())
    }

    #[test]
    fn test_read_parallelism_respects_thread_budget() {
        assert_eq!(read_parallelism(1, None), None);
        assert_eq!(read_parallelism(4, Some(1)), None);
        assert_eq!(read_parallelism(4, Some(2)), Some(2));
        assert_eq!(read_parallelism(4, Some(8)), Some(4));
    }
}
