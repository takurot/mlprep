use crate::errors::{MlPrepError, MlPrepResult};
use polars::prelude::*;
use std::path::Path;

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
    use std::fs;

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
}
