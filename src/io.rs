use anyhow::Result;
use polars::prelude::*;
use std::path::Path;

pub fn read_csv<P: AsRef<Path>>(path: P) -> Result<LazyFrame> {
    Ok(LazyCsvReader::new(path).finish()?)
}

pub fn read_parquet<P: AsRef<Path>>(path: P) -> Result<LazyFrame> {
    Ok(LazyFrame::scan_parquet(path, Default::default())?)
}

pub fn write_parquet<P: AsRef<Path>>(df: DataFrame, path: P) -> Result<()> {
    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file).finish(&mut df.clone())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_csv_io() -> Result<()> {
        let csv_path = "test.csv";
        let csv_content = "a,b,c\n1,2,3\n4,5,6";
        fs::write(csv_path, csv_content)?;

        let lf = read_csv(csv_path)?;
        let df = lf.collect()?;

        assert_eq!(df.shape(), (2, 3));
        assert_eq!(df.get_column_names(), vec!["a", "b", "c"]);

        fs::remove_file(csv_path)?;
        Ok(())
    }

    #[test]
    fn test_parquet_io() -> Result<()> {
        let csv_path = "test_p.csv";
        let parquet_path = "test.parquet";
        let csv_content = "a,b,c\n1,2,3\n4,5,6";
        fs::write(csv_path, csv_content)?;

        let df = read_csv(csv_path)?.collect()?;
        write_parquet(df, parquet_path)?;

        let lf = read_parquet(parquet_path)?;
        let df_read = lf.collect()?;

        assert_eq!(df_read.shape(), (2, 3));

        fs::remove_file(csv_path)?;
        fs::remove_file(parquet_path)?;
        Ok(())
    }
}
