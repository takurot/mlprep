use polars::prelude::*;
use anyhow::Result;

pub struct DataPipeline {
    df: LazyFrame,
}

impl DataPipeline {
    pub fn new(df: LazyFrame) -> Self {
        Self { df }
    }

    pub fn collect(self) -> Result<DataFrame> {
        Ok(self.df.collect()?)
    }

    pub fn get_df(&self) -> &LazyFrame {
        &self.df
    }
}
