use anyhow::Result;
use polars::prelude::*;

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

    pub fn apply_transforms(self, pipeline: crate::dsl::Pipeline) -> Result<Self> {
        let new_lf = crate::compute::apply_pipeline(self.df, pipeline)?;
        Ok(Self { df: new_lf })
    }
}
