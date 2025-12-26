use crate::errors::{MlPrepError, MlPrepResult};
use polars::prelude::*;

pub struct DataPipeline {
    df: LazyFrame,
}

impl DataPipeline {
    pub fn new(df: LazyFrame) -> Self {
        Self { df }
    }

    pub fn collect(self) -> MlPrepResult<DataFrame> {
        self.df.collect().map_err(MlPrepError::PolarsError)
    }

    pub fn get_df(&self) -> &LazyFrame {
        &self.df
    }

    pub fn apply_transforms(self, pipeline: crate::dsl::Pipeline, security_context: &crate::security::SecurityContext) -> MlPrepResult<Self> {
        let new_lf = crate::compute::apply_pipeline(self.df, pipeline, security_context)?;
        Ok(Self { df: new_lf })
    }
}
