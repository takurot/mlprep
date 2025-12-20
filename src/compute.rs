use crate::dsl::{Pipeline, Step};
use anyhow::{anyhow, Result};
use polars::prelude::*;

pub fn apply_pipeline(lf: LazyFrame, pipeline: Pipeline) -> Result<LazyFrame> {
    let mut current_lf = lf;

    for step in pipeline.steps {
        current_lf = match step {
            Step::Select(s) => apply_select(current_lf, s)?,
            Step::Filter(f) => apply_filter(current_lf, f)?,
            Step::Cast(c) => apply_cast(current_lf, c)?,
        };
    }

    Ok(current_lf)
}

fn apply_select(lf: LazyFrame, select: crate::dsl::Select) -> Result<LazyFrame> {
    let cols: Vec<Expr> = select.columns.iter().map(|c| col(c)).collect();
    Ok(lf.select(cols))
}

fn apply_filter(lf: LazyFrame, filter: crate::dsl::Filter) -> Result<LazyFrame> {
    let mut ctx = polars::sql::SQLContext::new();
    ctx.register("df", lf);
    let sql = format!("SELECT * FROM df WHERE {}", filter.condition);
    ctx.execute(&sql)
        .map_err(|e| anyhow!("SQL execution failed: {}", e))
}

fn apply_cast(lf: LazyFrame, cast: crate::dsl::Cast) -> Result<LazyFrame> {
    let mut exprs = Vec::new();
    for (col_name, dtype_str) in cast.columns {
        let dtype = match dtype_str.as_str() {
            "Int64" => DataType::Int64,
            "Int32" => DataType::Int32,
            "Float64" => DataType::Float64,
            "Float32" => DataType::Float32,
            "String" | "Utf8" => DataType::String,
            "Boolean" => DataType::Boolean,
            _ => return Err(anyhow!("Unsupported data type: {}", dtype_str)),
        };
        exprs.push(col(col_name.as_str()).cast(dtype));
    }
    // We need to match/replace existing columns. `with_columns` does that.
    Ok(lf.with_columns(exprs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{Cast, Filter, Pipeline, Select, Step};
    use polars::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_apply_select() {
        let df = df! {
            "a" => [1, 2, 3],
            "b" => [4, 5, 6],
            "c" => [7, 8, 9],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::Select(Select {
            columns: vec!["a".to_string(), "c".to_string()],
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        assert_eq!(result.get_column_names(), &["a", "c"]);
    }

    #[test]
    fn test_apply_filter() {
        let df = df! {
            "a" => [1, 10, 20],
            "b" => [4, 5, 6],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::Filter(Filter {
            condition: "a > 5".to_string(),
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        assert_eq!(result.height(), 2);
        let a = result.column("a").unwrap().i32().unwrap();
        assert_eq!(a.get(0), Some(10));
        assert_eq!(a.get(1), Some(20));
    }

    #[test]
    fn test_apply_cast() {
        let df = df! {
            "a" => [1, 2, 3],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::Cast(Cast {
            columns: HashMap::from([("a".to_string(), "Float64".to_string())]),
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        assert_eq!(result.column("a").unwrap().dtype(), &DataType::Float64);
    }
}
