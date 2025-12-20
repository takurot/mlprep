use crate::dsl::{Agg, GroupBy, Join, Pipeline, Sort, Step, Window, WindowOp};
use crate::io;
use anyhow::{anyhow, Result};
use polars::prelude::*;

pub fn apply_pipeline(lf: LazyFrame, pipeline: Pipeline) -> Result<LazyFrame> {
    let mut current_lf = lf;

    for step in pipeline.steps {
        current_lf = match step {
            Step::Select(s) => apply_select(current_lf, s)?,
            Step::Filter(f) => apply_filter(current_lf, f)?,
            Step::Cast(c) => apply_cast(current_lf, c)?,
            Step::Sort(s) => apply_sort(current_lf, s)?,
            Step::Join(j) => apply_join(current_lf, j)?,
            Step::GroupBy(g) => apply_groupby(current_lf, g)?,
            Step::Window(w) => apply_window(current_lf, w)?,
        };
    }

    Ok(current_lf)
}

fn apply_select(lf: LazyFrame, select: crate::dsl::Select) -> Result<LazyFrame> {
    let cols: Vec<Expr> = select.columns.iter().map(col).collect();
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

fn apply_sort(lf: LazyFrame, sort: Sort) -> Result<LazyFrame> {
    if sort.by.is_empty() {
        return Err(anyhow!("Sort requires at least one column"));
    }

    let cols: Vec<PlSmallStr> = sort.by.iter().map(|s| s.as_str().into()).collect();

    // Build descending flags - default to false (ascending) if not specified
    let descending: Vec<bool> = if sort.descending.is_empty() {
        vec![false; sort.by.len()]
    } else if sort.descending.len() == sort.by.len() {
        sort.descending
    } else {
        return Err(anyhow!(
            "descending array length ({}) must match by array length ({})",
            sort.descending.len(),
            sort.by.len()
        ));
    };

    let sort_options = SortMultipleOptions::new().with_order_descending_multi(descending);

    Ok(lf.sort(cols, sort_options))
}

fn apply_join(lf: LazyFrame, join: Join) -> Result<LazyFrame> {
    // Load the right DataFrame from path
    let right_lf = if join.right_path.ends_with(".parquet") {
        io::read_parquet(&join.right_path)?
    } else {
        io::read_csv(&join.right_path)?
    };

    // Build join keys
    let left_on: Vec<Expr> = join.left_on.iter().map(col).collect();
    let right_on: Vec<Expr> = join.right_on.iter().map(col).collect();

    // Parse join type
    let join_type = match join.how.to_lowercase().as_str() {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "outer" | "full" => JoinType::Full,
        "cross" => JoinType::Cross,
        _ => return Err(anyhow!("Unsupported join type: {}", join.how)),
    };

    Ok(lf.join(right_lf, left_on, right_on, JoinArgs::new(join_type)))
}

fn apply_groupby(lf: LazyFrame, groupby: GroupBy) -> Result<LazyFrame> {
    if groupby.by.is_empty() {
        return Err(anyhow!("GroupBy requires at least one column"));
    }

    let group_cols: Vec<Expr> = groupby.by.iter().map(col).collect();

    // Build aggregation expressions
    let agg_exprs: Result<Vec<Expr>> = groupby
        .aggs
        .into_iter()
        .map(|(col_name, agg)| build_agg_expr(&col_name, &agg))
        .collect();

    Ok(lf.group_by(group_cols).agg(agg_exprs?))
}

fn build_agg_expr(col_name: &str, agg: &Agg) -> Result<Expr> {
    let base_expr = match agg.func.to_lowercase().as_str() {
        "sum" => col(col_name).sum(),
        "mean" | "avg" => col(col_name).mean(),
        "min" => col(col_name).min(),
        "max" => col(col_name).max(),
        "count" => col(col_name).count(),
        "first" => col(col_name).first(),
        "last" => col(col_name).last(),
        "std" | "stddev" => col(col_name).std(1), // ddof=1
        "var" | "variance" => col(col_name).var(1),
        _ => return Err(anyhow!("Unsupported aggregation function: {}", agg.func)),
    };

    // Apply alias if specified
    let expr = if let Some(ref alias) = agg.alias {
        base_expr.alias(alias)
    } else {
        base_expr
    };

    Ok(expr)
}

fn apply_window(lf: LazyFrame, window: Window) -> Result<LazyFrame> {
    if window.ops.is_empty() {
        return Ok(lf);
    }

    let partition_exprs: Vec<Expr> = window.partition_by.iter().map(col).collect();

    // Build window expressions
    let window_exprs: Result<Vec<Expr>> = window
        .ops
        .iter()
        .map(|op| build_window_expr(op, &partition_exprs, &window.order_by))
        .collect();

    Ok(lf.with_columns(window_exprs?))
}

fn build_window_expr(
    op: &WindowOp,
    partition_exprs: &[Expr],
    _order_by: &Option<String>,
) -> Result<Expr> {
    let base_expr = match op.func.to_lowercase().as_str() {
        "sum" => col(&op.column).sum(),
        "mean" | "avg" => col(&op.column).mean(),
        "min" => col(&op.column).min(),
        "max" => col(&op.column).max(),
        "count" => col(&op.column).count(),
        "first" => col(&op.column).first(),
        "last" => col(&op.column).last(),
        "cumsum" => col(&op.column).cum_sum(false),
        "cummax" => col(&op.column).cum_max(false),
        "cummin" => col(&op.column).cum_min(false),
        _ => return Err(anyhow!("Unsupported window function: {}", op.func)),
    };

    // Apply over() for window partitioning
    let windowed_expr = if partition_exprs.is_empty() {
        base_expr
    } else {
        base_expr.over(partition_exprs)
    };

    Ok(windowed_expr.alias(&op.alias))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{Agg, Cast, Filter, GroupBy, Pipeline, Select, Sort, Step, Window, WindowOp};
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

    #[test]
    fn test_apply_sort_ascending() {
        let df = df! {
            "a" => [3, 1, 2],
            "b" => ["c", "a", "b"],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::Sort(Sort {
            by: vec!["a".to_string()],
            descending: vec![],
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        let a = result.column("a").unwrap().i32().unwrap();
        assert_eq!(a.get(0), Some(1));
        assert_eq!(a.get(1), Some(2));
        assert_eq!(a.get(2), Some(3));
    }

    #[test]
    fn test_apply_sort_descending() {
        let df = df! {
            "a" => [3, 1, 2],
            "b" => ["c", "a", "b"],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::Sort(Sort {
            by: vec!["a".to_string()],
            descending: vec![true],
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        let a = result.column("a").unwrap().i32().unwrap();
        assert_eq!(a.get(0), Some(3));
        assert_eq!(a.get(1), Some(2));
        assert_eq!(a.get(2), Some(1));
    }

    #[test]
    fn test_apply_sort_multi_column() {
        let df = df! {
            "a" => [1, 1, 2, 2],
            "b" => [3, 1, 4, 2],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::Sort(Sort {
            by: vec!["a".to_string(), "b".to_string()],
            descending: vec![true, false], // a desc, b asc
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        let a = result.column("a").unwrap().i32().unwrap();
        let b = result.column("b").unwrap().i32().unwrap();
        // Expected order: (2,2), (2,4), (1,1), (1,3)
        assert_eq!(a.get(0), Some(2));
        assert_eq!(b.get(0), Some(2));
        assert_eq!(a.get(1), Some(2));
        assert_eq!(b.get(1), Some(4));
        assert_eq!(a.get(2), Some(1));
        assert_eq!(b.get(2), Some(1));
    }

    #[test]
    fn test_apply_groupby_sum() {
        let df = df! {
            "category" => ["a", "b", "a", "b"],
            "value" => [10, 20, 30, 40],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::GroupBy(GroupBy {
            by: vec!["category".to_string()],
            aggs: HashMap::from([(
                "value".to_string(),
                Agg {
                    func: "sum".to_string(),
                    alias: Some("total".to_string()),
                },
            )]),
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline)
            .unwrap()
            .sort(["category"], Default::default())
            .collect()
            .unwrap();

        assert_eq!(result.height(), 2);
        let total = result.column("total").unwrap().i32().unwrap();
        // category "a" has 10+30=40, category "b" has 20+40=60
        assert_eq!(total.get(0), Some(40));
        assert_eq!(total.get(1), Some(60));
    }

    #[test]
    fn test_apply_groupby_multiple_aggs() {
        let df = df! {
            "category" => ["a", "a", "a"],
            "value" => [10, 20, 30],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::GroupBy(GroupBy {
            by: vec!["category".to_string()],
            aggs: HashMap::from([
                (
                    "value".to_string(),
                    Agg {
                        func: "mean".to_string(),
                        alias: Some("avg_value".to_string()),
                    },
                ),
                (
                    "category".to_string(),
                    Agg {
                        func: "count".to_string(),
                        alias: Some("cnt".to_string()),
                    },
                ),
            ]),
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        assert_eq!(result.height(), 1);
        let avg = result.column("avg_value").unwrap().f64().unwrap();
        let cnt = result.column("cnt").unwrap().u32().unwrap();
        assert!((avg.get(0).unwrap() - 20.0).abs() < 0.01);
        assert_eq!(cnt.get(0), Some(3));
    }

    #[test]
    fn test_apply_window_sum() {
        let df = df! {
            "category" => ["a", "a", "b", "b"],
            "value" => [10, 20, 30, 40],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::Window(Window {
            partition_by: vec!["category".to_string()],
            order_by: None,
            ops: vec![WindowOp {
                column: "value".to_string(),
                func: "sum".to_string(),
                alias: "category_total".to_string(),
            }],
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        assert_eq!(result.height(), 4);
        let cat_total = result.column("category_total").unwrap().i32().unwrap();
        // "a" rows should have total 30, "b" rows should have total 70
        assert_eq!(cat_total.get(0), Some(30));
        assert_eq!(cat_total.get(1), Some(30));
        assert_eq!(cat_total.get(2), Some(70));
        assert_eq!(cat_total.get(3), Some(70));
    }

    #[test]
    fn test_apply_window_cumsum() {
        let df = df! {
            "category" => ["a", "a", "a"],
            "value" => [10, 20, 30],
        }
        .unwrap();
        let lf = df.lazy();

        let step = Step::Window(Window {
            partition_by: vec!["category".to_string()],
            order_by: None,
            ops: vec![WindowOp {
                column: "value".to_string(),
                func: "cumsum".to_string(),
                alias: "running_sum".to_string(),
            }],
        });

        let pipeline = Pipeline { steps: vec![step] };
        let result = apply_pipeline(lf, pipeline).unwrap().collect().unwrap();

        let running_sum = result.column("running_sum").unwrap().i32().unwrap();
        assert_eq!(running_sum.get(0), Some(10));
        assert_eq!(running_sum.get(1), Some(30));
        assert_eq!(running_sum.get(2), Some(60));
    }
}
