//! Feature Engineering module
//!
//! Implements fit/transform pattern for reproducible feature generation.
//! Supports scaling (MinMax, Standard) and encoding (OneHot, Count).

use anyhow::{anyhow, Result};
use polars::prelude::*;
use polars::prelude::UniqueKeepStrategy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// Feature transformation types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureTransform {
    MinMaxScale,
    StandardScale,
    OneHotEncode,
    CountEncode,
}

/// Specification for a single feature transformation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureSpec {
    pub column: String,
    pub transform: FeatureTransform,
    #[serde(default)]
    pub alias: Option<String>,
}

/// Configuration for feature engineering pipeline
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureConfig {
    pub features: Vec<FeatureSpec>,
}

/// Statistics for MinMax scaling
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MinMaxStats {
    pub min: f64,
    pub max: f64,
}

/// Statistics for Standard scaling
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StandardStats {
    pub mean: f64,
    pub std: f64,
}

/// Vocabulary for OneHot encoding
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OneHotVocab {
    pub categories: Vec<String>,
}

/// Frequency counts for Count encoding
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CountStats {
    pub counts: HashMap<String, u64>,
    pub total: u64,
}

/// State for a single feature (stores fit statistics)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FeatureStateEntry {
    MinMax {
        column: String,
        stats: MinMaxStats,
    },
    Standard {
        column: String,
        stats: StandardStats,
    },
    OneHot {
        column: String,
        vocab: OneHotVocab,
    },
    Count {
        column: String,
        stats: CountStats,
    },
}

/// Complete feature state for persistence
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct FeatureState {
    pub entries: Vec<FeatureStateEntry>,
}

impl FeatureState {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Save feature state to JSON file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = File::create(path.as_ref())
            .map_err(|e| anyhow!("Failed to create feature state file: {}", e))?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)
            .map_err(|e| anyhow!("Failed to write feature state: {}", e))?;
        Ok(())
    }

    /// Load feature state from JSON file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())
            .map_err(|e| anyhow!("Failed to open feature state file: {}", e))?;
        let reader = BufReader::new(file);
        let state: FeatureState = serde_json::from_reader(reader)
            .map_err(|e| anyhow!("Failed to parse feature state: {}", e))?;
        Ok(state)
    }

    pub fn add_entry(&mut self, entry: FeatureStateEntry) {
        self.entries.push(entry);
    }

    pub fn get_entry(
        &self,
        column: &str,
        transform: &FeatureTransform,
    ) -> Option<&FeatureStateEntry> {
        self.entries.iter().find(|e| match (e, transform) {
            (FeatureStateEntry::MinMax { column: c, .. }, FeatureTransform::MinMaxScale) => {
                c == column
            }
            (FeatureStateEntry::Standard { column: c, .. }, FeatureTransform::StandardScale) => {
                c == column
            }
            (FeatureStateEntry::OneHot { column: c, .. }, FeatureTransform::OneHotEncode) => {
                c == column
            }
            (FeatureStateEntry::Count { column: c, .. }, FeatureTransform::CountEncode) => {
                c == column
            }
            _ => false,
        })
    }
}

/// Fit MinMax scaler on a column
pub fn fit_minmax(df: &DataFrame, column: &str) -> Result<MinMaxStats> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let float_col = col
        .cast(&DataType::Float64)
        .map_err(|e| anyhow!("Cannot cast column '{}' to float: {}", column, e))?;

    let ca = float_col
        .f64()
        .map_err(|e| anyhow!("Failed to get f64 chunked array: {}", e))?;

    let min = ca
        .min()
        .ok_or_else(|| anyhow!("Column '{}' has no values", column))?;
    let max = ca
        .max()
        .ok_or_else(|| anyhow!("Column '{}' has no values", column))?;

    Ok(MinMaxStats { min, max })
}

/// Transform column using MinMax scaling
pub fn transform_minmax(
    df: &DataFrame,
    column: &str,
    stats: &MinMaxStats,
    alias: Option<&str>,
) -> Result<DataFrame> {
    let range = stats.max - stats.min;

    // Avoid division by zero for constant columns
    let scale_expr = if range.abs() < f64::EPSILON {
        lit(0.5) // Constant column maps to midpoint
    } else {
        (col(column).cast(DataType::Float64) - lit(stats.min)) / lit(range)
    };

    let output_name = alias.unwrap_or(column);
    let result = df
        .clone()
        .lazy()
        .with_column(scale_expr.alias(output_name))
        .collect()
        .map_err(|e| anyhow!("Failed to apply MinMax transform: {}", e))?;

    Ok(result)
}

/// Fit Standard scaler on a column
pub fn fit_standard(df: &DataFrame, column: &str) -> Result<StandardStats> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let float_col = col
        .cast(&DataType::Float64)
        .map_err(|e| anyhow!("Cannot cast column '{}' to float: {}", column, e))?;

    let ca = float_col
        .f64()
        .map_err(|e| anyhow!("Failed to get f64 chunked array: {}", e))?;

    let mean = ca
        .mean()
        .ok_or_else(|| anyhow!("Column '{}' has no values", column))?;
    let std = ca
        .std(1)
        .ok_or_else(|| anyhow!("Cannot compute std for column '{}'", column))?;

    Ok(StandardStats { mean, std })
}

/// Transform column using Standard scaling (z-score)
pub fn transform_standard(
    df: &DataFrame,
    column: &str,
    stats: &StandardStats,
    alias: Option<&str>,
) -> Result<DataFrame> {
    // Avoid division by zero for constant columns
    let scale_expr = if stats.std.abs() < f64::EPSILON {
        lit(0.0) // Constant column maps to 0
    } else {
        (col(column).cast(DataType::Float64) - lit(stats.mean)) / lit(stats.std)
    };

    let output_name = alias.unwrap_or(column);
    let result = df
        .clone()
        .lazy()
        .with_column(scale_expr.alias(output_name))
        .collect()
        .map_err(|e| anyhow!("Failed to apply Standard transform: {}", e))?;

    Ok(result)
}

/// Fit OneHot encoder on a column
pub fn fit_onehot(df: &DataFrame, column: &str) -> Result<OneHotVocab> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let str_col = col
        .str()
        .map_err(|e| anyhow!("Column '{}' is not a string type: {}", column, e))?;

    let mut categories: Vec<String> = str_col
        .into_iter()
        .filter_map(|opt| opt.map(|s| s.to_string()))
        .collect();

    categories.sort();
    categories.dedup();

    Ok(OneHotVocab { categories })
}

/// Transform column using OneHot encoding
pub fn transform_onehot(
    df: &DataFrame,
    column: &str,
    vocab: &OneHotVocab,
    _alias: Option<&str>,
) -> Result<DataFrame> {
    let mut result = df.clone();

    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let str_col = col
        .str()
        .map_err(|e| anyhow!("Column '{}' is not a string type: {}", column, e))?;

    for category in &vocab.categories {
        let col_name = format!("{}_{}", column, category);
        let mut values: Vec<i32> = Vec::with_capacity(str_col.len());

        for opt_val in str_col.into_iter() {
            match opt_val {
                Some(val) if val == category => values.push(1),
                _ => values.push(0),
            }
        }

        let series = Series::new(col_name.into(), values);
        result = result
            .hstack(&[series.into()])
            .map_err(|e| anyhow!("Failed to add one-hot column: {}", e))?;
    }

    Ok(result)
}

/// Fit Count encoder on a column
pub fn fit_count(df: &DataFrame, column: &str) -> Result<CountStats> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let str_col = col
        .str()
        .map_err(|e| anyhow!("Column '{}' is not a string type: {}", column, e))?;

    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut total: u64 = 0;

    for val in str_col.into_iter().flatten() {
        *counts.entry(val.to_string()).or_insert(0) += 1;
        total += 1;
    }

    Ok(CountStats { counts, total })
}

/// Transform column using Count encoding (frequency)
pub fn transform_count(
    df: &DataFrame,
    column: &str,
    stats: &CountStats,
    alias: Option<&str>,
) -> Result<DataFrame> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let str_col = col
        .str()
        .map_err(|e| anyhow!("Column '{}' is not a string type: {}", column, e))?;

    let mut values: Vec<f64> = Vec::with_capacity(str_col.len());

    for opt_val in str_col.into_iter() {
        match opt_val {
            Some(val) => {
                let count = *stats.counts.get(val).unwrap_or(&0);
                // Normalize by total to get frequency ratio
                let freq = if stats.total > 0 {
                    count as f64 / stats.total as f64
                } else {
                    0.0
                };
                values.push(freq);
            }
            None => values.push(0.0),
        }
    }

    let output_name = alias.unwrap_or(column);
    let series = Series::new(output_name.into(), values);

    let mut result = df.clone();
    result = result
        .with_column(series)
        .map_err(|e| anyhow!("Failed to add count-encoded column: {}", e))?
        .clone();

    Ok(result)
}

/// Fit all features in config and return combined state
pub fn fit_features(df: &DataFrame, config: &FeatureConfig) -> Result<FeatureState> {
    let mut state = FeatureState::new();

    for spec in &config.features {
        let entry = match spec.transform {
            FeatureTransform::MinMaxScale => {
                let stats = fit_minmax(df, &spec.column)?;
                FeatureStateEntry::MinMax {
                    column: spec.column.clone(),
                    stats,
                }
            }
            FeatureTransform::StandardScale => {
                let stats = fit_standard(df, &spec.column)?;
                FeatureStateEntry::Standard {
                    column: spec.column.clone(),
                    stats,
                }
            }
            FeatureTransform::OneHotEncode => {
                let vocab = fit_onehot(df, &spec.column)?;
                FeatureStateEntry::OneHot {
                    column: spec.column.clone(),
                    vocab,
                }
            }
            FeatureTransform::CountEncode => {
                let stats = fit_count(df, &spec.column)?;
                FeatureStateEntry::Count {
                    column: spec.column.clone(),
                    stats,
                }
            }
        };
        state.add_entry(entry);
    }

    Ok(state)
}

/// Transform all features using fitted state
pub fn transform_features(
    df: &DataFrame,
    config: &FeatureConfig,
    state: &FeatureState,
) -> Result<DataFrame> {
    let mut result = df.clone();

    for spec in &config.features {
        let entry = state
            .get_entry(&spec.column, &spec.transform)
            .ok_or_else(|| {
                anyhow!(
                    "No fitted state for column '{}' with transform {:?}",
                    spec.column,
                    spec.transform
                )
            })?;

        result = match entry {
            FeatureStateEntry::MinMax { stats, .. } => {
                transform_minmax(&result, &spec.column, stats, spec.alias.as_deref())?
            }
            FeatureStateEntry::Standard { stats, .. } => {
                transform_standard(&result, &spec.column, stats, spec.alias.as_deref())?
            }
            FeatureStateEntry::OneHot { vocab, .. } => {
                transform_onehot(&result, &spec.column, vocab, spec.alias.as_deref())?
            }
            FeatureStateEntry::Count { stats, .. } => {
                transform_count(&result, &spec.column, stats, spec.alias.as_deref())?
            }
        };
    }

    Ok(result)
}

/// Fit feature statistics lazily using a `LazyFrame`.
pub fn fit_features_lazy(
    lf: LazyFrame,
    config: &FeatureConfig,
    streaming: bool,
) -> Result<FeatureState> {
    let mut state = FeatureState::new();

    // Collect numeric stats together to minimize scans.
    let mut numeric_exprs = Vec::new();
    for spec in &config.features {
        match spec.transform {
            FeatureTransform::MinMaxScale => {
                numeric_exprs.push(
                    col(&spec.column)
                        .cast(DataType::Float64)
                        .min()
                        .alias(format!("{}__min", spec.column)),
                );
                numeric_exprs.push(
                    col(&spec.column)
                        .cast(DataType::Float64)
                        .max()
                        .alias(format!("{}__max", spec.column)),
                );
            }
            FeatureTransform::StandardScale => {
                numeric_exprs.push(
                    col(&spec.column)
                        .cast(DataType::Float64)
                        .mean()
                        .alias(format!("{}__mean", spec.column)),
                );
                numeric_exprs.push(
                    col(&spec.column)
                        .cast(DataType::Float64)
                        .std(1)
                        .alias(format!("{}__std", spec.column)),
                );
            }
            _ => {}
        }
    }

    let numeric_stats = if numeric_exprs.is_empty() {
        None
    } else {
        Some(
            lf.clone()
                .with_streaming(streaming)
                .select(numeric_exprs)
                .collect()
                .map_err(|e| anyhow!("Failed to collect numeric feature stats: {}", e))?,
        )
    };

    for spec in &config.features {
        match spec.transform {
            FeatureTransform::MinMaxScale => {
                let stats_df = numeric_stats.as_ref().ok_or_else(|| {
                    anyhow!("Numeric stats unavailable for MinMax transform on {}", spec.column)
                })?;
                let min_col = format!("{}__min", spec.column);
                let max_col = format!("{}__max", spec.column);
                let min = stats_df
                    .column(&min_col)?
                    .f64()?
                    .get(0)
                    .ok_or_else(|| anyhow!("Missing min value for {}", spec.column))?;
                let max = stats_df
                    .column(&max_col)?
                    .f64()?
                    .get(0)
                    .ok_or_else(|| anyhow!("Missing max value for {}", spec.column))?;
                state.add_entry(FeatureStateEntry::MinMax {
                    column: spec.column.clone(),
                    stats: MinMaxStats { min, max },
                });
            }
            FeatureTransform::StandardScale => {
                let stats_df = numeric_stats.as_ref().ok_or_else(|| {
                    anyhow!(
                        "Numeric stats unavailable for Standard transform on {}",
                        spec.column
                    )
                })?;
                let mean_col = format!("{}__mean", spec.column);
                let std_col = format!("{}__std", spec.column);
                let mean = stats_df
                    .column(&mean_col)?
                    .f64()?
                    .get(0)
                    .ok_or_else(|| anyhow!("Missing mean value for {}", spec.column))?;
                let std = stats_df
                    .column(&std_col)?
                    .f64()?
                    .get(0)
                    .ok_or_else(|| anyhow!("Missing std value for {}", spec.column))?;
                state.add_entry(FeatureStateEntry::Standard {
                    column: spec.column.clone(),
                    stats: StandardStats { mean, std },
                });
            }
            FeatureTransform::OneHotEncode => {
                let vocab_df = lf
                    .clone()
                    .with_streaming(streaming)
                    .select([col(&spec.column)
                        .cast(DataType::String)
                        .alias("value")])
                    .unique(None, UniqueKeepStrategy::First)
                    .collect()
                    .map_err(|e| anyhow!("Failed to collect one-hot vocab: {}", e))?;

                let categories = vocab_df
                    .column("value")?
                    .str()?
                    .into_iter()
                    .flatten()
                    .map(|s| s.to_string())
                    .collect();

                state.add_entry(FeatureStateEntry::OneHot {
                    column: spec.column.clone(),
                    vocab: OneHotVocab { categories },
                });
            }
            FeatureTransform::CountEncode => {
                let counts_df = lf
                    .clone()
                    .with_streaming(streaming)
                    .select([col(&spec.column)
                        .cast(DataType::String)
                        .alias("value")])
                    .group_by([col("value")])
                    .agg([col("value").count().alias("count")])
                    .collect()
                    .map_err(|e| anyhow!("Failed to collect count stats: {}", e))?;

                let counts_series = counts_df.column("count")?.u32()?;
                let values_series = counts_df.column("value")?.str()?;

                let mut counts = HashMap::new();
                let mut total: u64 = 0;
                for (value_opt, count_opt) in values_series.into_iter().zip(counts_series.into_iter())
                {
                    if let Some(count) = count_opt {
                        total += count as u64;
                        if let Some(value) = value_opt {
                            counts.insert(value.to_string(), count as u64);
                        }
                    }
                }

                state.add_entry(FeatureStateEntry::Count {
                    column: spec.column.clone(),
                    stats: CountStats { counts, total },
                });
            }
        }
    }

    Ok(state)
}

/// Build lazy expressions for a feature transform using fitted state.
pub fn exprs_from_state(
    spec: &FeatureSpec,
    entry: &FeatureStateEntry,
) -> Result<Vec<Expr>> {
    match (spec.transform.clone(), entry) {
        (FeatureTransform::MinMaxScale, FeatureStateEntry::MinMax { stats, .. }) => {
            let base = col(&spec.column).cast(DataType::Float64);
            let range = stats.max - stats.min;
            let scaled = if range.abs() < f64::EPSILON {
                lit(0.5)
            } else {
                (base - lit(stats.min)) / lit(range)
            };
            let name = spec.alias.as_deref().unwrap_or(&spec.column);
            Ok(vec![scaled.alias(name)])
        }
        (FeatureTransform::StandardScale, FeatureStateEntry::Standard { stats, .. }) => {
            let base = col(&spec.column).cast(DataType::Float64);
            let scaled = if stats.std.abs() < f64::EPSILON {
                lit(0.0)
            } else {
                (base - lit(stats.mean)) / lit(stats.std)
            };
            let name = spec.alias.as_deref().unwrap_or(&spec.column);
            Ok(vec![scaled.alias(name)])
        }
        (FeatureTransform::OneHotEncode, FeatureStateEntry::OneHot { vocab, .. }) => {
            let mut exprs = Vec::new();
            let base = col(&spec.column).cast(DataType::String);
            for category in &vocab.categories {
                let col_name = format!(
                    "{}_{}",
                    spec.alias.as_deref().unwrap_or(&spec.column),
                    category
                );
                let expr = when(base.clone().eq(lit(category.as_str())))
                    .then(lit(1i32))
                    .otherwise(lit(0i32))
                    .alias(col_name);
                exprs.push(expr);
            }
            Ok(exprs)
        }
        (FeatureTransform::CountEncode, FeatureStateEntry::Count { stats, .. }) => {
            let output_name = spec.alias.clone().unwrap_or_else(|| spec.column.clone());
            let base = col(&spec.column).cast(DataType::String);
            let mut expr = lit(0.0);
            for (value, count) in &stats.counts {
                let freq = if stats.total == 0 {
                    0.0
                } else {
                    *count as f64 / stats.total as f64
                };
                expr = when(base.clone().eq(lit(value.as_str())))
                    .then(lit(freq))
                    .otherwise(expr);
            }
            Ok(vec![expr.alias(output_name)])
        }
        _ => Err(anyhow!(
            "State {:?} does not match requested transform {:?}",
            entry,
            spec.transform
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ============================================================================
    // MinMax Scaler Tests
    // ============================================================================

    #[test]
    fn test_fit_minmax() {
        let df = df! {
            "value" => &[10.0, 20.0, 30.0, 40.0, 50.0]
        }
        .unwrap();

        let stats = fit_minmax(&df, "value").unwrap();
        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 50.0);
    }

    #[test]
    fn test_transform_minmax() {
        let df = df! {
            "value" => &[10.0, 20.0, 30.0, 40.0, 50.0]
        }
        .unwrap();

        let stats = MinMaxStats {
            min: 10.0,
            max: 50.0,
        };
        let result = transform_minmax(&df, "value", &stats, None).unwrap();

        let scaled = result.column("value").unwrap().f64().unwrap();
        assert!((scaled.get(0).unwrap() - 0.0).abs() < 1e-10);
        assert!((scaled.get(2).unwrap() - 0.5).abs() < 1e-10);
        assert!((scaled.get(4).unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_minmax_constant_column() {
        let df = df! {
            "value" => &[5.0, 5.0, 5.0]
        }
        .unwrap();

        let stats = fit_minmax(&df, "value").unwrap();
        let result = transform_minmax(&df, "value", &stats, None).unwrap();

        let scaled = result.column("value").unwrap().f64().unwrap();
        // Constant column should map to 0.5
        assert!((scaled.get(0).unwrap() - 0.5).abs() < 1e-10);
    }

    // ============================================================================
    // Standard Scaler Tests
    // ============================================================================

    #[test]
    fn test_fit_standard() {
        // Use simple data: [0, 10] => mean=5, std=7.07... (sample std)
        let df = df! {
            "value" => &[0.0, 10.0]
        }
        .unwrap();

        let stats = fit_standard(&df, "value").unwrap();
        assert!((stats.mean - 5.0).abs() < 1e-10);
        // Sample std of [0, 10] with ddof=1: sqrt(50) ≈ 7.07
        assert!((stats.std - 7.0710678118654752).abs() < 0.01);
    }

    #[test]
    fn test_transform_standard() {
        let df = df! {
            "value" => &[0.0, 5.0, 10.0]
        }
        .unwrap();

        let stats = StandardStats {
            mean: 5.0,
            std: 5.0,
        };
        let result = transform_standard(&df, "value", &stats, None).unwrap();

        let scaled = result.column("value").unwrap().f64().unwrap();
        assert!((scaled.get(0).unwrap() - (-1.0)).abs() < 1e-10);
        assert!((scaled.get(1).unwrap() - 0.0).abs() < 1e-10);
        assert!((scaled.get(2).unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_standard_constant_column() {
        let df = df! {
            "value" => &[5.0, 5.0, 5.0]
        }
        .unwrap();

        let stats = fit_standard(&df, "value").unwrap();
        let result = transform_standard(&df, "value", &stats, None).unwrap();

        let scaled = result.column("value").unwrap().f64().unwrap();
        // Constant column (std=0) should map to 0
        assert!((scaled.get(0).unwrap() - 0.0).abs() < 1e-10);
    }

    // ============================================================================
    // OneHot Encoder Tests
    // ============================================================================

    #[test]
    fn test_fit_onehot() {
        let df = df! {
            "category" => &["cat", "dog", "bird", "cat", "dog"]
        }
        .unwrap();

        let vocab = fit_onehot(&df, "category").unwrap();
        assert_eq!(vocab.categories.len(), 3);
        assert!(vocab.categories.contains(&"cat".to_string()));
        assert!(vocab.categories.contains(&"dog".to_string()));
        assert!(vocab.categories.contains(&"bird".to_string()));
    }

    #[test]
    fn test_transform_onehot() {
        let df = df! {
            "category" => &["cat", "dog", "bird"]
        }
        .unwrap();

        let vocab = OneHotVocab {
            categories: vec!["bird".to_string(), "cat".to_string(), "dog".to_string()],
        };
        let result = transform_onehot(&df, "category", &vocab, None).unwrap();

        // Check that new columns exist
        assert!(result.column("category_bird").is_ok());
        assert!(result.column("category_cat").is_ok());
        assert!(result.column("category_dog").is_ok());

        // Check values
        let cat_col = result.column("category_cat").unwrap().i32().unwrap();
        assert_eq!(cat_col.get(0), Some(1)); // "cat"
        assert_eq!(cat_col.get(1), Some(0)); // "dog"
        assert_eq!(cat_col.get(2), Some(0)); // "bird"
    }

    // ============================================================================
    // Count Encoder Tests
    // ============================================================================

    #[test]
    fn test_fit_count() {
        let df = df! {
            "category" => &["a", "b", "a", "a", "b", "c"]
        }
        .unwrap();

        let stats = fit_count(&df, "category").unwrap();
        assert_eq!(stats.total, 6);
        assert_eq!(*stats.counts.get("a").unwrap(), 3);
        assert_eq!(*stats.counts.get("b").unwrap(), 2);
        assert_eq!(*stats.counts.get("c").unwrap(), 1);
    }

    #[test]
    fn test_transform_count() {
        let df = df! {
            "category" => &["a", "b", "c"]
        }
        .unwrap();

        let mut counts = HashMap::new();
        counts.insert("a".to_string(), 3);
        counts.insert("b".to_string(), 2);
        counts.insert("c".to_string(), 1);
        let stats = CountStats { counts, total: 6 };

        let result = transform_count(&df, "category", &stats, None).unwrap();

        let encoded = result.column("category").unwrap().f64().unwrap();
        assert!((encoded.get(0).unwrap() - 0.5).abs() < 1e-10); // 3/6
        assert!((encoded.get(1).unwrap() - (2.0 / 6.0)).abs() < 1e-10); // 2/6
        assert!((encoded.get(2).unwrap() - (1.0 / 6.0)).abs() < 1e-10); // 1/6
    }

    #[test]
    fn test_count_unknown_category() {
        let df = df! {
            "category" => &["a", "unknown"]
        }
        .unwrap();

        let mut counts = HashMap::new();
        counts.insert("a".to_string(), 3);
        let stats = CountStats { counts, total: 3 };

        let result = transform_count(&df, "category", &stats, None).unwrap();

        let encoded = result.column("category").unwrap().f64().unwrap();
        assert!((encoded.get(0).unwrap() - 1.0).abs() < 1e-10); // 3/3
        assert!((encoded.get(1).unwrap() - 0.0).abs() < 1e-10); // unknown = 0
    }

    // ============================================================================
    // Feature State Persistence Tests
    // ============================================================================

    #[test]
    fn test_feature_state_save_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("feature_state.json");

        let mut state = FeatureState::new();
        state.add_entry(FeatureStateEntry::MinMax {
            column: "value".to_string(),
            stats: MinMaxStats {
                min: 0.0,
                max: 100.0,
            },
        });
        state.add_entry(FeatureStateEntry::Standard {
            column: "score".to_string(),
            stats: StandardStats {
                mean: 50.0,
                std: 10.0,
            },
        });

        state.save(&path).unwrap();
        let loaded = FeatureState::load(&path).unwrap();

        assert_eq!(state, loaded);
    }

    // ============================================================================
    // Fit/Transform Integration Tests
    // ============================================================================

    #[test]
    fn test_fit_features() {
        let df = df! {
            "value" => &[10.0, 20.0, 30.0],
            "category" => &["a", "b", "a"]
        }
        .unwrap();

        let config = FeatureConfig {
            features: vec![
                FeatureSpec {
                    column: "value".to_string(),
                    transform: FeatureTransform::MinMaxScale,
                    alias: None,
                },
                FeatureSpec {
                    column: "category".to_string(),
                    transform: FeatureTransform::CountEncode,
                    alias: None,
                },
            ],
        };

        let state = fit_features(&df, &config).unwrap();
        assert_eq!(state.entries.len(), 2);
    }

    #[test]
    fn test_transform_features() {
        let train_df = df! {
            "value" => &[0.0, 50.0, 100.0],
            "category" => &["a", "b", "a"]
        }
        .unwrap();

        let test_df = df! {
            "value" => &[25.0, 75.0],
            "category" => &["a", "b"]
        }
        .unwrap();

        let config = FeatureConfig {
            features: vec![FeatureSpec {
                column: "value".to_string(),
                transform: FeatureTransform::MinMaxScale,
                alias: None,
            }],
        };

        let state = fit_features(&train_df, &config).unwrap();
        let result = transform_features(&test_df, &config, &state).unwrap();

        let scaled = result.column("value").unwrap().f64().unwrap();
        assert!((scaled.get(0).unwrap() - 0.25).abs() < 1e-10);
        assert!((scaled.get(1).unwrap() - 0.75).abs() < 1e-10);
    }

    #[test]
    fn test_train_test_consistency() {
        // Simulate train/test split scenario
        let train_df = df! {
            "age" => &[20, 30, 40, 50, 60],
            "city" => &["NYC", "LA", "NYC", "LA", "NYC"]
        }
        .unwrap();

        let test_df = df! {
            "age" => &[25, 55],
            "city" => &["NYC", "LA"]
        }
        .unwrap();

        let config = FeatureConfig {
            features: vec![
                FeatureSpec {
                    column: "age".to_string(),
                    transform: FeatureTransform::StandardScale,
                    alias: Some("age_scaled".to_string()),
                },
                FeatureSpec {
                    column: "city".to_string(),
                    transform: FeatureTransform::OneHotEncode,
                    alias: None,
                },
            ],
        };

        // Fit on train, transform both
        let state = fit_features(&train_df, &config).unwrap();
        let train_result = transform_features(&train_df, &config, &state).unwrap();
        let test_result = transform_features(&test_df, &config, &state).unwrap();

        // Verify columns exist
        assert!(train_result.column("age_scaled").is_ok());
        assert!(train_result.column("city_LA").is_ok());
        assert!(train_result.column("city_NYC").is_ok());

        assert!(test_result.column("age_scaled").is_ok());
        assert!(test_result.column("city_LA").is_ok());
        assert!(test_result.column("city_NYC").is_ok());
    }
}
