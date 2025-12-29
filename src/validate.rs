//! Validation engine for data quality checks
//!
//! Implements NotNull, Unique, Range, Regex, and Enum checks with
//! strict, warn, and quarantine execution modes.

use crate::dsl::{CheckConfig, ColumnCheck, ValidationMode};
use anyhow::{anyhow, Result};
use polars::prelude::*;

/// Represents a single validation violation
#[derive(Debug, Clone, PartialEq)]
pub struct Violation {
    pub column: String,
    pub check_type: String,
    pub message: String,
    pub count: usize,
}

/// Result of validation run
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub passed: bool,
    pub violations: Vec<Violation>,
}

/// Report containing all validation results
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub results: Vec<ValidationResult>,
    pub total_violations: usize,
    pub passed: bool,
}

impl ValidationReport {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
            total_violations: 0,
            passed: true,
        }
    }

    pub fn add_result(&mut self, result: ValidationResult) {
        if !result.passed {
            self.passed = false;
            self.total_violations += result.violations.iter().map(|v| v.count).sum::<usize>();
        }
        self.results.push(result);
    }
}

impl Default for ValidationReport {
    fn default() -> Self {
        Self::new()
    }
}

fn check_label(check: &ColumnCheck) -> String {
    format!("{}:{}", check.name, check_label_suffix(check))
}

fn check_label_suffix(check: &ColumnCheck) -> &'static str {
    if check.not_null {
        "not_null"
    } else if check.unique {
        "unique"
    } else if check.range.is_some() {
        "range"
    } else if check.regex.is_some() {
        "regex"
    } else if check.allowed_values.is_some() {
        "enum"
    } else {
        "unknown"
    }
}

/// Build a violation expression for a single column check.
/// The expression evaluates to `true` for rows that violate the check.
pub fn build_violation_expr(check: &ColumnCheck) -> Result<Expr> {
    let mut parts: Vec<Expr> = Vec::new();

    if check.not_null {
        parts.push(col(&check.name).is_null());
    }

    if check.unique {
        let dup_mask = col(&check.name)
            .count()
            .over([col(&check.name)])
            .gt(lit(1u32));
        parts.push(dup_mask);
    }

    if let Some((min, max)) = check.range {
        let col_expr = col(&check.name).cast(DataType::Float64);
        parts.push(col_expr.clone().lt(lit(min)).or(col_expr.gt(lit(max))));
    }

    if let Some(ref pattern) = check.regex {
        // Validate regex upfront for early erroring
        regex::Regex::new(pattern)?;
        let regex_miss = col(&check.name)
            .cast(DataType::String)
            .str()
            .contains(lit(pattern.clone()), false)
            .not()
            .fill_null(false);
        parts.push(regex_miss);
    }

    if let Some(ref allowed) = check.allowed_values {
        let series = Series::new("allowed".into(), allowed.clone());
        let not_allowed = col(&check.name)
            .cast(DataType::String)
            .is_in(lit(series))
            .not()
            .fill_null(false);
        parts.push(not_allowed);
    }

    if parts.is_empty() {
        // No-op check, never matches violations
        return Ok(lit(false));
    }

    let mut iter = parts.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| anyhow!("Empty validation expression"))?;
    Ok(iter.fold(first, |acc, expr| acc.or(expr)))
}

/// Build a combined violation mask for all column checks.
/// Returns None when there are no checks to evaluate.
pub fn violation_mask_expr(config: &CheckConfig) -> Result<Option<Expr>> {
    let mut exprs = Vec::new();
    for check in &config.columns {
        exprs.push(build_violation_expr(check)?);
    }

    if exprs.is_empty() {
        return Ok(None);
    }

    let mut iter = exprs.into_iter();
    let first = iter.next().unwrap_or_else(|| lit(false));
    Ok(Some(iter.fold(first, |acc, expr| acc.or(expr))))
}

fn violation_from_count(check: &ColumnCheck, count: usize) -> Option<Violation> {
    if count == 0 {
        return None;
    }

    let message = if check.not_null {
        format!("Column '{}' has {} null values", check.name, count)
    } else if check.unique {
        format!("Column '{}' has {} duplicate values", check.name, count)
    } else if let Some((min, max)) = check.range {
        format!(
            "Column '{}' has {} values outside range [{}, {}]",
            check.name, count, min, max
        )
    } else if let Some(ref pattern) = check.regex {
        format!(
            "Column '{}' has {} values not matching pattern '{}'",
            check.name, count, pattern
        )
    } else if let Some(ref allowed) = check.allowed_values {
        format!(
            "Column '{}' has {} values not in allowed set {:?}",
            check.name, count, allowed
        )
    } else {
        format!("Column '{}' failed validation {} times", check.name, count)
    };

    Some(Violation {
        column: check.name.clone(),
        check_type: check_label_suffix(check).to_string(),
        message,
        count,
    })
}

/// Summarize violations lazily by aggregating violation counts per check.
pub fn summarize_violations_lazy(
    lf: LazyFrame,
    config: &CheckConfig,
    streaming: bool,
) -> Result<ValidationReport> {
    let mut agg_exprs: Vec<Expr> = Vec::new();
    for (idx, check) in config.columns.iter().enumerate() {
        let mask_expr = build_violation_expr(check)?;
        let alias = format!("check{}_{}", idx, check_label(check));
        agg_exprs.push(mask_expr.cast(DataType::UInt64).sum().alias(&alias));
    }

    if agg_exprs.is_empty() {
        return Ok(ValidationReport::new());
    }

    let counts_df = lf
        .with_streaming(streaming)
        .select(agg_exprs)
        .collect()
        .map_err(|e| anyhow!("Failed to collect validation summary: {}", e))?;

    let mut report = ValidationReport::new();
    for (idx, check) in config.columns.iter().enumerate() {
        let col_name = format!("check{}_{}", idx, check_label(check));
        let count = counts_df
            .column(&col_name)
            .ok()
            .and_then(|c| c.u64().ok())
            .and_then(|ca| ca.get(0))
            .unwrap_or(0) as usize;

        let violation = violation_from_count(check, count);
        let passed = violation.is_none();
        report.add_result(ValidationResult {
            passed,
            violations: violation.into_iter().collect(),
        });
    }

    Ok(report)
}

/// Validate that a column has no null values
pub fn validate_not_null(df: &DataFrame, column: &str) -> Result<ValidationResult> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;
    let null_count = col.null_count();

    if null_count == 0 {
        Ok(ValidationResult {
            passed: true,
            violations: vec![],
        })
    } else {
        Ok(ValidationResult {
            passed: false,
            violations: vec![Violation {
                column: column.to_string(),
                check_type: "not_null".to_string(),
                message: format!("Column '{}' has {} null values", column, null_count),
                count: null_count,
            }],
        })
    }
}

/// Validate that a column has unique values
pub fn validate_unique(df: &DataFrame, column: &str) -> Result<ValidationResult> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let total = col.len();
    let unique = col
        .n_unique()
        .map_err(|e| anyhow!("Failed to count unique values: {}", e))?;
    let duplicates = total - unique;

    if duplicates == 0 {
        Ok(ValidationResult {
            passed: true,
            violations: vec![],
        })
    } else {
        Ok(ValidationResult {
            passed: false,
            violations: vec![Violation {
                column: column.to_string(),
                check_type: "unique".to_string(),
                message: format!(
                    "Column '{}' has {} duplicate values ({} total, {} unique)",
                    column, duplicates, total, unique
                ),
                count: duplicates,
            }],
        })
    }
}

/// Validate that column values are within a range
pub fn validate_range(
    df: &DataFrame,
    column: &str,
    min: f64,
    max: f64,
) -> Result<ValidationResult> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    // Cast to f64 for comparison
    let float_col = col
        .cast(&DataType::Float64)
        .map_err(|e| anyhow!("Cannot cast column '{}' to float: {}", column, e))?;

    let ca = float_col
        .f64()
        .map_err(|e| anyhow!("Failed to get f64 chunked array: {}", e))?;

    let mut out_of_range_count = 0;
    for val in ca.into_iter().flatten() {
        if val < min || val > max {
            out_of_range_count += 1;
        }
    }

    if out_of_range_count == 0 {
        Ok(ValidationResult {
            passed: true,
            violations: vec![],
        })
    } else {
        Ok(ValidationResult {
            passed: false,
            violations: vec![Violation {
                column: column.to_string(),
                check_type: "range".to_string(),
                message: format!(
                    "Column '{}' has {} values outside range [{}, {}]",
                    column, out_of_range_count, min, max
                ),
                count: out_of_range_count,
            }],
        })
    }
}

/// Validate that column values match a regex pattern
pub fn validate_regex(df: &DataFrame, column: &str, pattern: &str) -> Result<ValidationResult> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let str_col = col
        .str()
        .map_err(|e| anyhow!("Column '{}' is not a string type: {}", column, e))?;

    let regex = regex::Regex::new(pattern)
        .map_err(|e| anyhow!("Invalid regex pattern '{}': {}", pattern, e))?;

    let mut non_matching_count = 0;
    for val in str_col.into_iter().flatten() {
        if !regex.is_match(val) {
            non_matching_count += 1;
        }
    }

    if non_matching_count == 0 {
        Ok(ValidationResult {
            passed: true,
            violations: vec![],
        })
    } else {
        Ok(ValidationResult {
            passed: false,
            violations: vec![Violation {
                column: column.to_string(),
                check_type: "regex".to_string(),
                message: format!(
                    "Column '{}' has {} values not matching pattern '{}'",
                    column, non_matching_count, pattern
                ),
                count: non_matching_count,
            }],
        })
    }
}

/// Validate that column values are in an allowed set
pub fn validate_enum(df: &DataFrame, column: &str, allowed: &[String]) -> Result<ValidationResult> {
    let col = df
        .column(column)
        .map_err(|e| anyhow!("Column '{}' not found: {}", column, e))?;

    let str_col = col
        .str()
        .map_err(|e| anyhow!("Column '{}' is not a string type: {}", column, e))?;

    let mut invalid_count = 0;
    for val in str_col.into_iter().flatten() {
        if !allowed.iter().any(|a| a == val) {
            invalid_count += 1;
        }
    }

    if invalid_count == 0 {
        Ok(ValidationResult {
            passed: true,
            violations: vec![],
        })
    } else {
        Ok(ValidationResult {
            passed: false,
            violations: vec![Violation {
                column: column.to_string(),
                check_type: "enum".to_string(),
                message: format!(
                    "Column '{}' has {} values not in allowed set {:?}",
                    column, invalid_count, allowed
                ),
                count: invalid_count,
            }],
        })
    }
}

/// Build a boolean mask for rows that pass all column checks
fn build_violation_mask(df: &DataFrame, check: &ColumnCheck) -> Result<BooleanChunked> {
    let n_rows = df.height();
    let mut mask = BooleanChunked::from_iter(std::iter::repeat_n(Some(false), n_rows));

    // Check not_null
    if check.not_null {
        let col = df.column(&check.name)?;
        let nulls = col.is_null();
        mask = mask | nulls;
    }

    // Check range
    if let Some((min, max)) = check.range {
        let col = df.column(&check.name)?;
        let float_col = col.cast(&DataType::Float64)?;
        let ca = float_col.f64()?;

        let below_min = ca.lt(min);
        let above_max = ca.gt(max);
        let out_of_range = below_min | above_max;
        mask = mask | out_of_range;
    }

    // Check regex
    if let Some(ref pattern) = check.regex {
        let col = df.column(&check.name)?;
        if let Ok(str_col) = col.str() {
            let regex = regex::Regex::new(pattern)?;
            let mut non_match = Vec::with_capacity(n_rows);
            for opt_val in str_col.into_iter() {
                match opt_val {
                    Some(val) => non_match.push(Some(!regex.is_match(val))),
                    None => non_match.push(Some(false)), // null values don't violate regex
                }
            }
            let non_match_ca = BooleanChunked::from_iter(non_match);
            mask = mask | non_match_ca;
        }
    }

    // Check enum
    if let Some(ref allowed) = check.allowed_values {
        let col = df.column(&check.name)?;
        if let Ok(str_col) = col.str() {
            let mut not_allowed = Vec::with_capacity(n_rows);
            for opt_val in str_col.into_iter() {
                match opt_val {
                    Some(val) => not_allowed.push(Some(!allowed.iter().any(|a| a == val))),
                    None => not_allowed.push(Some(false)), // null values don't violate enum
                }
            }
            let not_allowed_ca = BooleanChunked::from_iter(not_allowed);
            mask = mask | not_allowed_ca;
        }
    }

    Ok(mask)
}

/// Run validation on a DataFrame with the given configuration and mode
///
/// Returns:
/// - Valid DataFrame (rows that passed all checks, or all rows in strict/warn mode)
/// - Optional quarantine DataFrame (rows that failed checks, only in quarantine mode)
/// - Validation report
pub fn run_validation(
    df: DataFrame,
    config: &CheckConfig,
    mode: &ValidationMode,
    _masker: &crate::security::Masker,
) -> Result<(DataFrame, Option<DataFrame>, ValidationReport)> {
    let mut report = ValidationReport::new();

    // Run all column checks and collect results
    for check in &config.columns {
        if check.not_null {
            let result = validate_not_null(&df, &check.name)?;
            report.add_result(result);
        }

        if check.unique {
            let result = validate_unique(&df, &check.name)?;
            report.add_result(result);
        }

        if let Some((min, max)) = check.range {
            let result = validate_range(&df, &check.name, min, max)?;
            report.add_result(result);
        }

        if let Some(ref pattern) = check.regex {
            let result = validate_regex(&df, &check.name, pattern)?;
            report.add_result(result);
        }

        if let Some(ref allowed) = check.allowed_values {
            let result = validate_enum(&df, &check.name, allowed)?;
            report.add_result(result);
        }
    }

    // Handle based on mode
    match mode {
        ValidationMode::Strict => {
            if !report.passed {
                return Err(anyhow!(
                    "Validation failed with {} violations",
                    report.total_violations
                ));
            }
            Ok((df, None, report))
        }
        ValidationMode::Warn => {
            // Just return the data with warnings in the report
            Ok((df, None, report))
        }
        ValidationMode::Quarantine => {
            if !report.passed {
                // Build combined violation mask
                let n_rows = df.height();
                let mut combined_mask =
                    BooleanChunked::from_iter(std::iter::repeat_n(Some(false), n_rows));

                for check in &config.columns {
                    let check_mask = build_violation_mask(&df, check)?;
                    combined_mask = combined_mask | check_mask;
                }

                // Split into valid and quarantine DataFrames
                let valid_mask = !combined_mask.clone();
                let valid_df = df.filter(&valid_mask)?;
                let quarantine_df = df.filter(&combined_mask)?;

                Ok((valid_df, Some(quarantine_df), report))
            } else {
                Ok((df, None, report))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_df() -> DataFrame {
        df! {
            "id" => &[1, 2, 3, 4, 5],
            "name" => &["Alice", "Bob", "Charlie", "David", "Eve"],
            "age" => &[25, 30, 35, 40, 45],
            "status" => &["active", "active", "inactive", "active", "pending"]
        }
        .unwrap()
    }

    #[test]
    fn test_validate_not_null_pass() {
        let df = create_test_df();
        let result = validate_not_null(&df, "id").unwrap();
        assert!(result.passed);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_validate_not_null_fail() {
        let df = df! {
            "id" => &[Some(1), None, Some(3), None, Some(5)]
        }
        .unwrap();

        let result = validate_not_null(&df, "id").unwrap();
        assert!(!result.passed);
        assert_eq!(result.violations.len(), 1);
        assert_eq!(result.violations[0].count, 2);
        assert_eq!(result.violations[0].check_type, "not_null");
    }

    #[test]
    fn test_validate_unique_pass() {
        let df = create_test_df();
        let result = validate_unique(&df, "id").unwrap();
        assert!(result.passed);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_validate_unique_fail() {
        let df = df! {
            "id" => &[1, 2, 2, 3, 3]
        }
        .unwrap();

        let result = validate_unique(&df, "id").unwrap();
        assert!(!result.passed);
        assert_eq!(result.violations.len(), 1);
        assert_eq!(result.violations[0].count, 2); // 2 duplicate entries (5 total - 3 unique)
        assert_eq!(result.violations[0].check_type, "unique");
    }

    #[test]
    fn test_validate_range_pass() {
        let df = create_test_df();
        let result = validate_range(&df, "age", 0.0, 100.0).unwrap();
        assert!(result.passed);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_validate_range_fail() {
        let df = df! {
            "age" => &[25, 150, 35, -5, 45]
        }
        .unwrap();

        let result = validate_range(&df, "age", 0.0, 120.0).unwrap();
        assert!(!result.passed);
        assert_eq!(result.violations.len(), 1);
        assert_eq!(result.violations[0].count, 2); // 150 and -5 are out of range
        assert_eq!(result.violations[0].check_type, "range");
    }

    #[test]
    fn test_validate_regex_pass() {
        let df = df! {
            "email" => &["alice@example.com", "bob@test.org", "charlie@demo.net"]
        }
        .unwrap();

        let result = validate_regex(&df, "email", r"^[a-z]+@[a-z]+\.[a-z]+$").unwrap();
        assert!(result.passed);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_validate_regex_fail() {
        let df = df! {
            "email" => &["alice@example.com", "invalid-email", "bob@test.org"]
        }
        .unwrap();

        let result = validate_regex(&df, "email", r"^[a-z]+@[a-z]+\.[a-z]+$").unwrap();
        assert!(!result.passed);
        assert_eq!(result.violations.len(), 1);
        assert_eq!(result.violations[0].count, 1);
        assert_eq!(result.violations[0].check_type, "regex");
    }

    #[test]
    fn test_validate_enum_pass() {
        let df = create_test_df();
        let allowed = vec![
            "active".to_string(),
            "inactive".to_string(),
            "pending".to_string(),
        ];
        let result = validate_enum(&df, "status", &allowed).unwrap();
        assert!(result.passed);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_validate_enum_fail() {
        let df = df! {
            "status" => &["active", "unknown", "inactive", "invalid"]
        }
        .unwrap();

        let allowed = vec!["active".to_string(), "inactive".to_string()];
        let result = validate_enum(&df, "status", &allowed).unwrap();
        assert!(!result.passed);
        assert_eq!(result.violations.len(), 1);
        assert_eq!(result.violations[0].count, 2); // "unknown" and "invalid"
        assert_eq!(result.violations[0].check_type, "enum");
    }

    #[test]
    fn test_quarantine_mode() {
        let df = df! {
            "id" => &[1, 2, 3, 4, 5],
            "age" => &[25, 150, 35, -5, 45]
        }
        .unwrap();

        let config = CheckConfig {
            columns: vec![ColumnCheck {
                name: "age".to_string(),
                not_null: false,
                unique: false,
                range: Some((0.0, 120.0)),
                regex: None,
                allowed_values: None,
            }],
            dataset: None,
        };

        let masker = crate::security::Masker::new(vec![]);
        let (valid_df, quarantine_df, report) =
            run_validation(df, &config, &ValidationMode::Quarantine, &masker).unwrap();

        assert!(!report.passed);
        assert_eq!(valid_df.height(), 3); // rows with age 25, 35, 45
        assert!(quarantine_df.is_some());
        assert_eq!(quarantine_df.unwrap().height(), 2); // rows with age 150, -5
    }

    #[test]
    fn test_strict_mode_fail() {
        let df = df! {
            "id" => &[Some(1), None, Some(3)]
        }
        .unwrap();

        let config = CheckConfig {
            columns: vec![ColumnCheck {
                name: "id".to_string(),
                not_null: true,
                unique: false,
                range: None,
                regex: None,
                allowed_values: None,
            }],
            dataset: None,
        };

        let masker = crate::security::Masker::new(vec![]);
        let result = run_validation(df, &config, &ValidationMode::Strict, &masker);
        assert!(result.is_err());
    }

    #[test]
    fn test_warn_mode() {
        let df = df! {
            "id" => &[Some(1), None, Some(3)]
        }
        .unwrap();

        let config = CheckConfig {
            columns: vec![ColumnCheck {
                name: "id".to_string(),
                not_null: true,
                unique: false,
                range: None,
                regex: None,
                allowed_values: None,
            }],
            dataset: None,
        };

        let masker = crate::security::Masker::new(vec![]);
        let (valid_df, quarantine_df, report) =
            run_validation(df, &config, &ValidationMode::Warn, &masker).unwrap();

        assert!(!report.passed); // validation failed
        assert_eq!(valid_df.height(), 3); // but all rows are kept
        assert!(quarantine_df.is_none()); // no quarantine in warn mode
    }
}
