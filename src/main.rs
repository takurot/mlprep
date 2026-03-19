use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, Shell as CompletionShell};
use miette::Result;
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Clone, ValueEnum, Debug)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Parser)]
#[command(name = "mlprep")]
#[command(version = "0.3.1")]
#[command(about = "High-performance no-code data preprocessing engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Increase logging verbosity (Info -> Debug)
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Silence all logs
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Log format (text or json)
    #[arg(long, value_enum, global = true, default_value_t = LogFormat::Text)]
    log_format: LogFormat,

    /// Allowed paths for I/O sandboxing (files must be within these paths)
    #[arg(long, value_name = "PATH", global = true)]
    allowed_paths: Option<Vec<PathBuf>>,

    /// Columns to mask in logs
    #[arg(long, value_name = "COL", global = true)]
    mask_columns: Option<Vec<String>>,

    /// Enable streaming execution mode (low memory usage)
    #[arg(long, global = true)]
    streaming: bool,

    /// Memory limit for execution (e.g. "4GB", "500MB")
    #[arg(long, global = true)]
    memory_limit: Option<String>,

    /// Maximum threads for Polars (overrides POLARS_MAX_THREADS)
    #[arg(long, value_name = "N", global = true)]
    threads: Option<String>,

    /// Enable/disable plan cache (maps to POLARS_CACHE)
    #[arg(
        long,
        value_name = "true|false",
        num_args = 0..=1,
        default_missing_value = "true",
        global = true
    )]
    cache: Option<bool>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a pipeline from a YAML configuration file
    Run {
        /// One or more pipeline YAML files to execute sequentially
        #[arg(value_name = "PIPELINE_FILE", num_args = 1..)]
        pipelines: Vec<PathBuf>,

        /// Validate and show plan without executing (dry run)
        #[arg(long)]
        dry_run: bool,
    },

    /// Generate a pipeline template YAML
    Init {
        /// Output path for the generated template
        #[arg(value_name = "OUTPUT", default_value = "pipeline.yaml")]
        output: PathBuf,

        /// CSV file to infer schema from
        #[arg(long, value_name = "CSV_FILE")]
        from: Option<PathBuf>,
    },

    /// Check a pipeline YAML for syntax and consistency errors
    Lint {
        /// Pipeline YAML file to check
        #[arg(value_name = "PIPELINE_FILE")]
        pipeline: PathBuf,
    },

    /// Show pipeline execution plan without running
    Plan {
        /// Pipeline YAML file to plan
        #[arg(value_name = "PIPELINE_FILE")]
        pipeline: PathBuf,
    },

    /// Profile a data file and report column statistics
    Profile {
        /// Input data file (CSV or Parquet)
        #[arg(value_name = "DATA_FILE")]
        input: PathBuf,

        /// Output path for the JSON report (default: print to stdout)
        #[arg(long, value_name = "PATH")]
        out: Option<PathBuf>,
    },

    /// Validate data against a checks specification
    Validate {
        /// Checks YAML file (CheckConfig format)
        #[arg(value_name = "CHECKS_FILE")]
        checks: PathBuf,

        /// Data file to validate (CSV or Parquet)
        #[arg(value_name = "DATA_FILE")]
        data: PathBuf,
    },

    /// Feature engineering commands (fit / transform)
    Features {
        #[command(subcommand)]
        command: FeaturesCommands,
    },

    /// Generate shell completion scripts
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum, value_name = "SHELL")]
        shell: CompletionShell,
    },
}

#[derive(Subcommand)]
enum FeaturesCommands {
    /// Fit feature transformers on training data and save state
    Fit {
        /// Feature configuration YAML file
        #[arg(value_name = "CONFIG_FILE")]
        config: PathBuf,

        /// Input training data file (CSV or Parquet)
        #[arg(long = "in", value_name = "INPUT_FILE")]
        input: PathBuf,

        /// Output path for the feature state JSON file or directory
        #[arg(long, value_name = "OUTPUT_PATH")]
        out: PathBuf,
    },

    /// Transform data using a previously fitted feature state
    Transform {
        /// Feature state JSON file (produced by `features fit`)
        #[arg(long, value_name = "STATE_FILE")]
        state: PathBuf,

        /// Input data file to transform (CSV or Parquet)
        #[arg(long = "in", value_name = "INPUT_FILE")]
        input: PathBuf,

        /// Output Parquet file path
        #[arg(long, value_name = "OUTPUT_FILE")]
        out: PathBuf,
    },
}

fn main() -> Result<()> {
    // Parse CLI args first
    let cli = Cli::parse();

    // Determine default log level
    let default_level = if cli.quiet {
        Level::ERROR
    } else if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };

    // Initialize logging with EnvFilter (MLPREP_LOG > CLI args)
    let filter = EnvFilter::builder()
        .with_default_directive(default_level.into())
        .with_env_var("MLPREP_LOG")
        .from_env_lossy();

    match cli.log_format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .with_span_list(false)
                .with_current_span(false)
                .init();
        }
        LogFormat::Text => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(std::io::stderr)
                .init();
        }
    }

    // Root span for the CLI session
    let run_id = Uuid::new_v4();
    let _span = tracing::info_span!("root", run_id = %run_id).entered();

    match &cli.command {
        Commands::Run { pipelines, dry_run } => {
            let security_config = mlprep::security::SecurityConfig {
                allowed_paths: cli.allowed_paths,
                mask_columns: cli.mask_columns,
            };
            let runtime_override = mlprep::dsl::RuntimeConfig {
                streaming: cli.streaming,
                memory_limit: cli.memory_limit,
                threads: cli.threads.clone(),
                cache: cli.cache,
            };

            if *dry_run {
                for pipeline_path in pipelines {
                    cmd_plan(pipeline_path)?;
                }
            } else {
                for pipeline in pipelines {
                    let pipeline_run = Uuid::new_v4();
                    let _metrics = mlprep::runner::execution_pipeline(
                        pipeline,
                        pipeline_run,
                        security_config.clone(),
                        Some(runtime_override.clone()),
                    )?;
                }
            }
        }

        Commands::Init { output, from } => {
            cmd_init(output, from.as_ref())?;
        }

        Commands::Lint { pipeline } => {
            cmd_lint(pipeline)?;
        }

        Commands::Plan { pipeline } => {
            cmd_plan(pipeline)?;
        }

        Commands::Profile { input, out } => {
            cmd_profile(input, out.as_ref())?;
        }

        Commands::Validate { checks, data } => {
            cmd_validate(checks, data)?;
        }

        Commands::Features {
            command: FeaturesCommands::Fit { config, input, out },
        } => {
            cmd_features_fit(config, input, out)?;
        }

        Commands::Features {
            command: FeaturesCommands::Transform { state, input, out },
        } => {
            cmd_features_transform(state, input, out)?;
        }

        Commands::Completions { shell } => {
            cmd_completions(*shell);
        }
    }

    Ok(())
}

// ============================================================================
// Command implementations
// ============================================================================

/// Generate a pipeline template YAML.
fn cmd_init(output: &PathBuf, from: Option<&PathBuf>) -> Result<()> {
    use mlprep::errors::MlPrepError;

    let template = if let Some(csv_path) = from {
        // Infer column names from CSV header
        let mut lf = mlprep::io::read_csv(csv_path).map_err(miette::Report::new)?;
        let schema = lf
            .collect_schema()
            .map_err(MlPrepError::PolarsError)
            .map_err(miette::Report::new)?;
        let columns: Vec<String> = schema.iter_names().map(|n| n.to_string()).collect();
        let csv_str = csv_path.display().to_string();
        let quoted_csv_path = yaml_quote(&csv_str);
        let cols_yaml = columns
            .iter()
            .map(|c| format!("      - {}", yaml_quote(c)))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            r#"# mlprep pipeline – generated from {csv_str}
inputs:
  - path: {quoted_csv_path}

steps:
  - type: select
    columns:
{cols_yaml}

outputs:
  - path: "output.parquet"
"#,
            csv_str = csv_str,
            quoted_csv_path = quoted_csv_path,
            cols_yaml = cols_yaml
        )
    } else {
        r#"# mlprep pipeline template
# See https://github.com/takurot/mlprep for documentation

inputs:
  - path: "data/input.csv"

steps:
  - type: select
    columns:
      - "col_a"
      - "col_b"
  # - type: filter
  #   condition: "col_a > 0"
  # - type: cast
  #   columns:
  #     col_a: "Int64"

outputs:
  - path: "data/output.parquet"
"#
        .to_string()
    };

    std::fs::write(output, &template)
        .map_err(mlprep::errors::MlPrepError::IoError)
        .map_err(miette::Report::new)?;

    println!("Template written to {}", output.display());
    Ok(())
}

fn yaml_quote(value: &str) -> String {
    serde_json::to_string(value).expect("JSON string escaping should never fail")
}

/// Lint a pipeline YAML for errors.
fn cmd_lint(pipeline: &PathBuf) -> Result<()> {
    use mlprep::dsl::Pipeline;

    // 1. Parse YAML – catches syntax errors
    let parsed = Pipeline::from_path(pipeline);
    match parsed {
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
        Ok(p) => {
            let mut errors: Vec<String> = Vec::new();
            let mut warnings: Vec<String> = Vec::new();

            // 2. Check input paths exist
            for input in &p.inputs {
                let path = std::path::Path::new(&input.path);
                if !path.exists() {
                    errors.push(format!("input path does not exist: {}", input.path));
                }
            }

            // 3. Check output parent directories exist
            for output in &p.outputs {
                let path = std::path::Path::new(&output.path);
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        errors.push(format!(
                            "output directory does not exist: {}",
                            parent.display()
                        ));
                    }
                }
            }

            // 4. Warn on empty steps
            if p.steps.is_empty() {
                warnings.push("pipeline has no steps".to_string());
            }

            if errors.is_empty() {
                for warning in &warnings {
                    eprintln!("warning: {}", warning);
                }
                println!("OK: pipeline looks valid ({} steps)", p.steps.len());
            } else {
                for error in &errors {
                    eprintln!("lint: {}", error);
                }
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

/// Show pipeline execution plan without running.
fn cmd_plan(pipeline: &PathBuf) -> Result<()> {
    use mlprep::dsl::Pipeline;

    let p = Pipeline::from_path(pipeline).map_err(miette::Report::new)?;

    println!("Pipeline: {}", pipeline.display());
    println!();

    println!("Inputs ({}):", p.inputs.len());
    for input in &p.inputs {
        let fmt = input
            .format
            .as_deref()
            .unwrap_or_else(|| infer_format(&input.path));
        println!("  [{}] {}", fmt.to_uppercase(), input.path);
    }
    println!();

    println!("Steps ({}):", p.steps.len());
    for (i, step) in p.steps.iter().enumerate() {
        let desc = describe_step(step);
        println!("  {:2}. {}", i + 1, desc);
    }
    println!();

    println!("Outputs ({}):", p.outputs.len());
    for output in &p.outputs {
        let fmt = output
            .format
            .as_deref()
            .unwrap_or_else(|| infer_format(&output.path));
        println!("  [{}] {}", fmt.to_uppercase(), output.path);
    }

    if let Some(runtime) = &p.runtime {
        println!();
        println!("Runtime:");
        if runtime.streaming {
            println!("  streaming: true");
        }
        if let Some(ml) = &runtime.memory_limit {
            println!("  memory_limit: {}", ml);
        }
        if let Some(t) = &runtime.threads {
            println!("  threads: {}", t);
        }
    }

    Ok(())
}

fn infer_format(path: &str) -> &'static str {
    if path.ends_with(".parquet") {
        "parquet"
    } else if path.ends_with(".csv") {
        "csv"
    } else {
        "unknown"
    }
}

fn describe_step(step: &mlprep::dsl::Step) -> String {
    use mlprep::dsl::Step;
    match step {
        Step::Select(s) => format!("select  columns={:?}", s.columns),
        Step::Filter(f) => format!("filter  condition={:?}", f.condition),
        Step::Cast(c) => format!("cast    {} columns", c.columns.len()),
        Step::Sort(s) => format!("sort    by={:?}", s.by),
        Step::Join(j) => format!("join    {} on {:?}", j.how, j.left_on),
        Step::GroupBy(g) => format!("groupby by={:?} aggs={}", g.by, g.aggs.len()),
        Step::Window(w) => format!("window  partition_by={:?}", w.partition_by),
        Step::FillNull(f) => format!("fill_null columns={:?}", f.columns),
        Step::DropNull(d) => format!("drop_null columns={:?}", d.columns),
        Step::Validate(v) => format!("validate {} checks", v.checks.columns.len()),
        Step::Features(f) => format!("features {} transforms", f.config.features.len()),
    }
}

/// Profile a data file and output column statistics.
fn cmd_profile(input: &PathBuf, out: Option<&PathBuf>) -> Result<()> {
    use mlprep::errors::MlPrepError;
    use polars::prelude::*;
    use serde_json::{json, Value};

    let ext = input
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    let lf = if ext == "parquet" {
        mlprep::io::read_parquet(input).map_err(miette::Report::new)?
    } else {
        mlprep::io::read_csv(input).map_err(miette::Report::new)?
    };

    let df = lf
        .collect()
        .map_err(MlPrepError::PolarsError)
        .map_err(miette::Report::new)?;

    let row_count = df.height() as u64;
    let column_count = df.width() as u64;

    let mut columns: Vec<Value> = Vec::new();
    for col_name in df.get_column_names() {
        let series = df.column(col_name).unwrap();
        let dtype = format!("{:?}", series.dtype());
        let null_count = series.null_count() as u64;
        let null_pct = if row_count > 0 {
            null_count as f64 / row_count as f64 * 100.0
        } else {
            0.0
        };

        let col_name_str = col_name.to_string();
        let mut col_info = json!({
            "name": col_name_str,
            "dtype": dtype,
            "null_count": null_count,
            "null_pct": format!("{:.1}", null_pct),
        });

        // Numeric stats
        if let Ok(ca) = series.cast(&DataType::Float64) {
            if let Ok(fa) = ca.f64() {
                let min = fa.min().map(|v| json!(v)).unwrap_or(Value::Null);
                let max = fa.max().map(|v| json!(v)).unwrap_or(Value::Null);
                let mean = fa.mean().map(|v| json!(v)).unwrap_or(Value::Null);
                col_info["min"] = min;
                col_info["max"] = max;
                col_info["mean"] = mean;
            }
        }

        // Distinct count
        if let Ok(n_unique) = series.n_unique() {
            col_info["n_unique"] = json!(n_unique as u64);
        }

        columns.push(col_info);
    }

    let report = json!({
        "source": input.display().to_string(),
        "row_count": row_count,
        "column_count": column_count,
        "columns": columns,
    });

    let report_str = serde_json::to_string_pretty(&report).unwrap();

    if let Some(out_path) = out {
        std::fs::write(out_path, &report_str)
            .map_err(MlPrepError::IoError)
            .map_err(miette::Report::new)?;
        println!("Profile written to {}", out_path.display());
    } else {
        println!("{}", report_str);
    }

    Ok(())
}

/// Validate data against a checks specification.
fn cmd_validate(checks_path: &PathBuf, data_path: &PathBuf) -> Result<()> {
    use mlprep::dsl::CheckConfig;
    use mlprep::errors::MlPrepError;

    // Parse checks YAML
    let checks_content = std::fs::read_to_string(checks_path)
        .map_err(MlPrepError::IoError)
        .map_err(miette::Report::new)?;
    let check_config: CheckConfig = serde_yaml::from_str(&checks_content)
        .map_err(|e| MlPrepError::ConfigError(e, None))
        .map_err(miette::Report::new)?;

    // Read data
    let ext = data_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let lf = if ext == "parquet" {
        mlprep::io::read_parquet(data_path).map_err(miette::Report::new)?
    } else {
        mlprep::io::read_csv(data_path).map_err(miette::Report::new)?
    };

    // Run validation
    let report = mlprep::validate::summarize_violations_lazy(lf, &check_config, false)
        .map_err(|e| MlPrepError::ValidationError(e.to_string()))
        .map_err(miette::Report::new)?;

    if report.passed {
        println!(
            "OK: all checks passed ({} checks)",
            check_config.columns.len()
        );
    } else {
        eprintln!("FAIL: {} violation(s) found", report.total_violations);
        for result in &report.results {
            for violation in &result.violations {
                eprintln!("  - {}", violation.message);
            }
        }
        std::process::exit(2);
    }

    Ok(())
}

/// Fit feature transformers and save state.
fn cmd_features_fit(
    config_path: &PathBuf,
    input_path: &PathBuf,
    out_path: &std::path::Path,
) -> Result<()> {
    use mlprep::errors::MlPrepError;
    use mlprep::features::{fit_features, FeatureConfig, FeatureState};

    // Parse feature config
    let config_content = std::fs::read_to_string(config_path)
        .map_err(MlPrepError::IoError)
        .map_err(miette::Report::new)?;
    let config: FeatureConfig = serde_yaml::from_str(&config_content)
        .map_err(|e| MlPrepError::ConfigError(e, None))
        .map_err(miette::Report::new)?;

    // Read input data
    let ext = input_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let lf = if ext == "parquet" {
        mlprep::io::read_parquet(input_path).map_err(miette::Report::new)?
    } else {
        mlprep::io::read_csv(input_path).map_err(miette::Report::new)?
    };
    let df = lf
        .collect()
        .map_err(MlPrepError::PolarsError)
        .map_err(miette::Report::new)?;

    // Fit features
    let state: FeatureState = fit_features(&df, &config)
        .map_err(|e| MlPrepError::FeatureError(e.to_string()))
        .map_err(miette::Report::new)?;

    let resolved_out_path = resolve_feature_state_output(out_path)
        .map_err(MlPrepError::IoError)
        .map_err(miette::Report::new)?;

    // Save state
    state
        .save(&resolved_out_path)
        .map_err(|e| MlPrepError::FeatureError(e.to_string()))
        .map_err(miette::Report::new)?;

    println!(
        "Feature state ({} entries) saved to {}",
        state.entries.len(),
        resolved_out_path.display()
    );
    Ok(())
}

fn resolve_feature_state_output(out_path: &std::path::Path) -> std::io::Result<PathBuf> {
    let raw = out_path.as_os_str().to_string_lossy();
    let looks_like_dir = raw.ends_with(std::path::MAIN_SEPARATOR) || out_path.extension().is_none();

    if out_path.exists() {
        if out_path.is_dir() {
            return Ok(out_path.join("feature_state.json"));
        }
        return Ok(out_path.to_path_buf());
    }

    if looks_like_dir {
        std::fs::create_dir_all(out_path)?;
        return Ok(out_path.join("feature_state.json"));
    }

    Ok(out_path.to_path_buf())
}

/// Transform data using a previously fitted feature state.
fn cmd_features_transform(
    state_path: &PathBuf,
    input_path: &PathBuf,
    out_path: &PathBuf,
) -> Result<()> {
    use mlprep::errors::MlPrepError;
    use mlprep::features::{transform_features, FeatureConfig, FeatureState};

    // Load state
    let state = FeatureState::load(state_path)
        .map_err(|e| MlPrepError::FeatureError(e.to_string()))
        .map_err(miette::Report::new)?;

    // Build a FeatureConfig from the state entries so we can call transform_features
    let config = FeatureConfig {
        features: state
            .entries
            .iter()
            .map(|entry| {
                use mlprep::features::{FeatureSpec, FeatureStateEntry, FeatureTransform};
                let (column, transform) = match entry {
                    FeatureStateEntry::MinMax { column, .. } => {
                        (column.clone(), FeatureTransform::MinMaxScale)
                    }
                    FeatureStateEntry::Standard { column, .. } => {
                        (column.clone(), FeatureTransform::StandardScale)
                    }
                    FeatureStateEntry::OneHot { column, .. } => {
                        (column.clone(), FeatureTransform::OneHotEncode)
                    }
                    FeatureStateEntry::Count { column, .. } => {
                        (column.clone(), FeatureTransform::CountEncode)
                    }
                };
                FeatureSpec {
                    column,
                    transform,
                    alias: None,
                }
            })
            .collect(),
    };

    // Read input data
    let ext = input_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let lf = if ext == "parquet" {
        mlprep::io::read_parquet(input_path).map_err(miette::Report::new)?
    } else {
        mlprep::io::read_csv(input_path).map_err(miette::Report::new)?
    };
    let df = lf
        .collect()
        .map_err(MlPrepError::PolarsError)
        .map_err(miette::Report::new)?;

    // Transform
    let result = transform_features(&df, &config, &state)
        .map_err(|e| MlPrepError::FeatureError(e.to_string()))
        .map_err(miette::Report::new)?;

    // Write output
    mlprep::io::write_parquet(result, out_path).map_err(miette::Report::new)?;

    println!("Transformed data written to {}", out_path.display());
    Ok(())
}

/// Print shell completion script.
fn cmd_completions(shell: CompletionShell) {
    let mut cmd = Cli::command();
    let bin_name = cmd.get_name().to_string();
    generate(shell, &mut cmd, bin_name, &mut std::io::stdout());
}
