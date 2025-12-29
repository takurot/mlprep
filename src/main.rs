use clap::{Parser, Subcommand, ValueEnum};
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
#[command(version = "0.3.0")]
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
        Commands::Run { pipelines } => {
            // miette::Result handles returning errors nicely
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

            for pipeline in pipelines {
                let pipeline_run = Uuid::new_v4();
                mlprep::runner::execution_pipeline(
                    pipeline,
                    pipeline_run,
                    security_config.clone(),
                    Some(runtime_override.clone()),
                )?;
            }
        }
    }

    Ok(())
}
