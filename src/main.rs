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
#[command(version = "0.1.0")]
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
}

#[derive(Subcommand)]
enum Commands {
    /// Run a pipeline from a YAML configuration file
    Run {
        /// Path to the pipeline YAML file
        #[arg(value_name = "PIPELINE_FILE")]
        pipeline: PathBuf,
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

    let run_id = Uuid::new_v4();

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
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }

    // Root span with run_id
    let _span = tracing::info_span!("root", run_id = %run_id).entered();

    match &cli.command {
        Commands::Run { pipeline } => {
            // miette::Result handles returning errors nicely
            let security_config = mlprep::security::SecurityConfig {
                allowed_paths: cli.allowed_paths,
                mask_columns: cli.mask_columns,
            };
            let runtime_override = mlprep::dsl::RuntimeConfig {
                streaming: cli.streaming,
                memory_limit: cli.memory_limit,
                ..Default::default()
            };

            mlprep::runner::execution_pipeline(pipeline, run_id, security_config, Some(runtime_override))?;
        }
    }

    Ok(())
}
