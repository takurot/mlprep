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
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .init();
        }
    }

    // Root span with run_id
    let _span = tracing::info_span!("root", run_id = %run_id).entered();

    match &cli.command {
        Commands::Run { pipeline } => {
            // miette::Result handles returning errors nicely
            mlprep::runner::execution_pipeline(pipeline, run_id)?;
        }
    }

    Ok(())
}
