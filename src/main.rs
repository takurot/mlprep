use clap::{Parser, Subcommand};
use miette::Result;
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
    // Parse CLI args first to configure logging
    let cli = Cli::parse();

    // Determine log level
    let log_level = if cli.quiet {
        Level::ERROR
    } else if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };

    // Initialize logging
    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    match &cli.command {
        Commands::Run { pipeline } => {
            // miette::Result handles returning errors nicely
            mlprep::runner::execution_pipeline(pipeline)?;
        }
    }

    Ok(())
}
