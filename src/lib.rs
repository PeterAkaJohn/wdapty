use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use commands::{configure::initialize, handle_pattern};
use file_processing::dataframe::{file::handle_output, parq::ParqProcessor, processor::Runnable};
use polars::lazy::prelude::*;
use std::path::PathBuf;
mod commands;

#[derive(Parser, Debug)]
struct CliArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Args)]
struct DefaultProcessingOpts {
    #[arg(long, short)]
    profile: Option<String>,
    #[arg(long)]
    pattern: Option<String>,
    #[arg(long, short, default_value = "parq")]
    execution_type: String,
    #[arg(long)]
    file_name: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Configure {
        #[arg(long)]
        pattern: Option<Vec<String>>,
    },
    #[command(arg_required_else_help = true)]
    #[command(subcommand)]
    Processing(ProcessingCommands),
}

#[derive(Debug, Subcommand)]
enum ProcessingCommands {
    #[command(arg_required_else_help = true)]
    Download {
        #[arg(long)]
        output_file: String,
        #[command(flatten)]
        defaults: DefaultProcessingOpts,
    },
    #[command(arg_required_else_help = true)]
    Search {
        #[arg(long)]
        index_name: String,
        #[arg(long)]
        index_value: String,
        #[arg(long)]
        output_file: Option<String>,
        #[arg(long, num_args = 1..)]
        cols: Option<Vec<String>>,
        #[command(flatten)]
        defaults: DefaultProcessingOpts,
    },
}

enum Processors<'a> {
    Parq(ParqProcessor<'a>),
}

impl Runnable for Processors<'_> {
    fn run(&self) -> Result<LazyFrame> {
        match self {
            Processors::Parq(parq_processor) => return parq_processor.run(),
        }
    }
}

pub fn run() -> Result<()> {
    let args = CliArgs::parse();
    match args.command {
        Commands::Configure { .. } => {
            let config_path = initialize()?;
            println!("Saved config.ini in {}", config_path);
            Ok(())
        }
        Commands::Processing(subcom) => {
            let (index_name, index_value, cols, output_file, file_name, execution_type, profile) =
                match subcom {
                    ProcessingCommands::Download {
                        output_file,
                        defaults,
                    } => {
                        let defaults = defaults;
                        let DefaultProcessingOpts {
                            profile,
                            file_name,
                            execution_type,
                            pattern,
                            ..
                        } = defaults;
                        let file_name = acquire_file_name(pattern, file_name)?;
                        println!("Preparing for Download Command");
                        (
                            None,
                            None,
                            None,
                            Some(output_file),
                            file_name,
                            execution_type,
                            profile,
                        )
                    }
                    ProcessingCommands::Search {
                        index_name,
                        index_value,
                        output_file,
                        cols,
                        defaults,
                    } => {
                        let defaults = defaults;
                        let DefaultProcessingOpts {
                            profile,
                            file_name,
                            execution_type,
                            pattern,
                            ..
                        } = defaults;
                        let file_name = acquire_file_name(pattern, file_name)?;
                        println!("Preparing for Search Command");
                        (
                            Some(index_name),
                            Some(index_value),
                            cols,
                            output_file,
                            file_name,
                            execution_type,
                            profile,
                        )
                    }
                };

            let processor = match execution_type.as_str() {
                "parq" => Processors::Parq(ParqProcessor::new(
                    index_name,
                    index_value,
                    cols,
                    file_name,
                    profile.as_deref(),
                )),
                _ => return Err(anyhow::anyhow!("Invalid Execution type")),
            };

            let result_df = processor
                .run()
                .with_context(|| format!("Failed to run processor"))?
                .collect()?;

            handle_output(output_file, result_df)
        }
    }
}

fn acquire_file_name(
    pattern: Option<String>,
    file_name: Option<PathBuf>,
) -> Result<PathBuf, anyhow::Error> {
    let file_name = if let Some(pat) = pattern {
        let file_name_from_pattern: PathBuf = handle_pattern(pat.as_str())?.into();
        Some(file_name_from_pattern)
    } else {
        file_name
    }
    .with_context(|| {
        "file name should be valued by option or by setting pattern and reading file"
    })?;
    Ok(file_name)
}
