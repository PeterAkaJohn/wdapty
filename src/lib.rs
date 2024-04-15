use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use commands::{configure::initialize, handle_pattern, pattern::get_available_patterns};
use file_processing::{
    dataframe::{file::handle_output, parq::ParqProcessor, processor::Runnable},
    Processors,
};
use std::path::PathBuf;

use crate::commands::pattern::{add_pattern_to_config, remove_pattern_from_config};
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
        patterns: Option<Vec<String>>,
    },
    #[command(subcommand)]
    Patterns(PatternsCommands),
    #[command(arg_required_else_help = true)]
    #[command(subcommand)]
    Processing(ProcessingCommands),
}

#[derive(Debug, Subcommand)]
enum PatternsCommands {
    List,
    #[command(arg_required_else_help = true)]
    Add {
        #[arg(long)]
        name: String,
        #[arg(long)]
        value: String,
    },
    #[command(arg_required_else_help = true)]
    Remove {
        #[arg(long)]
        name: String,
    },
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

trait RunCommand {
    fn run(self) -> Result<()>;
}

impl RunCommand for ProcessingCommands {
    fn run(self) -> Result<()> {
        let (index_name, index_value, cols, output_file, file_name, execution_type, profile) =
            match self {
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

impl RunCommand for PatternsCommands {
    fn run(self) -> Result<()> {
        match self {
            PatternsCommands::List => {
                let patterns_available = get_available_patterns();
                patterns_available.map(|available| {
                    if available.keys().len() > 0 {
                        for (key, value) in available {
                            println!("Pattern {} has value {}", key, value);
                        }
                    } else {
                        println!("No patterns available in config.ini");
                    };
                })
            }
            PatternsCommands::Add { name, value } => {
                println!(
                    "Performing Add Pattern with name {} and value {}",
                    name, value
                );
                add_pattern_to_config(name, value)
            }
            PatternsCommands::Remove { name } => {
                println!("Performing Remove Pattern with name {}", name);
                remove_pattern_from_config(name)
            }
        }
    }
}

pub fn run() -> Result<()> {
    let args = CliArgs::parse();
    match args.command {
        Commands::Configure { patterns } => {
            let config_path = initialize(patterns)?;
            println!("Saved config.ini in {}", config_path);
            Ok(())
        }
        Commands::Processing(subcom) => subcom.run(),
        Commands::Patterns(subcom) => subcom.run(),
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
