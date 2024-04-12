use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use file_processing::dataframe::{file::handle_output, parq::ParqProcessor, processor::Runnable};
use input::handle_pattern;
use polars::lazy::prelude::*;
use std::path::PathBuf;
mod input;

#[derive(Parser, Debug)]
struct CliArgs {
    #[command(subcommand)]
    command: Commands,

    #[command(flatten)]
    defaults: DefaultOpts,
}

#[derive(Debug, Args)]
struct DefaultOpts {
    #[arg(long, short)]
    profile: Option<String>,
    #[arg(long, action)]
    pattern: bool,
    #[arg(long, action)]
    load_config: bool,
    #[arg(long, short, default_value = "parq")]
    execution_type: String,
    #[arg(long)]
    file_name: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(arg_required_else_help = true)]
    Download {
        #[arg(long)]
        output_file: String,
    },
    #[command(arg_required_else_help = true)]
    Search {
        #[arg(long)]
        index_name: Option<String>,
        #[arg(long)]
        index_value: Option<String>,
        #[arg(long)]
        output_file: Option<String>,
        #[arg(long, num_args = 1..)]
        cols: Option<Vec<String>>,
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
    let defaults = args.defaults;
    let DefaultOpts {profile, file_name, execution_type, pattern, ..} = defaults;
    let file_name = acquire_file_name(pattern, file_name)?;

    let (index_name, index_value, cols, output_file) = match args.command {
        Commands::Download {
            output_file,
        } => {
            println!("Preparing for Download Command");
            (None, None, None, Some(output_file))
        }
        Commands::Search {
            index_name,
            index_value,
            output_file,
            cols,
        } => {
            println!("Preparing for Search Command");
            (index_name, index_value, cols, output_file)
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

fn acquire_file_name(pattern: bool, file_name: Option<PathBuf>) -> Result<PathBuf, anyhow::Error> {
    let file_name = if pattern {
        let file_name_from_pattern: PathBuf = handle_pattern()?.into();
        Some(file_name_from_pattern)
    } else {
        file_name
    }.with_context(|| "file name should be valued by option or by setting pattern and reading file")?;
    Ok(file_name)
}
