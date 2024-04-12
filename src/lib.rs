use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use file_processing::dataframe::{parq::ParqProcessor, file::handle_output, processor::Runnable};
use input::handle_pattern;
use polars::lazy::prelude::*;
use std::path::PathBuf;
mod input;

#[derive(Parser, Debug)]
struct CliArgs {
    #[command(subcommand)]
    command: Commands
}


#[derive(Debug, Subcommand)]
enum Commands {
    #[command(arg_required_else_help = true)]
    Download {
        #[arg(long)]
        file_name: Option<PathBuf>,
        #[arg(long)]
        output_file: Option<String>,
        #[arg(long, short, default_value = "parq")]
        execution_type: String,
        #[arg(long, num_args = 1..)]
        cols: Option<Vec<String>>,
        #[arg(long, short)]
        profile: Option<String>,
        #[arg(long, action)]
        pattern: bool,
        #[arg(long, action)]
        load_config: bool
    },
    #[command(arg_required_else_help = true)]
    Search {
        #[arg(long)]
        index_name: Option<String>,
        #[arg(long)]
        index_value: Option<String>,
        #[arg(long)]
        file_name: Option<PathBuf>,
        #[arg(long)]
        output_file: Option<String>,
        #[arg(long, short, default_value = "parq")]
        execution_type: String,
        #[arg(long, num_args = 1..)]
        cols: Option<Vec<String>>,
        #[arg(long, short)]
        profile: Option<String>,
        #[arg(long, action)]
        pattern: bool,
        #[arg(long, action)]
        load_config: bool
    }
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
        Commands::Download { file_name, output_file, execution_type, cols, profile, pattern, .. } => {
            println!("Running Download");
            let file_name = if pattern {
                let file_name_from_pattern: PathBuf = handle_pattern()?.into();
                Some(file_name_from_pattern)
            } else {
                file_name
            }.unwrap();
        
            let processor = match execution_type.as_str() {
                "parq" => Processors::Parq(ParqProcessor::new(None, None, cols, file_name, profile.as_deref())),
                _ => return Err(anyhow::anyhow!("Invalid Execution type")),
            };
        
            let result_df = processor
                .run()
                .with_context(|| format!("Failed to run processor"))?
                .collect()?;
        
            handle_output(output_file, result_df)
        },
        Commands::Search { index_name, index_value, file_name, output_file, execution_type, cols, profile, pattern, .. } => {
            println!("Running Search Command");
            let file_name = if pattern {
                let file_name_from_pattern: PathBuf = handle_pattern()?.into();
                Some(file_name_from_pattern)
            } else {
                file_name
            }.unwrap();
        
            let processor = match execution_type.as_str() {
                "parq" => Processors::Parq(ParqProcessor::new(index_name, index_value, cols, file_name, profile.as_deref())),
                _ => return Err(anyhow::anyhow!("Invalid Execution type")),
            };
        
            let result_df = processor
                .run()
                .with_context(|| format!("Failed to run processor"))?
                .collect()?;
        
            handle_output(output_file, result_df)
        },
    }

}
