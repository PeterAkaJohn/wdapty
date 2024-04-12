use anyhow::{Context, Result};
use clap::Parser;
use file_processing::dataframe::{parq::ParqProcessor, file::handle_output, processor::Runnable};
use polars::lazy::prelude::*;
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct CliArgs {
    #[arg(long)]
    index_name: Option<String>,
    #[arg(long)]
    index_value: Option<String>,
    #[arg(long, num_args = 1..)]
    cols: Option<Vec<String>>,
    #[arg(long)]
    file_name: PathBuf,
    #[arg(long)]
    output_file: Option<String>,
    #[arg(long, short, default_value = "parq")]
    execution_type: String,
    #[arg(long, short)]
    profile: Option<String>,
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

    let processor = match args.execution_type.as_str() {
        "parq" => Processors::Parq(ParqProcessor::new(args.index_name, args.index_value, args.cols, args.file_name, args.profile.as_deref())),
        _ => return Err(anyhow::anyhow!("Invalid Execution type")),
    };

    let result_df = processor
        .run()
        .with_context(|| format!("Failed to run processor"))?
        .collect()?;

    handle_output(args.output_file, result_df)
}
