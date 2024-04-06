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
}

enum Processors {
    Parq(ParqProcessor),
}

impl Runnable for Processors {
    fn run(&self) -> Result<LazyFrame> {
        match self {
            Processors::Parq(parq_processor) => return parq_processor.run(),
        }
    }
}

pub fn run() -> Result<()> {
    let args = CliArgs::parse();

    let processor = match args.execution_type.as_str() {
        "parq" => Processors::Parq(ParqProcessor {
            cols: args.cols,
            file_name: args.file_name,
            index_name: args.index_name,
            index_value: args.index_value,
        }),
        _ => return Err(anyhow::anyhow!("Invalid Execution type")),
    };

    let result_df = processor
        .run()
        .with_context(|| format!("Failed to run processor"))?
        .collect()?;

    handle_output(args.output_file, result_df)
}
