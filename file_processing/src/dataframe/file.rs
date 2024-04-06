use anyhow::{anyhow, Context};
use polars::io::{csv::CsvWriter, SerWriter};

pub fn handle_output(output_file: Option<String>, mut df: polars::prelude::DataFrame) -> Result<(), anyhow::Error> {
    Ok(if let Some(output_file_path) = output_file {
        let file = std::fs::File::create(&output_file_path)
            .with_context(|| anyhow!("Failed to create file"))?;
        let mut writer = CsvWriter::new(file);
        writer
            .finish(&mut df)
            .with_context(|| anyhow!("Failed to write csv output file"))?;
        println!("Results are available in {}", &output_file_path);
    } else {
        println!("{}", df);
    })
}