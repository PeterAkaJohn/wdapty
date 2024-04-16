use anyhow::{anyhow, Context, Result};
use polars::io::{csv::CsvWriter, SerWriter};

pub fn handle_output(
    output_file: Option<String>,
    mut df: polars::prelude::DataFrame,
) -> Result<()> {
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

#[cfg(test)]
mod test {
    use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};

    use crate::generated_test_files_path;

    use super::*;

    #[test]
    fn test_handle_output_success_with_output_file() {
        let series = (0..10)
            .map(|col_name| Series::new(&col_name.to_string(), &vec!["A", "B", "C"]))
            .collect::<Vec<Series>>();
        let df = DataFrame::new(series);
        assert!(df.is_ok());
        let df = df.unwrap();
        let result = handle_output(
            Some(generated_test_files_path!("test_handle_output.parq")),
            df,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_output_success_without_output_file() {
        let series = (0..10)
            .map(|col_name| Series::new(&col_name.to_string(), &vec!["A", "B", "C"]))
            .collect::<Vec<Series>>();
        let df = DataFrame::new(series);
        assert!(df.is_ok());
        let df = df.unwrap();
        let result = handle_output(None, df);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_output_failure_path_does_not_exist() {
        let series = (0..10)
            .map(|col_name| Series::new(&col_name.to_string(), &vec!["A", "B", "C"]))
            .collect::<Vec<Series>>();
        let df = DataFrame::new(series);
        assert!(df.is_ok());
        let df = df.unwrap();
        let result = handle_output(
            Some(generated_test_files_path!(
                "/do/not/exist/test_handle_output.parq"
            )),
            df,
        );
        assert!(result.is_err());
    }
}
