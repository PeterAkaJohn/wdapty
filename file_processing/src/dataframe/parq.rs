use std::path::PathBuf;

use anyhow::{Context, Ok, Result};
use polars::lazy::{dsl::col, frame::LazyFrame};

use super::{
    expressions::get_index_expr_if_needed,
    operations::filter_columns,
    processor::{Runnable, ScanFile},
};

pub struct ParqProcessor {
    pub index_name: Option<String>,
    pub index_value: Option<String>,
    pub cols: Option<Vec<String>>,
    pub file_name: PathBuf,
}

impl ParqProcessor {
    pub fn new(
        index_name: Option<String>,
        index_value: Option<String>,
        cols: Option<Vec<String>>,
        file_name: PathBuf,
    ) -> Self {
        let file_name = if file_name.starts_with("~") {
            let expanded_path =
                shellexpand::tilde(&file_name.to_string_lossy().into_owned()).to_string();
            PathBuf::from(expanded_path)
        } else {
            file_name
        };
        return Self {
            index_name,
            index_value,
            cols,
            file_name,
        };
    }
}

impl ScanFile for ParqProcessor {
    fn scan(&self) -> Result<LazyFrame> {
        return LazyFrame::scan_parquet(&self.file_name, Default::default())
            .with_context(|| format!("File does not exist"));
    }
}

impl Runnable for ParqProcessor {
    fn run(&self) -> Result<LazyFrame> {
        let index_name = &self.index_name;
        let index_value = &self.index_value;

        let lf1 = self.scan()?;
        let exprs = self
            .cols
            .as_ref()
            .map(|values| values.iter().map(|column| col(column)).collect::<Vec<_>>());
        match (index_name, index_value) {
            (Some(idx_name), Some(idx_value)) => {
                let index_expr = get_index_expr_if_needed(idx_name, idx_value)?;
                Ok(filter_columns(lf1, &exprs).filter(index_expr))
            }
            _ => Ok(filter_columns(lf1, &exprs)),
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::frame::DataFrame;
    use polars::io::parquet::ParquetWriter;

    use anyhow::{Context, Ok, Result};
    use polars::prelude::NamedFrom;
    use polars::series::Series;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use crate::generated_test_files_path;

    use super::*;

    fn random_numbers(num_of_results: u32) -> Vec<u32> {
        return [0..num_of_results]
            .iter()
            .map(|_| rand::thread_rng().gen())
            .collect::<Vec<u32>>();
    }

    fn write_test_file(
        test_name: String,
        num_cols: u32,
        required_cols: Option<Vec<String>>,
    ) -> Result<String> {
        let random_file_name = format!("test_{}.parq", test_name);
        let random_file_path = generated_test_files_path!(random_file_name);
        let test_file = std::fs::File::create(&random_file_path)
            .with_context(|| format!("failed to create {} test file", random_file_path))?;
        let mut cols: Vec<String> = if let Some(required_cols) = required_cols {
            required_cols
        } else {
            Vec::<String>::new()
        };
        for _ in 0..num_cols {
            cols.push(
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(10)
                    .map(char::from)
                    .collect(),
            )
        }

        let series = cols
            .iter()
            .map(|column_name| Series::new(column_name, random_numbers(10)))
            .collect::<Vec<_>>();

        let mut df = DataFrame::new(series).unwrap();

        let writer = ParquetWriter::new(test_file);

        writer
            .finish(&mut df)
            .with_context(|| format!("failed to save test df {}", &random_file_path))?;

        return Ok(random_file_path);
    }

    #[test]
    fn test_scan() {
        let test_file_path = write_test_file("parq_processor_scan".to_string(), 10, None);
        assert!(test_file_path.is_ok());
        let test_file_path = test_file_path.unwrap();
        let test_file_path = PathBuf::from(test_file_path);
        let processor = ParqProcessor::new(None, None, None, test_file_path);
        let result = processor.scan();
        assert!(result.is_ok());
        let lazy_frame = result.unwrap();
        assert_eq!(lazy_frame.schema().unwrap().iter_fields().len(), 10); // Replace 0 with the expected number of fields
    }

    #[test]
    fn test_run() {
        let test_file_path = write_test_file("parq_processor_run".to_string(), 10, None);
        assert!(test_file_path.is_ok());
        let test_file_path = test_file_path.unwrap();
        let test_file_path = PathBuf::from(test_file_path);
        let processor = ParqProcessor::new(None, None, None, test_file_path);
        let result = processor.run();
        assert!(result.is_ok());
        let lazy_frame = result.unwrap();
        assert_eq!(lazy_frame.schema().unwrap().iter_fields().len(), 10); // Replace 0 with the expected number of fields
    }
}
