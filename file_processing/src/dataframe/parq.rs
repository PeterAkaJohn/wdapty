use std::path::PathBuf;

use super::{
    expressions::get_index_expr_if_needed,
    file::{HandleOutput, ScanFile},
    operations::filter_columns,
    processor::Runnable,
};
use anyhow::{anyhow, Context, Ok, Result};
use cloud::AmazonS3ConfigKey as Key;
use credentials::get_credentials;
use polars::{
    frame::DataFrame,
    io::{cloud, csv::CsvWriter, SerWriter},
    lazy::{
        dsl::col,
        frame::{LazyFrame, ScanArgsParquet},
    },
};

pub struct ParqProcessor<'a> {
    pub index_name: Option<String>,
    pub index_value: Option<String>,
    pub cols: Option<Vec<String>>,
    pub file_name: PathBuf,
    pub profile: Option<&'a str>,
    output_file: Option<String>,
}

impl<'a> ParqProcessor<'a> {
    pub fn new(
        index_name: Option<String>,
        index_value: Option<String>,
        cols: Option<Vec<String>>,
        file_name: PathBuf,
        profile: Option<&'a str>,
        output_file: Option<String>,
    ) -> Self {
        let file_name = if file_name.starts_with("~") {
            let expanded_path =
                shellexpand::tilde(&file_name.to_string_lossy().into_owned()).to_string();
            PathBuf::from(expanded_path)
        } else {
            file_name
        };
        Self {
            index_name,
            index_value,
            cols,
            file_name,
            profile,
            output_file,
        }
    }
}

impl ScanFile for ParqProcessor<'_> {
    fn scan(&self) -> Result<LazyFrame> {
        if self.file_name.starts_with("s3://") {
            let credentials = get_credentials("aws", self.profile, None)?;
            let cloud_options = cloud::CloudOptions::default().with_aws([
                (Key::AccessKeyId, &credentials.access_key_id),
                (Key::SecretAccessKey, &credentials.secret_access_key),
                (Key::Region, &credentials.region),
                (Key::Token, &credentials.session_token),
            ]);
            let args = ScanArgsParquet {
                cloud_options: Some(cloud_options),
                ..Default::default()
            };
            LazyFrame::scan_parquet(&self.file_name, args).with_context(|| {
                "File does not exist. Might need to pass --profile option".to_string()
            })
        } else {
            LazyFrame::scan_parquet(&self.file_name, Default::default())
                .with_context(|| "File does not exist".to_string())
        }
    }
}

impl Runnable for ParqProcessor<'_> {
    fn run(&self) -> Result<DataFrame> {
        let index_name = &self.index_name;
        let index_value = &self.index_value;

        let lf1 = self.scan()?;
        let exprs = self
            .cols
            .as_ref()
            .map(|values| values.iter().map(|column| col(column)).collect::<Vec<_>>());
        let lf1 = match (index_name, index_value) {
            (Some(idx_name), Some(idx_value)) => {
                let index_expr = get_index_expr_if_needed(idx_name, idx_value)?;
                let lf1 = filter_columns(lf1.filter(index_expr), &exprs);
                Ok(lf1)
            }
            (None, Some(_)) | (Some(_), None) => Err(anyhow!(
                "Search failed. Either index-name or index-value is missing"
            )),
            _ => Ok(filter_columns(lf1, &exprs)),
        };

        self.handle(lf1?.collect()?)
    }
}

impl HandleOutput for ParqProcessor<'_> {
    fn handle(&self, mut df: DataFrame) -> Result<DataFrame> {
        if let Some(output_file_path) = self.output_file.to_owned() {
            let file = std::fs::File::create(&output_file_path)
                .with_context(|| anyhow!("Failed to create file"))?;
            let mut writer = CsvWriter::new(file);
            writer
                .finish(&mut df)
                .with_context(|| anyhow!("Failed to write csv output file"))?;
            println!("Results are available in {}", &output_file_path);
        } else {
            println!("{}", df);
        }
        Ok(df)
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
        let processor = ParqProcessor::new(None, None, None, test_file_path, None, None);
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
        let processor = ParqProcessor::new(None, None, None, test_file_path, None, None);
        let result = processor.run();
        assert!(result.is_ok());
        let lazy_frame = result.unwrap();
        assert_eq!(lazy_frame.schema().iter_fields().len(), 10); // Replace 0 with the expected number of fields
    }
}
