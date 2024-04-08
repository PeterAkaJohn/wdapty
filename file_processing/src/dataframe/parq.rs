use std::path::{Path, PathBuf};

use anyhow::{Context, Ok, Result};
use polars::lazy::{dsl::col, frame::LazyFrame};

use super::{expressions::get_index_expr_if_needed, operations::filter_columns, processor::{Runnable, ScanFile}};

pub struct ParqProcessor {
    pub index_name: Option<String>,
    pub index_value: Option<String>,
    pub cols: Option<Vec<String>>,
    pub file_name: PathBuf,
}

impl ScanFile for ParqProcessor {
    fn scan(&self) -> Result<LazyFrame> {
        let file_name = match self.file_name.starts_with("~") {
            true => {
                let expanded_path = shellexpand::tilde(
                    self.file_name
                        .as_os_str()
                        .to_str()
                        .expect("Failed in casting file_name to string"),
                )
                .to_string();
                Path::new(&expanded_path).to_path_buf()
            },
            false => {
                self.file_name.clone()
            },
        };
        return LazyFrame::scan_parquet(&file_name, Default::default()).with_context(|| format!("File does not exist"))
    }
}

impl Runnable for ParqProcessor {
    fn run(&self) -> Result<LazyFrame> {
        let index_name = &self.index_name;
        let index_value = &self.index_value;

        let lf1 = self.scan()?;
        let exprs = self.cols.as_ref().map(|values| values.iter().map(|column| col(column)).collect::<Vec<_>>());
        match (index_name, index_value) {
            (Some(idx_name), Some(idx_value)) => {
                let index_expr = get_index_expr_if_needed(idx_name, idx_value)?;
                Ok(filter_columns(lf1, &exprs).filter(index_expr))
            }
            _ => Ok(filter_columns(lf1, &exprs)),
        }
    }
}


