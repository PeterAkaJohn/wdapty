use std::path::PathBuf;

use anyhow::Result;
use polars::frame::DataFrame;

#[derive(Debug)]
pub struct Processor {
    pub index_name: Option<String>,
    pub index_value: Option<String>,
    pub cols: Option<Vec<String>>,
    pub file_name: PathBuf,
    pub output_file: Option<String>,
}

pub trait Runnable {
    // returns the file name
    fn run(&self) -> Result<DataFrame>;
}
