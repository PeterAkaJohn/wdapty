use anyhow::Result;
use polars::{frame::DataFrame, lazy::frame::LazyFrame};

pub trait ScanFile {
    fn scan(&self) -> Result<LazyFrame>;
}

pub trait HandleOutput {
    fn handle(&self, df: DataFrame) -> Result<DataFrame>;
}
