use anyhow::Result;
use dataframe::{parq::ParqProcessor, processor::Runnable};
use polars::lazy::frame::LazyFrame;

pub mod dataframe;

pub enum Processors<'a> {
    Parq(ParqProcessor<'a>),
}

impl Runnable for Processors<'_> {
    fn run(&self) -> Result<LazyFrame> {
        match self {
            Processors::Parq(parq_processor) => return parq_processor.run(),
        }
    }
}
