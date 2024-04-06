use polars::lazy::{dsl::{col, Expr}, frame::LazyFrame};

pub fn filter_columns(df: LazyFrame, columns: &Option<Vec<Expr>>) -> LazyFrame {
    if let Some(exprs) = columns {
        return df.select(exprs);
    } else {
        return df.select([col("*")]);
    }
}
