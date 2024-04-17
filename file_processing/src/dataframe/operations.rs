use polars::lazy::{
    dsl::{col, Expr},
    frame::LazyFrame,
};

pub fn filter_columns(df: LazyFrame, columns: &Option<Vec<Expr>>) -> LazyFrame {
    if let Some(exprs) = columns {
        df.select(exprs)
    } else {
        df.select([col("*")])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_filter_columns() {
        // Create a DataFrame for testing
        let df = DataFrame::new(vec![
            Series::new("A", &[1, 2, 3]),
            Series::new("B", &[4, 5, 6]),
            Series::new("C", &[7, 8, 9]),
        ])
        .unwrap();

        // Create a LazyFrame from the DataFrame
        let lazy_df = df.lazy();

        let df = DataFrame::new(vec![
            Series::new("A", &[1, 2, 3]),
            Series::new("B", &[4, 5, 6]),
            Series::new("C", &[7, 8, 9]),
        ])
        .unwrap();

        // Test case 1: Filter specific columns
        let columns = Some(vec![col("A"), col("C")]);
        let filtered_df = filter_columns(lazy_df.clone(), &columns);
        let expected_df = DataFrame::new(vec![
            Series::new("A", &[1, 2, 3]),
            Series::new("C", &[7, 8, 9]),
        ])
        .unwrap();
        assert_eq!(filtered_df.collect().unwrap(), expected_df);

        // Test case 2: Filter all columns
        let columns = None;
        let filtered_df = filter_columns(lazy_df.clone(), &columns).collect().unwrap();
        assert_eq!(filtered_df, df);
    }
}
