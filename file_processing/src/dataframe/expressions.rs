use anyhow::Context;
use polars::lazy::dsl::Expr;
use polars::lazy::dsl::col;

use super::datetime::to_datetime_expression;

pub fn get_index_expr_if_needed(
    index_name: &Option<String>,
    index_value: &Option<String>,
) -> Option<polars::lazy::dsl::Expr> {
    return match (index_name, index_value) {
        (Some(index_n), Some(index_val)) => {
            let index_value_dt: Expr = to_datetime_expression(&index_val)
                .with_context(|| "Failed to format index-value")
                .unwrap();
            let index_expr = col(&index_n).eq(index_value_dt);
            Some(index_expr)
        }
        _ => None,
    };
}
