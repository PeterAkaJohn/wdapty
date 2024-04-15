use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use polars::lazy::dsl::col;

use super::datetime::to_datetime_expression;

pub fn get_index_expr_if_needed(
    index_name: &str,
    index_value: &str,
) -> Result<polars::lazy::dsl::Expr> {
    let index_value_dt =
        to_datetime_expression(&index_value).with_context(|| "Failed to format index-value");
    match index_value_dt {
        Ok(dt_value_expr) => {
            let index_expr = col(&index_name).eq(dt_value_expr);
            return Ok(index_expr);
        }
        Err(e) => Err(anyhow!(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::lazy::dsl::Expr;

    #[test]
    fn test_get_index_expr_if_needed_success() {
        let index_name = "random_datetime_column";
        let index_value = "2024-02-01 17:02:00";

        let result = get_index_expr_if_needed(index_name, index_value);

        match result {
            Ok(Expr::BinaryExpr { .. }) => assert!(true),
            _ => assert!(false, "Expected an Expr"),
        }
    }

    #[test]
    fn test_get_index_expr_if_needed_failure_if_value_wrong() {
        let index_name = "random_datetime_column";
        let index_value = "2024-02- 17:02:00";

        let result = get_index_expr_if_needed(index_name, index_value);

        assert!(result.is_err(), "Expected None");
    }
}
