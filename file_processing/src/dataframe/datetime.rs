use anyhow::Result;
use polars::lazy::dsl::{datetime, lit, DatetimeArgs, Expr};

pub fn create_datetime(value: &str) -> Result<Expr> {
    let (date_parts, time_parts) = value
        .split_once(" ")
        .ok_or_else(|| anyhow::anyhow!("Invalid datetime format"))?;
    let date_parts: Vec<&str> = date_parts.split("-").collect();
    let time_parts: Vec<&str> = time_parts.split(":").collect();

    let year = date_parts[0]
        .parse::<i32>()
        .map_err(|_| anyhow::anyhow!("Failed to parse year"))?;
    let month = date_parts[1]
        .parse::<i32>()
        .map_err(|_| anyhow::anyhow!("Failed to parse month"))?;
    let day = date_parts[2]
        .parse::<i32>()
        .map_err(|_| anyhow::anyhow!("Failed to parse day"))?;
    let hour = time_parts[0]
        .parse::<i32>()
        .map_err(|_| anyhow::anyhow!("Failed to parse hour"))?;
    let minute = time_parts[1]
        .parse::<i32>()
        .map_err(|_| anyhow::anyhow!("Failed to parse minute"))?;
    let second = time_parts[2]
        .parse::<i32>()
        .map_err(|_| anyhow::anyhow!("Failed to parse second"))?;

    Ok(datetime(
        DatetimeArgs::new(lit(year), lit(month), lit(day)).with_hms(
            lit(hour),
            lit(minute),
            lit(second),
        ),
    ))
}