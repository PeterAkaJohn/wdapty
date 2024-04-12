use anyhow::{anyhow, Result};
use polars::lazy::dsl::{datetime, lit, DatetimeArgs, Expr};

pub fn to_datetime_expression(value: &str) -> Result<Expr> {
    let DateParts(year, month, day, hour, minute, second): DateParts = extract_date_parts(value)?;

    Ok(datetime(
        DatetimeArgs::new(lit(year), lit(month), lit(day)).with_hms(
            lit(hour),
            lit(minute),
            lit(second),
        ),
    ))
}

struct DateParts(i32, i32, i32, i32, i32, i32);

fn extract_date_parts(value: &str) -> Result<DateParts> {
    let (date_parts, time_parts) = value
        .split_once(" ")
        .ok_or_else(|| anyhow::anyhow!("Invalid datetime format"))?;
    let date_parts: Vec<&str> = date_parts.split("-").collect();
    let time_parts: Vec<&str> = time_parts.split(":").collect();

    if date_parts.len() != 3 || time_parts.len() != 3 {
        return Err(anyhow!(
            "Wrong datetime format. Needs to be 'YYYY-MM-DD hh-mm-ss'"
        ));
    }

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
    Ok(DateParts(year, month, day, hour, minute, second))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn check_extract_date_parts() {
        let DateParts(year, month, day, hour, minute, second) =
            extract_date_parts("2024-02-01 17:02:00").expect("Value should be parsed correctly");
    
        assert_eq!(year, 2024);
        assert_eq!(month, 02);
        assert_eq!(day, 01);
        assert_eq!(hour, 17);
        assert_eq!(minute, 02);
        assert_eq!(second, 00);

    }

    #[test]
    fn check_extract_date_parts_throws() {
        assert!(extract_date_parts("2024-01 17:02:00").is_err());
        assert!(extract_date_parts("2024-02-01 sadsd:02:00").is_err());
        assert!(extract_date_parts("2024-01-01 17:02").is_err());
        assert!(extract_date_parts("2024-01-01 17:02:").is_err());
        assert!(extract_date_parts("2024-01 17:02").is_err());
        assert!(extract_date_parts("2024-01 17:02:").is_err());
    }
    
}

