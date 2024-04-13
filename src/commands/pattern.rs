use std::collections::HashMap;

use anyhow::Result;

use super::{does_pattern_match_pattern_format, read_config_file};

pub fn get_patterns_available() -> Result<HashMap<String, String>> {
    Ok(read_config_file()?.lines().filter_map(|line| {
        if does_pattern_match_pattern_format(line) {
            let mut parts = line.splitn(2, "=");
            Some((parts.next()?.trim().to_string(), parts.next()?.trim().to_string()))
        } else {
            None
        }
    }).collect::<HashMap<String, String>>())
}
