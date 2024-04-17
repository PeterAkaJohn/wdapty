use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use regex::Regex;

use super::{
    ask_user_variables_value, collect_user_input_from_string,
    config::{get_config_file, save_string_to_config, save_string_to_config_with_overwrite},
    read_config_file, replace_string_variables_with_value,
};

pub fn get_available_patterns() -> Result<HashMap<String, String>> {
    let delimiter = '=';
    let patterns = read_config_file()?
        .lines()
        .filter_map(|line| {
            if does_pattern_match_pattern_format(line) {
                let mut parts = line.splitn(2, delimiter);
                Some((
                    parts.next()?.trim().to_string(),
                    parts.next()?.trim().to_string(),
                ))
            } else {
                None
            }
        })
        .collect::<HashMap<String, String>>();
    Ok(patterns)
}

pub fn add_pattern_to_config(name: String, value: String) -> Result<()> {
    let available_patterns = get_available_patterns();
    if let Ok(available) = available_patterns {
        if available.get_key_value(&name).is_some() {
            return Err(anyhow!(
                "Pattern supplied with name {} is already in config",
                &name
            ));
        }
    }
    let (_, mut file) = get_config_file(None)?;
    save_string_to_config(format!("{}={}", name, value), &mut file)
}

pub fn remove_pattern_from_config(name: String) -> Result<()> {
    let content = read_config_file()?;
    let new_file_content = content
        .lines()
        .filter_map(|line| {
            if line.contains(format!("{}=", name.trim()).trim()) {
                None
            } else {
                Some(line.to_string())
            }
        })
        .collect::<Vec<String>>();
    let (_, file) = get_config_file(None)?;
    let _ = save_string_to_config_with_overwrite(new_file_content, file);
    Ok(())
}

pub fn handle_pattern(pattern_name: &str) -> Result<String> {
    parse_config_file_for_pattern(pattern_name)
        .map(|pat| {
            let variables_to_ask = collect_user_input_from_string(&pat);
            let user_filled_variables = ask_user_variables_value(variables_to_ask);
            replace_string_variables_with_value(&pat, user_filled_variables)
        })
        .with_context(|| format!("Failed to handle pattern {}", pattern_name))
}

fn parse_config_file_for_pattern(pattern_name: &str) -> Result<String> {
    let file = read_config_file()?;
    let delimiter = '=';
    let pattern = file
        .lines()
        .filter_map(|line| {
            if line.contains(pattern_name) {
                let mut line_parts = line.splitn(2, delimiter);
                let property = line_parts.next()?.trim().to_string();
                let value = line_parts.next()?.trim().to_string();
                Some((property, value))
            } else {
                None
            }
        })
        .collect::<HashMap<String, String>>();

    pattern
        .get(pattern_name)
        .map(|pat| pat.to_string())
        .ok_or(anyhow!("No property named {} in wdapty.ini", pattern_name))
}

pub fn does_pattern_match_pattern_format(pattern: &str) -> bool {
    let pattern_format = Regex::new(r"^[A-Za-z0-9_\-.]+=([^\s\n]+)$").unwrap();
    pattern_format.is_match(pattern)
}

#[test]
fn test_does_pattern_match_pattern_format() {
    let pattern = "amazingtestname=s3://somethingsomthineasdas//asas//assa/{asdasd}/{asdas}";
    assert!(does_pattern_match_pattern_format(pattern));

    let pattern = "amazingtestname=s3://somethingsomthin easdas//asas//assa/{asdasd}/{asdas}";
    assert!(does_pattern_match_pattern_format(pattern) == false);

    let pattern = "amazingtestname={PATH}/test.parq";
    assert!(does_pattern_match_pattern_format(pattern));

    let pattern = "amazing-test-name={PATH}/test.parq";
    assert!(does_pattern_match_pattern_format(pattern));
}
