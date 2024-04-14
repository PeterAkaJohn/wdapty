pub mod configure;
pub mod pattern;
use anyhow::{anyhow, Context, Result};
use regex::Regex;
use std::{collections::HashMap, fs::{self, File}, io::{self, Write}, path::PathBuf};

fn expand_config_path(path: &str) -> Result<PathBuf> {
    return shellexpand::tilde(path)
        .parse::<PathBuf>()
        .with_context(|| format!("Failed to expand config path {}", path));
}

fn read_config_file() -> Result<String> {
    let config_path = expand_config_path("~/.wdapty/config.ini")?;
    Ok(fs::read_to_string(config_path)?)
}

fn does_pattern_match_pattern_format(pattern: &str) -> bool {
    let pattern_format = Regex::new(r"^\w+=([^\s\n]+)$").unwrap();
    return pattern_format.is_match(pattern)
}

fn parse_config_file_for_pattern(pattern_name: &str) -> Result<String> {
    let file = read_config_file()?;
    let pattern = file
        .lines()
        .filter_map(|line| {
            if line.contains(pattern_name) {
                let mut line_parts = line.splitn(2, "=");
                let property = line_parts.next()?.trim().to_string();
                let value = line_parts.next()?.trim().to_string();
                Some((property, value))
            } else {
                None
            }
        })
        .collect::<HashMap<String, String>>();

    return pattern
        .get(pattern_name)
        .map(|pat| pat.to_string())
        .ok_or(anyhow!("No property named {} in wdapty.ini", pattern_name));
}

fn collect_user_input_from_pattern(pattern: &str) -> Vec<String> {
    let re = Regex::new(r"\{(.*?)\}").unwrap();
    let variables_to_ask = re
        .captures_iter(pattern)
        .map(|cap| cap[1].to_string())
        .collect::<Vec<_>>();
    return variables_to_ask;
}

fn ask_user_variables_value(variables_to_ask: Vec<String>) -> HashMap<String, String> {
    return variables_to_ask
        .iter()
        .map(|var_key| {
            let mut input_string = String::new();
            println!("Please type value for {}:", var_key);
            io::stdin().read_line(&mut input_string).unwrap();
            (var_key.to_string(), input_string)
        })
        .collect::<HashMap<String, String>>();
}

fn fill_pattern_with_variables(pattern: &str, user_input: HashMap<String, String>) -> String {
    let mut result = pattern.to_string();
    for (key, value) in user_input {
        result = result.replace(&format!("{{{}}}", key), &value.trim());
    }
    return result;
}

pub fn save_patterns_to_config(patterns: Vec<String>, mut file: File) -> Result<bool> {
    for pattern in patterns {
        save_pattern_to_config(pattern, &mut file)?;
    }

    Ok(true)
}

fn save_pattern_to_config(pattern: String, file: &mut File) -> Result<(), anyhow::Error> {
    Ok(if does_pattern_match_pattern_format(&pattern) {
        println!("Saving pattern {} to config.ini", pattern);
        file.write(format!("{}\n", pattern).as_bytes())
            .with_context(|| format!("Failed to save pattern {}", pattern))?;
    } else {
        return Err(anyhow!("Pattern {} is not compliant with pattern_format name=value", pattern));
    })
}

pub fn handle_pattern(pattern_name: &str) -> Result<String> {
    return parse_config_file_for_pattern(pattern_name).map(|pat| {
        let variables_to_ask = collect_user_input_from_pattern(&pat);
        let user_filled_variables = ask_user_variables_value(variables_to_ask);
        fill_pattern_with_variables(&pat, user_filled_variables)
    }).with_context(|| format!("Failed to handle pattern {}", pattern_name));
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::commands::{collect_user_input_from_pattern, fill_pattern_with_variables};

    use super::does_pattern_match_pattern_format;
    #[test]
    fn test_collect_user_input_from_pattern() {
        let pattern = "{test1}/{test2}/{test3}";
        let result = collect_user_input_from_pattern(&pattern);
        assert_eq!(result.concat(), "test1test2test3");
    }

    #[test]
    fn test_fill_pattern_with_variable() {
        let pattern = "{test1}/{test2}/{test3}";
        let mut user_input: HashMap<String, String> = HashMap::new();
        user_input.insert("test1".to_string(), "test1value".to_string());
        user_input.insert("test2".to_string(), "test2value".to_string());
        user_input.insert("test3".to_string(), "test3value".to_string());
        let result = fill_pattern_with_variables(pattern, user_input);
        assert_eq!(result, "test1value/test2value/test3value");
    }

    #[test]
    fn test_does_pattern_match_pattern_format() {
        let pattern = "amazingtestname=s3://somethingsomthineasdas//asas//assa/{asdasd}/{asdas}";
        assert!(does_pattern_match_pattern_format(pattern));

        let pattern = "amazingtestname=s3://somethingsomthin easdas//asas//assa/{asdasd}/{asdas}";
        assert!(does_pattern_match_pattern_format(pattern) == false);
    }
}
