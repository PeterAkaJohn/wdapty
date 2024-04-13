use anyhow::{anyhow, Result};
use core::result::Result::Ok;
use regex::Regex;
use std::{collections::HashMap, fs, io};

fn parse_config_file_for_pattern() -> Result<String> {
    // will create a command that generates this, for now it's in the root
    let config_path = "./wdapty.ini";
    let file = fs::read_to_string(config_path)?;

    let pattern = file
        .lines()
        .filter_map(|line| {
            if line.contains("pattern") {
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
        .get("pattern")
        .map(|pat| pat.to_string())
        .ok_or(anyhow!("No property named pattern in wdapty.ini"));
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

pub fn handle_pattern() -> Result<String> {
    let pattern = parse_config_file_for_pattern();
    if let Ok(pat) = pattern {
        let variables_to_ask = collect_user_input_from_pattern(&pat);
        let user_filled_variables = ask_user_variables_value(variables_to_ask);
        return Ok(fill_pattern_with_variables(&pat, user_filled_variables));
    }
    return Err(anyhow!("Failed in handling pattern"));
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::commands::{collect_user_input_from_pattern, fill_pattern_with_variables};
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
}

pub mod configure;