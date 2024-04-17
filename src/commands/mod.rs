pub mod config;
pub mod configure;
pub mod input;
pub mod pattern;
use anyhow::{Context, Result};
use regex::Regex;
use std::{collections::HashMap, io, path::PathBuf};

use self::{config::read_config_file, pattern::handle_pattern};

fn collect_user_input_from_string(value: &str) -> Vec<String> {
    let re = Regex::new(r"\{(.*?)\}").unwrap();
    let variables_to_ask = re
        .captures_iter(value)
        .map(|cap| cap[1].to_string())
        .collect::<Vec<_>>();
    variables_to_ask
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

fn replace_string_variables_with_value(
    string_with_variables: &str,
    user_input: HashMap<String, String>,
) -> String {
    let mut result = string_with_variables.to_string();
    for (key, value) in user_input {
        result = result.replace(&format!("{{{}}}", key), value.trim());
    }
    result
}

pub fn acquire_file_name(
    pattern: Option<String>,
    file_name: Option<PathBuf>,
) -> Result<PathBuf, anyhow::Error> {
    let file_name = if let Some(pat) = pattern {
        let file_name_from_pattern: PathBuf = handle_pattern(pat.as_str())?.into();
        Some(file_name_from_pattern)
    } else {
        file_name
    }
    .with_context(|| {
        "file name should be valued by option or by setting pattern and reading file"
    })?;
    Ok(file_name)
}

pub trait RunCommand {
    fn run(self) -> Result<()>;
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::commands::{collect_user_input_from_string, replace_string_variables_with_value};

    #[test]
    fn test_collect_user_input_from_pattern() {
        let pattern = "{test1}/{test2}/{test3}";
        let result = collect_user_input_from_string(&pattern);
        assert_eq!(result.concat(), "test1test2test3");
    }

    #[test]
    fn test_fill_pattern_with_variable() {
        let pattern = "{test1}/{test2}/{test3}";
        let mut user_input: HashMap<String, String> = HashMap::new();
        user_input.insert("test1".to_string(), "test1value".to_string());
        user_input.insert("test2".to_string(), "test2value".to_string());
        user_input.insert("test3".to_string(), "test3value".to_string());
        let result = replace_string_variables_with_value(pattern, user_input);
        assert_eq!(result, "test1value/test2value/test3value");
    }
}
