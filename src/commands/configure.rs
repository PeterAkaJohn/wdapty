use std::io::{self};

use anyhow::Result;

use crate::commands::input::request_user_input;

use super::config::{get_config_file, save_vec_of_string_to_config};

pub fn initialize(starting_patterns: Option<Vec<String>>) -> Result<String> {
    let (app_config_path, file) = get_config_file(None)?;
    let patterns: Vec<String> = if let Some(st_patterns) = starting_patterns {
        st_patterns
    } else {
        ask_user_for_patterns()
    };

    save_vec_of_string_to_config(patterns, file)?;

    Ok(app_config_path)
}

fn ask_user_for_patterns() -> Vec<String> {
    println!("Fill in your config with yout pattern, must be in the format name=something, type x to exit");
    let mut new_patterns = vec![];
    let prompt = "Paste your file pattern that you want to use one at the time";
    loop {
        let input = request_user_input(prompt, io::stdin().lock(), io::stdout());
        if let Ok(input_string) = input {
            let input_string = input_string.trim();
            if input_string == "x" {
                break;
            }
            new_patterns.push(input_string.to_string());
        }
    }
    new_patterns
}
