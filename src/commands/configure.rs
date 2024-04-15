use std::io::{self};

use anyhow::{Ok, Result};

use super::config::{get_config_file, save_vec_of_string_to_config};

pub fn initialize(starting_patterns: Option<Vec<String>>) -> Result<String> {
    let (app_config_path, file) = get_config_file(None)?;
    let patterns: Vec<String> = if let Some(st_patterns) = starting_patterns {
        st_patterns
    } else {
        println!("Fill in your config with yout pattern, must be in the format name=something, type x to exit");
        let mut new_patterns = vec![];
        loop {
            let mut input = String::new();
            println!("Paste your file pattern that you want to use one at the time");
            let _ = io::stdin().read_line(&mut input);
            if input.trim() == "x" {
                break;
            }
            new_patterns.push(input.trim().to_string());
        }
        new_patterns
    };

    save_vec_of_string_to_config(patterns, file)?;

    Ok(app_config_path)
}
