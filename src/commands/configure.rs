use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{anyhow, Context, Ok, Result};
use regex::Regex;

pub fn create_config_file() -> Result<(String, File)> {
    let app_config_path = "~/.wdapty/config.ini";
    let expanded_path: PathBuf = shellexpand::tilde(app_config_path).to_string().into();
    let prefix = expanded_path
        .parent()
        .with_context(|| format!("Failed to extract parent from {}", expanded_path.display()))?;
    create_dir_all(prefix)
        .with_context(|| format!("Failed to create config dir {}", prefix.display()))?;
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&expanded_path)
        .with_context(|| format!("Failed at File::Create {}", &app_config_path))?;
    Ok((app_config_path.to_string(), file))
}

pub fn save_patterns_to_config(patterns: Vec<String>, mut file: File) -> Result<bool> {
    for pattern in patterns {
        if does_pattern_match_pattern_format(&pattern) {
            println!("Saving pattern {} to config.ini", pattern);
            file.write(format!("{}\n", pattern).as_bytes())
                .with_context(|| format!("Failed to save pattern {}", pattern))?;
        } else {
            return Err(anyhow!("Pattern {} is not compliant with pattern_format name=value", pattern));
        }
    }

    Ok(true)
}

fn does_pattern_match_pattern_format(pattern: &str) -> bool {
    let pattern_format = Regex::new(r"^\w+=\w+$").unwrap();
    return pattern_format.is_match(pattern)
}

pub fn initialize(starting_patterns: Option<Vec<String>>) -> Result<String> {
    let (app_config_path, file) = create_config_file()?;
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

    save_patterns_to_config(patterns, file)?;

    Ok(app_config_path)
}
