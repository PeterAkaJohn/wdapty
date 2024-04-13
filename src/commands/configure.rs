use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{Context, Ok, Result};

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
        file.write(format!("{}\n", pattern).as_bytes())
            .with_context(|| format!("Failed to save pattern {}", pattern))?;
    }

    Ok(true)
}

pub fn initialize() -> Result<String> {
    let (app_config_path, file) = create_config_file()?;
    println!("Fill in your config with yout pattern, must be in the format name=something, type x to exit");
    let mut patterns: Vec<String> = vec![];
    loop {
        let mut input = String::new();
        println!("Paste your file pattern that you want to use one at the time");
        let _ = io::stdin().read_line(&mut input);
        if input.trim() == "x" {
            break;
        }
        patterns.push(input.trim().to_string());
    }

    save_patterns_to_config(patterns, file)?;

    Ok(app_config_path)
}
