use std::collections::HashMap;

use anyhow::{anyhow, Result};

use super::{
    configure::get_config_file, does_pattern_match_pattern_format, read_config_file,
    save_pattern_to_config, save_patterns_to_config_with_overwrite,
};

pub fn get_available_patterns() -> Result<HashMap<String, String>> {
    Ok(read_config_file()?
        .lines()
        .filter_map(|line| {
            if does_pattern_match_pattern_format(line) {
                let mut parts = line.splitn(2, "=");
                Some((
                    parts.next()?.trim().to_string(),
                    parts.next()?.trim().to_string(),
                ))
            } else {
                None
            }
        })
        .collect::<HashMap<String, String>>())
}

pub fn add_pattern_to_config(name: String, value: String) -> Result<()> {
    let available_patterns = get_available_patterns();
    if let Ok(available) = available_patterns {
        if let Some(_) = available.get_key_value(&name) {
            return Err(anyhow!(
                "Pattern supplied with name {} is already in config",
                &name
            ));
        }
    }
    let (_, mut file) = get_config_file()?;
    save_pattern_to_config(format!("{}={}", name, value), &mut file)
}

pub fn remove_pattern_from_config(name: String) -> Result<()> {
    let content = read_config_file()?;
    let new_file_content = content
        .lines()
        .filter_map(|line| {
            if line.contains(&format!("{}=", name.trim()).trim()) {
                None
            } else {
                Some(line.to_string())
            }
        })
        .collect::<Vec<String>>();
    let (_, file) = get_config_file()?;
    let _ = save_patterns_to_config_with_overwrite(new_file_content, file);
    Ok(())
}
