use std::{fs::{create_dir_all, File}, path::PathBuf};

use anyhow::{Context, Ok, Result};

pub fn initialize() -> Result<String> {
    let app_config_path = "~/.wdapty/config.ini";
    let expanded_path: PathBuf = shellexpand::tilde(app_config_path).to_string().into();
    let prefix = expanded_path.parent().with_context(|| format!("Failed to extract parent from {}", expanded_path.display()))?;
    create_dir_all(prefix).with_context(|| format!("Failed to create config dir {}", prefix.display()))?;
    let _ = File::create(expanded_path).with_context(|| format!("Failed at File::Create {}", &app_config_path));
    Ok(app_config_path.to_string())
}