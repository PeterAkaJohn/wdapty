use std::{
    fs::{self, create_dir_all, File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use anyhow::{anyhow, Context, Result};

use super::pattern::does_pattern_match_pattern_format;

pub fn expand_config_path(path: &str) -> Result<PathBuf> {
    return shellexpand::tilde(path)
        .parse::<PathBuf>()
        .with_context(|| format!("Failed to expand config path {}", path));
}

pub fn read_config_file() -> Result<String> {
    let config_path = expand_config_path("~/.wdapty/config.ini")?;
    Ok(fs::read_to_string(config_path)?)
}

pub fn get_config_file(file_name: Option<&str>) -> Result<(String, File)> {
    let app_config_path = if let Some(f_name) = file_name {
        f_name
    } else {
        "~/.wdapty/config.ini"
    };
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

pub fn save_vec_of_string_to_config(patterns: Vec<String>, mut file: File) -> Result<bool> {
    for pattern in patterns {
        save_string_to_config(pattern, &mut file)?;
    }

    Ok(true)
}

pub fn save_string_to_config_with_overwrite(patterns: Vec<String>, file: File) -> Result<bool> {
    let _ = file.set_len(0);
    let _ = save_vec_of_string_to_config(patterns, file);
    Ok(true)
}

pub fn save_string_to_config(pattern: String, file: &mut File) -> Result<(), anyhow::Error> {
    if does_pattern_match_pattern_format(&pattern) {
        println!("Saving pattern {} to config.ini", pattern);
        file.write(format!("{}\n", pattern).as_bytes())
            .with_context(|| format!("Failed to save pattern {}", pattern))?;
        Ok(())
    } else {
        Err(anyhow!(
            "Pattern {} is not compliant with pattern_format name=value",
            pattern
        ))
    }
}

#[cfg(test)]
mod test {
    use super::get_config_file;

    #[macro_export]
    macro_rules! integration_test_results_path {
        ($fname:expr) => {
            format!(
                "{}{}{}",
                env!("CARGO_MANIFEST_DIR"),
                "/tests/results/",
                $fname
            )
        };
    }

    #[test]
    fn test_get_config_file() {
        let config_path = integration_test_results_path!("config.ini");
        let result = get_config_file(Some(&config_path));
        assert!(result.is_ok())
    }
}
