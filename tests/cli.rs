#[cfg(test)]
mod test {
    use assert_cmd::prelude::*; // Add methods on commands
    use predicates::prelude::*; // Used for writing assertions
    use std::{env, process::Command}; // Run programs

    #[macro_export]
    macro_rules! integration_test_files_path {
        ($fname:expr) => {
            format!(
                "{}{}{}",
                env!("CARGO_MANIFEST_DIR"),
                "/tests/resources/",
                $fname
            )
        };
    }

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
    fn file_doesnt_exist() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("wdapty")?;
        cmd.arg("processing")
            .arg("search")
            .arg("--file-name")
            .arg("test/file/doesnt/exist")
            .arg("--index-value")
            .arg("doesnotmatter")
            .arg("--index-name")
            .arg("doesnotmatter");
        cmd.assert()
            .failure()
            .stderr(predicate::str::contains("File does not exist"));

        Ok(())
    }

    #[test]
    fn file_exists() -> Result<(), Box<dyn std::error::Error>> {
        let test_file_path = integration_test_files_path!("test_file1.parq");

        let mut cmd = Command::cargo_bin("wdapty")?;
        cmd.arg("processing")
            .arg("search")
            .arg("--file-name")
            .arg(test_file_path)
            .arg("--index-value")
            .arg("2024-02-01 17:01:00")
            .arg("--index-name")
            .arg("transaction_time");
        cmd.assert().success();

        Ok(())
    }

    #[test]
    fn with_valid_output_file() -> Result<(), Box<dyn std::error::Error>> {
        let test_file_path = integration_test_files_path!("test_file1.parq");
        let output_file = integration_test_results_path!("test_with_output_file.csv");

        let mut cmd = Command::cargo_bin("wdapty")?;
        cmd.arg("processing")
            .arg("download")
            .arg("--file-name")
            .arg(test_file_path)
            .arg("--output-file")
            .arg(output_file);
        cmd.assert().success();

        Ok(())
    }

    #[test]
    fn with_invalid_output_file() -> Result<(), Box<dyn std::error::Error>> {
        let test_file_path = integration_test_files_path!("test_file1.parq");
        let output_file = "random/path/that/doesnot/exist";

        let mut cmd = Command::cargo_bin("wdapty")?;
        cmd.arg("processing")
            .arg("download")
            .arg("--file-name")
            .arg(test_file_path)
            .arg("--output-file")
            .arg(output_file);
        cmd.assert()
            .failure()
            .stderr(predicate::str::contains("Failed to create file"));

        Ok(())
    }

    #[test]
    fn with_valid_cols() -> Result<(), Box<dyn std::error::Error>> {
        let test_file_path = integration_test_files_path!("test_file1.parq");
        let cols = vec!["open".to_string(), "close".to_string()];

        let mut cmd = Command::cargo_bin("wdapty")?;
        cmd.arg("processing")
            .arg("search")
            .arg("--file-name")
            .arg(test_file_path)
            .arg("--index-value")
            .arg("2024-02-01 17:01:00")
            .arg("--index-name")
            .arg("transaction_time")
            .arg("--cols")
            .args(cols);
        cmd.assert().success();

        Ok(())
    }

    #[test]
    fn with_invalid_cols() -> Result<(), Box<dyn std::error::Error>> {
        let test_file_path = integration_test_files_path!("test_file1.parq");
        let cols = vec!["open".to_string(), "donotexist".to_string()];

        let mut cmd = Command::cargo_bin("wdapty")?;
        cmd.arg("processing")
            .arg("search")
            .arg("--file-name")
            .arg(test_file_path)
            .arg("--index-value")
            .arg("2024-02-01 17:01:00")
            .arg("--index-name")
            .arg("transaction_time")
            .arg("--cols")
            .args(cols);
        cmd.assert()
            .failure()
            .stderr(predicate::str::contains("not found: donotexist"));

        Ok(())
    }

    #[test]
    fn with_invalid_index_name() -> Result<(), Box<dyn std::error::Error>> {
        let test_file_path = integration_test_files_path!("test_file1.parq");
        let index_name = "random-column";
        let index_value = "2024-02-01 18:13:00";

        let mut cmd = Command::cargo_bin("wdapty")?;
        cmd.arg("processing")
            .arg("search")
            .arg("--file-name")
            .arg(test_file_path)
            .arg("--index-value")
            .arg(index_value)
            .arg("--index-name")
            .arg(index_name);
        cmd.assert()
            .failure()
            .stderr(predicate::str::contains("not found"));

        Ok(())
    }

    #[test]
    fn with_invalid_index_value() -> Result<(), Box<dyn std::error::Error>> {
        let test_file_path = integration_test_files_path!("test_file1.parq");
        let index_name = "random-column";
        let index_value = "testinvalidindexvalue";

        let mut cmd = Command::cargo_bin("wdapty")?;
        cmd.arg("processing")
            .arg("search")
            .arg("--file-name")
            .arg(test_file_path)
            .arg("--index-value")
            .arg(index_value)
            .arg("--index-name")
            .arg(index_name);
        cmd.assert()
            .failure()
            .stderr(predicate::str::contains("Failed to format index-value"));

        Ok(())
    }
}
