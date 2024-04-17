use std::{collections::HashMap, env, fs};

use anyhow::{anyhow, Context};
use regex::Regex;

use super::provider::ParseCredentials;

pub struct Aws<'a> {
    profile: &'a str,
    credentials_path: String,
}

#[derive(Debug)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub region: String,
}

impl<'a> Aws<'a> {
    pub fn new(profile: Option<&'a str>, credentials_path: Option<String>) -> Self {
        let profile = profile.unwrap_or("default");
        let credentials_path = credentials_path.unwrap_or_else(|| {
            shellexpand::tilde("~/.aws/credentials")
                .parse::<String>()
                .unwrap()
        });
        Aws {
            profile,
            credentials_path,
        }
    }

    fn extract_credentials_from_file(&self, file: String) -> HashMap<String, String> {
        let mut parsing = false;
        let property_delimiter = '=';
        let re = Regex::new(r"\[.*\]").unwrap();
        let credentials: HashMap<String, String> = file
            .lines()
            .filter_map(|line| {
                if line.contains(&format!("[{}]", self.profile)) {
                    parsing = true;
                } else if re.is_match(line) {
                    parsing = false;
                }
                if parsing && !line.is_empty() {
                    let mut parts = line.trim().splitn(2, property_delimiter);
                    let property = parts.next()?.trim().to_string();
                    let value = parts.next()?.trim().to_string();
                    Some((property, value))
                } else {
                    None
                }
            })
            .collect();

        credentials
    }

    fn extract_credentials_from_env(&self) -> HashMap<String, String> {
        let mut credentials = HashMap::new();
        if let Ok(access_key_id) = env::var("AWS_ACCESS_KEY_ID") {
            credentials.insert("aws_access_key_id".to_string(), access_key_id);
        }
        if let Ok(secret_access_key) = env::var("AWS_SECRET_ACCESS_KEY") {
            credentials.insert("aws_secret_access_key".to_string(), secret_access_key);
        }
        if let Ok(session_token) = env::var("AWS_SESSION_TOKEN") {
            credentials.insert("aws_session_token".to_string(), session_token);
        }
        if let Ok(region) = env::var("AWS_REGION") {
            credentials.insert("region".to_string(), region);
        }
        credentials
    }
}

impl<'a> ParseCredentials<AwsCredentials> for Aws<'a> {
    fn parse(&self) -> anyhow::Result<AwsCredentials> {
        let file = fs::read_to_string(&self.credentials_path)
            .with_context(|| format!("File does not exist"))?;
        let file_credentials = self.extract_credentials_from_file(file);
        let env_credentials = self.extract_credentials_from_env();
        let credentials = file_credentials
            .into_iter()
            .chain(env_credentials)
            .collect::<HashMap<_, _>>();
        let access_key_id = credentials.get("aws_access_key_id");
        let secret_access_key = credentials.get("aws_secret_access_key");
        let session_token = credentials.get("aws_session_token");
        let region = credentials.get("region");
        match (access_key_id, secret_access_key, session_token, region) {
            (Some(key_id), Some(secret), Some(session_token), Some(region)) => Ok(AwsCredentials {
                access_key_id: key_id.to_string(),
                secret_access_key: secret.to_string(),
                session_token: session_token.to_string(),
                region: region.to_string(),
            }),
            _ => Err(anyhow!("Missing aws credentials")),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{env, fs::File, io::Write};

    use crate::providers::provider::ParseCredentials;

    use super::Aws;

    #[macro_export]
    macro_rules! generated_test_files_path {
        ($fname:expr) => {
            format!(
                "{}{}{}",
                env!("CARGO_MANIFEST_DIR"),
                "/resources/test/generated/",
                $fname
            )
        };
    }

    const TEST_FILE: &str = "[default]
    aws_access_key_id=defaultid
    aws_secret_access_key=defaultsecret
    aws_session_token=defaultsession
    region=default
    [test]
    aws_access_key_id=testid
    aws_secret_access_key=testsecret
    aws_session_token=test-session
    region=test
    ";

    const INCOMPLETE_TEST_FILE: &str = "[default]
    aws_access_key_id=defaultid
    aws_secret_access_key=defaultsecret
    region=default
    [test]
    aws_secret_access_key=testsecret
    aws_session_token=test-session
    region=test
    ";

    #[test]
    fn test_extract_credentials_default() {
        let aws_provider = Aws::new(None, None);

        let credentials = aws_provider.extract_credentials_from_file(TEST_FILE.to_string());
        assert!(credentials.get("aws_access_key_id").is_some());
        assert!(credentials.get("aws_secret_access_key").is_some());
        assert!(credentials.get("region").is_some());

        assert_eq!(credentials.get("aws_access_key_id").unwrap(), "defaultid");
        assert_eq!(
            credentials.get("aws_secret_access_key").unwrap(),
            "defaultsecret"
        );
        assert_eq!(credentials.get("region").unwrap(), "default");
    }

    #[test]
    fn test_extract_credentials_different_profile() {
        let aws_provider = Aws::new(Some("test"), None);

        let credentials = aws_provider.extract_credentials_from_file(TEST_FILE.to_string());
        assert!(credentials.get("aws_access_key_id").is_some());
        assert!(credentials.get("aws_secret_access_key").is_some());
        assert!(credentials.get("region").is_some());

        assert_eq!(credentials.get("aws_access_key_id").unwrap(), "testid");
        assert_eq!(
            credentials.get("aws_secret_access_key").unwrap(),
            "testsecret"
        );
        assert_eq!(credentials.get("region").unwrap(), "test");
    }

    #[test]
    fn test_extract_credentials_from_env() {
        let aws_provider = Aws::new(None, None);
        env::set_var("AWS_ACCESS_KEY_ID", "keyid");
        env::set_var("AWS_SECRET_ACCESS_KEY", "secret");
        env::set_var("AWS_SESSION_TOKEN", "token");
        env::set_var("AWS_REGION", "region");
        let credentials = aws_provider.extract_credentials_from_env();
        assert!(credentials.get("aws_access_key_id").is_some());
        assert!(credentials.get("aws_secret_access_key").is_some());
        assert!(credentials.get("region").is_some());

        assert_eq!(credentials.get("aws_access_key_id").unwrap(), "keyid");
        assert_eq!(credentials.get("aws_secret_access_key").unwrap(), "secret");
        assert_eq!(credentials.get("aws_session_token").unwrap(), "token");
        assert_eq!(credentials.get("region").unwrap(), "region");
        env::remove_var("AWS_ACCESS_KEY_ID");
        env::remove_var("AWS_SECRET_ACCESS_KEY");
        env::remove_var("AWS_SESSION_TOKEN");
        env::remove_var("AWS_REGION");
    }

    #[test]
    fn test_parse() {
        let credentials_path = generated_test_files_path!("test_parse");
        let mut file =
            File::create(&credentials_path).expect("should be able to create file in test");
        file.write_all(TEST_FILE.as_bytes())
            .expect("should be able to write to test file");
        let aws_provider = Aws::new(Some("test"), Some(credentials_path));
        let result = aws_provider.parse();
        assert!(result.is_ok());
        let credentials = result.unwrap();

        assert_eq!(credentials.access_key_id, "testid");
        assert_eq!(credentials.secret_access_key, "testsecret");
        assert_eq!(credentials.region, "test");
    }

    #[test]
    fn test_parse_env_priority() {
        let credentials_path = generated_test_files_path!("test_parse_env_priority");
        let mut file =
            File::create(&credentials_path).expect("should be able to create file in test");
        file.write_all(TEST_FILE.as_bytes())
            .expect("should be able to write to test file");
        env::set_var("AWS_ACCESS_KEY_ID", "env_keyid");
        env::set_var("AWS_SECRET_ACCESS_KEY", "env_secret");
        env::set_var("AWS_SESSION_TOKEN", "env_token");
        env::set_var("AWS_REGION", "env_region");
        let aws_provider = Aws::new(Some("test"), Some(credentials_path));
        let result = aws_provider.parse();
        assert!(result.is_ok());
        let credentials = result.unwrap();

        assert_eq!(credentials.access_key_id, "env_keyid");
        assert_eq!(credentials.secret_access_key, "env_secret");
        assert_eq!(credentials.session_token, "env_token");
        assert_eq!(credentials.region, "env_region");
        env::remove_var("AWS_ACCESS_KEY_ID");
        env::remove_var("AWS_SECRET_ACCESS_KEY");
        env::remove_var("AWS_SESSION_TOKEN");
        env::remove_var("AWS_REGION");
    }

    #[test]
    fn test_parse_env_priority_partial() {
        let credentials_path = generated_test_files_path!("test_parse_env_priority_partial");
        let mut file =
            File::create(&credentials_path).expect("should be able to create file in test");
        file.write_all(TEST_FILE.as_bytes())
            .expect("should be able to write to test file");
        env::set_var("AWS_ACCESS_KEY_ID", "env_keyid");
        env::set_var("AWS_SECRET_ACCESS_KEY", "env_secret");
        let aws_provider = Aws::new(Some("test"), Some(credentials_path));
        let result = aws_provider.parse();
        assert!(result.is_ok());
        let credentials = result.unwrap();

        assert_eq!(credentials.access_key_id, "env_keyid");
        assert_eq!(credentials.secret_access_key, "env_secret");
        assert_eq!(credentials.session_token, "test-session");
        assert_eq!(credentials.region, "test");
        env::remove_var("AWS_ACCESS_KEY_ID");
        env::remove_var("AWS_SECRET_ACCESS_KEY");
    }

    #[test]
    fn test_parse_failure_file_does_not_exist() {
        let aws_provider = Aws::new(
            Some("test"),
            Some("test_parse_failure_file_does_not_exist".to_string()),
        );
        let result = aws_provider.parse();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "File does not exist");
    }

    #[test]
    fn test_parse_failure_incomplete_credentials() {
        let credentials_path =
            generated_test_files_path!("test_parse_failure_incomplete_credentials");
        let mut file =
            File::create(&credentials_path).expect("should be able to create file in test");
        file.write_all(INCOMPLETE_TEST_FILE.as_bytes())
            .expect("should be able to write to test file");
        let aws_provider = Aws::new(Some("test"), Some(credentials_path));
        let result = aws_provider.parse();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Missing aws credentials");
    }
}
