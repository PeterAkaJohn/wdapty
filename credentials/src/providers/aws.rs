use std::{collections::HashMap, env, fs};

use anyhow::anyhow;
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
        return Aws {
            profile,
            credentials_path,
        };
    }

    fn extract_credentials_from_file(&self, file: String) -> HashMap<String, String> {
        let mut parsing = false;
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
                    let mut parts = line.trim().splitn(2, "=");
                    let property = parts.next()?.trim().to_string();
                    let value = parts.next()?.trim().to_string();
                    Some((property, value))
                } else {
                    None
                }
            })
            .collect();

        return credentials;
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
        return credentials;
    }
}

impl<'a> ParseCredentials<AwsCredentials> for Aws<'a> {
    fn parse(&self) -> anyhow::Result<AwsCredentials> {
        let file = fs::read_to_string(&self.credentials_path)?;
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
    use super::Aws;

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
}
