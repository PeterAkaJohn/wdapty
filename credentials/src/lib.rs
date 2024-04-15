pub mod providers;
use anyhow::Result;
use providers::{
    aws::{Aws, AwsCredentials},
    provider::ParseCredentials,
};

enum Providers<'a> {
    Aws(Aws<'a>),
}

impl<'a> ParseCredentials<AwsCredentials> for Providers<'a> {
    fn parse(&self) -> anyhow::Result<AwsCredentials> {
        match self {
            Providers::Aws(aws) => aws.parse(),
        }
    }
}

pub fn get_credentials(
    provider: &str,
    profile: Option<&str>,
    credentials_path: Option<String>,
) -> Result<AwsCredentials> {
    let provider = match provider {
        "aws" => Providers::Aws(Aws::new(profile, credentials_path)),
        _ => return Err(anyhow::anyhow!("Invalid Execution type")),
    };
    provider.parse()
}
