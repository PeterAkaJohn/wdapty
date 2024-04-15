use anyhow::Result;

pub trait ParseCredentials<T> {
    fn parse(&self) -> Result<T>;
}
