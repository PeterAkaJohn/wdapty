use std::io;

use anyhow::Result;

pub fn request_user_input<R, W>(prompt: &str, mut reader: R, mut writer: W) -> Result<String>
where
    R: io::BufRead,
    W: io::Write,
{
    let mut input = String::new();
    writeln!(writer, "{}", prompt)?;
    writer.flush()?;
    reader.read_line(&mut input)?;
    Ok(input)
}
