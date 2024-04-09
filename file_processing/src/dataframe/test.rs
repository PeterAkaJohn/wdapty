#[cfg(test)]
mod test {
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
}