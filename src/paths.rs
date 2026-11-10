use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

pub(crate) fn encode_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

pub(crate) fn valid_filename(value: &str) -> bool {
    !value.is_empty() && value != "." && value != ".." && !value.contains(['/', '\\', '\0'])
}

pub(crate) fn valid_index_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

pub(crate) fn artifact_path(index: &str, package: &str, hash: &str, file: &str) -> String {
    format!(
        "{}/{}/{}/{}/{hash}/{}",
        encode_segment(index),
        encode_segment(package),
        &hash[..2],
        &hash[2..4],
        encode_segment(file)
    )
}
