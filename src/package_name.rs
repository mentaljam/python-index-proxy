use std::{fmt, str::FromStr};

/// Validated and normalized according to the PyPA names specification:
/// <https://packaging.python.org/en/latest/specifications/name-normalization/>
#[derive(Debug)]
pub(crate) struct PackageName(String);

impl PackageName {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for PackageName {
    type Err = InvalidPackageName;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let bytes = name.as_bytes();
        if !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
            || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        {
            return Err(InvalidPackageName);
        }
        let mut normalized = String::with_capacity(name.len());
        let mut separator = false;
        for byte in bytes {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' => {
                    normalized.push(byte.to_ascii_lowercase() as char);
                    separator = false;
                }
                b'-' | b'_' | b'.' => {
                    if !separator {
                        normalized.push('-');
                    }
                    separator = true;
                }
                _ => return Err(InvalidPackageName),
            }
        }
        Ok(Self(normalized))
    }
}

#[derive(Debug)]
pub(crate) struct InvalidPackageName;

impl fmt::Display for InvalidPackageName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Package names must start and end with an ASCII letter or digit and contain only ASCII letters, digits, '.', '_' or '-'")
    }
}

impl std::error::Error for InvalidPackageName {}

#[cfg(test)]
mod tests {
    use super::PackageName;

    #[test]
    fn canonicalizes_valid_names() {
        for (input, expected) in [
            ("a", "a"),
            ("Z", "z"),
            ("0", "0"),
            ("numpy", "numpy"),
            ("Foo__..Bar", "foo-bar"),
            ("FrIeNdLy-._.-bArD", "friendly-bard"),
            ("A--B_C.D9", "a-b-c-d9"),
            ("1a2", "1a2"),
        ] {
            assert_eq!(input.parse::<PackageName>().unwrap().as_str(), expected);
        }
    }

    #[test]
    fn rejects_invalid_names_without_trimming_or_unicode_folding() {
        for input in [
            "", "-", ".", "_", "-pkg", ".pkg", "_pkg", "pkg-", "pkg.", "pkg_", " pkg", "pkg ",
            "a b", "pkg\n", "a\nb", "a\0b", "a/b", "a\\b", "a!b", "α", "aαb", "ａ", "a%20b",
        ] {
            assert!(input.parse::<PackageName>().is_err(), "accepted {input:?}");
        }
    }
}
