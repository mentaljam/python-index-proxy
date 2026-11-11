use crate::index::project::{FileMetadata, Metadata, PreparedProject, Yanked};

impl PreparedProject {
    pub(super) fn render_html(&self) -> Vec<u8> {
        let mut html = Vec::from(b"<!DOCTYPE html>\n<html>\n<head>\n");
        if let Some(status) = &self.project_status {
            for (name, value) in [
                ("pypi:project-status", &status.status),
                ("pypi:project-status-reason", &status.reason),
            ] {
                if let Some(value) = value {
                    html.extend(b"<meta name=\"");
                    html.extend(name.as_bytes());
                    html.extend(b"\" content=\"");
                    append_html_escaped(&mut html, value);
                    html.extend(b"\">\n");
                }
            }
        }
        html.extend(b"</head>\n<body>\n");
        for file in &self.files {
            html.extend(b"<a href=\"");
            append_html_escaped(&mut html, &file.url);
            if let Some(fragment) = &file.fragment {
                html.push(b'#');
                append_html_escaped(&mut html, fragment);
            }
            html.push(b'"');
            let attributes = file_attributes(&file.metadata);
            let extra = file
                .html
                .as_ref()
                .map_or(&[][..], |link| link.extra_attributes.as_slice());
            // Noncanonical source attributes override their normalized representation.
            for (name, value) in attributes
                .iter()
                .filter(|(name, _)| !extra.iter().any(|(key, _)| key == name))
                .chain(extra)
            {
                html.push(b' ');
                append_html_escaped(&mut html, name);
                html.extend(b"=\"");
                append_html_escaped(&mut html, value);
                html.push(b'"');
            }
            html.push(b'>');
            let text = file
                .html
                .as_ref()
                .map_or(file.metadata.filename.as_str(), |link| link.text.as_str());
            append_html_escaped(&mut html, text);
            html.extend(b"</a><br/>\n");
        }
        html.extend(b"</body>\n</html>");
        html
    }
}

fn file_attributes(file: &FileMetadata) -> Vec<(String, String)> {
    let mut attributes = Vec::new();
    if let Some(value) = &file.requires_python {
        attributes.push(("data-requires-python".into(), value.clone()));
    }
    match &file.yanked {
        Some(Yanked::Bool(true)) => attributes.push(("data-yanked".into(), String::new())),
        Some(Yanked::Reason(reason)) if !reason.is_empty() => {
            attributes.push(("data-yanked".into(), reason.clone()))
        }
        _ => {}
    }
    for (name, value) in [
        ("data-core-metadata", &file.core_metadata),
        ("data-dist-info-metadata", &file.dist_info_metadata),
    ] {
        if let Some(value) = value {
            let value = match value {
                Metadata::Bool(value) => value.to_string(),
                Metadata::Hashes(hashes) => {
                    let Some((algorithm, digest)) = hashes
                        .get_key_value("sha256")
                        .or_else(|| hashes.first_key_value())
                    else {
                        continue;
                    };
                    format!("{algorithm}={digest}")
                }
            };
            attributes.push((name.into(), value));
        }
    }
    if let Some(value) = file.gpg_sig {
        attributes.push(("data-gpg-sig".into(), value.to_string()));
    }
    if let Some(value) = &file.provenance {
        attributes.push(("data-provenance".into(), value.clone()));
    }
    attributes
}

fn append_html_escaped(output: &mut Vec<u8>, text: &str) {
    for character in text.chars() {
        let escaped = match character {
            '&' => "&amp;",
            '<' => "&lt;",
            '>' => "&gt;",
            '"' => "&quot;",
            '\'' => "&#39;",
            _ => {
                let mut bytes = [0; 4];
                output.extend(character.encode_utf8(&mut bytes).as_bytes());
                continue;
            }
        };
        output.extend(escaped.as_bytes());
    }
}
