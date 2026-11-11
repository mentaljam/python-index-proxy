use std::collections::BTreeMap;

use html5gum::{DefaultEmitter, StartTag, State as HtmlState, Token, Tokenizer};
use percent_encoding::percent_decode_str;
use url::Url;

use crate::paths::valid_filename;

use super::super::project::{FileMetadata, HtmlLink, Metadata, ParsedFile, ParsedProject, Yanked};

/// Only the unfinished HTML anchor lives in tokenizer state.
struct PendingAnchor {
    source_url: Url,
    filename: String,
    attributes: Vec<(String, String)>,
    text: String,
}

pub(super) fn parse(package_url: &Url, content: &[u8]) -> anyhow::Result<ParsedProject> {
    anyhow::ensure!(
        !content.iter().all(u8::is_ascii_whitespace),
        "Empty upstream HTML page"
    );
    let mut files = Vec::new();
    let mut anchor: Option<PendingAnchor> = None;
    // Context-aware tokenization excludes apparent links in raw text and RCDATA.
    let mut emitter = DefaultEmitter::default();
    emitter.naively_switch_states(true);
    let mut tokenizer = Tokenizer::new_with_emitter(std::str::from_utf8(content)?, emitter);
    while let Some(token) = tokenizer.next() {
        match token? {
            Token::StartTag(tag) if tag.name.as_ref() == b"a" => {
                finish_anchor(&mut files, &mut anchor);
                anchor = PendingAnchor::from_tag(package_url, tag)?;
            }
            Token::EndTag(tag) if tag.name.as_ref() == b"a" => {
                finish_anchor(&mut files, &mut anchor);
            }
            Token::String(text) => {
                if let Some(anchor) = &mut anchor {
                    anchor.text.push_str(std::str::from_utf8(&text)?);
                }
            }
            Token::StartTag(tag) if tag.name.as_ref() == b"noframes" => {
                // html5gum 0.9.1 naive_next_state spells this tag "noframe".
                tokenizer.set_state(HtmlState::RawText);
            }
            // HTML parse errors are recoverable diagnostics, not reader errors.
            _ => {}
        }
    }
    finish_anchor(&mut files, &mut anchor);
    Ok(ParsedProject {
        files,
        versions: None,
        project_status: None,
    })
}

fn finish_anchor(files: &mut Vec<ParsedFile>, anchor: &mut Option<PendingAnchor>) {
    if let Some(anchor) = anchor.take() {
        files.push(anchor.into_file());
    }
}

impl PendingAnchor {
    fn from_tag(package_url: &Url, tag: StartTag<()>) -> anyhow::Result<Option<Self>> {
        let Some(href) = tag.attributes.get(b"href".as_slice()) else {
            return Ok(None);
        };
        let source_url = package_url.join(std::str::from_utf8(href)?)?;
        let file = source_url
            .path_segments()
            .and_then(|mut parts| parts.next_back())
            .unwrap_or("");
        let filename = percent_decode_str(file).decode_utf8()?.into_owned();
        if !valid_filename(&filename) {
            return Ok(None);
        }
        let mut attributes = Vec::new();
        for (name, value) in &tag.attributes {
            if name.as_ref() != b"href" {
                attributes.push((
                    std::str::from_utf8(name)?.to_owned(),
                    std::str::from_utf8(value)?.to_owned(),
                ));
            }
        }
        Ok(Some(Self {
            source_url,
            filename,
            attributes,
            text: String::new(),
        }))
    }

    fn into_file(self) -> ParsedFile {
        let Self {
            source_url,
            filename,
            mut attributes,
            text,
        } = self;
        let metadata = FileMetadata {
            filename,
            hashes: source_url.fragment().map(parse_hash).unwrap_or_default(),
            requires_python: take_attribute(&mut attributes, "data-requires-python"),
            yanked: take_attribute(&mut attributes, "data-yanked").map(|value| {
                if value.is_empty() {
                    Yanked::Bool(true)
                } else {
                    Yanked::Reason(value)
                }
            }),
            core_metadata: take_metadata(&mut attributes, "data-core-metadata"),
            dist_info_metadata: take_metadata(&mut attributes, "data-dist-info-metadata"),
            gpg_sig: take_gpg_sig(&mut attributes),
            provenance: take_attribute(&mut attributes, "data-provenance"),
            size: None,
            upload_time: None,
        };
        ParsedFile {
            source_url,
            metadata,
            html: Some(HtmlLink {
                text,
                extra_attributes: attributes,
            }),
        }
    }
}

fn take_attribute(attributes: &mut Vec<(String, String)>, name: &str) -> Option<String> {
    let position = attributes.iter().position(|(key, _)| key == name)?;
    Some(attributes.remove(position).1)
}

fn parse_hash(value: &str) -> BTreeMap<String, String> {
    value
        .split('&')
        .filter_map(|part| part.split_once('='))
        .map(|(name, digest)| (name.to_ascii_lowercase(), digest.to_owned()))
        .collect()
}

fn parse_metadata(value: &str) -> Metadata {
    let is_true = value == "true";
    if is_true || value == "false" {
        Metadata::Bool(is_true)
    } else {
        Metadata::Hashes(parse_hash(value))
    }
}

// Preserve source spellings that cannot be reconstructed from normalized metadata.
fn take_metadata(attributes: &mut Vec<(String, String)>, name: &str) -> Option<Metadata> {
    let value = take_attribute(attributes, name)?;
    let metadata = parse_metadata(&value);
    let canonical = match &metadata {
        Metadata::Bool(_) => true,
        Metadata::Hashes(hashes) => {
            hashes.len() == 1
                && hashes
                    .first_key_value()
                    .is_some_and(|(algorithm, digest)| value == format!("{algorithm}={digest}"))
        }
    };
    if !canonical {
        attributes.push((name.to_owned(), value));
    }
    Some(metadata)
}

fn take_gpg_sig(attributes: &mut Vec<(String, String)>) -> Option<bool> {
    let position = attributes
        .iter()
        .position(|(name, _)| name == "data-gpg-sig")?;
    let value = attributes[position].1.parse().ok()?;
    attributes.remove(position);
    Some(value)
}
