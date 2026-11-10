use std::collections::HashMap;

use html5gum::{DefaultEmitter, StartTag, State as HtmlState, Token, Tokenizer};
use percent_encoding::percent_decode_str;
use sha2::{Digest, Sha256};
use url::Url;

use crate::paths::{encode_segment, valid_filename};

use super::{ArtifactSource, PackageDetail};

enum AnchorState {
    Outside,
    Inside(PendingAnchor),
}

struct PendingAnchor {
    id: String,
    source: ArtifactSource,
    fragment: Option<String>,
    attributes: Vec<(String, String)>,
    text: String,
}

pub(super) struct DetailParser {
    file_links: HashMap<String, ArtifactSource>,
    link_count: usize,
    html: Vec<u8>,
    anchor_start: String,
}

impl DetailParser {
    pub(super) fn new(link_base: &str, index: &str, package: &str) -> Self {
        Self {
            file_links: HashMap::new(),
            link_count: 0,
            html: Vec::from(b"<!DOCTYPE html>\n<html>\n<body>\n"),
            anchor_start: format!(
                r#"<a href="{}packages/{}/{}/"#,
                link_base.replace('&', "&amp;"),
                encode_segment(index),
                encode_segment(package)
            ),
        }
    }

    pub(super) fn parse_html(&mut self, package_url: &Url, content: &[u8]) -> anyhow::Result<()> {
        // Context-aware tokenization keeps apparent tags inside script/style and
        // RCDATA elements as text without constructing a DOM.
        let mut emitter = DefaultEmitter::default();
        emitter.naively_switch_states(true);
        let mut state = AnchorState::Outside;
        let mut tokenizer = Tokenizer::new_with_emitter(std::str::from_utf8(content)?, emitter);
        while let Some(token) = tokenizer.next() {
            match token? {
                Token::StartTag(tag) if tag.name.as_ref() == b"a" => {
                    self.finish_anchor(&mut state);
                    state = match Self::prepare_anchor(package_url, tag)? {
                        Some(anchor) => AnchorState::Inside(anchor),
                        None => AnchorState::Outside,
                    };
                }
                Token::EndTag(tag) if tag.name.as_ref() == b"a" => {
                    self.finish_anchor(&mut state);
                }
                Token::String(text) => {
                    if let AnchorState::Inside(anchor) = &mut state {
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
        self.finish_anchor(&mut state);
        Ok(())
    }

    fn prepare_anchor(
        package_url: &Url,
        tag: StartTag<()>,
    ) -> anyhow::Result<Option<PendingAnchor>> {
        let Some(href) = tag.attributes.get(b"href".as_slice()) else {
            return Ok(None);
        };
        let mut source_url = package_url.join(std::str::from_utf8(href)?)?;
        let id = hex::encode(Sha256::digest(source_url.as_str().as_bytes()));
        let fragment = source_url.fragment().map(str::to_owned);
        source_url.set_fragment(None);
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
        Ok(Some(PendingAnchor {
            id,
            source: ArtifactSource {
                url: source_url.to_string(),
                filename,
            },
            fragment,
            attributes,
            text: String::new(),
        }))
    }

    fn finish_anchor(&mut self, state: &mut AnchorState) {
        let AnchorState::Inside(anchor) = std::mem::replace(state, AnchorState::Outside) else {
            return;
        };
        self.html.extend(self.anchor_start.as_bytes());
        self.html.extend(anchor.id.as_bytes());
        self.html.push(b'/');
        self.html
            .extend(encode_segment(&anchor.source.filename).as_bytes());
        if let Some(fragment) = anchor.fragment {
            self.html.push(b'#');
            append_html_escaped(&mut self.html, &fragment);
        }
        self.html.push(b'"');
        for (name, value) in anchor.attributes {
            self.html.push(b' ');
            append_html_escaped(&mut self.html, &name);
            self.html.extend(b"=\"");
            append_html_escaped(&mut self.html, &value);
            self.html.push(b'"');
        }
        self.html.push(b'>');
        append_html_escaped(&mut self.html, &anchor.text);
        self.html.extend(b"</a><br/>\n");
        self.file_links.insert(anchor.id, anchor.source);
        self.link_count += 1;
    }

    pub(super) fn finalize(self) -> Option<PackageDetail> {
        let Self {
            mut html,
            link_count,
            file_links,
            ..
        } = self;
        if link_count > 0 {
            html.extend(b"</body>\n</html>");
            Some(PackageDetail {
                html: html.into(),
                files: file_links,
            })
        } else {
            None
        }
    }
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

#[cfg(test)]
mod tests;
