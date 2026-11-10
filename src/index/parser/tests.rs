use super::*;

#[test]
fn html5_links_preserve_decoded_attributes_text_and_source_urls() {
    let base = Url::parse("https://index.example/simple/pkg/").unwrap();
    let mut parser = DetailParser::new("/proxy/", "default", "pkg");
    parser
        .parse_html(
            &base,
            br#"<!doctype html><HTML><HEAD>
        <meta charset=utf-8><TITLE>Packages</TITLE></HEAD><BODY>
        <A HREF=first.whl DATA-REQUIRES-PYTHON='&gt;=3.12' DATA-YANKED
           DATA-CORE-METADATA='sha256=abc' DATA-DIST-INFO-METADATA='sha256=abc'
           TITLE='a &quot;quote&quot; &amp; &lt;tag&gt;'>first<span>.whl</span></A><br>
        <a>not a file</a>
        <a href='second.whl?x=1&amp;y=2#sha256=ab&amp;extra=cd'>second&#46;whl &amp; &lt;tag&gt;
        "#,
        )
        .unwrap();
    let detail = parser.finalize().unwrap();
    assert_eq!(detail.files.len(), 2);
    let html = std::str::from_utf8(&detail.html).unwrap();
    assert!(html.contains("data-requires-python=\"&gt;=3.12\""));
    assert!(html.contains("data-yanked=\"\""));
    assert!(html.contains("data-core-metadata=\"sha256=abc\""));
    assert!(html.contains("data-dist-info-metadata=\"sha256=abc\""));
    assert!(html.contains("title=\"a &quot;quote&quot; &amp; &lt;tag&gt;\""));
    assert!(html.contains(">first.whl</a>"));
    assert!(html.contains(">second.whl &amp; &lt;tag&gt;"));
    assert!(html.contains("/second%2Ewhl#sha256=ab&amp;extra=cd\""));
    assert!(!html.contains("not a file"));
    let mut urls: Vec<_> = detail
        .files
        .values()
        .map(|source| source.url.as_str())
        .collect();
    urls.sort();
    assert_eq!(
        urls,
        [
            "https://index.example/simple/pkg/first.whl",
            "https://index.example/simple/pkg/second.whl?x=1&y=2",
        ]
    );
}

#[test]
fn tokenizer_ignores_apparent_links_in_raw_text_and_rcdata() {
    let base = Url::parse("https://index.example/simple/pkg/").unwrap();
    for tag in [
        "script", "style", "title", "textarea", "iframe", "xmp", "noembed", "noframes",
    ] {
        let mut parser = DetailParser::new("/", "default", "pkg");
        let input =
            format!("<{tag}><a href=fake.whl>fake.whl</a></{tag}><a href=real.whl>real.whl</a>");
        parser.parse_html(&base, input.as_bytes()).unwrap();
        let detail = parser.finalize().unwrap();
        assert_eq!(detail.files.len(), 1, "{tag}");
        let source = detail.files.values().next().unwrap();
        assert_eq!(
            source.url, "https://index.example/simple/pkg/real.whl",
            "{tag}"
        );
        let html = std::str::from_utf8(&detail.html).unwrap();
        assert!(html.contains(">real.whl</a>"), "{tag}");
        assert!(!html.contains("fake.whl"), "{tag}");
    }
}

#[test]
fn tokenizer_closes_adjacent_and_unterminated_anchors() {
    let base = Url::parse("https://index.example/simple/pkg/").unwrap();
    let mut parser = DetailParser::new("/", "default", "pkg");
    parser.parse_html(&base,
        b"<a href=first.whl>first<span>.whl</span><a href=second.whl>second.whl<a>not a file</a><a href=last.whl>last.whl"
    ).unwrap();
    let detail = parser.finalize().unwrap();
    assert_eq!(detail.files.len(), 3);
    let html = std::str::from_utf8(&detail.html).unwrap();
    for filename in ["first.whl", "second.whl", "last.whl"] {
        assert!(html.contains(&format!(">{filename}</a><br/>")), "{html}");
    }
    assert_eq!(html.matches("</a>").count(), 3);
    assert!(!html.contains("not a file"));
}

#[test]
fn ignored_anchor_targets_end_the_previous_link_without_leaking_text() {
    let base = Url::parse("https://index.example/simple/pkg/").unwrap();
    let mut parser = DetailParser::new("/", "default", "pkg");
    parser.parse_html(&base, b"outside</a><a href=first.whl>first.whl<a href=directory/>ignored</a>outside<a href=last.whl>last.whl</a>outside").unwrap();
    let detail = parser.finalize().unwrap();
    let html = std::str::from_utf8(&detail.html).unwrap();
    assert_eq!(detail.files.len(), 2);
    assert!(html.contains(">first.whl</a><br/>"));
    assert!(html.contains(">last.whl</a><br/>"));
    assert_eq!(html.matches("</a>").count(), 2);
    assert!(!html.contains("outside"));
    assert!(!html.contains("ignored"));
}

#[test]
fn rewritten_file_links_preserve_fragments_and_resolve_sources() {
    let base = Url::parse("https://index.example.test/simple/pkg/").unwrap();
    let mut parser = DetailParser::new("/proxy/", "default", "pkg");
    parser.parse_html(&base, b"<a href='../../files/pkg.whl?download=1&amp;x=2#sha256=abc'>pkg.whl</a><a href='plain.zip'>plain.zip</a>").unwrap();
    let detail = parser.finalize().unwrap();
    let html = std::str::from_utf8(&detail.html).unwrap();
    assert!(html.contains("/proxy/packages/default/pkg/"));
    assert!(html.contains("/pkg%2Ewhl#sha256=abc\""));
    assert!(html.contains("/plain%2Ezip\""));
    let source = detail
        .files
        .values()
        .find(|source| source.filename == "pkg.whl")
        .unwrap();
    assert_eq!(
        source.url,
        "https://index.example.test/files/pkg.whl?download=1&x=2"
    );
    let mut other = DetailParser::new("/", "default", "pkg");
    other
        .parse_html(
            &Url::parse("https://other.example/simple/pkg/").unwrap(),
            b"<a href='plain.zip'>plain.zip</a>",
        )
        .unwrap();
    let other = other.finalize().unwrap();
    let first_id = detail
        .files
        .iter()
        .find(|(_, source)| source.filename == "plain.zip")
        .unwrap()
        .0;
    assert!(
        !other.files.contains_key(first_id),
        "relative links on different hosts must not collide"
    );
}
