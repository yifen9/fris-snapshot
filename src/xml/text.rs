fn invalid_char(c: char) -> bool {
    matches!(c, '\u{0}'..='\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}' | '\u{fffe}' | '\u{ffff}')
}

fn invalid_ref(body: &str) -> bool {
    let n = body
        .strip_prefix('x')
        .or_else(|| body.strip_prefix('X'))
        .map_or_else(
            || body.parse::<u32>().ok(),
            |h| u32::from_str_radix(h, 16).ok(),
        );
    n.and_then(char::from_u32).is_none_or(invalid_char)
}

pub fn decode(body: Vec<u8>) -> (String, usize) {
    match String::from_utf8(body) {
        Ok(text) => (text, 0),
        Err(e) => {
            let bytes = e.into_bytes();
            let bad = bytes
                .utf8_chunks()
                .filter(|c| !c.invalid().is_empty())
                .count();
            (String::from_utf8_lossy(&bytes).into_owned(), bad)
        }
    }
}

pub fn clean(text: &str) -> (String, usize) {
    let mut out = String::with_capacity(text.len());
    let mut fixed = 0;
    let mut rest = text;
    while let Some(pos) = rest.find(|c: char| c == '&' || invalid_char(c)) {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        if let Some(after) = tail.strip_prefix('&') {
            let reference = after
                .strip_prefix('#')
                .and_then(|r| r.find(';').filter(|&e| e <= 10).map(|e| &r[..e]));
            match reference {
                Some(body) if invalid_ref(body) => {
                    out.push_str("&#xFFFD;");
                    fixed += 1;
                    rest = &tail[body.len() + 3..];
                }
                _ => {
                    out.push('&');
                    rest = after;
                }
            }
        } else {
            let c = tail.chars().next().unwrap_or('\u{fffd}');
            out.push('\u{fffd}');
            fixed += 1;
            rest = &tail[c.len_utf8()..];
        }
    }
    out.push_str(rest);
    (out, fixed)
}

#[cfg(test)]
mod tests {
    use super::{clean, decode};

    #[test]
    fn decode_counts_lossy_sequences() {
        assert_eq!(decode("é ok".as_bytes().to_vec()), ("é ok".to_owned(), 0));
        let (t, n) = decode(b"a\xffb\xe9\xe9c\xc3".to_vec());
        assert_eq!(n, 4);
        assert_eq!(t, "a\u{fffd}b\u{fffd}\u{fffd}c\u{fffd}");
        let (t, n) = decode(b"x\xf0\x9f\x98y".to_vec());
        assert_eq!((t.as_str(), n), ("x\u{fffd}y", 1));
    }

    #[test]
    fn cleans_invalid_xml_chars() {
        let (t, n) = clean("a\u{2}b&#2;c&#x1F;d&amp;e&#65;f&#x9;");
        assert_eq!(t, "a\u{fffd}b&#xFFFD;c&#xFFFD;d&amp;e&#65;f&#x9;");
        assert_eq!(n, 3);
        let doc = format!("<r>{t}</r>");
        assert!(roxmltree::Document::parse(&doc).is_ok());
    }
}
