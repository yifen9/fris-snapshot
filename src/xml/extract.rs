use std::collections::{BTreeMap, HashSet};

use roxmltree::{Document, Node};

pub fn wrap(open: &str, ns: &str, xml: &str) -> String {
    let mut s = String::with_capacity(open.len() + ns.len() + xml.len() + 5);
    s.push_str(open);
    s.push_str(ns);
    s.push('>');
    s.push_str(xml);
    s.push_str("</r>");
    s
}

pub fn record_root<'a, 'i>(doc: &'a Document<'i>) -> Option<Node<'a, 'i>> {
    doc.root_element().children().find(Node::is_element)
}

fn split_top(s: &str, sep: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth -= 1,
            c if c == sep && depth == 0 => {
                out.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

fn attr<'a>(node: Node<'a, '_>, name: &str) -> Option<&'a str> {
    let (namespaced, local) = match name.split_once(':') {
        Some((_, local)) => (true, local),
        None => (false, name),
    };
    node.attributes()
        .find(|a| a.namespace().is_some() == namespaced && a.name() == local)
        .map(|a| a.value())
}

fn predicates(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' => {
                if depth == 0 {
                    start = i + 1;
                }
                depth += 1;
            }
            ']' => {
                depth -= 1;
                if depth == 0 {
                    out.push(&s[start..i]);
                }
            }
            _ => {}
        }
    }
    out
}

fn holds(node: Node, pred: &str) -> bool {
    if let Some(a) = pred.strip_prefix('@') {
        return match a.split_once('=') {
            Some((k, v)) => attr(node, k) == Some(v),
            None => attr(node, a).is_some(),
        };
    }
    if let Some(prefix) = pred.strip_prefix(".^=") {
        return text_of(node).is_some_and(|t| t.starts_with(prefix));
    }
    if let Some(path) = pred.strip_prefix('!') {
        return select(node, path).is_empty();
    }
    !select(node, pred).is_empty()
}

pub fn select<'a, 'i>(node: Node<'a, 'i>, path: &str) -> Vec<Node<'a, 'i>> {
    if split_top(path, '|').len() > 1 {
        let mut seen = HashSet::new();
        return split_top(path, '|')
            .into_iter()
            .flat_map(|p| select(node, p.trim()))
            .filter(|n| seen.insert(n.id()))
            .collect();
    }
    let mut current = vec![node];
    for step in split_top(path, '/').into_iter().filter(|s| !s.is_empty()) {
        let mut next = Vec::new();
        for n in current {
            match step {
                "." => next.push(n),
                ".." => {
                    if let Some(p) = n
                        .parent_element()
                        .filter(|p| *p != n.document().root_element())
                    {
                        next.push(p);
                    }
                }
                "*" => next.extend(n.children().filter(Node::is_element)),
                step => {
                    let (name, preds) = match step.find('[') {
                        Some(i) => (&step[..i], predicates(&step[i..])),
                        None => (step, Vec::new()),
                    };
                    next.extend(n.children().filter(|c| {
                        c.is_element()
                            && (name == "*" || c.tag_name().name() == name)
                            && preds.iter().all(|p| holds(*c, p))
                    }));
                }
            }
        }
        current = next;
    }
    current
}

fn split_attr(expr: &str) -> (&str, Option<&str>) {
    let mut depth = 0i32;
    let mut at = None;
    for (i, c) in expr.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth -= 1,
            '@' if depth == 0 => at = Some(i),
            _ => {}
        }
    }
    match at {
        Some(i) => (&expr[..i], Some(&expr[i + 1..])),
        None => (expr, None),
    }
}

fn position(node: Node) -> usize {
    node.parent().map_or(0, |p| {
        p.children()
            .filter(|c| c.is_element() && c.tag_name().name() == node.tag_name().name())
            .take_while(|c| c.id() != node.id())
            .count()
    })
}

fn text_of(node: Node) -> Option<String> {
    let t: String = node
        .children()
        .filter(Node::is_text)
        .filter_map(|c| c.text())
        .collect();
    let t = t.trim();
    (!t.is_empty()).then(|| t.to_owned())
}

pub fn value(node: Node, expr: &str, source: &str) -> Option<String> {
    let expr = expr.trim();
    if let Some(path) = expr.strip_suffix("name()") {
        return select(node, path.trim_end_matches('/'))
            .first()
            .map(|n| n.tag_name().name().to_owned());
    }
    if let Some(path) = expr.strip_suffix("position()") {
        return select(node, path.trim_end_matches('/'))
            .first()
            .map(|n| position(*n).to_string());
    }
    if let Some(inner) = expr.strip_prefix("tail(").and_then(|s| s.strip_suffix(')')) {
        return value(node, inner, source)
            .map(|v| v.rsplit('/').next().unwrap_or_default().to_owned());
    }
    if expr == "xml()" {
        return Some(source[node.range()].to_owned());
    }
    if let Some(inner) = expr
        .strip_prefix("count(")
        .and_then(|s| s.strip_suffix(')'))
    {
        return Some(select(node, inner).len().to_string());
    }
    if let Some(inner) = expr.strip_prefix("join(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<String> = match split_attr(inner) {
            (p, Some(a)) => select(node, p)
                .into_iter()
                .filter_map(|n| attr(n, a).map(str::to_owned))
                .collect(),
            (_, None) => select(node, inner)
                .into_iter()
                .filter_map(text_of)
                .collect(),
        };
        return (!parts.is_empty()).then(|| parts.join("|"));
    }
    match split_attr(expr) {
        (path, Some(a)) => select(node, path)
            .into_iter()
            .find_map(|n| attr(n, a).map(str::to_owned)),
        (path, None) => select(node, path).into_iter().find_map(text_of),
    }
}

pub fn inventory(
    node: Node,
    prefix: &str,
    out: &mut BTreeMap<String, (u64, BTreeMap<String, u64>)>,
) {
    for child in node.children().filter(Node::is_element) {
        let path = if prefix.is_empty() {
            child.tag_name().name().to_owned()
        } else {
            format!("{prefix}/{}", child.tag_name().name())
        };
        let entry = out.entry(path.clone()).or_default();
        entry.0 += 1;
        for a in child.attributes() {
            *entry.1.entry(a.name().to_owned()).or_default() += 1;
        }
        inventory(child, &path, out);
    }
}

#[cfg(test)]
mod tests {
    use super::{record_root, select, value, wrap};

    const OPEN: &str = "<r xmlns:fris=\"http://fris.ewi.be/\"";
    const REC: &str = r#"<fris:book uuid="u1"><fris:lastModified>2024</fris:lastModified><fris:participants><fris:participant id="p1"><fris:name><fris:lastName>Doe</fris:lastName></fris:name><fris:assignment><fris:organisation uuid="o1"/></fris:assignment><fris:assignment><fris:organisation uuid="o2"/></fris:assignment></fris:participant></fris:participants><fris:sources><fris:source authority="DOI">https://doi.org/10.1/x</fris:source><fris:source authority="WoS Id">000</fris:source></fris:sources></fris:book>"#;

    #[test]
    fn namespaced_attributes_and_slashes() -> anyhow::Result<()> {
        let rec = r#"<m:rec xmlns:m="urn:m" xmlns:p="urn:p"><m:name xml:lang="nl">Naam</m:name><m:name xml:lang="en">Name</m:name><m:type p:category="/a/b" type="t">x</m:type><m:note type="info:eu/sem/status">done</m:note></m:rec>"#;
        let text = wrap("<r", "", rec);
        let doc = roxmltree::Document::parse(&text)?;
        let root = record_root(&doc).ok_or_else(|| anyhow::anyhow!("no root"))?;
        assert_eq!(
            value(root, "name[@xml:lang=en]", &text).as_deref(),
            Some("Name")
        );
        assert_eq!(
            value(root, "name[@xml:lang=de]|name", &text).as_deref(),
            Some("Naam")
        );
        assert_eq!(value(root, "name@xml:lang", &text).as_deref(), Some("nl"));
        assert_eq!(
            value(root, "type@pure:category", &text).as_deref(),
            Some("/a/b")
        );
        assert_eq!(value(root, "type@type", &text).as_deref(), Some("t"));
        assert_eq!(
            value(root, "note[@type=info:eu/sem/status]", &text).as_deref(),
            Some("done")
        );
        assert_eq!(
            value(root, "tail(type@pure:category)", &text).as_deref(),
            Some("b")
        );
        assert_eq!(value(root, "note[.^=do]", &text).as_deref(), Some("done"));
        assert_eq!(value(root, "note[.^=x]", &text), None);
        assert_eq!(
            value(root, "name[@xml:lang=en][.^=Na]", &text).as_deref(),
            Some("Name")
        );
        assert_eq!(select(root, "*[!note]").len(), 4);
        assert_eq!(
            value(root, "join(name@xml:lang)", &text).as_deref(),
            Some("nl|en")
        );
        Ok(())
    }

    #[test]
    fn paths_and_values() -> anyhow::Result<()> {
        let text = wrap(OPEN, "", REC);
        let doc = roxmltree::Document::parse(&text)?;
        let root = record_root(&doc).ok_or_else(|| anyhow::anyhow!("no root"))?;
        assert_eq!(value(root, "@uuid", &text).as_deref(), Some("u1"));
        assert_eq!(value(root, "name()", &text).as_deref(), Some("book"));
        assert_eq!(value(root, "lastModified", &text).as_deref(), Some("2024"));
        let rows = select(root, "participants/participant/assignment");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            value(rows[1], "organisation@uuid", &text).as_deref(),
            Some("o2")
        );
        assert_eq!(
            value(rows[1], "../name/lastName", &text).as_deref(),
            Some("Doe")
        );
        assert_eq!(value(rows[0], "../@id", &text).as_deref(), Some("p1"));
        assert_eq!(
            value(root, "count(sources/source)", &text).as_deref(),
            Some("2")
        );
        assert_eq!(
            value(root, "join(sources/source@authority)", &text).as_deref(),
            Some("DOI|WoS Id")
        );
        assert_eq!(value(root, "missing", &text), None);
        assert_eq!(
            value(rows[0], "../name()", &text).as_deref(),
            Some("participant")
        );
        assert_eq!(select(root, "sources/source[@authority]").len(), 2);
        assert_eq!(
            select(root, "sources/source|sources/*[@authority]").len(),
            2
        );
        assert_eq!(value(root, "../name()", &text), None);
        assert_eq!(value(rows[1], "position()", &text).as_deref(), Some("1"));
        assert_eq!(value(rows[1], "../position()", &text).as_deref(), Some("0"));
        assert_eq!(
            value(root, "sources/source[@authority=WoS Id]/position()", &text).as_deref(),
            Some("1")
        );
        assert_eq!(
            select(root, "sources/source|participants/participant").len(),
            3
        );
        assert_eq!(
            value(root, "sources/source[@authority=WoS Id]", &text).as_deref(),
            Some("000")
        );
        assert_eq!(
            value(root, "sources/source[@authority=DOI]@authority", &text).as_deref(),
            Some("DOI")
        );
        assert_eq!(
            value(root, "count(sources/source[@authority=DOI])", &text).as_deref(),
            Some("1")
        );
        Ok(())
    }

    const DOC: &str = r#"<m:rec xmlns:m="urn:m" xmlns:p="urn:p" id="R"><m:title/><m:title>  </m:title><m:title>First <m:i>real</m:i> title</m:title><m:note lang="plain" xml:lang="en">N1</m:note><m:note xml:lang="nl">N2</m:note><m:id type="doi">10.1/abc</m:id><m:id type="isbn">978</m:id><m:id>bare</m:id><m:class><m:Type p:scheme="s1">t1</m:Type></m:class><m:class><m:Type>t2</m:Type></m:class><m:class><m:Type p:scheme="s2">t3</m:Type></m:class><m:link href="https://x.org/a/b/leaf"/><m:link/></m:rec>"#;

    fn with_doc(check: impl Fn(roxmltree::Node, &str)) -> anyhow::Result<()> {
        let text = wrap("<r", "", DOC);
        let doc = roxmltree::Document::parse(&text)?;
        let root = record_root(&doc).ok_or_else(|| anyhow::anyhow!("no root"))?;
        check(root, &text);
        Ok(())
    }

    #[test]
    fn text_is_first_non_empty_direct_text() -> anyhow::Result<()> {
        with_doc(|root, text| {
            assert_eq!(value(root, "title", text).as_deref(), Some("First  title"));
            assert_eq!(value(root, "missing", text), None);
            assert_eq!(value(root, "link", text), None);
        })
    }

    #[test]
    fn prefixed_attributes_match_namespaced_only() -> anyhow::Result<()> {
        with_doc(|root, text| {
            assert_eq!(value(root, "note@lang", text).as_deref(), Some("plain"));
            assert_eq!(value(root, "note@xml:lang", text).as_deref(), Some("en"));
            assert_eq!(value(root, "note@any:lang", text).as_deref(), Some("en"));
            assert_eq!(select(root, "note[@lang]").len(), 1);
            assert_eq!(select(root, "note[@xml:lang]").len(), 2);
            assert_eq!(
                value(root, "note[@xml:lang=nl]", text).as_deref(),
                Some("N2")
            );
            assert_eq!(value(root, "note[@lang=nl]", text), None);
            assert_eq!(value(root, "note[@xml:lang=nl]@lang", text), None);
            assert_eq!(value(root, "@id", text).as_deref(), Some("R"));
        })
    }

    #[test]
    fn predicates() -> anyhow::Result<()> {
        with_doc(|root, text| {
            assert_eq!(select(root, "id[@type]").len(), 2);
            assert_eq!(value(root, "id[@type=isbn]", text).as_deref(), Some("978"));
            assert_eq!(value(root, "id[.^=10.]", text).as_deref(), Some("10.1/abc"));
            assert_eq!(value(root, "id[.^=bar][@type]", text), None);
            assert_eq!(select(root, "class[Type]").len(), 3);
            assert_eq!(select(root, "*[!Type]").len(), 10);
            assert_eq!(select(root, "class[Type[@p:scheme]]").len(), 2);
            assert_eq!(select(root, "class[!Type[@p:scheme]]").len(), 1);
            assert_eq!(
                value(root, "class[Type[@p:scheme=s2]]/Type", text).as_deref(),
                Some("t3")
            );
            assert_eq!(
                value(root, "class[Type[@p:scheme=s2]]/Type@p:scheme", text).as_deref(),
                Some("s2")
            );
            assert_eq!(
                value(root, "class[!Type[@p:scheme]]/Type", text).as_deref(),
                Some("t2")
            );
        })
    }

    #[test]
    fn unions_keep_branch_order_and_dedup() -> anyhow::Result<()> {
        with_doc(|root, text| {
            assert_eq!(
                value(root, "id[@type=pmid]|id[@type=isbn]", text).as_deref(),
                Some("978")
            );
            assert_eq!(
                value(root, "id[@type=isbn]|id[@type=doi]", text).as_deref(),
                Some("978")
            );
            assert_eq!(select(root, "id|id[@type]").len(), 3);
            assert_eq!(
                value(root, "id[@type=x]|id[@type=doi]@type", text).as_deref(),
                Some("doi")
            );
            assert_eq!(
                value(root, "join(id[@type=isbn]|id[@type=doi]@type)", text).as_deref(),
                Some("isbn|doi")
            );
            assert_eq!(
                select(root, "note|title")
                    .first()
                    .map(|n| n.tag_name().name()),
                Some("note")
            );
        })
    }

    #[test]
    fn join_count_and_tail() -> anyhow::Result<()> {
        with_doc(|root, text| {
            assert_eq!(
                value(root, "join(id@type)", text).as_deref(),
                Some("doi|isbn")
            );
            assert_eq!(
                value(root, "join(title)", text).as_deref(),
                Some("First  title")
            );
            assert_eq!(
                value(root, "join(class/Type)", text).as_deref(),
                Some("t1|t2|t3")
            );
            assert_eq!(value(root, "join(missing)", text), None);
            assert_eq!(value(root, "join(link@missing)", text), None);
            assert_eq!(value(root, "count(id)", text).as_deref(), Some("3"));
            assert_eq!(value(root, "count(id[@type])", text).as_deref(), Some("2"));
            assert_eq!(value(root, "count(missing)", text).as_deref(), Some("0"));
            assert_eq!(
                value(root, "count(class[Type[@p:scheme]])", text).as_deref(),
                Some("2")
            );
            assert_eq!(
                value(root, "tail(link@href)", text).as_deref(),
                Some("leaf")
            );
            assert_eq!(
                value(root, "tail(id[@type=isbn])", text).as_deref(),
                Some("978")
            );
            assert_eq!(
                value(root, "tail(id[@type=doi])", text).as_deref(),
                Some("abc")
            );
            assert_eq!(value(root, "tail(missing@href)", text), None);
        })
    }

    #[test]
    fn position_and_name() -> anyhow::Result<()> {
        with_doc(|root, text| {
            let types = select(root, "class/Type");
            assert_eq!(types.len(), 3);
            assert_eq!(value(types[2], "position()", text).as_deref(), Some("0"));
            assert_eq!(value(types[2], "../position()", text).as_deref(), Some("2"));
            assert_eq!(
                value(root, "id[@type=isbn]/position()", text).as_deref(),
                Some("1")
            );
            assert_eq!(value(root, "id[3]/position()", text), None);
            assert_eq!(value(root, "missing/position()", text), None);
            assert_eq!(value(root, "name()", text).as_deref(), Some("rec"));
            assert_eq!(
                value(root, "class/Type/name()", text).as_deref(),
                Some("Type")
            );
            assert_eq!(value(types[0], "../name()", text).as_deref(), Some("class"));
            assert_eq!(value(root, "missing/name()", text), None);
        })
    }

    #[test]
    fn parent_steps_stop_at_the_record_root() -> anyhow::Result<()> {
        with_doc(|root, text| {
            let types = select(root, "class/Type");
            assert!(select(root, "..").is_empty());
            assert_eq!(select(root, ".").len(), 1);
            assert_eq!(
                select(types[0], "../..")
                    .first()
                    .map(|n| n.tag_name().name()),
                Some("rec")
            );
            assert!(select(types[0], "../../..").is_empty());
            assert_eq!(value(types[0], "../../@id", text).as_deref(), Some("R"));
            assert_eq!(value(types[0], "../../..@id", text), None);
            assert_eq!(value(root, "..@id", text), None);
            assert_eq!(value(root, "../name()", text), None);
        })
    }
}
