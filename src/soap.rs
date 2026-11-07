use std::fmt::Write as _;

use roxmltree::{Document, Node};

pub const FRIS_NS: &str = "http://fris.ewi.be/";
pub const CRITERIA_NS: &str = "http://fris.ewi.be/criteria";

pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

pub fn envelope(op: &str, inner: &str) -> Vec<u8> {
    format!(
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body>\
         <f:{op} xmlns:f=\"{FRIS_NS}\" xmlns:c=\"{CRITERIA_NS}\">{inner}</f:{op}></soap:Body></soap:Envelope>"
    )
    .into_bytes()
}

#[derive(Debug, Clone, Default)]
pub struct Query<'a> {
    pub page_size: usize,
    pub page: usize,
    pub order: Option<&'a str>,
    pub locale: &'a str,
    pub descending: bool,
    pub provider: Option<&'a str>,
    pub start: Option<&'a str>,
    pub end: Option<&'a str>,
    pub uuids: &'a [String],
}

impl Query<'_> {
    pub fn criteria(&self) -> String {
        let mut s = String::from("<criteria><c:window>");
        let _ = write!(
            s,
            "<c:pageSize>{}</c:pageSize><c:pageNumber>{}</c:pageNumber>",
            self.page_size, self.page
        );
        if let Some(order) = self.order {
            let dir = if self.descending {
                "DESCENDING"
            } else {
                "ASCENDING"
            };
            let _ = write!(
                s,
                "<c:orderings><c:order><c:id>{}</c:id><c:locale>{}</c:locale><c:direction>{dir}</c:direction></c:order></c:orderings>",
                escape(order),
                escape(self.locale)
            );
        }
        s.push_str("</c:window>");
        if !self.uuids.is_empty() {
            s.push_str("<c:uuids>");
            for u in self.uuids {
                let _ = write!(s, "<c:identifier>{}</c:identifier>", escape(u));
            }
            s.push_str("</c:uuids>");
        }
        if let Some(p) = self.provider {
            let _ = write!(
                s,
                "<c:dataProviders><c:identifier>{}</c:identifier></c:dataProviders>",
                escape(p)
            );
        }
        if self.start.is_some() || self.end.is_some() {
            s.push_str("<c:lastModifiedDate>");
            if let Some(t) = self.start {
                let _ = write!(s, "<c:start>{}</c:start>", escape(t));
            }
            if let Some(t) = self.end {
                let _ = write!(s, "<c:end>{}</c:end>", escape(t));
            }
            s.push_str("</c:lastModifiedDate>");
        }
        s.push_str("</criteria>");
        s
    }
}

#[derive(Debug, Clone)]
pub struct Rec {
    pub uuid: Option<String>,
    pub kind: String,
    pub provider: Option<String>,
    pub modified: Option<String>,
    pub xml: String,
}

#[derive(Debug, Clone)]
pub struct Page {
    pub total: Option<u64>,
    pub records: Vec<Rec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fault {
    pub code: String,
    pub reason: String,
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.code, self.reason)
    }
}

#[derive(Debug)]
pub enum Reply {
    Page(Page),
    Fault(Fault),
    Malformed(String),
}

const SOAP_NS: [&str; 2] = [
    "http://schemas.xmlsoap.org/soap/envelope/",
    "http://www.w3.org/2003/05/soap-envelope",
];

fn child<'a, 'i>(node: Node<'a, 'i>, name: &str) -> Option<Node<'a, 'i>> {
    node.children()
        .find(|c| c.is_element() && c.tag_name().name() == name)
}

fn child_text<'a>(node: Node<'a, 'a>, name: &str) -> Option<&'a str> {
    child(node, name).and_then(|c| c.text())
}

fn soap<'a, 'i>(node: Node<'a, 'i>, name: &str) -> Option<Node<'a, 'i>> {
    child(node, name).filter(|c| {
        c.tag_name()
            .namespace()
            .is_some_and(|n| SOAP_NS.contains(&n))
    })
}

fn find_fault(doc: &Document) -> Option<Fault> {
    let envelope = doc.root_element();
    if envelope.tag_name().name() != "Envelope"
        || !envelope
            .tag_name()
            .namespace()
            .is_some_and(|n| SOAP_NS.contains(&n))
    {
        return None;
    }
    let fault = soap(envelope, "Body").and_then(|b| soap(b, "Fault"))?;
    let nested = |outer: &str, inner: &str| child(fault, outer).and_then(|o| child_text(o, inner));
    let code = child_text(fault, "faultcode").or_else(|| nested("Code", "Value"));
    let reason = child_text(fault, "faultstring").or_else(|| nested("Reason", "Text"));
    Some(Fault {
        code: code.unwrap_or_default().trim().to_owned(),
        reason: reason.unwrap_or_default().trim().to_owned(),
    })
}

pub fn parse(text: &str) -> Reply {
    let doc = match Document::parse(text) {
        Ok(d) => d,
        Err(e) => return Reply::Malformed(e.to_string()),
    };
    if let Some(f) = find_fault(&doc) {
        return Reply::Fault(f);
    }
    let Some(result) = doc
        .descendants()
        .find(|n| n.is_element() && n.has_attribute("total"))
    else {
        return Reply::Malformed("no element with a total attribute".into());
    };
    let total = result.attribute("total").and_then(|t| t.parse().ok());
    let records = result
        .children()
        .filter(Node::is_element)
        .map(|n| Rec {
            uuid: ["uuid", "code", "id"]
                .iter()
                .find_map(|a| n.attribute(*a))
                .map(str::to_owned),
            kind: n.tag_name().name().to_owned(),
            provider: child_text(n, "dataProvider").map(str::to_owned),
            modified: child_text(n, "lastModified").map(str::to_owned),
            xml: text[n.range()].to_owned(),
        })
        .collect();
    Reply::Page(Page { total, records })
}

pub fn fault_of(text: &str) -> Option<Fault> {
    Document::parse(text).ok().and_then(|d| find_fault(&d))
}

pub fn operations(wsdl: &str) -> Vec<String> {
    let Ok(doc) = Document::parse(wsdl) else {
        return Vec::new();
    };
    let mut ops: Vec<String> = doc
        .descendants()
        .filter(|n| n.is_element() && n.tag_name().name() == "operation")
        .filter_map(|n| n.attribute("name").map(str::to_owned))
        .collect();
    ops.sort();
    ops.dedup();
    ops
}

pub fn names(text: &str, local: &str) -> Vec<String> {
    let Ok(doc) = Document::parse(text) else {
        return Vec::new();
    };
    doc.descendants()
        .filter(|n| n.is_element() && n.tag_name().name() == local)
        .filter_map(|n| n.text().map(str::to_owned))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{parse, Query, Reply};

    const PAGE: &str = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><fris:getResearchOutputResponse xmlns:fris="http://fris.ewi.be/"><queryResult total="2" pageSize="2" pageNumber="0"><fris:book uuid="a"><fris:lastModified>2024-01-01T00:00:00.001Z</fris:lastModified><fris:dataProvider>X</fris:dataProvider><fris:org><fris:lastModified>1999-01-01T00:00:00.000Z</fris:lastModified></fris:org></fris:book><fris:patent uuid="b"><fris:lastModified>2024-01-01T00:00:00.002Z</fris:lastModified></fris:patent></queryResult></fris:getResearchOutputResponse></soap:Body></soap:Envelope>"#;

    #[test]
    fn parses_records_with_direct_children_only() {
        let Reply::Page(p) = parse(PAGE) else {
            panic!("not a page")
        };
        assert_eq!(p.total, Some(2));
        assert_eq!(p.records.len(), 2);
        assert_eq!(
            p.records[0].modified.as_deref(),
            Some("2024-01-01T00:00:00.001Z")
        );
        assert_eq!(p.records[0].provider.as_deref(), Some("X"));
        assert_eq!(p.records[1].kind, "patent");
        assert!(p.records[0].xml.starts_with("<fris:book uuid=\"a\">"));
        assert!(p.records[0].xml.ends_with("</fris:book>"));
    }

    #[test]
    fn keys_records_without_uuid() {
        let page = r#"<x><fundingCodeResult total="1"><fris:fundingCode xmlns:fris="http://fris.ewi.be/" code="3923"/></fundingCodeResult></x>"#;
        let Reply::Page(p) = parse(page) else {
            panic!("not a page")
        };
        assert_eq!(p.records[0].uuid.as_deref(), Some("3923"));
    }

    #[test]
    fn detects_fault() {
        let f = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><soap:Fault><faultcode>soap:Server</faultcode><faultstring>Page size cannot be more then (50)</faultstring></soap:Fault></soap:Body></soap:Envelope>"#;
        assert!(
            matches!(parse(f), Reply::Fault(m) if m.code == "soap:Server" && m.reason.contains("Page size"))
        );
        let soap12 = r#"<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"><env:Body><env:Fault><env:Code><env:Value>env:Receiver</env:Value></env:Code><env:Reason><env:Text xml:lang="en">busy</env:Text></env:Reason></env:Fault></env:Body></env:Envelope>"#;
        assert!(
            matches!(parse(soap12), Reply::Fault(m) if m.code == "env:Receiver" && m.reason == "busy")
        );
    }

    #[test]
    fn fault_elements_outside_a_soap_envelope_are_not_faults() {
        let html = r"<html><body><Fault><faultstring>blocked</faultstring></Fault></body></html>";
        assert!(matches!(parse(html), Reply::Malformed(_)));
        let foreign = r#"<x:Envelope xmlns:x="urn:not-soap"><x:Body><x:Fault><faultstring>no</faultstring></x:Fault></x:Body></x:Envelope>"#;
        assert!(matches!(parse(foreign), Reply::Malformed(_)));
        let deep = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><r total="0"><soap:Fault/></r></soap:Body></soap:Envelope>"#;
        assert!(matches!(parse(deep), Reply::Page(_)));
    }

    #[test]
    fn criteria_order_and_escape() {
        let query = Query {
            page_size: 49,
            order: Some("entity.lastModified"),
            locale: "en",
            provider: Some("A & B "),
            start: Some("2024-01-01T00:00:00.000Z"),
            ..Query::default()
        };
        let crit = query.criteria();
        let at_window = crit.find("<c:window>").unwrap_or(usize::MAX);
        let at_provider = crit.find("<c:dataProviders>").unwrap_or(0);
        let at_date = crit.find("<c:lastModifiedDate>").unwrap_or(0);
        assert!(at_window < at_provider && at_provider < at_date);
        assert!(crit.contains("A &amp; B </c:identifier>"));
    }
}
