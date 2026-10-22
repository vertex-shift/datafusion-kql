use crate::ast::Span;
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Segment {
    Key(String),
    Index(i64),
}

pub(crate) fn parse(text: &str, span: Span) -> Result<Vec<Segment>> {
    if !text.starts_with('$') {
        return Err(Error::syntax("JSONPath must start with '$'", span));
    }
    let mut at = 1;
    let mut segments = Vec::new();
    let bytes = text.as_bytes();
    while at < text.len() {
        if segments.len() == 64 {
            return Err(Error::syntax("JSONPath nesting limit exceeded", span));
        }
        match bytes[at] {
            b'.' => {
                at += 1;
                let start = at;
                while at < text.len() && !matches!(bytes[at], b'.' | b'[') {
                    at += 1;
                }
                let key = &text[start..at];
                if key.is_empty() || key.contains(['*', ']']) {
                    return Err(Error::unsupported(
                        "JSONPath wildcards and recursive descent",
                        span,
                    ));
                }
                segments.push(Segment::Key(key.to_owned()));
            }
            b'[' => {
                at += 1;
                if matches!(bytes.get(at), Some(b'\'' | b'"')) {
                    let quote = bytes[at];
                    at += 1;
                    let mut key = String::new();
                    let mut closed = false;
                    while at < text.len() {
                        if bytes[at] == quote {
                            at += 1;
                            closed = true;
                            break;
                        }
                        if bytes[at] == b'\\' {
                            at += 1;
                            if at == text.len() {
                                break;
                            }
                            let ch = text[at..]
                                .chars()
                                .next()
                                .ok_or_else(|| Error::syntax("invalid JSONPath key", span))?;
                            key.push(ch);
                            at += ch.len_utf8();
                        } else {
                            let ch = text[at..]
                                .chars()
                                .next()
                                .ok_or_else(|| Error::syntax("invalid JSONPath key", span))?;
                            key.push(ch);
                            at += ch.len_utf8();
                        }
                    }
                    if !closed || bytes.get(at) != Some(&b']') {
                        return Err(Error::syntax("unterminated JSONPath key", span));
                    }
                    at += 1;
                    segments.push(Segment::Key(key));
                } else {
                    let start = at;
                    while at < text.len() && bytes[at] != b']' {
                        at += 1;
                    }
                    if at == text.len() {
                        return Err(Error::syntax("unterminated JSONPath index", span));
                    }
                    let index = text[start..at].parse::<i64>().map_err(|_| {
                        Error::unsupported("JSONPath requires a literal array index", span)
                    })?;
                    at += 1;
                    segments.push(Segment::Index(index));
                }
            }
            _ => return Err(Error::syntax("expected '.' or '[' in JSONPath", span)),
        }
    }
    Ok(segments)
}
