use std::borrow::Cow;

use crate::ast::Span;
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Token<'a> {
    pub kind: Kind<'a>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Kind<'a> {
    Ident(&'a str),
    Number(&'a str),
    String(Cow<'a, str>),
    Timespan(&'a str),
    Typed { ty: &'a str, value: &'a str },
    Pipe,
    Comma,
    Dot,
    DotDot,
    Colon,
    Semi,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Assign,
    Eq,
    NotEq,
    EqInsensitive,
    NotEqInsensitive,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Bang,
    Tilde,
    Eof,
}

pub(crate) fn lex(query: &str) -> Result<Vec<Token<'_>>> {
    let mut lexer = Lexer { query, pos: 0 };
    let mut tokens = Vec::with_capacity(query.len() / 4);
    while lexer.pos < query.len() {
        let start = lexer.pos;
        let ch = lexer.peek().expect("position is within the input");
        if ch.is_whitespace() {
            lexer.bump();
            continue;
        }
        if query[start..].starts_with("//") {
            lexer.pos = query[start..]
                .find('\n')
                .map_or(query.len(), |n| start + n + 1);
            continue;
        }
        let kind = if ch == '\'' || ch == '"' {
            Kind::String(lexer.string(false)?)
        } else if ch == '@' && matches!(query.as_bytes().get(start + 1), Some(b'\'' | b'"')) {
            lexer.bump();
            Kind::String(lexer.string(true)?)
        } else if ch.is_ascii_digit() {
            lexer.number()?
        } else if ch.is_alphabetic() || ch == '_' || ch == '$' {
            lexer.bump();
            while lexer
                .peek()
                .is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '$')
            {
                lexer.bump();
            }
            let name = &query[start..lexer.pos];
            let mut after = lexer.pos;
            while query
                .as_bytes()
                .get(after)
                .is_some_and(u8::is_ascii_whitespace)
            {
                after += 1;
            }
            if matches!(
                name,
                "datetime"
                    | "date"
                    | "timespan"
                    | "time"
                    | "guid"
                    | "uuid"
                    | "uniqueid"
                    | "decimal"
            ) && query.as_bytes().get(after) == Some(&b'(')
            {
                let end = query[after + 1..].find(')').ok_or_else(|| {
                    Error::syntax("unterminated typed literal", Span::new(start, query.len()))
                })? + after
                    + 1;
                let value = query[after + 1..end].trim();
                lexer.pos = end + 1;
                Kind::Typed { ty: name, value }
            } else {
                Kind::Ident(name)
            }
        } else {
            lexer.bump();
            match ch {
                '|' => Kind::Pipe,
                ',' => Kind::Comma,
                ':' => Kind::Colon,
                ';' => Kind::Semi,
                '(' => Kind::LParen,
                ')' => Kind::RParen,
                '[' => Kind::LBracket,
                ']' => Kind::RBracket,
                '{' => Kind::LBrace,
                '}' => Kind::RBrace,
                '+' => Kind::Plus,
                '-' => Kind::Minus,
                '*' => Kind::Star,
                '/' => Kind::Slash,
                '%' => Kind::Percent,
                '.' if lexer.consume('.') => Kind::DotDot,
                '.' => Kind::Dot,
                '=' if lexer.consume('=') => Kind::Eq,
                '=' if lexer.consume('~') => Kind::EqInsensitive,
                '=' => Kind::Assign,
                '!' if lexer.consume('=') => Kind::NotEq,
                '!' if lexer.consume('~') => Kind::NotEqInsensitive,
                '!' => Kind::Bang,
                '~' => Kind::Tilde,
                '<' if lexer.consume('=') => Kind::LtEq,
                '<' if lexer.consume('>') => Kind::NotEq,
                '<' => Kind::Lt,
                '>' if lexer.consume('=') => Kind::GtEq,
                '>' => Kind::Gt,
                _ => {
                    return Err(Error::syntax(
                        format!("unexpected character {ch:?}"),
                        Span::new(start, lexer.pos),
                    ));
                }
            }
        };
        tokens.push(Token {
            kind,
            span: Span::new(start, lexer.pos),
        });
    }
    tokens.push(Token {
        kind: Kind::Eof,
        span: Span::new(query.len(), query.len()),
    });
    Ok(tokens)
}

struct Lexer<'a> {
    query: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn peek(&self) -> Option<char> {
        self.query[self.pos..].chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn number(&mut self) -> Result<Kind<'a>> {
        let start = self.pos;
        if self.query[start..].starts_with("0x") || self.query[start..].starts_with("0X") {
            self.pos += 2;
            let digits = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_hexdigit()) {
                self.bump();
            }
            if digits == self.pos
                || self
                    .peek()
                    .is_some_and(|ch| ch.is_alphanumeric() || ch == '_')
            {
                return Err(Error::syntax(
                    "invalid hexadecimal integer literal",
                    Span::new(start, self.pos),
                ));
            }
            return Ok(Kind::Number(&self.query[start..self.pos]));
        }
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.bump();
        }
        if self.peek() == Some('.') && !self.query[self.pos..].starts_with("..") {
            self.bump();
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.bump();
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            self.bump();
            if matches!(self.peek(), Some('+' | '-')) {
                self.bump();
            }
            let digits = self.pos;
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.bump();
            }
            if digits == self.pos {
                return Err(Error::syntax(
                    "expected exponent digits",
                    Span::new(start, self.pos),
                ));
            }
        }
        let suffix = self.pos;
        while self.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
            self.bump();
        }
        let text = &self.query[start..self.pos];
        if suffix != self.pos {
            if crate::literals::timespan_unit(&self.query[suffix..self.pos]).is_none() {
                return Err(Error::syntax(
                    "unrecognized timespan unit",
                    Span::new(suffix, self.pos),
                ));
            }
            Ok(Kind::Timespan(text))
        } else {
            Ok(Kind::Number(text))
        }
    }

    fn string(&mut self, verbatim: bool) -> Result<Cow<'a, str>> {
        let start = self.pos;
        let quote = self.bump().expect("string starts with a quote");
        let content = self.pos;
        let mut decoded: Option<String> = None;
        let mut segment = content;
        while let Some(ch) = self.bump() {
            let before = self.pos - ch.len_utf8();
            if ch == quote {
                if self.consume(quote) {
                    let out = decoded.get_or_insert_with(String::new);
                    out.push_str(&self.query[segment..before]);
                    out.push(quote);
                    segment = self.pos;
                    continue;
                }
                return Ok(match decoded {
                    Some(mut out) => {
                        out.push_str(&self.query[segment..before]);
                        Cow::Owned(out)
                    }
                    None => Cow::Borrowed(&self.query[content..before]),
                });
            }
            if ch == '\\' && !verbatim {
                let out = decoded.get_or_insert_with(String::new);
                out.push_str(&self.query[segment..before]);
                let escaped = match self.bump() {
                    Some('n') => '\n',
                    Some('r') => '\r',
                    Some('t') => '\t',
                    Some('0') => '\0',
                    Some('a') => '\u{7}',
                    Some('b') => '\u{8}',
                    Some('f') => '\u{c}',
                    Some('v') => '\u{b}',
                    Some(c @ ('\\' | '\'' | '"')) => c,
                    Some(c @ ('u' | 'U')) => {
                        let count = if c == 'u' { 4 } else { 8 };
                        let end = self.pos + count;
                        let digits = self.query.get(self.pos..end).ok_or_else(|| {
                            Error::syntax("incomplete Unicode escape", Span::new(before, self.pos))
                        })?;
                        let scalar = u32::from_str_radix(digits, 16)
                            .ok()
                            .and_then(char::from_u32)
                            .ok_or_else(|| {
                                Error::syntax("invalid Unicode escape", Span::new(before, end))
                            })?;
                        self.pos = end;
                        scalar
                    }
                    _ => {
                        return Err(Error::syntax(
                            "invalid string escape",
                            Span::new(before, self.pos),
                        ));
                    }
                };
                out.push(escaped);
                segment = self.pos;
            }
        }
        Err(Error::syntax(
            "unterminated string literal",
            Span::new(start, self.pos),
        ))
    }
}
