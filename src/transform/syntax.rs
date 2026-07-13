use std::fmt;

const MAX_EXPRESSION_DEPTH: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Expr {
    pub kind: ExprKind,
    pub span: Span,
    parenthesized: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExprKind {
    Literal(Literal),
    Path(Path),
    Array(Vec<Expr>),
    Object(Vec<ObjectField>),
    Unary(Box<Expr>),
    Binary(Box<Expr>, BinaryOp, Box<Expr>),
    Call(String, Vec<Expr>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOp {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
}

impl BinaryOp {
    pub fn is_comparison(self) -> bool {
        !matches!(self, Self::And | Self::Or)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Path(pub Vec<PathSegment>);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum PathSegment {
    Field(String),
    Index(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObjectField {
    pub key: String,
    pub value: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseError {
    pub category: &'static str,
    pub span: Span,
    pub message: String,
    source: String,
}

impl ParseError {
    fn new(category: &'static str, span: Span, message: impl Into<String>, source: &str) -> Self {
        Self {
            category,
            span,
            message: message.into(),
            source: source.to_owned(),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let line_start = self.source[..self.span.start.min(self.source.len())]
            .rfind('\n')
            .map_or(0, |index| index + 1);
        let line_end = self.source[self.span.start.min(self.source.len())..]
            .find('\n')
            .map_or(self.source.len(), |index| self.span.start + index);
        let line = &self.source[line_start..line_end];
        let caret = " ".repeat(
            self.source[line_start..self.span.start.min(self.source.len())]
                .chars()
                .count(),
        );
        write!(
            f,
            "{} expression at byte {}: {}\n{}\n{}^",
            self.category, self.span.start, self.message, line, caret
        )
    }
}

impl std::error::Error for ParseError {}

#[derive(Clone, Debug, PartialEq)]
struct Token {
    kind: TokenKind,
    span: Span,
}

#[derive(Clone, Debug, PartialEq)]
enum TokenKind {
    Dot,
    LeftBracket,
    RightBracket,
    LeftBrace,
    RightBrace,
    LeftParen,
    RightParen,
    Comma,
    Colon,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
    Not,
    True,
    False,
    Null,
    Identifier(String),
    String(String),
    Number(String),
    End,
}

pub fn parse(source: &str, category: &'static str) -> Result<Expr, ParseError> {
    let tokens = lex(source, category)?;
    let mut parser = Parser {
        source,
        category,
        tokens,
        cursor: 0,
    };
    let expression = parser.parse_bp(0, 1)?;
    if !matches!(parser.current().kind, TokenKind::End) {
        return Err(parser.error_current("expected end of expression"));
    }
    validate_depth(&expression, source, category)?;
    Ok(expression)
}

fn validate_depth(
    expression: &Expr,
    source: &str,
    category: &'static str,
) -> Result<(), ParseError> {
    let mut pending = vec![(expression, 1_usize)];
    while let Some((expression, depth)) = pending.pop() {
        if depth > MAX_EXPRESSION_DEPTH {
            return Err(ParseError::new(
                category,
                expression.span,
                format!("expression nesting exceeds {MAX_EXPRESSION_DEPTH} levels"),
                source,
            ));
        }
        match &expression.kind {
            ExprKind::Array(values) | ExprKind::Call(_, values) => {
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            ExprKind::Object(fields) => {
                pending.extend(fields.iter().map(|field| (&field.value, depth + 1)));
            }
            ExprKind::Unary(value) => pending.push((value, depth + 1)),
            ExprKind::Binary(left, _, right) => {
                pending.push((left, depth + 1));
                pending.push((right, depth + 1));
            }
            ExprKind::Literal(_) | ExprKind::Path(_) => {}
        }
    }
    Ok(())
}

fn lex(source: &str, category: &'static str) -> Result<Vec<Token>, ParseError> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        let start = cursor;
        let kind = match bytes[cursor] {
            b'.' if bytes.get(cursor + 1).is_some_and(u8::is_ascii_digit) => {
                return Err(ParseError::new(
                    category,
                    Span {
                        start,
                        end: start + 1,
                    },
                    "a decimal requires a digit before '.'",
                    source,
                ));
            }
            b'.' => {
                cursor += 1;
                TokenKind::Dot
            }
            b'[' => one(&mut cursor, TokenKind::LeftBracket),
            b']' => one(&mut cursor, TokenKind::RightBracket),
            b'{' => one(&mut cursor, TokenKind::LeftBrace),
            b'}' => one(&mut cursor, TokenKind::RightBrace),
            b'(' => one(&mut cursor, TokenKind::LeftParen),
            b')' => one(&mut cursor, TokenKind::RightParen),
            b',' => one(&mut cursor, TokenKind::Comma),
            b':' => one(&mut cursor, TokenKind::Colon),
            b'=' if bytes.get(cursor + 1) == Some(&b'=') => two(&mut cursor, TokenKind::Equal),
            b'!' if bytes.get(cursor + 1) == Some(&b'=') => two(&mut cursor, TokenKind::NotEqual),
            b'<' if bytes.get(cursor + 1) == Some(&b'=') => two(&mut cursor, TokenKind::LessEqual),
            b'>' if bytes.get(cursor + 1) == Some(&b'=') => {
                two(&mut cursor, TokenKind::GreaterEqual)
            }
            b'<' => one(&mut cursor, TokenKind::Less),
            b'>' => one(&mut cursor, TokenKind::Greater),
            b'"' => TokenKind::String(lex_string(source, &mut cursor, category)?),
            b'-' | b'0'..=b'9' => TokenKind::Number(lex_number(source, &mut cursor, category)?),
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                cursor += 1;
                while bytes
                    .get(cursor)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    cursor += 1;
                }
                match &source[start..cursor] {
                    "and" => TokenKind::And,
                    "or" => TokenKind::Or,
                    "not" => TokenKind::Not,
                    "true" => TokenKind::True,
                    "false" => TokenKind::False,
                    "null" => TokenKind::Null,
                    value => TokenKind::Identifier(value.to_owned()),
                }
            }
            _ => {
                return Err(ParseError::new(
                    category,
                    Span {
                        start,
                        end: start + 1,
                    },
                    "unexpected character",
                    source,
                ));
            }
        };
        tokens.push(Token {
            kind,
            span: Span { start, end: cursor },
        });
    }
    tokens.push(Token {
        kind: TokenKind::End,
        span: Span {
            start: source.len(),
            end: source.len(),
        },
    });
    Ok(tokens)
}

fn one(cursor: &mut usize, kind: TokenKind) -> TokenKind {
    *cursor += 1;
    kind
}

fn two(cursor: &mut usize, kind: TokenKind) -> TokenKind {
    *cursor += 2;
    kind
}

fn lex_number(
    source: &str,
    cursor: &mut usize,
    category: &'static str,
) -> Result<String, ParseError> {
    let bytes = source.as_bytes();
    let start = *cursor;
    if bytes[*cursor] == b'-' {
        *cursor += 1;
    }
    if !bytes.get(*cursor).is_some_and(u8::is_ascii_digit) {
        return Err(ParseError::new(
            category,
            Span {
                start,
                end: *cursor,
            },
            "expected a digit after '-'",
            source,
        ));
    }
    if bytes[*cursor] == b'0' {
        *cursor += 1;
        if bytes.get(*cursor).is_some_and(u8::is_ascii_digit) {
            return Err(ParseError::new(
                category,
                Span {
                    start,
                    end: *cursor + 1,
                },
                "leading zero is not valid JSON number syntax",
                source,
            ));
        }
    } else {
        while bytes.get(*cursor).is_some_and(u8::is_ascii_digit) {
            *cursor += 1;
        }
    }
    if bytes.get(*cursor) == Some(&b'.') {
        *cursor += 1;
        let fraction = *cursor;
        while bytes.get(*cursor).is_some_and(u8::is_ascii_digit) {
            *cursor += 1;
        }
        if fraction == *cursor {
            return Err(ParseError::new(
                category,
                Span {
                    start,
                    end: *cursor,
                },
                "expected digits after decimal point",
                source,
            ));
        }
    }
    if matches!(bytes.get(*cursor), Some(b'e' | b'E')) {
        *cursor += 1;
        if matches!(bytes.get(*cursor), Some(b'+' | b'-')) {
            *cursor += 1;
        }
        let exponent = *cursor;
        while bytes.get(*cursor).is_some_and(u8::is_ascii_digit) {
            *cursor += 1;
        }
        if exponent == *cursor {
            return Err(ParseError::new(
                category,
                Span {
                    start,
                    end: *cursor,
                },
                "expected exponent digits",
                source,
            ));
        }
    }
    Ok(source[start..*cursor].to_owned())
}

fn lex_string(
    source: &str,
    cursor: &mut usize,
    category: &'static str,
) -> Result<String, ParseError> {
    let bytes = source.as_bytes();
    let start = *cursor;
    *cursor += 1;
    let mut result = String::new();
    while *cursor < bytes.len() {
        match bytes[*cursor] {
            b'"' => {
                *cursor += 1;
                return Ok(result);
            }
            b'\\' => {
                *cursor += 1;
                let escaped = *bytes.get(*cursor).ok_or_else(|| {
                    ParseError::new(
                        category,
                        Span {
                            start,
                            end: *cursor,
                        },
                        "unterminated string escape",
                        source,
                    )
                })?;
                *cursor += 1;
                match escaped {
                    b'"' => result.push('"'),
                    b'\\' => result.push('\\'),
                    b'/' => result.push('/'),
                    b'b' => result.push('\u{8}'),
                    b'f' => result.push('\u{c}'),
                    b'n' => result.push('\n'),
                    b'r' => result.push('\r'),
                    b't' => result.push('\t'),
                    b'u' => result.push(read_unicode_escape(source, cursor, category, start)?),
                    _ => {
                        return Err(ParseError::new(
                            category,
                            Span {
                                start: *cursor - 2,
                                end: *cursor,
                            },
                            "unsupported string escape",
                            source,
                        ));
                    }
                }
            }
            byte if byte < 0x20 => {
                return Err(ParseError::new(
                    category,
                    Span {
                        start: *cursor,
                        end: *cursor + 1,
                    },
                    "unescaped control character in string",
                    source,
                ));
            }
            _ => {
                let character = source[*cursor..].chars().next().expect("valid UTF-8");
                result.push(character);
                *cursor += character.len_utf8();
            }
        }
    }
    Err(ParseError::new(
        category,
        Span {
            start,
            end: source.len(),
        },
        "unterminated string",
        source,
    ))
}

fn read_unicode_escape(
    source: &str,
    cursor: &mut usize,
    category: &'static str,
    string_start: usize,
) -> Result<char, ParseError> {
    let first = read_hex_quad(source, cursor, category, string_start)?;
    let code = if (0xd800..=0xdbff).contains(&first) {
        if source.as_bytes().get(*cursor..*cursor + 2) != Some(b"\\u") {
            return Err(ParseError::new(
                category,
                Span {
                    start: *cursor - 4,
                    end: *cursor,
                },
                "high surrogate requires a low surrogate",
                source,
            ));
        }
        *cursor += 2;
        let second = read_hex_quad(source, cursor, category, string_start)?;
        if !(0xdc00..=0xdfff).contains(&second) {
            return Err(ParseError::new(
                category,
                Span {
                    start: *cursor - 4,
                    end: *cursor,
                },
                "invalid low surrogate",
                source,
            ));
        }
        0x10000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
    } else {
        u32::from(first)
    };
    char::from_u32(code).ok_or_else(|| {
        ParseError::new(
            category,
            Span {
                start: string_start,
                end: *cursor,
            },
            "invalid Unicode escape",
            source,
        )
    })
}

fn read_hex_quad(
    source: &str,
    cursor: &mut usize,
    category: &'static str,
    string_start: usize,
) -> Result<u16, ParseError> {
    let end = *cursor + 4;
    let digits = source.get(*cursor..end).ok_or_else(|| {
        ParseError::new(
            category,
            Span {
                start: string_start,
                end: source.len(),
            },
            "incomplete Unicode escape",
            source,
        )
    })?;
    let value = u16::from_str_radix(digits, 16).map_err(|_| {
        ParseError::new(
            category,
            Span {
                start: *cursor,
                end,
            },
            "Unicode escape must contain four hexadecimal digits",
            source,
        )
    })?;
    *cursor = end;
    Ok(value)
}

struct Parser<'a> {
    source: &'a str,
    category: &'static str,
    tokens: Vec<Token>,
    cursor: usize,
}

impl Parser<'_> {
    fn parse_bp(&mut self, minimum: u8, depth: usize) -> Result<Expr, ParseError> {
        if depth > MAX_EXPRESSION_DEPTH {
            return Err(self.error_current(&format!(
                "expression nesting exceeds {MAX_EXPRESSION_DEPTH} levels"
            )));
        }
        let mut left = if matches!(self.current().kind, TokenKind::Not) {
            let start = self.take().span.start;
            let expression = self.parse_bp(7, depth + 1)?;
            Expr {
                span: Span {
                    start,
                    end: expression.span.end,
                },
                kind: ExprKind::Unary(Box::new(expression)),
                parenthesized: false,
            }
        } else {
            self.parse_primary(depth)?
        };
        while let Some((operator, left_bp, right_bp)) = self.binary_operator() {
            if left_bp < minimum {
                break;
            }
            if operator.is_comparison()
                && !left.parenthesized
                && matches!(left.kind, ExprKind::Binary(_, previous, _) if previous.is_comparison())
            {
                return Err(self.error_current("chained comparisons are not supported"));
            }
            self.take();
            let right = self.parse_bp(right_bp, depth + 1)?;
            let span = Span {
                start: left.span.start,
                end: right.span.end,
            };
            left = Expr {
                kind: ExprKind::Binary(Box::new(left), operator, Box::new(right)),
                span,
                parenthesized: false,
            };
        }
        Ok(left)
    }

    fn parse_primary(&mut self, depth: usize) -> Result<Expr, ParseError> {
        let token = self.take();
        match token.kind {
            TokenKind::True => self.literal(token.span, Literal::Bool(true)),
            TokenKind::False => self.literal(token.span, Literal::Bool(false)),
            TokenKind::Null => self.literal(token.span, Literal::Null),
            TokenKind::String(value) => self.literal(token.span, Literal::String(value)),
            TokenKind::Number(value) => self.number(token.span, &value),
            TokenKind::Dot => self.path(token.span.start),
            TokenKind::Identifier(name) => self.call(token.span.start, name, depth),
            TokenKind::LeftParen => {
                let mut expression = self.parse_bp(0, depth + 1)?;
                let end = self
                    .expect(TokenKind::RightParen, "expected ')' after expression")?
                    .span
                    .end;
                expression.span = Span {
                    start: token.span.start,
                    end,
                };
                expression.parenthesized = true;
                Ok(expression)
            }
            TokenKind::LeftBracket => self.array(token.span.start, depth),
            TokenKind::LeftBrace => self.object(token.span.start, depth),
            _ => Err(ParseError::new(
                self.category,
                token.span,
                "expected literal, path, function call, array, object, or parenthesized expression",
                self.source,
            )),
        }
    }

    fn literal(&self, span: Span, literal: Literal) -> Result<Expr, ParseError> {
        Ok(Expr {
            kind: ExprKind::Literal(literal),
            span,
            parenthesized: false,
        })
    }

    fn number(&self, span: Span, value: &str) -> Result<Expr, ParseError> {
        let literal = if value.contains(['.', 'e', 'E']) {
            let number = value.parse::<f64>().map_err(|_| {
                ParseError::new(self.category, span, "number is out of range", self.source)
            })?;
            if !number.is_finite() {
                return Err(ParseError::new(
                    self.category,
                    span,
                    "number is out of range",
                    self.source,
                ));
            }
            Literal::F64(number)
        } else if value.starts_with('-') {
            Literal::I64(value.parse().map_err(|_| {
                ParseError::new(self.category, span, "integer is out of range", self.source)
            })?)
        } else {
            Literal::U64(value.parse().map_err(|_| {
                ParseError::new(self.category, span, "integer is out of range", self.source)
            })?)
        };
        self.literal(span, literal)
    }

    fn path(&mut self, start: usize) -> Result<Expr, ParseError> {
        let mut segments = Vec::new();
        match self.current().kind.clone() {
            TokenKind::Identifier(field) => {
                self.take();
                segments.push(PathSegment::Field(field));
            }
            TokenKind::LeftBracket => self.bracket_segment(&mut segments)?,
            _ => return Err(self.error_current("expected path field after '.'")),
        }
        loop {
            match self.current().kind.clone() {
                TokenKind::Dot => {
                    self.take();
                    let TokenKind::Identifier(field) = self.take().kind else {
                        return Err(self.error_previous("expected field name after '.'"));
                    };
                    segments.push(PathSegment::Field(field));
                }
                TokenKind::LeftBracket => self.bracket_segment(&mut segments)?,
                _ => break,
            }
        }
        let end = self.tokens[self.cursor - 1].span.end;
        Ok(Expr {
            kind: ExprKind::Path(Path(segments)),
            span: Span { start, end },
            parenthesized: false,
        })
    }

    fn bracket_segment(&mut self, segments: &mut Vec<PathSegment>) -> Result<(), ParseError> {
        self.take();
        match self.take().kind {
            TokenKind::String(field) => segments.push(PathSegment::Field(field)),
            TokenKind::Number(index)
                if !index.starts_with('-') && !index.contains(['.', 'e', 'E']) =>
            {
                segments.push(PathSegment::Index(
                    index
                        .parse()
                        .map_err(|_| self.error_previous("array index is too large"))?,
                ));
            }
            _ => return Err(self.error_previous("expected non-negative integer or quoted field")),
        }
        self.expect(TokenKind::RightBracket, "expected ']' after path segment")?;
        Ok(())
    }

    fn call(&mut self, start: usize, name: String, depth: usize) -> Result<Expr, ParseError> {
        self.expect(TokenKind::LeftParen, "expected '(' after function name")?;
        let mut arguments = Vec::new();
        if !matches!(self.current().kind, TokenKind::RightParen) {
            loop {
                arguments.push(self.parse_bp(0, depth + 1)?);
                if !matches!(self.current().kind, TokenKind::Comma) {
                    break;
                }
                self.take();
            }
        }
        let end = self
            .expect(
                TokenKind::RightParen,
                "expected ')' after function arguments",
            )?
            .span
            .end;
        Ok(Expr {
            kind: ExprKind::Call(name, arguments),
            span: Span { start, end },
            parenthesized: false,
        })
    }

    fn array(&mut self, start: usize, depth: usize) -> Result<Expr, ParseError> {
        let mut values = Vec::new();
        if !matches!(self.current().kind, TokenKind::RightBracket) {
            loop {
                values.push(self.parse_bp(0, depth + 1)?);
                if !matches!(self.current().kind, TokenKind::Comma) {
                    break;
                }
                self.take();
            }
        }
        let end = self
            .expect(TokenKind::RightBracket, "expected ']' after array")?
            .span
            .end;
        Ok(Expr {
            kind: ExprKind::Array(values),
            span: Span { start, end },
            parenthesized: false,
        })
    }

    fn object(&mut self, start: usize, depth: usize) -> Result<Expr, ParseError> {
        let mut fields = Vec::new();
        if !matches!(self.current().kind, TokenKind::RightBrace) {
            loop {
                let key = self.take();
                let name = match key.kind {
                    TokenKind::Identifier(name) | TokenKind::String(name) => name,
                    _ => return Err(self.error_previous("expected object key")),
                };
                self.expect(TokenKind::Colon, "expected ':' after object key")?;
                let value = self.parse_bp(0, depth + 1)?;
                fields.push(ObjectField {
                    key: name,
                    value,
                    span: key.span,
                });
                if !matches!(self.current().kind, TokenKind::Comma) {
                    break;
                }
                self.take();
            }
        }
        let end = self
            .expect(TokenKind::RightBrace, "expected '}' after object")?
            .span
            .end;
        Ok(Expr {
            kind: ExprKind::Object(fields),
            span: Span { start, end },
            parenthesized: false,
        })
    }

    fn binary_operator(&self) -> Option<(BinaryOp, u8, u8)> {
        Some(match self.current().kind {
            TokenKind::Or => (BinaryOp::Or, 1, 2),
            TokenKind::And => (BinaryOp::And, 3, 4),
            TokenKind::Equal => (BinaryOp::Equal, 5, 6),
            TokenKind::NotEqual => (BinaryOp::NotEqual, 5, 6),
            TokenKind::Less => (BinaryOp::Less, 5, 6),
            TokenKind::LessEqual => (BinaryOp::LessEqual, 5, 6),
            TokenKind::Greater => (BinaryOp::Greater, 5, 6),
            TokenKind::GreaterEqual => (BinaryOp::GreaterEqual, 5, 6),
            _ => return None,
        })
    }

    fn current(&self) -> &Token {
        &self.tokens[self.cursor]
    }

    fn take(&mut self) -> Token {
        let token = self.current().clone();
        self.cursor += 1;
        token
    }

    fn expect(&mut self, expected: TokenKind, message: &str) -> Result<Token, ParseError> {
        if std::mem::discriminant(&self.current().kind) == std::mem::discriminant(&expected) {
            Ok(self.take())
        } else {
            Err(self.error_current(message))
        }
    }

    fn error_current(&self, message: &str) -> ParseError {
        ParseError::new(self.category, self.current().span, message, self.source)
    }

    fn error_previous(&self, message: &str) -> ParseError {
        ParseError::new(
            self.category,
            self.tokens[self.cursor - 1].span,
            message,
            self.source,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexer_and_parser_accept_documented_primitives() {
        for source in [
            ".id",
            ".items[0][\"a.b\"]",
            "-12",
            "12.5e2",
            "\"a\\n\\u263a\"",
            "[.a, null]",
            "{id: .id, \"active-now\": true}",
            "contains(.name, \"x\")",
        ] {
            parse(source, "test").unwrap_or_else(|error| panic!("{source}: {error}"));
        }
    }

    #[test]
    fn precedence_is_or_then_and_then_comparison() {
        let expression = parse(".a == 1 or .b == 2 and .c == 3", "predicate").unwrap();
        let ExprKind::Binary(_, BinaryOp::Or, right) = expression.kind else {
            panic!("expected top-level or");
        };
        assert!(matches!(right.kind, ExprKind::Binary(_, BinaryOp::And, _)));
    }

    #[test]
    fn malformed_syntax_reports_position() {
        for source in ["\"unterminated", "1 < 2 < 3", ".[-1]", "{a .x}"] {
            let error = parse(source, "predicate").unwrap_err();
            assert!(error.to_string().contains("byte"), "{source}: {error}");
        }
    }

    #[test]
    fn expression_nesting_is_bounded_at_startup() {
        let accepted = format!(
            "{}true{}",
            "(".repeat(MAX_EXPRESSION_DEPTH - 1),
            ")".repeat(MAX_EXPRESSION_DEPTH - 1)
        );
        parse(&accepted, "predicate").unwrap();

        let rejected = format!(
            "{}true{}",
            "(".repeat(MAX_EXPRESSION_DEPTH),
            ")".repeat(MAX_EXPRESSION_DEPTH)
        );
        let error = parse(&rejected, "predicate").unwrap_err();
        assert!(error.message.contains("exceeds 128 levels"));
    }

    #[test]
    fn parenthesized_comparison_results_can_be_compared() {
        parse("(true == true) == true", "predicate").unwrap();
        parse("true == (true == true)", "predicate").unwrap();
        assert!(parse("true == true == true", "predicate").is_err());
    }
}
