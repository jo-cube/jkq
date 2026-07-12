use std::fmt;

use crate::transform::json::write_string;

#[derive(Clone, Debug, Eq, PartialEq)]
enum FormatToken {
    Literal(Vec<u8>),
    Offset,
    Key,
    KeyLength,
    Payload,
    PayloadLength,
    PayloadLengthBinary,
    Topic,
    Partition,
    Timestamp,
    Headers,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledFormat(Vec<FormatToken>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header<'a> {
    pub name: &'a str,
    pub value: Option<&'a [u8]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Timestamp {
    pub milliseconds: i64,
    pub kind: TimestampType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Payload<'a> {
    Tombstone,
    Bytes(&'a [u8]),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum EmittedAction {
    Tombstone,
    PassThrough,
    Project,
}

pub struct OutputRecord<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<Timestamp>,
    pub key: Option<&'a [u8]>,
    pub headers: &'a [Header<'a>],
    pub payload: Payload<'a>,
    pub action: EmittedAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormatError(String);

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FormatError {}

impl CompiledFormat {
    pub fn compile(source: &str) -> Result<Self, FormatError> {
        let bytes = source.as_bytes();
        let mut tokens = Vec::new();
        let mut literal = Vec::new();
        let mut cursor = 0;
        while cursor < bytes.len() {
            match bytes[cursor] {
                b'%' => {
                    flush_literal(&mut tokens, &mut literal);
                    cursor += 1;
                    let placeholder = *bytes.get(cursor).ok_or_else(|| {
                        FormatError("format ends with an incomplete '%' placeholder".to_owned())
                    })?;
                    cursor += 1;
                    tokens.push(match placeholder {
                        b'o' => FormatToken::Offset,
                        b'k' => FormatToken::Key,
                        b'K' => FormatToken::KeyLength,
                        b's' => FormatToken::Payload,
                        b'S' => FormatToken::PayloadLength,
                        b'R' => FormatToken::PayloadLengthBinary,
                        b't' => FormatToken::Topic,
                        b'p' => FormatToken::Partition,
                        b'T' => FormatToken::Timestamp,
                        b'h' => FormatToken::Headers,
                        b'%' => FormatToken::Literal(vec![b'%']),
                        value => {
                            return Err(FormatError(format!(
                                "unsupported format placeholder '%{}'",
                                char::from(value)
                            )));
                        }
                    });
                }
                b'\\' => {
                    cursor += 1;
                    let escaped = *bytes.get(cursor).ok_or_else(|| {
                        FormatError("format ends with an incomplete escape".to_owned())
                    })?;
                    cursor += 1;
                    match escaped {
                        b'n' => literal.push(b'\n'),
                        b'r' => literal.push(b'\r'),
                        b't' => literal.push(b'\t'),
                        b'\\' => literal.push(b'\\'),
                        b'x' => {
                            let digits = source.get(cursor..cursor + 2).ok_or_else(|| {
                                FormatError("'\\x' requires two hexadecimal digits".to_owned())
                            })?;
                            literal.push(u8::from_str_radix(digits, 16).map_err(|_| {
                                FormatError("'\\x' requires two hexadecimal digits".to_owned())
                            })?);
                            cursor += 2;
                        }
                        value => {
                            return Err(FormatError(format!(
                                "unsupported format escape '\\{}'",
                                char::from(value)
                            )));
                        }
                    }
                }
                _ => {
                    let character = source[cursor..].chars().next().expect("valid UTF-8");
                    let mut encoded = [0; 4];
                    literal.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
                    cursor += character.len_utf8();
                }
            }
        }
        flush_literal(&mut tokens, &mut literal);
        Ok(Self(tokens))
    }

    pub fn render(&self, record: &OutputRecord<'_>) -> Result<Vec<u8>, FormatError> {
        let mut output = Vec::new();
        for token in &self.0 {
            match token {
                FormatToken::Literal(bytes) => output.extend_from_slice(bytes),
                FormatToken::Offset => decimal(record.offset, &mut output),
                FormatToken::Key => output.extend_from_slice(record.key.unwrap_or_default()),
                FormatToken::KeyLength => optional_length(record.key, &mut output),
                FormatToken::Payload => {
                    if let Payload::Bytes(bytes) = record.payload {
                        output.extend_from_slice(bytes);
                    }
                }
                FormatToken::PayloadLength => match record.payload {
                    Payload::Tombstone => decimal(-1, &mut output),
                    Payload::Bytes(bytes) => decimal(bytes.len(), &mut output),
                },
                FormatToken::PayloadLengthBinary => {
                    let length = match record.payload {
                        Payload::Tombstone => -1,
                        Payload::Bytes(bytes) => i32::try_from(bytes.len()).map_err(|_| {
                            FormatError(format!(
                                "payload length {} exceeds %R signed 32-bit limit",
                                bytes.len()
                            ))
                        })?,
                    };
                    output.extend_from_slice(&length.to_be_bytes());
                }
                FormatToken::Topic => output.extend_from_slice(record.topic.as_bytes()),
                FormatToken::Partition => decimal(record.partition, &mut output),
                FormatToken::Timestamp => decimal(
                    record.timestamp.map_or(-1, |value| value.milliseconds),
                    &mut output,
                ),
                FormatToken::Headers => {
                    for (index, header) in record.headers.iter().enumerate() {
                        if index != 0 {
                            output.push(b',');
                        }
                        output.extend_from_slice(header.name.as_bytes());
                        output.push(b'=');
                        match header.value {
                            None => output.extend_from_slice(b"NULL"),
                            Some(value) => output.extend_from_slice(value),
                        }
                    }
                }
            }
        }
        Ok(output)
    }
}

fn flush_literal(tokens: &mut Vec<FormatToken>, literal: &mut Vec<u8>) {
    if !literal.is_empty() {
        tokens.push(FormatToken::Literal(std::mem::take(literal)));
    }
}

fn decimal(value: impl ToString, output: &mut Vec<u8>) {
    output.extend_from_slice(value.to_string().as_bytes());
}

fn optional_length(value: Option<&[u8]>, output: &mut Vec<u8>) {
    match value {
        None => decimal(-1, output),
        Some(value) => decimal(value.len(), output),
    }
}

pub fn render_envelope(record: &OutputRecord<'_>) -> Vec<u8> {
    let mut output = Vec::new();
    output.push(b'{');
    field_string("topic", record.topic, &mut output);
    field_number("partition", record.partition, &mut output);
    field_number("offset", record.offset, &mut output);
    output.extend_from_slice(b",\"timestamp\":");
    match record.timestamp {
        Some(timestamp) => decimal(timestamp.milliseconds, &mut output),
        None => output.extend_from_slice(b"null"),
    }
    output.extend_from_slice(b",\"timestampType\":");
    match record.timestamp.map(|value| value.kind) {
        Some(TimestampType::CreateTime) => write_string("createTime", &mut output),
        Some(TimestampType::LogAppendTime) => write_string("logAppendTime", &mut output),
        None => output.extend_from_slice(b"null"),
    }
    bytes_fields("key", record.key, &mut output);
    output.extend_from_slice(b",\"headers\":[");
    for (index, header) in record.headers.iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        output.push(b'{');
        write_string("name", &mut output);
        output.push(b':');
        write_string(header.name, &mut output);
        bytes_fields("value", header.value, &mut output);
        output.push(b'}');
    }
    output.push(b']');
    field_string(
        "action",
        match record.action {
            EmittedAction::Tombstone => "tombstone",
            EmittedAction::PassThrough => "pass",
            EmittedAction::Project => "project",
        },
        &mut output,
    );
    match record.payload {
        Payload::Tombstone => bytes_fields("payload", None, &mut output),
        Payload::Bytes(bytes) => bytes_fields("payload", Some(bytes), &mut output),
    }
    output.extend_from_slice(b"}\n");
    output
}

fn field_string(name: &str, value: &str, output: &mut Vec<u8>) {
    if output.last() != Some(&b'{') {
        output.push(b',');
    }
    write_string(name, output);
    output.push(b':');
    write_string(value, output);
}

fn field_number(name: &str, value: impl ToString, output: &mut Vec<u8>) {
    output.push(b',');
    write_string(name, output);
    output.push(b':');
    decimal(value, output);
}

fn bytes_fields(name: &str, value: Option<&[u8]>, output: &mut Vec<u8>) {
    output.push(b',');
    write_string(name, output);
    output.push(b':');
    let encoding = match value {
        None => {
            output.extend_from_slice(b"null");
            None
        }
        Some(bytes) => {
            let (value, encoding) = match std::str::from_utf8(bytes) {
                Ok(value) => (value.to_owned(), "utf8"),
                Err(_) => (base64(bytes), "base64"),
            };
            write_string(&value, output);
            Some(encoding)
        }
    };
    output.push(b',');
    write_string(&format!("{name}Encoding"), output);
    output.push(b':');
    match encoding {
        Some(encoding) => write_string(encoding, output),
        None => output.extend_from_slice(b"null"),
    }
    output.push(b',');
    write_string(&format!("{name}Length"), output);
    output.push(b':');
    match value {
        Some(bytes) => decimal(bytes.len(), output),
        None => decimal(-1, output),
    }
}

fn base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let bits = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(TABLE[((bits >> 18) & 63) as usize] as char);
        output.push(TABLE[((bits >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((bits >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(bits & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record<'a>(key: Option<&'a [u8]>, payload: Payload<'a>) -> OutputRecord<'a> {
        OutputRecord {
            topic: "events",
            partition: 3,
            offset: 42,
            timestamp: None,
            key,
            headers: &[],
            payload,
            action: EmittedAction::Tombstone,
        }
    }

    #[test]
    fn formatter_emits_byte_exact_metadata_escapes_and_headers() {
        let headers = [
            Header {
                name: "null",
                value: None,
            },
            Header {
                name: "empty",
                value: Some(b""),
            },
            Header {
                name: "raw",
                value: Some(b"x"),
            },
        ];
        let record = OutputRecord {
            headers: &headers,
            timestamp: Some(Timestamp {
                milliseconds: 7,
                kind: TimestampType::CreateTime,
            }),
            action: EmittedAction::PassThrough,
            ..record(Some(b"k"), Payload::Bytes(b"v"))
        };
        let format =
            CompiledFormat::compile("%t\\t%p\\t%o\\t%T\\t%K%k\\t%S%s\\t%h%%\\x0a").unwrap();
        assert_eq!(
            format.render(&record).unwrap(),
            b"events\t3\t42\t7\t1k\t1v\tnull=NULL,empty=,raw=x%\n"
        );
    }

    #[test]
    fn binary_payload_length_distinguishes_tombstone_empty_and_json_null() {
        let format = CompiledFormat::compile("%R%S%s").unwrap();
        assert_eq!(
            format.render(&record(None, Payload::Tombstone)).unwrap(),
            [vec![0xff; 4], b"-1".to_vec()].concat()
        );
        assert_eq!(
            format.render(&record(None, Payload::Bytes(b""))).unwrap(),
            [vec![0; 4], b"0".to_vec()].concat()
        );
        assert_eq!(
            format
                .render(&record(None, Payload::Bytes(b"null")))
                .unwrap(),
            [vec![0, 0, 0, 4], b"4null".to_vec()].concat()
        );
    }

    #[test]
    fn malformed_formats_fail_at_compile_time() {
        for source in ["%z", "%", "\\q", "\\x0g", "\\x0"] {
            assert!(CompiledFormat::compile(source).is_err(), "{source}");
        }
    }

    #[test]
    fn envelope_schema_preserves_nulls_and_binary_bytes() {
        let headers = [Header {
            name: "trace",
            value: Some(&[0xff]),
        }];
        let record = OutputRecord {
            headers: &headers,
            ..record(Some(&[0xff]), Payload::Tombstone)
        };
        assert_eq!(
            render_envelope(&record),
            br#"{"topic":"events","partition":3,"offset":42,"timestamp":null,"timestampType":null,"key":"/w==","keyEncoding":"base64","keyLength":1,"headers":[{"name":"trace","value":"/w==","valueEncoding":"base64","valueLength":1}],"action":"tombstone","payload":null,"payloadEncoding":null,"payloadLength":-1}
"#
        );
    }
}
