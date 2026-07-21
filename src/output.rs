use std::{
    fmt,
    io::{self, Write},
};

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
    Action,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledFormat(Vec<FormatToken>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputRequirements {
    pub key: bool,
    pub headers: bool,
    pub timestamp: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub name: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
pub enum EmittedAction {
    Tombstone,
    PassThrough,
    Project,
}

impl EmittedAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tombstone => "tombstone",
            Self::PassThrough => "pass",
            Self::Project => "project",
        }
    }
}

pub struct OutputRecord<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<Timestamp>,
    pub key: Option<&'a [u8]>,
    pub headers: &'a [Header],
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
                        b'a' => FormatToken::Action,
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

    #[cfg(test)]
    pub fn render(&self, record: &OutputRecord<'_>) -> Result<Vec<u8>, FormatError> {
        let mut output = Vec::new();
        self.write_to(record, &mut output)
            .map_err(|error| FormatError(error.to_string()))?;
        Ok(output)
    }

    pub fn write_to(
        &self,
        record: &OutputRecord<'_>,
        output: &mut impl Write,
    ) -> io::Result<usize> {
        if let Payload::Bytes(bytes) = record.payload
            && bytes.len() > i32::MAX as usize
            && self
                .0
                .iter()
                .any(|token| matches!(token, FormatToken::PayloadLengthBinary))
        {
            return Err(io::Error::other(FormatError(format!(
                "payload length {} exceeds %R signed 32-bit limit",
                bytes.len()
            ))));
        }
        let mut written = 0;
        for token in &self.0 {
            match token {
                FormatToken::Literal(bytes) => write_bytes(output, bytes, &mut written)?,
                FormatToken::Offset => write_decimal(output, record.offset, &mut written)?,
                FormatToken::Key => {
                    write_bytes(output, record.key.unwrap_or_default(), &mut written)?
                }
                FormatToken::KeyLength => write_optional_length(output, record.key, &mut written)?,
                FormatToken::Payload => {
                    if let Payload::Bytes(bytes) = record.payload {
                        write_bytes(output, bytes, &mut written)?;
                    }
                }
                FormatToken::PayloadLength => match record.payload {
                    Payload::Tombstone => write_decimal(output, -1, &mut written)?,
                    Payload::Bytes(bytes) => write_decimal(output, bytes.len(), &mut written)?,
                },
                FormatToken::PayloadLengthBinary => {
                    let length = match record.payload {
                        Payload::Tombstone => -1,
                        Payload::Bytes(bytes) => {
                            i32::try_from(bytes.len()).expect("%R payload length was prevalidated")
                        }
                    };
                    write_bytes(output, &length.to_be_bytes(), &mut written)?;
                }
                FormatToken::Topic => write_bytes(output, record.topic.as_bytes(), &mut written)?,
                FormatToken::Partition => write_decimal(output, record.partition, &mut written)?,
                FormatToken::Timestamp => write_decimal(
                    output,
                    record.timestamp.map_or(-1, |value| value.milliseconds),
                    &mut written,
                )?,
                FormatToken::Headers => {
                    for (index, header) in record.headers.iter().enumerate() {
                        if index != 0 {
                            write_bytes(output, b",", &mut written)?;
                        }
                        write_bytes(output, header.name.as_bytes(), &mut written)?;
                        write_bytes(output, b"=", &mut written)?;
                        match header.value.as_deref() {
                            None => write_bytes(output, b"NULL", &mut written)?,
                            Some(value) => write_bytes(output, value, &mut written)?,
                        }
                    }
                }
                FormatToken::Action => {
                    write_bytes(output, record.action.as_str().as_bytes(), &mut written)?
                }
            }
        }
        Ok(written)
    }

    pub fn requirements(&self) -> OutputRequirements {
        OutputRequirements {
            key: self
                .0
                .iter()
                .any(|token| matches!(token, FormatToken::Key | FormatToken::KeyLength)),
            headers: self
                .0
                .iter()
                .any(|token| matches!(token, FormatToken::Headers)),
            timestamp: self
                .0
                .iter()
                .any(|token| matches!(token, FormatToken::Timestamp)),
        }
    }
}

fn flush_literal(tokens: &mut Vec<FormatToken>, literal: &mut Vec<u8>) {
    if !literal.is_empty() {
        tokens.push(FormatToken::Literal(std::mem::take(literal)));
    }
}

fn write_bytes(output: &mut impl Write, bytes: &[u8], written: &mut usize) -> io::Result<()> {
    *written = written
        .checked_add(bytes.len())
        .ok_or_else(|| io::Error::other("formatted record length overflowed usize"))?;
    output.write_all(bytes)
}

fn write_decimal(
    output: &mut impl Write,
    value: impl fmt::Display,
    written: &mut usize,
) -> io::Result<()> {
    write!(&mut DecimalWriter { output, written }, "{value}")
}

struct DecimalWriter<'a, W> {
    output: &'a mut W,
    written: &'a mut usize,
}

impl<W: Write> Write for DecimalWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.output.write(bytes)?;
        *self.written = self
            .written
            .checked_add(count)
            .ok_or_else(|| io::Error::other("formatted record length overflowed usize"))?;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

fn write_optional_length(
    output: &mut impl Write,
    value: Option<&[u8]>,
    written: &mut usize,
) -> io::Result<()> {
    match value {
        None => write_decimal(output, -1, written),
        Some(value) => write_decimal(output, value.len(), written),
    }
}

#[cfg(test)]
pub fn render_envelope(record: &OutputRecord<'_>) -> Vec<u8> {
    let mut output = Vec::new();
    write_envelope(record, &mut output).expect("writing to a vector cannot fail");
    output
}

pub fn write_envelope(record: &OutputRecord<'_>, output: &mut impl Write) -> io::Result<usize> {
    let mut written = 0;
    write_bytes(output, b"{\"topic\":", &mut written)?;
    write_json_string(output, record.topic, &mut written)?;
    write_bytes(output, b",\"partition\":", &mut written)?;
    write_decimal(output, record.partition, &mut written)?;
    write_bytes(output, b",\"offset\":", &mut written)?;
    write_decimal(output, record.offset, &mut written)?;
    write_bytes(output, b",\"timestamp\":", &mut written)?;
    match record.timestamp {
        Some(timestamp) => write_decimal(output, timestamp.milliseconds, &mut written)?,
        None => write_bytes(output, b"null", &mut written)?,
    }
    write_bytes(output, b",\"timestampType\":", &mut written)?;
    match record.timestamp.map(|value| value.kind) {
        Some(TimestampType::CreateTime) => write_json_string(output, "createTime", &mut written)?,
        Some(TimestampType::LogAppendTime) => {
            write_json_string(output, "logAppendTime", &mut written)?
        }
        None => write_bytes(output, b"null", &mut written)?,
    }
    write_bytes_fields(output, "key", record.key, &mut written)?;
    write_bytes(output, b",\"headers\":[", &mut written)?;
    for (index, header) in record.headers.iter().enumerate() {
        if index != 0 {
            write_bytes(output, b",", &mut written)?;
        }
        write_bytes(output, b"{\"name\":", &mut written)?;
        write_json_string(output, &header.name, &mut written)?;
        write_bytes_fields(output, "value", header.value.as_deref(), &mut written)?;
        write_bytes(output, b"}", &mut written)?;
    }
    write_bytes(output, b"],\"action\":", &mut written)?;
    write_json_string(output, record.action.as_str(), &mut written)?;
    match record.payload {
        Payload::Tombstone => write_bytes_fields(output, "payload", None, &mut written)?,
        Payload::Bytes(bytes) => write_bytes_fields(output, "payload", Some(bytes), &mut written)?,
    }
    write_bytes(output, b"}\n", &mut written)?;
    Ok(written)
}

fn write_bytes_fields(
    output: &mut impl Write,
    name: &str,
    value: Option<&[u8]>,
    written: &mut usize,
) -> io::Result<()> {
    write_bytes(output, b",\"", written)?;
    write_bytes(output, name.as_bytes(), written)?;
    write_bytes(output, b"\":", written)?;
    let encoding = match value {
        None => {
            write_bytes(output, b"null", written)?;
            None
        }
        Some(bytes) => {
            write_bytes(output, b"\"", written)?;
            let encoding = match std::str::from_utf8(bytes) {
                Ok(value) => {
                    write_json_string_contents(output, value, written)?;
                    "utf8"
                }
                Err(_) => {
                    write_base64(output, bytes, written)?;
                    "base64"
                }
            };
            write_bytes(output, b"\"", written)?;
            Some(encoding)
        }
    };
    write_bytes(output, b",\"", written)?;
    write_bytes(output, name.as_bytes(), written)?;
    write_bytes(output, b"Encoding\":", written)?;
    match encoding {
        Some(encoding) => write_json_string(output, encoding, written)?,
        None => write_bytes(output, b"null", written)?,
    }
    write_bytes(output, b",\"", written)?;
    write_bytes(output, name.as_bytes(), written)?;
    write_bytes(output, b"Length\":", written)?;
    match value {
        Some(bytes) => write_decimal(output, bytes.len(), written),
        None => write_decimal(output, -1, written),
    }
}

fn write_json_string(output: &mut impl Write, value: &str, written: &mut usize) -> io::Result<()> {
    write_bytes(output, b"\"", written)?;
    write_json_string_contents(output, value, written)?;
    write_bytes(output, b"\"", written)
}

fn write_json_string_contents(
    output: &mut impl Write,
    value: &str,
    written: &mut usize,
) -> io::Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = value.as_bytes();
    let mut start = 0;
    for (index, byte) in bytes.iter().copied().enumerate() {
        let escaped = match byte {
            b'"' => Some(b"\\\"".as_slice()),
            b'\\' => Some(b"\\\\".as_slice()),
            b'\x08' => Some(b"\\b".as_slice()),
            b'\x0c' => Some(b"\\f".as_slice()),
            b'\n' => Some(b"\\n".as_slice()),
            b'\r' => Some(b"\\r".as_slice()),
            b'\t' => Some(b"\\t".as_slice()),
            0..=0x1f => {
                if start != index {
                    write_bytes(output, &bytes[start..index], written)?;
                }
                write_bytes(
                    output,
                    &[
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        HEX[(byte >> 4) as usize],
                        HEX[(byte & 15) as usize],
                    ],
                    written,
                )?;
                start = index + 1;
                None
            }
            _ => None,
        };
        if let Some(escaped) = escaped {
            if start != index {
                write_bytes(output, &bytes[start..index], written)?;
            }
            write_bytes(output, escaped, written)?;
            start = index + 1;
        }
    }
    write_bytes(output, &bytes[start..], written)
}

fn write_base64(output: &mut impl Write, input: &[u8], written: &mut usize) -> io::Result<()> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = [0; 4096];
    let mut length = 0;
    for chunk in input.chunks(3) {
        if length + 4 > encoded.len() {
            write_bytes(output, &encoded[..length], written)?;
            length = 0;
        }
        let bits = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        encoded[length] = TABLE[((bits >> 18) & 63) as usize];
        encoded[length + 1] = TABLE[((bits >> 12) & 63) as usize];
        encoded[length + 2] = if chunk.len() > 1 {
            TABLE[((bits >> 6) & 63) as usize]
        } else {
            b'='
        };
        encoded[length + 3] = if chunk.len() > 2 {
            TABLE[(bits & 63) as usize]
        } else {
            b'='
        };
        length += 4;
    }
    write_bytes(output, &encoded[..length], written)
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
                name: "null".to_owned(),
                value: None,
            },
            Header {
                name: "empty".to_owned(),
                value: Some(Vec::new()),
            },
            Header {
                name: "raw".to_owned(),
                value: Some(b"x".to_vec()),
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
            CompiledFormat::compile("%a\\t%t\\t%p\\t%o\\t%T\\t%K%k\\t%S%s\\t%h%%\\x0a").unwrap();
        let mut output = Vec::new();
        let written = format.write_to(&record, &mut output).unwrap();
        assert_eq!(
            output,
            b"pass\tevents\t3\t42\t7\t1k\t1v\tnull=NULL,empty=,raw=x%\n"
        );
        assert_eq!(written, output.len());
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
            name: "trace".to_owned(),
            value: Some(vec![0xff]),
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

    #[test]
    fn streaming_envelope_primitives_escape_and_base64_encode_exactly() {
        let mut output = Vec::new();
        let mut written = 0;
        write_json_string(&mut output, "\"\\\n\u{1}", &mut written).unwrap();
        assert_eq!(output, b"\"\\\"\\\\\\n\\u0001\"");
        assert_eq!(written, output.len());

        output.clear();
        written = 0;
        write_base64(&mut output, &[0xff, 0xee], &mut written).unwrap();
        assert_eq!(output, b"/+4=");
        assert_eq!(written, output.len());
    }
}
