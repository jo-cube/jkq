use std::{cmp::Ordering, fmt, io::Write};

use simd_json::{Buffers, Node, Tape, ValueType, prelude::*, tape::Value as TapeValue};

use super::{
    compile::{CompiledExpr, CompiledKind, Constant, Function, TransformPlan},
    syntax::{BinaryOp, Literal, PathSegment, Span},
};

const MAX_JSON_DEPTH: usize = 128;
const MAX_RETAINED_BUFFER_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidJsonPolicy {
    Fail,
    Drop,
    Tombstone,
    Pass,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvaluationPolicy {
    Fail,
    Drop,
    Tombstone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrorPolicies {
    pub invalid_json: InvalidJsonPolicy,
    pub evaluation: EvaluationPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    Drop,
    Tombstone,
    PassThrough(Vec<u8>),
    Project(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionIssue {
    InvalidJson,
    Evaluation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Execution {
    pub action: Action,
    pub issue: Option<ExecutionIssue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransformError {
    InvalidJson(String),
    Evaluation {
        category: &'static str,
        span: Span,
        message: String,
    },
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson(message) => write!(f, "invalid JSON: {message}"),
            Self::Evaluation {
                category,
                span,
                message,
            } => {
                write!(
                    f,
                    "{category} evaluation error at byte {}: {message}",
                    span.start
                )
            }
        }
    }
}

impl std::error::Error for TransformError {}

#[cfg(test)]
pub fn execute(
    plan: &TransformPlan,
    payload: Option<Vec<u8>>,
    policies: ErrorPolicies,
) -> Result<Action, TransformError> {
    execute_report(plan, payload, policies).map(|execution| execution.action)
}

#[cfg(test)]
pub fn execute_report(
    plan: &TransformPlan,
    payload: Option<Vec<u8>>,
    policies: ErrorPolicies,
) -> Result<Execution, TransformError> {
    Backend::default().execute_report(plan, payload, policies)
}

pub(crate) struct Backend {
    buffers: Buffers,
    // Resetting clears borrowed nodes before the tape is stored for the next input lifetime.
    tape: Option<Tape<'static>>,
    parse_buffer: Vec<u8>,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            buffers: Buffers::default(),
            tape: Some(Tape::null()),
            parse_buffer: Vec::new(),
        }
    }
}

impl Backend {
    pub fn execute_report(
        &mut self,
        plan: &TransformPlan,
        payload: Option<Vec<u8>>,
        policies: ErrorPolicies,
    ) -> Result<Execution, TransformError> {
        let Some(payload) = payload else {
            return Ok(Execution {
                action: Action::Tombstone,
                issue: None,
            });
        };
        if !plan.capabilities.parses_json {
            return Ok(Execution {
                action: Action::PassThrough(payload),
                issue: None,
            });
        }

        let (mut parse_buffer, original) = if plan.capabilities.requires_original_bytes {
            let mut parse_buffer = std::mem::take(&mut self.parse_buffer);
            parse_buffer.clear();
            parse_buffer.extend_from_slice(&payload);
            (parse_buffer, Some(payload))
        } else {
            (payload, None)
        };
        let input_bytes = parse_buffer.len();
        let mut tape = self.tape.take().unwrap_or_else(Tape::null).reset();
        let result = match simd_json::fill_tape(&mut parse_buffer, &mut self.buffers, &mut tape) {
            Ok(()) => self.evaluate_tape(plan, &tape, original, policies),
            Err(error) => invalid_json(
                policies.invalid_json,
                original,
                format!("{:?} at byte {}", error.error(), error.index()),
            ),
        };
        let tape_bytes = tape
            .0
            .capacity()
            .saturating_mul(std::mem::size_of::<Node<'_>>());
        if input_bytes <= MAX_RETAINED_BUFFER_BYTES && tape_bytes <= MAX_RETAINED_BUFFER_BYTES {
            self.tape = Some(tape.reset());
            if plan.capabilities.requires_original_bytes
                && parse_buffer.capacity() <= MAX_RETAINED_BUFFER_BYTES
            {
                self.parse_buffer = parse_buffer;
            }
        } else {
            self.buffers = Buffers::default();
            self.tape = Some(Tape::null());
        }
        result
    }

    fn evaluate_tape(
        &self,
        plan: &TransformPlan,
        tape: &Tape<'_>,
        original: Option<Vec<u8>>,
        policies: ErrorPolicies,
    ) -> Result<Execution, TransformError> {
        if let Err(error) = validate_depth(tape) {
            return invalid_json(policies.invalid_json, original, error);
        }
        let action = match evaluate(plan, tape.as_value()) {
            Ok(EvaluatedAction::Drop) => Action::Drop,
            Ok(EvaluatedAction::Tombstone) => Action::Tombstone,
            Ok(EvaluatedAction::PassThrough) => {
                Action::PassThrough(original.expect("pass-through plan requires original bytes"))
            }
            Ok(EvaluatedAction::Project(bytes)) => Action::Project(bytes),
            Err(error) => match policies.evaluation {
                EvaluationPolicy::Fail => return Err(error),
                EvaluationPolicy::Drop => {
                    return Ok(Execution {
                        action: Action::Drop,
                        issue: Some(ExecutionIssue::Evaluation),
                    });
                }
                EvaluationPolicy::Tombstone => {
                    return Ok(Execution {
                        action: Action::Tombstone,
                        issue: Some(ExecutionIssue::Evaluation),
                    });
                }
            },
        };
        Ok(Execution {
            action,
            issue: None,
        })
    }
}

fn invalid_json(
    policy: InvalidJsonPolicy,
    original: Option<Vec<u8>>,
    message: String,
) -> Result<Execution, TransformError> {
    match policy {
        InvalidJsonPolicy::Fail => Err(TransformError::InvalidJson(message)),
        InvalidJsonPolicy::Drop => Ok(Execution {
            action: Action::Drop,
            issue: Some(ExecutionIssue::InvalidJson),
        }),
        InvalidJsonPolicy::Tombstone => Ok(Execution {
            action: Action::Tombstone,
            issue: Some(ExecutionIssue::InvalidJson),
        }),
        InvalidJsonPolicy::Pass => Ok(Execution {
            action: Action::PassThrough(original.expect("pass policy requires original bytes")),
            issue: Some(ExecutionIssue::InvalidJson),
        }),
    }
}

fn validate_depth(tape: &Tape<'_>) -> Result<(), String> {
    let mut ends = [0; MAX_JSON_DEPTH];
    let mut depth = 0;
    for (index, node) in tape.0.iter().enumerate() {
        while depth > 0 && ends[depth - 1] <= index {
            depth -= 1;
        }
        let count = match node {
            Node::Array { count, .. } | Node::Object { count, .. } => *count,
            _ => continue,
        };
        if depth == MAX_JSON_DEPTH {
            return Err(format!(
                "JSON nesting exceeds the maximum depth of {MAX_JSON_DEPTH}"
            ));
        }
        ends[depth] = index
            .checked_add(count)
            .and_then(|end| end.checked_add(1))
            .ok_or_else(|| "JSON nesting range overflowed usize".to_owned())?;
        depth += 1;
    }
    Ok(())
}

enum EvaluatedAction {
    Drop,
    Tombstone,
    PassThrough,
    Project(Vec<u8>),
}

fn evaluate<'a>(
    plan: &'a TransformPlan,
    document: TapeValue<'a, 'a>,
) -> Result<EvaluatedAction, TransformError> {
    let slots = plan
        .paths
        .iter()
        .map(|path| {
            let mut value = Some(document);
            for segment in &path.0 {
                value = match (value, segment) {
                    (Some(value), PathSegment::Field(field)) => value.get(field.as_str()),
                    (Some(value), PathSegment::Index(index)) => value.get_idx(*index),
                    _ => None,
                };
            }
            value
        })
        .collect::<Vec<_>>();

    for predicate in &plan.drops {
        if predicate_bool(predicate, &slots).map_err(|error| error.in_category("drop predicate"))? {
            return Ok(EvaluatedAction::Drop);
        }
    }
    for predicate in &plan.tombstones {
        if predicate_bool(predicate, &slots)
            .map_err(|error| error.in_category("tombstone predicate"))?
        {
            return Ok(EvaluatedAction::Tombstone);
        }
    }
    if let Some(projection) = &plan.projection {
        let value = eval(projection, &slots).map_err(|error| error.in_category("projection"))?;
        if matches!(value, Value::Missing) {
            return Err(
                evaluation_error(projection.span, "projection produced a missing value")
                    .in_category("projection"),
            );
        }
        let mut bytes = Vec::new();
        write_json(&value, &mut bytes, projection.span)
            .map_err(|error| error.in_category("projection"))?;
        Ok(EvaluatedAction::Project(bytes))
    } else {
        Ok(EvaluatedAction::PassThrough)
    }
}

fn predicate_bool(
    expression: &CompiledExpr,
    slots: &[Option<TapeValue<'_, '_>>],
) -> Result<bool, TransformError> {
    match eval(expression, slots)? {
        Value::Bool(value) => Ok(value),
        _ => Err(evaluation_error(
            expression.span,
            "predicate result must be boolean",
        )),
    }
}

#[derive(Debug)]
enum Value<'a> {
    Missing,
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(&'a str),
    Array(Vec<Value<'a>>),
    Object(Vec<(&'a str, Value<'a>)>),
    Source(TapeValue<'a, 'a>),
    Constant(&'a Constant),
}

fn eval<'a>(
    expression: &'a CompiledExpr,
    slots: &[Option<TapeValue<'a, 'a>>],
) -> Result<Value<'a>, TransformError> {
    match &expression.kind {
        CompiledKind::Literal(literal) => Ok(literal_value(literal)),
        CompiledKind::Slot(slot) => Ok(slots[*slot].map_or(Value::Missing, source_value)),
        CompiledKind::Variable { root, path } => {
            Ok(root.at(path).map_or(Value::Missing, constant_value))
        }
        CompiledKind::Array(values) => {
            let mut array = Vec::with_capacity(values.len());
            for value in values {
                let evaluated = eval(value, slots)?;
                if matches!(evaluated, Value::Missing) {
                    return Err(evaluation_error(
                        value.span,
                        "array element produced a missing value",
                    ));
                }
                array.push(evaluated);
            }
            Ok(Value::Array(array))
        }
        CompiledKind::Object(fields) => {
            let mut object = Vec::with_capacity(fields.len());
            for (key, value) in fields {
                let evaluated = eval(value, slots)?;
                if matches!(evaluated, Value::Missing) {
                    return Err(evaluation_error(
                        value.span,
                        format!("object field {key:?} produced a missing value"),
                    ));
                }
                object.push((key.as_str(), evaluated));
            }
            Ok(Value::Object(object))
        }
        CompiledKind::Not(value) => match eval(value, slots)? {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            _ => Err(evaluation_error(
                expression.span,
                "'not' operand must be boolean",
            )),
        },
        CompiledKind::Binary(left, BinaryOp::And, right) => match eval(left, slots)? {
            Value::Bool(false) => Ok(Value::Bool(false)),
            Value::Bool(true) => match eval(right, slots)? {
                Value::Bool(value) => Ok(Value::Bool(value)),
                _ => Err(evaluation_error(
                    right.span,
                    "'and' operand must be boolean",
                )),
            },
            _ => Err(evaluation_error(left.span, "'and' operand must be boolean")),
        },
        CompiledKind::Binary(left, BinaryOp::Or, right) => match eval(left, slots)? {
            Value::Bool(true) => Ok(Value::Bool(true)),
            Value::Bool(false) => match eval(right, slots)? {
                Value::Bool(value) => Ok(Value::Bool(value)),
                _ => Err(evaluation_error(right.span, "'or' operand must be boolean")),
            },
            _ => Err(evaluation_error(left.span, "'or' operand must be boolean")),
        },
        CompiledKind::Binary(left, operator, right) => {
            let left = eval(left, slots)?;
            let right = eval(right, slots)?;
            compare(left, *operator, right, expression.span).map(Value::Bool)
        }
        CompiledKind::Call(function, arguments) => {
            call(*function, arguments, slots, expression.span)
        }
    }
}

fn call<'a>(
    function: Function,
    arguments: &'a [CompiledExpr],
    slots: &[Option<TapeValue<'a, 'a>>],
    span: Span,
) -> Result<Value<'a>, TransformError> {
    if function == Function::If {
        return match eval(&arguments[0], slots)? {
            Value::Bool(true) => eval(&arguments[1], slots),
            Value::Bool(false) => eval(&arguments[2], slots),
            _ => Err(evaluation_error(span, "if condition must be boolean")),
        };
    }
    if function == Function::Coalesce {
        for (index, argument) in arguments.iter().enumerate() {
            let value = eval(argument, slots)?;
            if index + 1 == arguments.len()
                || !matches!(value_type(&value), Type::Missing | Type::Null)
            {
                return Ok(value);
            }
        }
        unreachable!("coalesce arity is checked at compile time");
    }
    if function == Function::In {
        let needle = eval(&arguments[0], slots)?;
        let haystack = eval(&arguments[1], slots)?;
        return in_array(&needle, &haystack, span).map(Value::Bool);
    }

    let first = eval(&arguments[0], slots)?;
    match function {
        Function::Exists => Ok(Value::Bool(!matches!(first, Value::Missing))),
        Function::Missing => Ok(Value::Bool(matches!(first, Value::Missing))),
        Function::IsNull => Ok(Value::Bool(value_type(&first) == Type::Null)),
        Function::IsBoolean => Ok(Value::Bool(value_type(&first) == Type::Bool)),
        Function::IsNumber => Ok(Value::Bool(value_type(&first) == Type::Number)),
        Function::IsString => Ok(Value::Bool(value_type(&first) == Type::String)),
        Function::IsArray => Ok(Value::Bool(value_type(&first) == Type::Array)),
        Function::IsObject => Ok(Value::Bool(value_type(&first) == Type::Object)),
        Function::Length => match first {
            Value::Missing => Ok(Value::Missing),
            Value::String(value) => Ok(Value::U64(value.chars().count() as u64)),
            Value::Array(value) => Ok(Value::U64(value.len() as u64)),
            Value::Object(value) => Ok(Value::U64(value.len() as u64)),
            Value::Constant(Constant::Array(value)) => Ok(Value::U64(value.len() as u64)),
            Value::Constant(Constant::Object(value)) => Ok(Value::U64(value.len() as u64)),
            Value::Constant(Constant::Literal(Literal::String(value))) => {
                Ok(Value::U64(value.chars().count() as u64))
            }
            Value::Source(value) if value.value_type() == ValueType::String => Ok(Value::U64(
                value.as_str().expect("string tape value").chars().count() as u64,
            )),
            Value::Source(value) if value.value_type() == ValueType::Array => Ok(Value::U64(
                value.as_array().expect("array tape value").len() as u64,
            )),
            Value::Source(value) if value.value_type() == ValueType::Object => Ok(Value::U64(
                value.as_object().expect("object tape value").len() as u64,
            )),
            _ => Err(evaluation_error(
                span,
                "length expects a string, array, or object",
            )),
        },
        Function::Contains | Function::StartsWith | Function::EndsWith => {
            let second = eval(&arguments[1], slots)?;
            if matches!(first, Value::Missing) || matches!(second, Value::Missing) {
                return Ok(Value::Bool(false));
            }
            let left = as_string(&first).ok_or_else(|| {
                evaluation_error(span, "string function expects string arguments")
            })?;
            let right = as_string(&second).ok_or_else(|| {
                evaluation_error(span, "string function expects string arguments")
            })?;
            Ok(Value::Bool(match function {
                Function::Contains => left.contains(right),
                Function::StartsWith => left.starts_with(right),
                Function::EndsWith => left.ends_with(right),
                _ => unreachable!(),
            }))
        }
        Function::Coalesce | Function::If | Function::In => unreachable!(),
    }
}

fn in_array(needle: &Value<'_>, haystack: &Value<'_>, span: Span) -> Result<bool, TransformError> {
    if matches!(needle, Value::Missing) || matches!(haystack, Value::Missing) {
        return Ok(false);
    }
    if matches!(value_type(needle), Type::Array | Type::Object) {
        return Err(evaluation_error(span, "in supports only scalar values"));
    }
    match haystack {
        Value::Array(values) => {
            for value in values {
                if equal(needle, value, span)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Value::Constant(Constant::Array(values)) => {
            // ponytail: membership is linear; index constant arrays if large allowlists profile hot.
            for value in values {
                if equal(needle, &constant_value(value), span)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Value::Source(value) if value.value_type() == ValueType::Array => {
            let values = value.as_array().expect("array tape value");
            for value in values.iter() {
                if equal(needle, &source_value(value), span)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Err(evaluation_error(
            span,
            "in expects an array as its second argument",
        )),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Type {
    Missing,
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

fn value_type(value: &Value<'_>) -> Type {
    match value {
        Value::Missing => Type::Missing,
        Value::Null => Type::Null,
        Value::Bool(_) => Type::Bool,
        Value::I64(_) | Value::U64(_) | Value::F64(_) => Type::Number,
        Value::String(_) => Type::String,
        Value::Array(_) => Type::Array,
        Value::Object(_) => Type::Object,
        Value::Constant(value) => match value {
            Constant::Literal(Literal::Null) => Type::Null,
            Constant::Literal(Literal::Bool(_)) => Type::Bool,
            Constant::Literal(Literal::I64(_) | Literal::U64(_) | Literal::F64(_)) => Type::Number,
            Constant::Literal(Literal::String(_)) => Type::String,
            Constant::Array(_) => Type::Array,
            Constant::Object(_) => Type::Object,
        },
        Value::Source(value) => match value.value_type() {
            ValueType::Null => Type::Null,
            ValueType::Bool => Type::Bool,
            ValueType::I64 | ValueType::U64 | ValueType::F64 => Type::Number,
            ValueType::String => Type::String,
            ValueType::Array => Type::Array,
            ValueType::Object => Type::Object,
            _ => unreachable!("unsupported tape value type"),
        },
    }
}

fn source_value<'a>(value: TapeValue<'a, 'a>) -> Value<'a> {
    match value.value_type() {
        ValueType::Null => Value::Null,
        ValueType::Bool => Value::Bool(value.as_bool().expect("boolean tape value")),
        ValueType::I64 => Value::I64(value.as_i64().expect("signed tape value")),
        ValueType::U64 => Value::U64(value.as_u64().expect("unsigned tape value")),
        ValueType::F64 => Value::F64(value.as_f64().expect("float tape value")),
        _ => Value::Source(value),
    }
}

fn literal_value(value: &Literal) -> Value<'_> {
    match value {
        Literal::Null => Value::Null,
        Literal::Bool(value) => Value::Bool(*value),
        Literal::I64(value) => Value::I64(*value),
        Literal::U64(value) => Value::U64(*value),
        Literal::F64(value) => Value::F64(*value),
        Literal::String(value) => Value::String(value),
    }
}

fn constant_value(value: &Constant) -> Value<'_> {
    match value {
        Constant::Literal(value) => literal_value(value),
        Constant::Array(_) | Constant::Object(_) => Value::Constant(value),
    }
}

fn as_string<'a>(value: &'a Value<'a>) -> Option<&'a str> {
    match value {
        Value::String(value) => Some(value),
        Value::Constant(Constant::Literal(Literal::String(value))) => Some(value),
        Value::Source(value) => value.as_str(),
        _ => None,
    }
}

fn compare(
    left: Value<'_>,
    operator: BinaryOp,
    right: Value<'_>,
    span: Span,
) -> Result<bool, TransformError> {
    if matches!(operator, BinaryOp::Equal | BinaryOp::NotEqual) {
        if matches!(left, Value::Missing) || matches!(right, Value::Missing) {
            return Ok(operator == BinaryOp::NotEqual);
        }
        let equal = equal(&left, &right, span)?;
        return Ok(if operator == BinaryOp::Equal {
            equal
        } else {
            !equal
        });
    }
    if matches!(left, Value::Missing) || matches!(right, Value::Missing) {
        return Ok(false);
    }
    let ordering = if let (Some(left), Some(right)) = (number(&left), number(&right)) {
        numeric_order(left, right)
    } else if let (Some(left), Some(right)) = (as_string(&left), as_string(&right)) {
        Some(left.cmp(right))
    } else {
        return Err(evaluation_error(
            span,
            "ordering comparison requires two numbers or two strings",
        ));
    }
    .ok_or_else(|| evaluation_error(span, "numbers are not orderable"))?;
    Ok(match operator {
        BinaryOp::Less => ordering == Ordering::Less,
        BinaryOp::LessEqual => ordering != Ordering::Greater,
        BinaryOp::Greater => ordering == Ordering::Greater,
        BinaryOp::GreaterEqual => ordering != Ordering::Less,
        _ => unreachable!(),
    })
}

fn equal(left: &Value<'_>, right: &Value<'_>, span: Span) -> Result<bool, TransformError> {
    if let (Some(left), Some(right)) = (number(left), number(right)) {
        return Ok(numeric_order(left, right) == Some(Ordering::Equal));
    }
    Ok(match (value_type(left), value_type(right)) {
        (Type::Array | Type::Object, Type::Array | Type::Object) => {
            return Err(evaluation_error(
                span,
                "array and object equality is not supported",
            ));
        }
        (Type::Null, Type::Null) => true,
        (Type::Bool, Type::Bool) => bool_value(left) == bool_value(right),
        (Type::String, Type::String) => as_string(left) == as_string(right),
        _ => false,
    })
}

#[derive(Clone, Copy)]
enum Number {
    I64(i64),
    U64(u64),
    F64(f64),
}

const I64_EXCLUSIVE_MAX_F64: f64 = 9_223_372_036_854_775_808.0;
const U64_EXCLUSIVE_MAX_F64: f64 = 18_446_744_073_709_551_616.0;

fn number(value: &Value<'_>) -> Option<Number> {
    Some(match value {
        Value::I64(value) => Number::I64(*value),
        Value::U64(value) => Number::U64(*value),
        Value::F64(value) => Number::F64(*value),
        Value::Constant(Constant::Literal(Literal::I64(value))) => Number::I64(*value),
        Value::Constant(Constant::Literal(Literal::U64(value))) => Number::U64(*value),
        Value::Constant(Constant::Literal(Literal::F64(value))) => Number::F64(*value),
        Value::Source(value) if value.value_type() == ValueType::I64 => {
            Number::I64(value.as_i64().expect("signed tape value"))
        }
        Value::Source(value) if value.value_type() == ValueType::U64 => {
            Number::U64(value.as_u64().expect("unsigned tape value"))
        }
        Value::Source(value) if value.value_type() == ValueType::F64 => {
            Number::F64(value.as_f64().expect("float tape value"))
        }
        _ => return None,
    })
}

fn numeric_order(left: Number, right: Number) -> Option<Ordering> {
    match (left, right) {
        (Number::I64(left), Number::I64(right)) => Some(left.cmp(&right)),
        (Number::U64(left), Number::U64(right)) => Some(left.cmp(&right)),
        (Number::I64(left), Number::U64(right)) => {
            if left < 0 {
                Some(Ordering::Less)
            } else {
                Some((left as u64).cmp(&right))
            }
        }
        (Number::U64(left), Number::I64(right)) => {
            if right < 0 {
                Some(Ordering::Greater)
            } else {
                Some(left.cmp(&(right as u64)))
            }
        }
        (Number::F64(left), Number::F64(right)) => left.partial_cmp(&right),
        (Number::F64(left), Number::I64(right)) => {
            integer_float_order(right, left).map(Ordering::reverse)
        }
        (Number::F64(left), Number::U64(right)) => {
            unsigned_float_order(right, left).map(Ordering::reverse)
        }
        (Number::I64(left), Number::F64(right)) => integer_float_order(left, right),
        (Number::U64(left), Number::F64(right)) => unsigned_float_order(left, right),
    }
}

fn integer_float_order(integer: i64, float: f64) -> Option<Ordering> {
    if float.is_nan() {
        return None;
    }
    if float < i64::MIN as f64 {
        return Some(Ordering::Greater);
    }
    if float >= I64_EXCLUSIVE_MAX_F64 {
        return Some(Ordering::Less);
    }
    let truncated = float as i64;
    Some(match integer.cmp(&truncated) {
        Ordering::Equal if float.fract() == 0.0 => Ordering::Equal,
        Ordering::Equal if float.is_sign_negative() => Ordering::Greater,
        Ordering::Equal => Ordering::Less,
        ordering => ordering,
    })
}

fn unsigned_float_order(integer: u64, float: f64) -> Option<Ordering> {
    if float.is_nan() {
        return None;
    }
    if float < 0.0 {
        return Some(Ordering::Greater);
    }
    if float >= U64_EXCLUSIVE_MAX_F64 {
        return Some(Ordering::Less);
    }
    let truncated = float as u64;
    Some(match integer.cmp(&truncated) {
        Ordering::Equal if float.fract() == 0.0 => Ordering::Equal,
        Ordering::Equal => Ordering::Less,
        ordering => ordering,
    })
}

fn bool_value(value: &Value<'_>) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Constant(Constant::Literal(Literal::Bool(value))) => Some(*value),
        Value::Source(value) => value.as_bool(),
        _ => None,
    }
}

fn write_json(value: &Value<'_>, output: &mut Vec<u8>, span: Span) -> Result<(), TransformError> {
    match value {
        Value::Missing => return Err(evaluation_error(span, "cannot serialize missing value")),
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::I64(value) => write!(output, "{value}").expect("writing to a vector cannot fail"),
        Value::U64(value) => write!(output, "{value}").expect("writing to a vector cannot fail"),
        Value::F64(value) => {
            if !value.is_finite() {
                return Err(evaluation_error(span, "cannot serialize non-finite number"));
            }
            write!(output, "{value:?}").expect("writing to a vector cannot fail");
        }
        Value::String(value) => write_string(value, output),
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_json(value, output, span)?;
            }
            output.push(b']');
        }
        Value::Object(fields) => {
            output.push(b'{');
            for (index, (key, value)) in fields.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_string(key, output);
                output.push(b':');
                write_json(value, output, span)?;
            }
            output.push(b'}');
        }
        Value::Constant(value) => write_constant(value, output, span)?,
        Value::Source(value) => value
            .write(output)
            .map_err(|error| evaluation_error(span, format!("cannot serialize value: {error}")))?,
    }
    Ok(())
}

fn write_constant(
    value: &Constant,
    output: &mut Vec<u8>,
    span: Span,
) -> Result<(), TransformError> {
    match value {
        Constant::Literal(value) => write_json(&literal_value(value), output, span),
        Constant::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_constant(value, output, span)?;
            }
            output.push(b']');
            Ok(())
        }
        Constant::Object(fields) => {
            output.push(b'{');
            for (index, (key, value)) in fields.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_string(key, output);
                output.push(b':');
                write_constant(value, output, span)?;
            }
            output.push(b'}');
            Ok(())
        }
    }
}

pub(crate) fn write_string(value: &str, output: &mut Vec<u8>) {
    output.push(b'"');
    for character in value.chars() {
        match character {
            '"' => output.extend_from_slice(b"\\\""),
            '\\' => output.extend_from_slice(b"\\\\"),
            '\u{8}' => output.extend_from_slice(b"\\b"),
            '\u{c}' => output.extend_from_slice(b"\\f"),
            '\n' => output.extend_from_slice(b"\\n"),
            '\r' => output.extend_from_slice(b"\\r"),
            '\t' => output.extend_from_slice(b"\\t"),
            value if value < '\u{20}' => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let value = value as usize;
                output.extend_from_slice(b"\\u00");
                output.push(HEX[(value >> 4) & 15]);
                output.push(HEX[value & 15]);
            }
            value => {
                let mut bytes = [0; 4];
                output.extend_from_slice(value.encode_utf8(&mut bytes).as_bytes());
            }
        }
    }
    output.push(b'"');
}

fn evaluation_error(span: Span, message: impl Into<String>) -> TransformError {
    TransformError::Evaluation {
        category: "expression",
        span,
        message: message.into(),
    }
}

impl TransformError {
    fn in_category(mut self, category: &'static str) -> Self {
        if let Self::Evaluation {
            category: current, ..
        } = &mut self
        {
            *current = category;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::compile::{JsonRequirement, build_plan};

    const FAIL: ErrorPolicies = ErrorPolicies {
        invalid_json: InvalidJsonPolicy::Fail,
        evaluation: EvaluationPolicy::Fail,
    };

    fn run(
        drops: &[&str],
        tombstones: &[&str],
        projection: Option<&str>,
        input: Option<&[u8]>,
    ) -> Result<Action, TransformError> {
        run_with_vars(drops, tombstones, projection, None, input)
    }

    fn run_with_vars(
        drops: &[&str],
        tombstones: &[&str],
        projection: Option<&str>,
        variables: Option<&str>,
        input: Option<&[u8]>,
    ) -> Result<Action, TransformError> {
        let plan = build_plan(
            &drops.iter().map(ToString::to_string).collect::<Vec<_>>(),
            &tombstones
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            projection,
            variables,
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        execute(&plan, input.map(<[u8]>::to_vec), FAIL)
    }

    #[test]
    fn input_tombstone_bypasses_invalid_predicates() {
        assert_eq!(
            run(&["length(true) == 1"], &[], Some(".missing"), None).unwrap(),
            Action::Tombstone
        );
    }

    #[test]
    fn missing_and_null_remain_distinct() {
        let action = run(
            &[],
            &[],
            Some("[exists(.value), missing(.value), exists(.absent), missing(.absent)]"),
            Some(br#"{"value":null}"#),
        )
        .unwrap();
        assert_eq!(action, Action::Project(b"[true,false,false,true]".to_vec()));
    }

    #[test]
    fn action_precedence_is_drop_tombstone_projection_pass() {
        assert_eq!(
            run(&["true"], &["true"], Some("1"), Some(b"{}")).unwrap(),
            Action::Drop
        );
        assert_eq!(
            run(&["false"], &["true"], Some("1"), Some(b"{}")).unwrap(),
            Action::Tombstone
        );
        assert_eq!(
            run(&[], &[], Some("1"), Some(b"{}")).unwrap(),
            Action::Project(b"1".to_vec())
        );
        assert_eq!(
            run(&[], &[], None, Some(b" { \"x\" : 1 } ")).unwrap(),
            Action::PassThrough(b" { \"x\" : 1 } ".to_vec())
        );
    }

    #[test]
    fn root_expression_projects_the_complete_document() {
        assert_eq!(
            run(&[], &[], Some("."), Some(b" { \"value\" : [1, 2] } ")).unwrap(),
            Action::Project(br#"{"value":[1,2]}"#.to_vec())
        );
        assert_eq!(run(&["."], &[], None, Some(b"true")).unwrap(), Action::Drop);
    }

    #[test]
    fn boolean_operators_short_circuit_errors() {
        assert_eq!(
            run(&["true or length(true) == 1"], &[], None, Some(b"{}")).unwrap(),
            Action::Drop
        );
        assert!(run(&["false or length(true) == 1"], &[], None, Some(b"{}")).is_err());
    }

    #[test]
    fn boolean_paths_are_valid_strict_predicates() {
        assert_eq!(
            run(&[".active"], &[], None, Some(br#"{"active":true}"#)).unwrap(),
            Action::Drop
        );
        assert!(run(&[".count"], &[], None, Some(br#"{"count":1}"#)).is_err());
    }

    #[test]
    fn missing_equality_and_ordering_follow_the_language_contract() {
        assert_eq!(
            run(
                &[".absent != 1 and not (.absent == 1) and not (.absent < 1)"],
                &[],
                None,
                Some(b"{}"),
            )
            .unwrap(),
            Action::Drop
        );
    }

    #[test]
    fn projected_null_is_payload_and_object_order_and_escaping_are_stable() {
        assert_eq!(
            run(&[], &[], Some("null"), Some(b"{}")).unwrap(),
            Action::Project(b"null".to_vec())
        );
        assert_eq!(
            run(
                &[],
                &[],
                Some("{second: .text, first: .number}"),
                Some(br#"{"text":"a\n\"b","number":1}"#),
            )
            .unwrap(),
            Action::Project(br#"{"second":"a\n\"b","first":1}"#.to_vec())
        );
    }

    #[test]
    fn coalesce_handles_missing_and_null_but_missing_projection_fails() {
        assert_eq!(
            run(
                &[],
                &[],
                Some("[coalesce(.a, 1), coalesce(.b, 2)]"),
                Some(br#"{"b":null}"#)
            )
            .unwrap(),
            Action::Project(b"[1,2]".to_vec())
        );
        assert!(run(&[], &[], Some(".a"), Some(b"{}")).is_err());
    }

    #[test]
    fn variables_project_nested_constants_and_follow_missing_semantics() {
        let action = run_with_vars(
            &[],
            &[],
            Some(
                "{root: $vars, tenant: $vars.tenant, dotted: $vars[\"a.b\"], first: $vars.statuses[0], count: length($vars.statuses), array: is_array($vars.statuses), policy: $vars.policy, fallback: coalesce($vars.absent, \"default\")}",
            ),
            Some(
                "{tenant: \"acme\", \"a.b\": 7, statuses: [\"open\", \"pending\"], policy: {cutoff: 10}}",
            ),
            Some(b"{}"),
        )
        .unwrap();
        assert_eq!(
            action,
            Action::Project(
                br#"{"root":{"tenant":"acme","a.b":7,"statuses":["open","pending"],"policy":{"cutoff":10}},"tenant":"acme","dotted":7,"first":"open","count":2,"array":true,"policy":{"cutoff":10},"fallback":"default"}"#
                    .to_vec()
            )
        );
    }

    #[test]
    fn if_evaluates_only_the_selected_branch_and_requires_a_boolean() {
        assert_eq!(
            run(
                &[],
                &[],
                Some("[if(true, 1, length(true)), if(false, length(true), 2)]"),
                Some(b"{}"),
            )
            .unwrap(),
            Action::Project(b"[1,2]".to_vec())
        );
        assert!(run(&[], &[], Some("if(1, 2, 3)"), Some(b"{}")).is_err());
    }

    #[test]
    fn in_supports_scalar_membership_across_array_representations() {
        assert_eq!(
            run_with_vars(
                &[
                    "in(.status, $vars.statuses) and in(.large, $vars.numbers) and in(null, $vars.values) and in(\"x\", .tags) and in(2, [1, 2])",
                ],
                &[],
                None,
                Some(
                    "{statuses: [\"open\", \"pending\"], numbers: [9007199254740993], values: [null, false]}",
                ),
                Some(
                    br#"{"status":"open","large":9007199254740993,"tags":["x","y"]}"#,
                ),
            )
            .unwrap(),
            Action::Drop
        );
        assert_eq!(
            run_with_vars(
                &["in(.missing, $vars.values)"],
                &[],
                None,
                Some("{values: [null]}"),
                Some(b"{}"),
            )
            .unwrap(),
            Action::PassThrough(b"{}".to_vec())
        );
        assert_eq!(
            run(&["in([1], .missing)"], &[], None, Some(b"{}")).unwrap(),
            Action::PassThrough(b"{}".to_vec())
        );
        assert!(run(&["in(1, 1)"], &[], None, Some(b"{}")).is_err());
        assert!(run(&["in([1], [[1]])"], &[], None, Some(b"{}")).is_err());
    }

    #[test]
    fn coalesce_accepts_multiple_fallbacks_and_short_circuits() {
        assert_eq!(
            run(
                &[],
                &[],
                Some("[coalesce(.a, .b, .c, \"fallback\"), coalesce(\"first\", length(true), 3)]"),
                Some(br#"{"a":null}"#),
            )
            .unwrap(),
            Action::Project(br#"["fallback","first"]"#.to_vec())
        );
        assert!(run(&[], &[], Some("coalesce(.a, .b, .c)"), Some(b"{}")).is_err());
    }

    #[test]
    fn invalid_json_pass_preserves_exact_bytes() {
        let plan = build_plan(
            &["true".to_owned()],
            &[],
            None,
            None,
            JsonRequirement::PreserveInvalid,
        )
        .unwrap();
        let input = b"not json".to_vec();
        let action = execute(
            &plan,
            Some(input.clone()),
            ErrorPolicies {
                invalid_json: InvalidJsonPolicy::Pass,
                evaluation: EvaluationPolicy::Fail,
            },
        )
        .unwrap();
        assert_eq!(action, Action::PassThrough(input));
    }

    #[test]
    fn worker_backend_reuses_normal_scratch_and_discards_oversized_scratch() {
        let pass = build_plan(
            &["false".to_owned()],
            &[],
            None,
            None,
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        let mut backend = Backend::default();
        for input in [br#"{"value":"first"}"#.as_slice(), br#"{"value":2}"#] {
            let execution = backend
                .execute_report(&pass, Some(input.to_vec()), FAIL)
                .unwrap();
            assert_eq!(execution.action, Action::PassThrough(input.to_vec()));
        }
        assert!(backend.parse_buffer.capacity() >= br#"{"value":"first"}"#.len());
        assert!(backend.tape.as_ref().unwrap().0.capacity() > 1);

        let drop = build_plan(
            &["true".to_owned()],
            &[],
            None,
            None,
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        let mut oversized = vec![b' '; MAX_RETAINED_BUFFER_BYTES + 1];
        oversized.extend_from_slice(b"null");
        assert_eq!(
            backend
                .execute_report(&drop, Some(oversized), FAIL)
                .unwrap()
                .action,
            Action::Drop
        );
        assert_eq!(backend.parse_buffer.capacity(), 0);
        assert_eq!(backend.tape.as_ref().unwrap().0.len(), 1);
    }

    #[test]
    fn native_integer_comparisons_do_not_lose_unsigned_precision() {
        for (left, right, expected) in [
            (
                Number::U64(9_007_199_254_740_993),
                Number::F64(9_007_199_254_740_992.0),
                Ordering::Greater,
            ),
            (
                Number::I64(i64::MIN),
                Number::F64(i64::MIN as f64),
                Ordering::Equal,
            ),
            (
                Number::I64(i64::MAX),
                Number::F64(I64_EXCLUSIVE_MAX_F64),
                Ordering::Less,
            ),
            (
                Number::U64(u64::MAX),
                Number::F64(U64_EXCLUSIVE_MAX_F64),
                Ordering::Less,
            ),
            (Number::I64(-1), Number::F64(-1.5), Ordering::Greater),
            (Number::U64(1), Number::F64(1.5), Ordering::Less),
        ] {
            assert_eq!(numeric_order(left, right), Some(expected));
            assert_eq!(numeric_order(right, left), Some(expected.reverse()));
        }
        assert_eq!(
            run(
                &[".large > 18446744073709551614"],
                &[],
                None,
                Some(br#"{"large":18446744073709551615}"#),
            )
            .unwrap(),
            Action::Drop
        );
        assert_eq!(
            run(
                &[".large != 9007199254740992.0"],
                &[],
                None,
                Some(br#"{"large":9007199254740993}"#),
            )
            .unwrap(),
            Action::Drop
        );
    }

    #[test]
    fn floating_point_projection_uses_short_round_trip_json() {
        assert_eq!(
            run(&[], &[], Some(".value"), Some(br#"{"value":1e200}"#)).unwrap(),
            Action::Project(b"1e200".to_vec())
        );
    }

    #[test]
    fn compiled_payload_budget_covers_serialized_projections() {
        for (projection, input) in [
            (".", b"1e15".as_slice()),
            (".value", br#"{"value":{"n":1e200,"s":"a\\nb"}}"#.as_slice()),
            ("[.value, .value]", br#"{"value":"repeated"}"#.as_slice()),
            (
                "{value: coalesce(.missing, .value), size: length(.value)}",
                br#"{"value":[1,2,3]}"#.as_slice(),
            ),
        ] {
            let plan =
                build_plan(&[], &[], Some(projection), None, JsonRequirement::AsNeeded).unwrap();
            let budget = plan.payload_budget().bytes(input.len()).unwrap();
            let execution = execute_report(&plan, Some(input.to_vec()), FAIL).unwrap();
            let Action::Project(output) = execution.action else {
                panic!("expected projection");
            };
            assert!(
                input.len() + output.len() <= budget,
                "{projection}: input={} output={} budget={budget}",
                input.len(),
                output.len()
            );
        }
    }

    #[test]
    fn string_functions_and_length_use_unicode_scalar_values() {
        assert_eq!(
            run(
                &[
                    "contains(.text, \"é\") and starts_with(.text, \"a\") and ends_with(.text, \"z\") and length(.text) == 3",
                ],
                &[],
                None,
                Some(r#"{"text":"aéz"}"#.as_bytes()),
            )
            .unwrap(),
            Action::Drop
        );
    }

    #[test]
    fn error_policies_convert_invalid_json_and_evaluation_failures() {
        let invalid = build_plan(
            &["true".to_owned()],
            &[],
            None,
            None,
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        for (policy, expected) in [
            (InvalidJsonPolicy::Drop, Action::Drop),
            (InvalidJsonPolicy::Tombstone, Action::Tombstone),
        ] {
            let execution = execute_report(
                &invalid,
                Some(b"invalid".to_vec()),
                ErrorPolicies {
                    invalid_json: policy,
                    evaluation: EvaluationPolicy::Fail,
                },
            )
            .unwrap();
            assert_eq!(execution.action, expected);
            assert_eq!(execution.issue, Some(ExecutionIssue::InvalidJson));
        }

        let evaluation = build_plan(
            &["length(true) == 1".to_owned()],
            &[],
            None,
            None,
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        for (policy, expected) in [
            (EvaluationPolicy::Drop, Action::Drop),
            (EvaluationPolicy::Tombstone, Action::Tombstone),
        ] {
            let execution = execute_report(
                &evaluation,
                Some(b"{}".to_vec()),
                ErrorPolicies {
                    invalid_json: InvalidJsonPolicy::Fail,
                    evaluation: policy,
                },
            )
            .unwrap();
            assert_eq!(execution.action, expected);
            assert_eq!(execution.issue, Some(ExecutionIssue::Evaluation));
        }
    }

    #[test]
    fn evaluation_errors_identify_the_expression_category() {
        let error = run(&[], &["length(true) == 1"], None, Some(b"{}")).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("tombstone predicate evaluation error")
        );
    }

    #[test]
    fn invalid_json_errors_report_coordinates_without_payload_content() {
        let plan = build_plan(
            &["false".to_owned()],
            &[],
            None,
            None,
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        let error = execute(&plan, Some(b"secret".to_vec()), FAIL).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("at byte"), "{message}");
        assert!(!message.contains("secret"), "{message}");
    }

    #[test]
    fn excessive_json_nesting_uses_the_invalid_json_policy() {
        let plan = build_plan(
            &["false".to_owned()],
            &[],
            None,
            None,
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        let accepted = format!(
            "{}0{}",
            "[".repeat(MAX_JSON_DEPTH),
            "]".repeat(MAX_JSON_DEPTH)
        )
        .into_bytes();
        assert_eq!(
            execute(&plan, Some(accepted.clone()), FAIL).unwrap(),
            Action::PassThrough(accepted)
        );

        let input = format!(
            "{}0{}",
            "[".repeat(MAX_JSON_DEPTH + 1),
            "]".repeat(MAX_JSON_DEPTH + 1)
        )
        .into_bytes();
        let error = execute(&plan, Some(input.clone()), FAIL).unwrap_err();
        assert!(error.to_string().contains("maximum depth"));

        let execution = execute_report(
            &plan,
            Some(input),
            ErrorPolicies {
                invalid_json: InvalidJsonPolicy::Drop,
                evaluation: EvaluationPolicy::Fail,
            },
        )
        .unwrap();
        assert_eq!(execution.action, Action::Drop);
        assert_eq!(execution.issue, Some(ExecutionIssue::InvalidJson));
    }

    #[test]
    fn tape_projection_preserves_nested_source_values() {
        assert_eq!(
            run(
                &[],
                &[],
                Some(".value"),
                Some(br#"{"value":{"b":[1,{"x":"y"}],"a":2}}"#),
            )
            .unwrap(),
            Action::Project(br#"{"b":[1,{"x":"y"}],"a":2}"#.to_vec())
        );
    }
}
