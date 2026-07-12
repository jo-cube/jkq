use std::{borrow::Cow, cmp::Ordering, fmt};

use simd_json::{BorrowedValue, StaticNode};

use super::{
    compile::{CompiledExpr, CompiledKind, Function, TransformPlan},
    syntax::{BinaryOp, Literal, PathSegment, Span},
};

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
    Evaluation { span: Span, message: String },
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson(message) => write!(f, "invalid JSON: {message}"),
            Self::Evaluation { span, message } => {
                write!(f, "evaluation error at byte {}: {message}", span.start)
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

pub fn execute_report(
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
        (payload.clone(), Some(payload))
    } else {
        (payload, None)
    };
    let document = match simd_json::to_borrowed_value(&mut parse_buffer) {
        Ok(document) => document,
        Err(error) => {
            return match policies.invalid_json {
                InvalidJsonPolicy::Fail => Err(TransformError::InvalidJson(error.to_string())),
                InvalidJsonPolicy::Drop => Ok(Execution {
                    action: Action::Drop,
                    issue: Some(ExecutionIssue::InvalidJson),
                }),
                InvalidJsonPolicy::Tombstone => Ok(Execution {
                    action: Action::Tombstone,
                    issue: Some(ExecutionIssue::InvalidJson),
                }),
                InvalidJsonPolicy::Pass => Ok(Execution {
                    action: Action::PassThrough(
                        original.expect("pass policy requires original bytes"),
                    ),
                    issue: Some(ExecutionIssue::InvalidJson),
                }),
            };
        }
    };
    let action = match evaluate(plan, &document) {
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

enum EvaluatedAction {
    Drop,
    Tombstone,
    PassThrough,
    Project(Vec<u8>),
}

fn evaluate(
    plan: &TransformPlan,
    document: &BorrowedValue<'_>,
) -> Result<EvaluatedAction, TransformError> {
    let slots = plan
        .paths
        .iter()
        .map(|path| {
            let mut value = Some(document);
            for segment in &path.0 {
                value = match (value, segment) {
                    (Some(BorrowedValue::Object(object)), PathSegment::Field(field)) => {
                        object.get(field.as_str())
                    }
                    (Some(BorrowedValue::Array(array)), PathSegment::Index(index)) => {
                        array.get(*index)
                    }
                    _ => None,
                };
            }
            value
        })
        .collect::<Vec<_>>();

    for predicate in &plan.drops {
        if predicate_bool(predicate, &slots)? {
            return Ok(EvaluatedAction::Drop);
        }
    }
    for predicate in &plan.tombstones {
        if predicate_bool(predicate, &slots)? {
            return Ok(EvaluatedAction::Tombstone);
        }
    }
    if let Some(projection) = &plan.projection {
        let value = eval(projection, &slots)?;
        if matches!(value, Value::Missing) {
            return Err(evaluation_error(
                projection.span,
                "projection produced a missing value",
            ));
        }
        let mut bytes = Vec::new();
        write_json(&value, &mut bytes, projection.span)?;
        Ok(EvaluatedAction::Project(bytes))
    } else {
        Ok(EvaluatedAction::PassThrough)
    }
}

fn predicate_bool(
    expression: &CompiledExpr,
    slots: &[Option<&BorrowedValue<'_>>],
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
    String(Cow<'a, str>),
    Array(Vec<Value<'a>>),
    Object(Vec<(String, Value<'a>)>),
    Source(&'a BorrowedValue<'a>),
}

fn eval<'a>(
    expression: &CompiledExpr,
    slots: &[Option<&'a BorrowedValue<'a>>],
) -> Result<Value<'a>, TransformError> {
    match &expression.kind {
        CompiledKind::Literal(literal) => Ok(match literal {
            Literal::Null => Value::Null,
            Literal::Bool(value) => Value::Bool(*value),
            Literal::I64(value) => Value::I64(*value),
            Literal::U64(value) => Value::U64(*value),
            Literal::F64(value) => Value::F64(*value),
            Literal::String(value) => Value::String(Cow::Owned(value.clone())),
        }),
        CompiledKind::Slot(slot) => Ok(slots[*slot].map_or(Value::Missing, source_value)),
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
                object.push((key.clone(), evaluated));
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
    arguments: &[CompiledExpr],
    slots: &[Option<&'a BorrowedValue<'a>>],
    span: Span,
) -> Result<Value<'a>, TransformError> {
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
            Value::Source(BorrowedValue::String(value)) => {
                Ok(Value::U64(value.chars().count() as u64))
            }
            Value::Source(BorrowedValue::Array(value)) => Ok(Value::U64(value.len() as u64)),
            Value::Source(BorrowedValue::Object(value)) => Ok(Value::U64(value.len() as u64)),
            _ => Err(evaluation_error(
                span,
                "length expects a string, array, or object",
            )),
        },
        Function::Coalesce => {
            if matches!(value_type(&first), Type::Missing | Type::Null) {
                eval(&arguments[1], slots)
            } else {
                Ok(first)
            }
        }
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
        Value::Null | Value::Source(BorrowedValue::Static(StaticNode::Null)) => Type::Null,
        Value::Bool(_) | Value::Source(BorrowedValue::Static(StaticNode::Bool(_))) => Type::Bool,
        Value::I64(_)
        | Value::U64(_)
        | Value::F64(_)
        | Value::Source(BorrowedValue::Static(
            StaticNode::I64(_) | StaticNode::U64(_) | StaticNode::F64(_),
        )) => Type::Number,
        Value::String(_) | Value::Source(BorrowedValue::String(_)) => Type::String,
        Value::Array(_) | Value::Source(BorrowedValue::Array(_)) => Type::Array,
        Value::Object(_) | Value::Source(BorrowedValue::Object(_)) => Type::Object,
    }
}

fn source_value<'a>(value: &'a BorrowedValue<'a>) -> Value<'a> {
    match value {
        BorrowedValue::Static(StaticNode::Null) => Value::Null,
        BorrowedValue::Static(StaticNode::Bool(value)) => Value::Bool(*value),
        BorrowedValue::Static(StaticNode::I64(value)) => Value::I64(*value),
        BorrowedValue::Static(StaticNode::U64(value)) => Value::U64(*value),
        BorrowedValue::Static(StaticNode::F64(value)) => Value::F64(*value),
        _ => Value::Source(value),
    }
}

fn as_string<'a>(value: &'a Value<'a>) -> Option<&'a str> {
    match value {
        Value::String(value) => Some(value),
        Value::Source(BorrowedValue::String(value)) => Some(value),
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

fn number(value: &Value<'_>) -> Option<Number> {
    Some(match value {
        Value::I64(value) => Number::I64(*value),
        Value::U64(value) => Number::U64(*value),
        Value::F64(value) => Number::F64(*value),
        Value::Source(BorrowedValue::Static(StaticNode::I64(value))) => Number::I64(*value),
        Value::Source(BorrowedValue::Static(StaticNode::U64(value))) => Number::U64(*value),
        Value::Source(BorrowedValue::Static(StaticNode::F64(value))) => Number::F64(*value),
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
        (Number::F64(left), Number::I64(right)) => left.partial_cmp(&(right as f64)),
        (Number::F64(left), Number::U64(right)) => left.partial_cmp(&(right as f64)),
        (Number::I64(left), Number::F64(right)) => (left as f64).partial_cmp(&right),
        (Number::U64(left), Number::F64(right)) => (left as f64).partial_cmp(&right),
    }
}

fn bool_value(value: &Value<'_>) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Source(BorrowedValue::Static(StaticNode::Bool(value))) => Some(*value),
        _ => None,
    }
}

fn write_json(value: &Value<'_>, output: &mut Vec<u8>, span: Span) -> Result<(), TransformError> {
    match value {
        Value::Missing => return Err(evaluation_error(span, "cannot serialize missing value")),
        Value::Null | Value::Source(BorrowedValue::Static(StaticNode::Null)) => {
            output.extend_from_slice(b"null")
        }
        Value::Bool(value) | Value::Source(BorrowedValue::Static(StaticNode::Bool(value))) => {
            output.extend_from_slice(if *value { b"true" } else { b"false" })
        }
        Value::I64(value) | Value::Source(BorrowedValue::Static(StaticNode::I64(value))) => {
            output.extend_from_slice(value.to_string().as_bytes())
        }
        Value::U64(value) | Value::Source(BorrowedValue::Static(StaticNode::U64(value))) => {
            output.extend_from_slice(value.to_string().as_bytes())
        }
        Value::F64(value) | Value::Source(BorrowedValue::Static(StaticNode::F64(value))) => {
            if !value.is_finite() {
                return Err(evaluation_error(span, "cannot serialize non-finite number"));
            }
            output.extend_from_slice(value.to_string().as_bytes());
        }
        Value::String(value) | Value::Source(BorrowedValue::String(value)) => {
            write_string(value, output)
        }
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
        Value::Source(BorrowedValue::Array(values)) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_json(&Value::Source(value), output, span)?;
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
        Value::Source(BorrowedValue::Object(fields)) => {
            output.push(b'{');
            for (index, (key, value)) in fields.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_string(key, output);
                output.push(b':');
                write_json(&Value::Source(value), output, span)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
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
                output.extend_from_slice(format!("\\u{:04x}", value as u32).as_bytes());
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
        span,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::compile::build_plan;

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
        let plan = build_plan(
            &drops.iter().map(ToString::to_string).collect::<Vec<_>>(),
            &tombstones
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            projection,
            false,
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
    fn invalid_json_pass_preserves_exact_bytes() {
        let plan = build_plan(&["true".to_owned()], &[], None, true).unwrap();
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
    fn native_integer_comparisons_do_not_lose_unsigned_precision() {
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
        let invalid = build_plan(&["true".to_owned()], &[], None, false).unwrap();
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

        let evaluation = build_plan(&["length(true) == 1".to_owned()], &[], None, false).unwrap();
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
}
