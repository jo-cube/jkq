use std::cell::RefCell;

use jsonata_core::{
    ast::{AstNode, BinaryOp, PathStep},
    value::JValue,
};
use simd_json::{Buffers, Tape, prelude::*, value::tape::Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TapeAction {
    Drop,
    Tombstone,
    Pass,
}

pub(super) struct TapePlan {
    drops: Vec<Predicate>,
    tombstones: Vec<Predicate>,
    scratch: RefCell<Scratch>,
}

impl TapePlan {
    pub(super) fn new(
        drops: &[AstNode],
        tombstones: &[AstNode],
        projection: Option<&AstNode>,
        variables: Option<&JValue>,
        embeds_json: bool,
    ) -> Option<Self> {
        if projection.is_some() || embeds_json {
            return None;
        }
        Some(Self {
            drops: drops
                .iter()
                .map(|expression| Predicate::compile(expression, variables))
                .collect::<Option<_>>()?,
            tombstones: tombstones
                .iter()
                .map(|expression| Predicate::compile(expression, variables))
                .collect::<Option<_>>()?,
            scratch: RefCell::new(Scratch::default()),
        })
    }

    pub(super) fn execute(&self, source: &[u8]) -> Option<TapeAction> {
        let mut scratch = self.scratch.borrow_mut();
        scratch.input.clear();
        scratch.input.extend_from_slice(source);

        let mut tape = scratch.tape.take().unwrap_or_else(Tape::null).reset();
        let parsed = {
            let Scratch { input, buffers, .. } = &mut *scratch;
            simd_json::fill_tape(input, buffers, &mut tape).is_ok()
        };
        let action = parsed.then(|| self.evaluate(tape.as_value())).flatten();
        scratch.tape = Some(tape.reset());
        action
    }

    fn evaluate(&self, document: Value<'_, '_>) -> Option<TapeAction> {
        for predicate in &self.drops {
            if predicate.evaluate(document)? {
                return Some(TapeAction::Drop);
            }
        }
        for predicate in &self.tombstones {
            if predicate.evaluate(document)? {
                return Some(TapeAction::Tombstone);
            }
        }
        Some(TapeAction::Pass)
    }
}

struct Scratch {
    input: Vec<u8>,
    buffers: Buffers,
    tape: Option<Tape<'static>>,
}

impl Default for Scratch {
    fn default() -> Self {
        Self {
            input: Vec::new(),
            buffers: Buffers::new(0),
            tape: None,
        }
    }
}

enum Predicate {
    Boolean(bool),
    Path(Vec<String>),
    Comparison {
        op: BinaryOp,
        left: Operand,
        right: Operand,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl Predicate {
    fn compile(expression: &AstNode, variables: Option<&JValue>) -> Option<Self> {
        match expression {
            AstNode::Boolean(value) => Some(Self::Boolean(*value)),
            AstNode::Path { steps } => Some(Self::Path(input_path(steps)?)),
            AstNode::Variable(name) if name.is_empty() => Some(Self::Path(Vec::new())),
            AstNode::Block(expressions) if expressions.len() == 1 => {
                Self::compile(&expressions[0], variables)
            }
            AstNode::Binary { op, lhs, rhs } => match op {
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => Some(Self::Comparison {
                    op: *op,
                    left: Operand::compile(lhs, variables)?,
                    right: Operand::compile(rhs, variables)?,
                }),
                BinaryOp::And => Some(Self::And(
                    Box::new(Self::compile(lhs, variables)?),
                    Box::new(Self::compile(rhs, variables)?),
                )),
                BinaryOp::Or => Some(Self::Or(
                    Box::new(Self::compile(lhs, variables)?),
                    Box::new(Self::compile(rhs, variables)?),
                )),
                _ => None,
            },
            _ => None,
        }
    }

    fn evaluate(&self, document: Value<'_, '_>) -> Option<bool> {
        match self {
            Self::Boolean(value) => Some(*value),
            Self::Path(path) => match value_at(document, path)? {
                Scalar::Boolean(value) => Some(value),
                _ => None,
            },
            Self::Comparison { op, left, right } => {
                compare(*op, left.value(document)?, right.value(document)?)
            }
            Self::And(left, right) => {
                if !left.evaluate(document)? {
                    Some(false)
                } else {
                    right.evaluate(document)
                }
            }
            Self::Or(left, right) => {
                if left.evaluate(document)? {
                    Some(true)
                } else {
                    right.evaluate(document)
                }
            }
        }
    }
}

enum Operand {
    Input(Vec<String>),
    Literal(Literal),
}

impl Operand {
    fn compile(expression: &AstNode, variables: Option<&JValue>) -> Option<Self> {
        match expression {
            AstNode::String(value) => Some(Self::Literal(Literal::String(value.clone()))),
            AstNode::Number(value) => Some(Self::Literal(Literal::Number(*value))),
            AstNode::Boolean(value) => Some(Self::Literal(Literal::Boolean(*value))),
            AstNode::Null => Some(Self::Literal(Literal::Null)),
            AstNode::Path { steps } => path_operand(steps, variables),
            AstNode::Variable(name) if name.is_empty() => Some(Self::Input(Vec::new())),
            _ => None,
        }
    }

    fn value<'a>(&'a self, document: Value<'a, 'a>) -> Option<Scalar<'a>> {
        match self {
            Self::Input(path) => value_at(document, path),
            Self::Literal(value) => Some(value.as_scalar()),
        }
    }
}

enum Literal {
    Undefined,
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
}

impl Literal {
    fn as_scalar(&self) -> Scalar<'_> {
        match self {
            Self::Undefined => Scalar::Undefined,
            Self::Null => Scalar::Null,
            Self::Boolean(value) => Scalar::Boolean(*value),
            Self::Number(value) => Scalar::Number(*value),
            Self::String(value) => Scalar::String(value),
        }
    }
}

enum Scalar<'a> {
    Undefined,
    Null,
    Boolean(bool),
    Number(f64),
    String(&'a str),
}

fn path_operand(steps: &[PathStep], variables: Option<&JValue>) -> Option<Operand> {
    if let Some(path) = input_path(steps) {
        return Some(Operand::Input(path));
    }
    let (first, rest) = steps.split_first()?;
    let AstNode::Variable(name) = &first.node else {
        return None;
    };
    if name != "vars" || !plain_step(first) {
        return None;
    }
    let path = names(rest)?;
    Some(Operand::Literal(variable_scalar(variables?, &path)?))
}

fn input_path(steps: &[PathStep]) -> Option<Vec<String>> {
    let steps = match steps.first() {
        Some(step) if matches!(&step.node, AstNode::Variable(name) if name.is_empty()) => {
            if !plain_step(step) {
                return None;
            }
            &steps[1..]
        }
        _ => steps,
    };
    names(steps)
}

fn names(steps: &[PathStep]) -> Option<Vec<String>> {
    steps
        .iter()
        .map(|step| match &step.node {
            AstNode::Name(name) if plain_step(step) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn plain_step(step: &PathStep) -> bool {
    step.stages.is_empty()
        && step.focus.is_none()
        && step.index_var.is_none()
        && step.ancestor_label.is_none()
        && !step.is_tuple
}

fn variable_scalar(variables: &JValue, path: &[String]) -> Option<Literal> {
    let mut value = variables;
    for name in path {
        if matches!(value, JValue::Array(_)) {
            return None;
        }
        value = value.get(name).unwrap_or(&JValue::Undefined);
    }
    match value {
        JValue::Undefined => Some(Literal::Undefined),
        JValue::Null => Some(Literal::Null),
        JValue::Bool(value) => Some(Literal::Boolean(*value)),
        JValue::Number(value) => Some(Literal::Number(*value)),
        JValue::String(value) => Some(Literal::String(value.to_string())),
        _ => None,
    }
}

fn value_at<'a>(mut value: Value<'a, 'a>, path: &[String]) -> Option<Scalar<'a>> {
    for name in path {
        let object = value.as_object()?;
        let found = object
            .iter()
            .filter_map(|(key, value)| (key == name).then_some(value))
            .last();
        let Some(found) = found else {
            return Some(Scalar::Undefined);
        };
        value = found;
    }
    if value.as_null().is_some() {
        Some(Scalar::Null)
    } else if let Some(value) = value.as_bool() {
        Some(Scalar::Boolean(value))
    } else if let Some(value) = value.cast_f64() {
        Some(Scalar::Number(value))
    } else {
        value.into_string().map(Scalar::String)
    }
}

fn compare(op: BinaryOp, left: Scalar<'_>, right: Scalar<'_>) -> Option<bool> {
    match op {
        BinaryOp::Equal => Some(equal(&left, &right)),
        BinaryOp::NotEqual => Some(!equal(&left, &right)),
        BinaryOp::LessThan => ordered(left, right, |ordering| ordering.is_lt()),
        BinaryOp::LessThanOrEqual => ordered(left, right, |ordering| ordering.is_le()),
        BinaryOp::GreaterThan => ordered(left, right, |ordering| ordering.is_gt()),
        BinaryOp::GreaterThanOrEqual => ordered(left, right, |ordering| ordering.is_ge()),
        _ => None,
    }
}

fn equal(left: &Scalar<'_>, right: &Scalar<'_>) -> bool {
    match (left, right) {
        (Scalar::Null, Scalar::Null) => true,
        (Scalar::Boolean(left), Scalar::Boolean(right)) => left == right,
        (Scalar::Number(left), Scalar::Number(right)) => left == right,
        (Scalar::String(left), Scalar::String(right)) => left == right,
        _ => false,
    }
}

fn ordered(
    left: Scalar<'_>,
    right: Scalar<'_>,
    test: impl FnOnce(std::cmp::Ordering) -> bool,
) -> Option<bool> {
    match (left, right) {
        (Scalar::Number(left), Scalar::Number(right)) => left.partial_cmp(&right).map(test),
        _ => None,
    }
}
