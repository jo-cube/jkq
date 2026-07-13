use std::{collections::HashSet, fmt};

use super::syntax::{self, BinaryOp, Expr, ExprKind, Literal, Path, Span};

#[derive(Clone, Debug)]
pub struct TransformPlan {
    pub paths: Vec<Path>,
    pub drops: Vec<CompiledExpr>,
    pub tombstones: Vec<CompiledExpr>,
    pub projection: Option<CompiledExpr>,
    pub capabilities: PlanCapabilities,
    payload_budget: PayloadBudget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PayloadBudget {
    input_factor: usize,
    projection: OutputBound,
}

impl PayloadBudget {
    // ponytail: affine whole-payload bounds favor safety over concurrency; transfer
    // byte permits after evaluation only if projection-heavy benchmarks require it.
    pub fn bytes(self, input_bytes: usize) -> Result<usize, String> {
        self.input_factor
            .checked_add(self.projection.factor)
            .and_then(|factor| factor.checked_mul(input_bytes))
            .and_then(|bytes| bytes.checked_add(self.projection.constant))
            .ok_or_else(|| "record retained-byte charge overflowed usize".to_owned())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct OutputBound {
    factor: usize,
    constant: usize,
}

impl OutputBound {
    const BOOLEAN: Self = Self {
        factor: 0,
        constant: 5,
    };

    fn add(self, other: Self) -> Option<Self> {
        Some(Self {
            factor: self.factor.checked_add(other.factor)?,
            constant: self.constant.checked_add(other.constant)?,
        })
    }

    fn with_constant(self, constant: usize) -> Option<Self> {
        Some(Self {
            factor: self.factor,
            constant: self.constant.checked_add(constant)?,
        })
    }

    fn max(self, other: Self) -> Self {
        Self {
            factor: self.factor.max(other.factor),
            constant: self.constant.max(other.constant),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanCapabilities {
    pub parses_json: bool,
    pub requires_original_bytes: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonRequirement {
    AsNeeded,
    Validate,
    PreserveInvalid,
}

#[derive(Clone, Debug)]
pub struct CompiledExpr {
    pub kind: CompiledKind,
    pub span: Span,
}

#[derive(Clone, Debug)]
pub enum CompiledKind {
    Literal(Literal),
    Slot(usize),
    Array(Vec<CompiledExpr>),
    Object(Vec<(String, CompiledExpr)>),
    Not(Box<CompiledExpr>),
    Binary(Box<CompiledExpr>, BinaryOp, Box<CompiledExpr>),
    Call(Function, Vec<CompiledExpr>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Function {
    Exists,
    Missing,
    IsNull,
    IsBoolean,
    IsNumber,
    IsString,
    IsArray,
    IsObject,
    Contains,
    StartsWith,
    EndsWith,
    Length,
    Coalesce,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompileError {
    pub category: &'static str,
    pub span: Span,
    pub message: String,
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} expression at byte {}: {}",
            self.category, self.span.start, self.message
        )
    }
}

impl std::error::Error for CompileError {}

pub fn build_plan(
    drops: &[String],
    tombstones: &[String],
    projection: Option<&str>,
    json_requirement: JsonRequirement,
) -> Result<TransformPlan, String> {
    let drops = drops
        .iter()
        .map(|source| syntax::parse(source, "drop predicate"))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let tombstones = tombstones
        .iter()
        .map(|source| syntax::parse(source, "tombstone predicate"))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let projection = projection
        .map(|source| syntax::parse(source, "projection"))
        .transpose()
        .map_err(|error| error.to_string())?;
    Compiler::compile(drops, tombstones, projection, json_requirement)
        .map_err(|error| error.to_string())
}

struct Compiler {
    paths: Vec<Path>,
}

impl Compiler {
    fn compile(
        drops: Vec<Expr>,
        tombstones: Vec<Expr>,
        projection: Option<Expr>,
        json_requirement: JsonRequirement,
    ) -> Result<TransformPlan, CompileError> {
        let mut compiler = Self { paths: Vec::new() };
        for (category, predicates) in [
            ("drop predicate", drops.as_slice()),
            ("tombstone predicate", tombstones.as_slice()),
        ] {
            for predicate in predicates {
                if matches!(predicate.kind, ExprKind::Literal(ref value) if !matches!(value, Literal::Bool(_)))
                {
                    return Err(CompileError {
                        category,
                        span: predicate.span,
                        message: "predicate literal must be boolean".to_owned(),
                    });
                }
            }
        }
        let drops = drops
            .into_iter()
            .map(|expression| compiler.expression(expression, "drop predicate"))
            .collect::<Result<Vec<_>, _>>()?;
        let tombstones = tombstones
            .into_iter()
            .map(|expression| compiler.expression(expression, "tombstone predicate"))
            .collect::<Result<Vec<_>, _>>()?;
        let projection = projection
            .map(|expression| compiler.expression(expression, "projection"))
            .transpose()?;
        let can_pass_through = projection.is_none();
        let parses_json = json_requirement != JsonRequirement::AsNeeded
            || !drops.is_empty()
            || !tombstones.is_empty()
            || projection.is_some();
        let requires_original_bytes =
            can_pass_through || json_requirement == JsonRequirement::PreserveInvalid;
        let projection_bound = projection
            .as_ref()
            .map(expression_bound)
            .transpose()
            .map_err(|()| CompileError {
                category: "projection",
                span: projection.as_ref().expect("projection bound failed").span,
                message: "maximum serialized size is too large".to_owned(),
            })?
            .unwrap_or_default();
        Ok(TransformPlan {
            paths: compiler.paths,
            drops,
            tombstones,
            projection,
            capabilities: PlanCapabilities {
                parses_json,
                requires_original_bytes,
            },
            payload_budget: PayloadBudget {
                input_factor: 1 + usize::from(parses_json && requires_original_bytes),
                projection: projection_bound,
            },
        })
    }

    fn expression(
        &mut self,
        expression: Expr,
        category: &'static str,
    ) -> Result<CompiledExpr, CompileError> {
        let span = expression.span;
        let kind = match expression.kind {
            ExprKind::Literal(value) => CompiledKind::Literal(value),
            ExprKind::Path(path) => {
                let slot = self
                    .paths
                    .iter()
                    .position(|candidate| candidate == &path)
                    .unwrap_or_else(|| {
                        self.paths.push(path);
                        self.paths.len() - 1
                    });
                CompiledKind::Slot(slot)
            }
            ExprKind::Array(values) => CompiledKind::Array(
                values
                    .into_iter()
                    .map(|value| self.expression(value, category))
                    .collect::<Result<_, _>>()?,
            ),
            ExprKind::Object(fields) => {
                let mut keys = HashSet::new();
                let mut compiled = Vec::with_capacity(fields.len());
                for field in fields {
                    if !keys.insert(field.key.clone()) {
                        return Err(CompileError {
                            category,
                            span: field.span,
                            message: format!("duplicate projection key {:?}", field.key),
                        });
                    }
                    compiled.push((field.key, self.expression(field.value, category)?));
                }
                CompiledKind::Object(compiled)
            }
            ExprKind::Unary(value) => {
                CompiledKind::Not(Box::new(self.expression(*value, category)?))
            }
            ExprKind::Binary(left, operator, right) => CompiledKind::Binary(
                Box::new(self.expression(*left, category)?),
                operator,
                Box::new(self.expression(*right, category)?),
            ),
            ExprKind::Call(name, arguments) => {
                let (function, arity) = function(&name).ok_or_else(|| CompileError {
                    category,
                    span,
                    message: format!("unsupported function {name:?}"),
                })?;
                if arguments.len() != arity {
                    return Err(CompileError {
                        category,
                        span,
                        message: format!(
                            "function {name:?} expects {arity} argument{}, received {}",
                            if arity == 1 { "" } else { "s" },
                            arguments.len()
                        ),
                    });
                }
                CompiledKind::Call(
                    function,
                    arguments
                        .into_iter()
                        .map(|argument| self.expression(argument, category))
                        .collect::<Result<_, _>>()?,
                )
            }
        };
        Ok(CompiledExpr { kind, span })
    }
}

impl TransformPlan {
    pub(crate) fn payload_budget(&self) -> PayloadBudget {
        self.payload_budget
    }
}

fn expression_bound(expression: &CompiledExpr) -> Result<OutputBound, ()> {
    Ok(match &expression.kind {
        CompiledKind::Literal(value) => OutputBound {
            factor: 0,
            constant: match value {
                Literal::Null => 4,
                Literal::Bool(true) => 4,
                Literal::Bool(false) => 5,
                Literal::I64(value) => value.to_string().len(),
                Literal::U64(value) => value.to_string().len(),
                Literal::F64(value) => format!("{value:?}").len(),
                Literal::String(value) => json_string_bytes(value).ok_or(())?,
            },
        },
        CompiledKind::Slot(_) => OutputBound {
            factor: 1,
            constant: 0,
        },
        CompiledKind::Array(values) => values
            .iter()
            .try_fold(OutputBound::default(), |bound, value| {
                bound.add(expression_bound(value).ok()?)
            })
            .and_then(|bound| bound.with_constant(2 + values.len().saturating_sub(1)))
            .ok_or(())?,
        CompiledKind::Object(fields) => fields
            .iter()
            .try_fold(OutputBound::default(), |bound, (key, value)| {
                bound
                    .add(expression_bound(value).ok()?)
                    .and_then(|bound| bound.with_constant(json_string_bytes(key)?.checked_add(1)?))
            })
            .and_then(|bound| bound.with_constant(2 + fields.len().saturating_sub(1)))
            .ok_or(())?,
        CompiledKind::Not(_) | CompiledKind::Binary(_, _, _) => OutputBound::BOOLEAN,
        CompiledKind::Call(Function::Length, _) => OutputBound {
            factor: 0,
            constant: 20,
        },
        CompiledKind::Call(Function::Coalesce, arguments) => {
            expression_bound(&arguments[0])?.max(expression_bound(&arguments[1])?)
        }
        CompiledKind::Call(_, _) => OutputBound::BOOLEAN,
    })
}

fn json_string_bytes(value: &str) -> Option<usize> {
    value.chars().try_fold(2_usize, |bytes, character| {
        bytes.checked_add(match character {
            '"' | '\\' | '\u{8}' | '\u{c}' | '\n' | '\r' | '\t' => 2,
            value if value < '\u{20}' => 6,
            value => value.len_utf8(),
        })
    })
}

fn function(name: &str) -> Option<(Function, usize)> {
    Some(match name {
        "exists" => (Function::Exists, 1),
        "missing" => (Function::Missing, 1),
        "is_null" => (Function::IsNull, 1),
        "is_boolean" => (Function::IsBoolean, 1),
        "is_number" => (Function::IsNumber, 1),
        "is_string" => (Function::IsString, 1),
        "is_array" => (Function::IsArray, 1),
        "is_object" => (Function::IsObject, 1),
        "contains" => (Function::Contains, 2),
        "starts_with" => (Function::StartsWith, 2),
        "ends_with" => (Function::EndsWith, 2),
        "length" => (Function::Length, 1),
        "coalesce" => (Function::Coalesce, 2),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_are_deduplicated_across_the_program() {
        let plan = build_plan(
            &[".customer.id == 1".to_owned()],
            &["exists(.customer.id)".to_owned()],
            Some("{id: .customer.id}"),
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        assert_eq!(plan.paths.len(), 1);
        assert!(!plan.capabilities.requires_original_bytes);
    }

    #[test]
    fn duplicate_object_keys_are_rejected() {
        let error =
            build_plan(&[], &[], Some("{id: 1, id: 2}"), JsonRequirement::AsNeeded).unwrap_err();
        assert!(error.contains("duplicate projection key"));
    }

    #[test]
    fn function_arity_is_checked_once_at_startup() {
        let error = build_plan(
            &["exists(.a, .b)".to_owned()],
            &[],
            None,
            JsonRequirement::AsNeeded,
        )
        .unwrap_err();
        assert!(error.contains("expects 1 argument"));
    }

    #[test]
    fn payload_budget_includes_parse_copies_and_projection_amplification() {
        let projected = build_plan(
            &[],
            &[],
            Some("[.value, .value]"),
            JsonRequirement::AsNeeded,
        )
        .unwrap();
        assert_eq!(projected.payload_budget().bytes(100).unwrap(), 303);

        let preserving = build_plan(
            &[],
            &[],
            Some("[.value, .value]"),
            JsonRequirement::PreserveInvalid,
        )
        .unwrap();
        assert_eq!(preserving.payload_budget().bytes(100).unwrap(), 403);

        let pass_through =
            build_plan(&["true".to_owned()], &[], None, JsonRequirement::AsNeeded).unwrap();
        assert_eq!(pass_through.payload_budget().bytes(100).unwrap(), 200);
    }
}
