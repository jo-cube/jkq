use std::{collections::HashSet, fmt};

use super::syntax::{self, BinaryOp, Expr, ExprKind, Literal, Path, Span};

#[derive(Clone, Debug)]
pub struct TransformPlan {
    pub paths: Vec<Path>,
    pub drops: Vec<CompiledExpr>,
    pub tombstones: Vec<CompiledExpr>,
    pub projection: Option<CompiledExpr>,
    pub capabilities: PlanCapabilities,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanCapabilities {
    pub parses_json: bool,
    pub can_pass_through: bool,
    pub requires_original_on_error: bool,
    pub requires_original_bytes: bool,
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
    preserve_invalid_json: bool,
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
    Compiler::compile(drops, tombstones, projection, preserve_invalid_json)
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
        preserve_invalid_json: bool,
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
        let parses_json = !drops.is_empty() || !tombstones.is_empty() || projection.is_some();
        Ok(TransformPlan {
            paths: compiler.paths,
            drops,
            tombstones,
            projection,
            capabilities: PlanCapabilities {
                parses_json,
                can_pass_through,
                requires_original_on_error: preserve_invalid_json,
                requires_original_bytes: can_pass_through || preserve_invalid_json,
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
            false,
        )
        .unwrap();
        assert_eq!(plan.paths.len(), 1);
        assert!(!plan.capabilities.requires_original_bytes);
    }

    #[test]
    fn duplicate_object_keys_are_rejected() {
        let error = build_plan(&[], &[], Some("{id: 1, id: 2}"), false).unwrap_err();
        assert!(error.contains("duplicate projection key"));
    }

    #[test]
    fn function_arity_is_checked_once_at_startup() {
        let error = build_plan(&["exists(.a, .b)".to_owned()], &[], None, false).unwrap_err();
        assert!(error.contains("expects 1 argument"));
    }
}
