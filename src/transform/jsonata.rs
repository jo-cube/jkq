use std::{fmt, str};

use jsonata_core::{
    ast::AstNode,
    evaluator::{Context, Evaluator},
    parser,
    value::JValue,
};

use super::TransformPlan;

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
    PassThrough(PassPayload),
    Project(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PassPayload {
    Exact(Vec<u8>),
    Json {
        bytes: Vec<u8>,
        source_length: usize,
    },
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
    Evaluation { category: String, message: String },
}

impl fmt::Display for TransformError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson(message) => write!(formatter, "invalid JSON: {message}"),
            Self::Evaluation { category, message } => {
                write!(formatter, "{category} evaluation error: {message}")
            }
        }
    }
}

impl std::error::Error for TransformError {}

pub(crate) struct Worker {
    parses_json: bool,
    embeds_json: bool,
    drops: Vec<AstNode>,
    tombstones: Vec<AstNode>,
    projection: Option<AstNode>,
    variables: Option<JValue>,
}

impl Worker {
    pub fn new(plan: &TransformPlan, embeds_json: bool) -> Self {
        Self {
            parses_json: plan.capabilities.parses_json,
            embeds_json,
            drops: plan.drops.iter().map(|source| parsed(source)).collect(),
            tombstones: plan
                .tombstones
                .iter()
                .map(|source| parsed(source))
                .collect(),
            projection: plan.projection.as_deref().map(parsed),
            variables: plan.variables.as_deref().map(|source| {
                JValue::from_json_str(source).expect("startup validated JSONata variables")
            }),
        }
    }

    pub fn execute_report(
        &self,
        payload: Option<Vec<u8>>,
        policies: ErrorPolicies,
    ) -> Result<Execution, TransformError> {
        let Some(payload) = payload else {
            return Ok(Execution {
                action: Action::Tombstone,
                issue: None,
            });
        };
        if !self.parses_json {
            return Ok(Execution {
                action: Action::PassThrough(PassPayload::Exact(payload)),
                issue: None,
            });
        }

        let source = match str::from_utf8(&payload) {
            Ok(source) => source,
            Err(error) => {
                return invalid_json(policies.invalid_json, payload, error.to_string());
            }
        };
        let document = match JValue::from_json_str(source) {
            Ok(document) => document,
            Err(error) => {
                return invalid_json(policies.invalid_json, payload, error.to_string());
            }
        };

        match self.evaluate(&document, payload) {
            Ok(action) => Ok(Execution {
                action,
                issue: None,
            }),
            Err(error) => match policies.evaluation {
                EvaluationPolicy::Fail => Err(error),
                EvaluationPolicy::Drop => Ok(Execution {
                    action: Action::Drop,
                    issue: Some(ExecutionIssue::Evaluation),
                }),
                EvaluationPolicy::Tombstone => Ok(Execution {
                    action: Action::Tombstone,
                    issue: Some(ExecutionIssue::Evaluation),
                }),
            },
        }
    }

    fn evaluate(&self, document: &JValue, original: Vec<u8>) -> Result<Action, TransformError> {
        for (index, expression) in self.drops.iter().enumerate() {
            if self.predicate(expression, document, "drop predicate", index)? {
                return Ok(Action::Drop);
            }
        }
        for (index, expression) in self.tombstones.iter().enumerate() {
            if self.predicate(expression, document, "tombstone predicate", index)? {
                return Ok(Action::Tombstone);
            }
        }
        let Some(expression) = &self.projection else {
            if !self.embeds_json {
                return Ok(Action::PassThrough(PassPayload::Exact(original)));
            }
            let source_length = original.len();
            // Strict parsing already limits this tree to JSON variants; avoid a second full walk.
            return document
                .to_json_string()
                .map(|json| {
                    Action::PassThrough(PassPayload::Json {
                        bytes: json.into_bytes(),
                        source_length,
                    })
                })
                .map_err(|error| evaluation_error("envelope payload", None, error.to_string()));
        };

        let value = self
            .evaluate_expression(expression, document)
            .map_err(|message| evaluation_error("projection", None, message))?;
        validate_json_result(&value)
            .map_err(|message| evaluation_error("projection", None, message))?;
        value
            .to_json_string()
            .map(|json| Action::Project(json.into_bytes()))
            .map_err(|error| evaluation_error("projection", None, error.to_string()))
    }

    fn predicate(
        &self,
        expression: &AstNode,
        document: &JValue,
        category: &'static str,
        index: usize,
    ) -> Result<bool, TransformError> {
        let value = self
            .evaluate_expression(expression, document)
            .map_err(|message| evaluation_error(category, Some(index), message))?;
        value.as_bool().ok_or_else(|| {
            evaluation_error(
                category,
                Some(index),
                format!("result must be a Boolean, received {}", value_type(&value)),
            )
        })
    }

    fn evaluate_expression(
        &self,
        expression: &AstNode,
        document: &JValue,
    ) -> Result<JValue, String> {
        let mut context = Context::new();
        if let Some(variables) = &self.variables {
            context.bind("vars".to_owned(), variables.clone());
        }
        Evaluator::with_context(context)
            .evaluate(expression, document)
            .map_err(|error| error.to_string())
    }
}

fn parsed(source: &str) -> AstNode {
    parser::parse(source).expect("startup validated JSONata expression")
}

fn invalid_json(
    policy: InvalidJsonPolicy,
    original: Vec<u8>,
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
            action: Action::PassThrough(PassPayload::Exact(original)),
            issue: Some(ExecutionIssue::InvalidJson),
        }),
    }
}

fn evaluation_error(category: &str, index: Option<usize>, message: String) -> TransformError {
    TransformError::Evaluation {
        category: index.map_or_else(
            || category.to_owned(),
            |index| format!("{category} #{}", index + 1),
        ),
        message,
    }
}

fn validate_json_result(value: &JValue) -> Result<(), String> {
    match value {
        JValue::Null | JValue::Bool(_) | JValue::String(_) => Ok(()),
        JValue::Number(number) if number.is_finite() => Ok(()),
        JValue::Number(_) => Err("result contains a non-finite number".to_owned()),
        JValue::Array(values) => values.iter().try_for_each(validate_json_result),
        JValue::Object(fields) => fields.values().try_for_each(validate_json_result),
        JValue::Undefined => Err("result is Undefined".to_owned()),
        JValue::Lambda { .. } | JValue::Builtin { .. } => {
            Err("result contains a function".to_owned())
        }
        JValue::Regex { .. } => Err("result contains a regular expression".to_owned()),
    }
}

fn value_type(value: &JValue) -> &'static str {
    match value {
        JValue::Null => "null",
        JValue::Bool(_) => "Boolean",
        JValue::Number(_) => "number",
        JValue::String(_) => "string",
        JValue::Array(_) => "array",
        JValue::Object(_) => "object",
        JValue::Undefined => "Undefined",
        JValue::Lambda { .. } | JValue::Builtin { .. } => "function",
        JValue::Regex { .. } => "regular expression",
    }
}

#[cfg(test)]
fn execute(
    plan: &TransformPlan,
    payload: Option<Vec<u8>>,
    policies: ErrorPolicies,
) -> Result<Action, TransformError> {
    Worker::new(plan, false)
        .execute_report(payload, policies)
        .map(|execution| execution.action)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::build_plan;

    const FAIL: ErrorPolicies = ErrorPolicies {
        invalid_json: InvalidJsonPolicy::Fail,
        evaluation: EvaluationPolicy::Fail,
    };

    fn plan(
        drops: &[&str],
        tombstones: &[&str],
        projection: Option<&str>,
        variables: Option<&str>,
        validate: bool,
    ) -> TransformPlan {
        build_plan(
            &drops
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>(),
            &tombstones
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>(),
            projection,
            variables,
            validate,
        )
        .unwrap()
    }

    fn run(plan: &TransformPlan, input: Option<&[u8]>) -> Result<Action, TransformError> {
        execute(plan, input.map(<[u8]>::to_vec), FAIL)
    }

    #[test]
    fn jsonata_filters_maps_and_aggregates() {
        let transform = plan(
            &[],
            &[],
            Some(r#"{"names": items[price >= 10].name, "total": $sum(items[price >= 10].price)}"#),
            None,
            false,
        );
        assert_eq!(
            run(
                &transform,
                Some(br#"{"items":[{"name":"a","price":4},{"name":"b","price":10},{"name":"c","price":12}]}"#)
            )
            .unwrap(),
            Action::Project(br#"{"names":["b","c"],"total":22}"#.to_vec())
        );
    }

    #[test]
    fn actions_follow_drop_tombstone_projection_pass_precedence() {
        let dropped = plan(&["true"], &["true"], Some("1"), None, false);
        assert_eq!(run(&dropped, Some(b"{}")).unwrap(), Action::Drop);

        let tombstone = plan(&["false"], &["true"], Some("1"), None, false);
        assert_eq!(run(&tombstone, Some(b"{}")).unwrap(), Action::Tombstone);

        let projected = plan(&[], &[], Some("1"), None, false);
        assert_eq!(
            run(&projected, Some(b"{}")).unwrap(),
            Action::Project(b"1".to_vec())
        );

        let passed = plan(&[], &[], None, None, false);
        assert_eq!(
            run(&passed, Some(b"{ \"a\" : 1 }")).unwrap(),
            Action::PassThrough(PassPayload::Exact(b"{ \"a\" : 1 }".to_vec()))
        );
    }

    #[test]
    fn json_value_pass_compacts_payload_and_retains_source_length() {
        let transform = plan(&[], &[], None, None, true);
        let source = b"{\n  \"a\": 1\n}";
        let execution = Worker::new(&transform, true)
            .execute_report(Some(source.to_vec()), FAIL)
            .unwrap();

        assert_eq!(
            execution.action,
            Action::PassThrough(PassPayload::Json {
                bytes: br#"{"a":1}"#.to_vec(),
                source_length: source.len(),
            })
        );
    }

    #[test]
    fn repeated_predicates_short_circuit_in_command_line_order() {
        for (drops, tombstones, expected) in [
            (
                vec!["first = 1", "$error(\"must not run\")"],
                vec![],
                Action::Drop,
            ),
            (
                vec!["false"],
                vec!["first = 1", "$error(\"must not run\")"],
                Action::Tombstone,
            ),
        ] {
            let transform = plan(&drops, &tombstones, None, None, false);
            assert_eq!(run(&transform, Some(br#"{"first":1}"#)).unwrap(), expected);
        }
    }

    #[test]
    fn action_predicates_require_boolean_results() {
        for expression in ["missing", "null", "0", r#""value""#, "[]", "{}", "$sum"] {
            let transform = plan(&[expression], &[], None, None, false);
            let error = run(&transform, Some(b"{}")).unwrap_err();
            assert!(
                error.to_string().contains("result must be a Boolean"),
                "{expression}: {error}"
            );
        }
    }

    #[test]
    fn source_tombstones_bypass_evaluation() {
        let transform = plan(
            &["$error(\"must not run\")"],
            &[],
            Some("missing"),
            None,
            false,
        );
        assert_eq!(run(&transform, None).unwrap(), Action::Tombstone);
    }

    #[test]
    fn invalid_json_policies_preserve_exact_pass_bytes() {
        let transform = plan(&[], &[], None, None, true);
        let invalid = b"{ not json \xff".to_vec();
        for (policy, expected) in [
            (InvalidJsonPolicy::Drop, Action::Drop),
            (InvalidJsonPolicy::Tombstone, Action::Tombstone),
            (
                InvalidJsonPolicy::Pass,
                Action::PassThrough(PassPayload::Exact(invalid.clone())),
            ),
        ] {
            let result = Worker::new(&transform, false)
                .execute_report(
                    Some(invalid.clone()),
                    ErrorPolicies {
                        invalid_json: policy,
                        evaluation: EvaluationPolicy::Fail,
                    },
                )
                .unwrap();
            assert_eq!(result.action, expected);
            assert_eq!(result.issue, Some(ExecutionIssue::InvalidJson));
        }
        assert!(run(&transform, Some(&invalid)).is_err());
    }

    #[test]
    fn evaluation_errors_and_undefined_follow_policy() {
        for projection in ["$error(\"failure\")", "missing"] {
            let transform = plan(&[], &[], Some(projection), None, false);
            for (policy, expected) in [
                (EvaluationPolicy::Drop, Action::Drop),
                (EvaluationPolicy::Tombstone, Action::Tombstone),
            ] {
                let result = Worker::new(&transform, false)
                    .execute_report(
                        Some(b"{}".to_vec()),
                        ErrorPolicies {
                            invalid_json: InvalidJsonPolicy::Fail,
                            evaluation: policy,
                        },
                    )
                    .unwrap();
                assert_eq!(result.action, expected);
                assert_eq!(result.issue, Some(ExecutionIssue::Evaluation));
            }
        }
    }

    #[test]
    fn evaluation_errors_identify_the_expression_category() {
        for (drops, tombstones, projection, category) in [
            (
                vec!["$error(\"failure\")"],
                vec![],
                None,
                "drop predicate #1",
            ),
            (
                vec![],
                vec!["$error(\"failure\")"],
                None,
                "tombstone predicate #1",
            ),
            (vec![], vec![], Some("$error(\"failure\")"), "projection"),
        ] {
            let transform = plan(&drops, &tombstones, projection, None, false);
            assert!(
                run(&transform, Some(b"{}"))
                    .unwrap_err()
                    .to_string()
                    .starts_with(category),
                "{category}"
            );
        }
    }

    #[test]
    fn projected_null_empty_payload_and_tombstone_are_distinct() {
        let projected = plan(&[], &[], Some("null"), None, false);
        assert_eq!(
            run(&projected, Some(b"{}")).unwrap(),
            Action::Project(b"null".to_vec())
        );

        let passed = plan(&[], &[], None, None, false);
        assert_eq!(
            run(&passed, Some(b"")).unwrap(),
            Action::PassThrough(PassPayload::Exact(Vec::new()))
        );
        assert_eq!(run(&passed, None).unwrap(), Action::Tombstone);
    }

    #[test]
    fn result_sequences_serialize_as_one_payload() {
        let transform = plan(&[], &[], Some("items.price"), None, false);
        assert_eq!(
            run(&transform, Some(br#"{"items":[{"price":2},{"price":3}]}"#)).unwrap(),
            Action::Project(b"[2,3]".to_vec())
        );
    }

    #[test]
    fn native_missing_values_are_omitted_from_constructed_results() {
        for (projection, expected) in [
            (
                r#"{"kept": 1, "missing": missing}"#,
                br#"{"kept":1}"#.as_slice(),
            ),
            ("[missing, 1]", b"[1]".as_slice()),
        ] {
            let transform = plan(&[], &[], Some(projection), None, false);
            assert_eq!(
                run(&transform, Some(b"{}")).unwrap(),
                Action::Project(expected.to_vec()),
                "{projection}"
            );
        }
    }

    #[test]
    fn vars_are_bound_immutably_for_every_expression() {
        let transform = plan(
            &["tenant != $vars.tenant"],
            &[],
            Some(r#"{"tenant": $vars.tenant, "cutoff": $vars.cutoff}"#),
            Some(r#"{"tenant":"acme","cutoff":1000}"#),
            false,
        );
        assert_eq!(
            run(&transform, Some(br#"{"tenant":"acme"}"#)).unwrap(),
            Action::Project(br#"{"tenant":"acme","cutoff":1000}"#.to_vec())
        );
    }

    #[test]
    fn evaluator_root_and_assignments_do_not_leak_between_records() {
        let transform = plan(&[], &[], Some("($seen := id; $seen)"), None, false);
        let worker = Worker::new(&transform, false);
        for (input, expected) in [
            (br#"{"id":1}"#.as_slice(), b"1".as_slice()),
            (br#"{"id":2}"#.as_slice(), b"2".as_slice()),
        ] {
            assert_eq!(
                worker
                    .execute_report(Some(input.to_vec()), FAIL)
                    .unwrap()
                    .action,
                Action::Project(expected.to_vec())
            );
        }
    }

    #[test]
    fn large_integers_follow_ieee_754_semantics() {
        let transform = plan(&[], &[], Some("value"), None, false);
        assert_eq!(
            run(&transform, Some(br#"{"value":9007199254740993}"#)).unwrap(),
            Action::Project(b"9007199254740992".to_vec())
        );
    }

    #[test]
    fn non_json_projection_values_are_errors_even_when_nested() {
        for projection in ["$sum", "/x/", "[$sum]", r#"{"value": $sum}"#] {
            let transform = plan(&[], &[], Some(projection), None, false);
            assert!(run(&transform, Some(b"{}")).is_err(), "{projection}");
        }
    }
}
