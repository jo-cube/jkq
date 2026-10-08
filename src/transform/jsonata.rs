use std::{fmt, mem};

use jx::{Evaluation, InputPlan, PreparedInput};

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

pub(crate) struct Worker<'a> {
    plan: &'a TransformPlan,
    input_plan: &'a InputPlan<'a>,
    embeds_json: bool,
    output: Vec<u8>,
    max_recycled_capacity: usize,
}

impl<'a> Worker<'a> {
    pub fn new(
        plan: &'a TransformPlan,
        input_plan: &'a InputPlan<'a>,
        embeds_json: bool,
        max_recycled_capacity: usize,
    ) -> Self {
        Self {
            plan,
            input_plan,
            embeds_json,
            output: Vec::new(),
            max_recycled_capacity,
        }
    }

    pub fn execute_report(
        &mut self,
        source: &mut Option<Vec<u8>>,
        policies: ErrorPolicies,
    ) -> Result<Execution, TransformError> {
        let Some(payload) = source.as_ref() else {
            return evaluation_result(Ok(self.tombstone_action()), policies.evaluation);
        };
        if !self.plan.capabilities.parses_json {
            return evaluation_result(
                Ok(Action::PassThrough(PassPayload::Exact(
                    source.take().expect("source payload"),
                ))),
                policies.evaluation,
            );
        }
        let input = match self.input_plan.prepare(payload) {
            Ok(input) => input,
            Err(error) => return invalid_json(policies.invalid_json, source, error.to_string()),
        };
        match self.evaluate_predicates(&input) {
            Ok(Some(action)) => return evaluation_result(Ok(action), policies.evaluation),
            Err(error) => return evaluation_result(Err(error), policies.evaluation),
            Ok(None) => {}
        }

        let source_length = payload.len();
        self.output.clear();
        let (result, category) = if self.plan.projection.is_some() {
            (
                input
                    .evaluate(self.plan.drops.len() + self.plan.tombstones.len())
                    .map_err(|error| error.to_string())
                    .and_then(|evaluation| serialize_projection(evaluation, &mut self.output)),
                "projection",
            )
        } else if self.embeds_json {
            (
                input
                    .as_raw()
                    .write_compact(&mut self.output)
                    .map_err(|error| error.to_string()),
                "envelope payload",
            )
        } else {
            return evaluation_result(
                Ok(Action::PassThrough(PassPayload::Exact(
                    source.take().expect("source payload"),
                ))),
                policies.evaluation,
            );
        };
        if let Err(message) = result {
            self.output.clear();
            return evaluation_result(
                Err(evaluation_error(category, None, message)),
                policies.evaluation,
            );
        }
        // Results no longer borrow the source. Recycle its allocation for the next output.
        let mut payload = source.take().expect("source payload");
        let bytes = if payload.capacity() <= self.max_recycled_capacity {
            payload.clear();
            mem::replace(&mut self.output, payload)
        } else {
            mem::take(&mut self.output)
        };
        let action = if self.plan.projection.is_some() {
            Action::Project(bytes)
        } else {
            Action::PassThrough(PassPayload::Json {
                bytes,
                source_length,
            })
        };
        evaluation_result(Ok(action), policies.evaluation)
    }

    fn evaluate_predicates(
        &self,
        input: &PreparedInput<'_, 'a, '_>,
    ) -> Result<Option<Action>, TransformError> {
        for index in 0..self.plan.drops.len() {
            if self.predicate(input, index, "drop predicate", index)? {
                return Ok(Some(Action::Drop));
            }
        }
        for index in 0..self.plan.tombstones.len() {
            if self.predicate(
                input,
                self.plan.drops.len() + index,
                "tombstone predicate",
                index,
            )? {
                return Ok(Some(self.tombstone_action()));
            }
        }
        Ok(None)
    }

    fn predicate(
        &self,
        input: &PreparedInput<'_, 'a, '_>,
        expression_index: usize,
        category: &'static str,
        index: usize,
    ) -> Result<bool, TransformError> {
        let value = input
            .evaluate(expression_index)
            .and_then(Evaluation::single)
            .map_err(|error| evaluation_error(category, Some(index), error.to_string()))?;
        value.as_ref().and_then(jx::Value::as_bool).ok_or_else(|| {
            evaluation_error(category, Some(index), "result must be a Boolean".to_owned())
        })
    }

    fn tombstone_action(&self) -> Action {
        if self.plan.drop_tombstones {
            Action::Drop
        } else {
            Action::Tombstone
        }
    }
}

fn serialize_projection(
    evaluation: Evaluation<'_, '_>,
    output: &mut Vec<u8>,
) -> Result<(), String> {
    let mut first = None;
    let mut sequence = false;
    evaluation
        .try_for_each(|value| {
            if sequence {
                output.push(b',');
            } else if let Some(first) = first.take() {
                sequence = true;
                output.push(b'[');
                jx::Value::write_compact(&first, &mut *output)?;
                output.push(b',');
            } else {
                first = Some(value);
                return Ok(());
            }
            value.write_compact(&mut *output)
        })
        .map_err(|error| error.to_string())?;
    if sequence {
        output.push(b']');
        Ok(())
    } else if let Some(first) = first {
        first
            .write_compact(output)
            .map_err(|error| error.to_string())
    } else {
        Err("projection emitted no results".to_owned())
    }
}

fn evaluation_result(
    result: Result<Action, TransformError>,
    policy: EvaluationPolicy,
) -> Result<Execution, TransformError> {
    match result {
        Ok(action) => Ok(Execution {
            action,
            issue: None,
        }),
        Err(error) => match policy {
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

fn invalid_json(
    policy: InvalidJsonPolicy,
    original: &mut Option<Vec<u8>>,
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
            action: Action::PassThrough(PassPayload::Exact(
                original.take().expect("source payload"),
            )),
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

#[cfg(test)]
fn execute(
    plan: &TransformPlan,
    mut payload: Option<Vec<u8>>,
    policies: ErrorPolicies,
) -> Result<Action, TransformError> {
    let input_plan = plan.input_plan();
    Worker::new(plan, &input_plan, false, usize::MAX)
        .execute_report(&mut payload, policies)
        .map(|execution| execution.action)
}

#[cfg(test)]
mod tests;
