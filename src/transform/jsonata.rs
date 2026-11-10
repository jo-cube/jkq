use std::{cell::LazyCell, fmt, mem};

use jx::{
    Evaluation, EvaluationOptions, Expression, InputDependency, InputPlan, PreparedInput, RawJson,
};

use crate::output::{SourceMetadata, write_metadata};

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
    metadata: Vec<u8>,
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
            metadata: Vec::new(),
            max_recycled_capacity,
        }
    }

    pub fn execute_report(
        &mut self,
        source: &mut Option<Vec<u8>>,
        metadata: &SourceMetadata<'_>,
        policies: ErrorPolicies,
    ) -> Result<Execution, TransformError> {
        if source.is_none() && (self.plan.drop_tombstones || !self.plan.defer_json) {
            return evaluation_result(Ok(self.tombstone_action()), policies.evaluation);
        }
        self.output.clear();
        let result = (|| {
            let payload = source.as_deref();
            let mut input = if !self.plan.defer_json && self.plan.capabilities.needs_worker {
                payload
                    .map(|bytes| self.input_plan.prepare(bytes))
                    .transpose()
                    .map_err(|error| TransformError::InvalidJson(error.to_string()))?
            } else {
                None
            };
            // The closure runs once, only if a reached expression can refer to $meta.
            let meta = LazyCell::new(|| prepare_metadata(&mut self.metadata, metadata));
            for (index, expression) in self
                .plan
                .drops
                .iter()
                .chain(&self.plan.tombstones)
                .chain(&self.plan.projection)
                .enumerate()
            {
                if input.is_none()
                    && expression.input_dependency() == InputDependency::MayRequireInput
                {
                    input = payload
                        .map(|bytes| self.input_plan.prepare(bytes))
                        .transpose()
                        .map_err(|error| TransformError::InvalidJson(error.to_string()))?;
                }
                let meta = self.plan.uses_metadata(index).then(|| *meta);
                if index < self.plan.drops.len() + self.plan.tombstones.len() {
                    let (category, position) = if index < self.plan.drops.len() {
                        ("drop predicate", index)
                    } else {
                        ("tombstone predicate", index - self.plan.drops.len())
                    };
                    let value = evaluate(expression, input.as_ref(), index, meta)
                        .and_then(Evaluation::single)
                        .map_err(|error| {
                            evaluation_error(category, Some(position), error.to_string())
                        })?;
                    let matched = value.as_ref().and_then(jx::Value::as_bool).ok_or_else(|| {
                        evaluation_error(
                            category,
                            Some(position),
                            "result must be a Boolean".to_owned(),
                        )
                    })?;
                    if matched {
                        return Ok(Some(
                            if index < self.plan.drops.len() || self.plan.drop_tombstones {
                                Action::Drop
                            } else {
                                Action::Tombstone
                            },
                        ));
                    }
                } else {
                    // Retain the ordinary prepared serializer's optimized delivery path.
                    let present = match (input.as_ref(), meta) {
                        (Some(input), None) => input.write_json(index, &mut self.output),
                        (Some(input), Some(meta)) => input.write_json_with(
                            index,
                            metadata_options(Some(meta)),
                            &mut self.output,
                        ),
                        (None, _) => evaluate(expression, input.as_ref(), index, meta)
                            .map_err(jx::ConsumeError::Evaluation)
                            .and_then(|evaluation| evaluation.write_json(&mut self.output)),
                    }
                    .map_err(|error| evaluation_error("projection", None, error.to_string()))?;
                    if !present {
                        return Err(evaluation_error(
                            "projection",
                            None,
                            "projection emitted no results".to_owned(),
                        ));
                    }
                }
            }
            if self.plan.projection.is_none()
                && self.embeds_json
                && let Some(payload) = payload
            {
                if input.is_none() {
                    input = Some(
                        self.input_plan
                            .prepare(payload)
                            .map_err(|error| TransformError::InvalidJson(error.to_string()))?,
                    );
                }
                input
                    .as_ref()
                    .expect("prepared envelope payload")
                    .as_raw()
                    .write_compact(&mut self.output)
                    .map_err(|error| {
                        evaluation_error("envelope payload", None, error.to_string())
                    })?;
            }
            Ok(None)
        })();
        if self.metadata.capacity() > self.max_recycled_capacity {
            self.metadata = Vec::new();
        }
        match result {
            Ok(Some(action)) => return evaluation_result(Ok(action), policies.evaluation),
            Err(error) => {
                if self.output.capacity() > self.max_recycled_capacity {
                    self.output = Vec::new();
                } else {
                    self.output.clear();
                }
                return match error {
                    TransformError::InvalidJson(message) => {
                        invalid_json(policies.invalid_json, source, message)
                    }
                    error => evaluation_result(Err(error), policies.evaluation),
                };
            }
            Ok(None) => {}
        }
        let source_length = source.as_ref().map(Vec::len);
        if self.plan.projection.is_none() && (!self.embeds_json || source_length.is_none()) {
            return evaluation_result(
                Ok(match source.take() {
                    Some(bytes) => Action::PassThrough(PassPayload::Exact(bytes)),
                    None => Action::Tombstone,
                }),
                policies.evaluation,
            );
        }
        // All results have been consumed; recycle source storage only after borrows end.
        let bytes = if let Some(mut payload) = source.take()
            && payload.capacity() <= self.max_recycled_capacity
        {
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
                source_length: source_length.expect("source payload"),
            })
        };
        evaluation_result(Ok(action), policies.evaluation)
    }

    fn tombstone_action(&self) -> Action {
        if self.plan.drop_tombstones {
            Action::Drop
        } else {
            Action::Tombstone
        }
    }
}

fn prepare_metadata<'i>(buffer: &'i mut Vec<u8>, metadata: &SourceMetadata<'_>) -> RawJson<'i> {
    buffer.clear();
    write_metadata(metadata, buffer);
    jx::validate(buffer).expect("source metadata is valid JSON")
}

fn evaluate<'e, 'i>(
    expression: &'e Expression,
    input: Option<&PreparedInput<'_, 'e, 'i>>,
    index: usize,
    metadata: Option<RawJson<'i>>,
) -> Result<Evaluation<'e, 'i>, jx::Error> {
    match (input, metadata) {
        (Some(input), None) => input.evaluate(index),
        (input, metadata) => {
            let options = metadata_options(metadata);
            match input {
                Some(input) => input.evaluate_with(index, options),
                None => expression.evaluate_with(None, options),
            }
        }
    }
}

fn metadata_options<'e, 'i>(metadata: Option<RawJson<'i>>) -> EvaluationOptions<'e, 'i> {
    EvaluationOptions {
        bindings: metadata
            .map(|raw| ("meta", jx::Value::Raw(raw)))
            .into_iter()
            .collect(),
        ..Default::default()
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
        .execute_report(&mut payload, &test_metadata(), policies)
        .map(|execution| execution.action)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
fn test_metadata() -> SourceMetadata<'static> {
    SourceMetadata {
        topic: "events",
        partition: 0,
        offset: 0,
        timestamp: None,
        key: None,
        headers: &[],
        value_length: Some(0),
    }
}

#[cfg(test)]
mod metadata_tests;
