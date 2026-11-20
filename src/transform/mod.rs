use jx::{CompileOptions, Expression, InputPlan, OwnedValue, ValueType};

use crate::output::MetadataFields;

pub mod jsonata;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanCapabilities {
    pub needs_worker: bool,
    pub metadata: MetadataFields,
}

#[derive(Debug)]
pub struct PredicateSource {
    pub source: String,
    pub trigger: bool,
    pub origin: String,
}

#[derive(Clone, Debug)]
pub struct Predicate {
    pub expression: Expression,
    pub trigger: bool,
    pub origin: String,
}

#[derive(Clone, Debug)]
pub struct TransformPlan {
    pub drops: Vec<Predicate>,
    pub tombstones: Vec<Predicate>,
    pub drop_tombstones: bool,
    pub projection: Option<Expression>,
    pub capabilities: PlanCapabilities,
    pub defer_json: bool,
    metadata_bindings: Vec<bool>,
}

impl TransformPlan {
    pub fn uses_metadata(&self, index: usize) -> bool {
        self.metadata_bindings[index]
    }

    pub fn input_plan(&self) -> InputPlan<'_> {
        InputPlan::new(
            self.drops
                .iter()
                .chain(&self.tombstones)
                .map(|predicate| &predicate.expression)
                .chain(&self.projection),
        )
    }
}

pub fn build_plan(
    drops: &[PredicateSource],
    tombstones: &[PredicateSource],
    drop_tombstones: bool,
    projection: Option<&str>,
    variables: Option<&str>,
    force_json_validation: bool,
) -> Result<TransformPlan, String> {
    let variables = variables
        .map(|source| {
            let value = OwnedValue::from_json(source.as_bytes())
                .map_err(|error| format!("$vars input must be a valid JSON object: {error}"))?;
            if value.as_value().value_type() != ValueType::Object {
                return Err("$vars input must be a JSON object".to_owned());
            }
            Ok(value)
        })
        .transpose()?;
    let options = match variables {
        Some(value) => CompileOptions::default().constant_binding("vars", value),
        None => CompileOptions::default(),
    };
    let predicates = |sources: &[PredicateSource]| {
        sources
            .iter()
            .map(|source| {
                Ok(Predicate {
                    expression: compile_expression(&options, &source.source, &source.origin)?,
                    trigger: source.trigger,
                    origin: source.origin.clone(),
                })
            })
            .collect::<Result<Vec<_>, String>>()
    };
    let drops = predicates(drops)?;
    let tombstones = predicates(tombstones)?;
    let projection = projection
        .map(|source| compile_expression(&options, source, "projection"))
        .transpose()?;
    let mut metadata_fields = MetadataFields::NONE;
    let mut metadata_bindings =
        Vec::with_capacity(drops.len() + tombstones.len() + usize::from(projection.is_some()));
    for expression in drops
        .iter()
        .chain(&tombstones)
        .map(|predicate| &predicate.expression)
        .chain(&projection)
    {
        let demand = expression.binding_demand("meta");
        let used = demand != jx::BindingDemand::Unused;
        metadata_bindings.push(used);
        metadata_fields.merge(MetadataFields::from_demand(demand));
    }
    Ok(TransformPlan {
        capabilities: PlanCapabilities {
            needs_worker: force_json_validation
                || !drops.is_empty()
                || !tombstones.is_empty()
                || projection.is_some(),
            metadata: metadata_fields,
        },
        drops,
        tombstones,
        drop_tombstones,
        projection,
        defer_json: false,
        metadata_bindings,
    })
}

fn compile_expression(
    options: &CompileOptions,
    source: &str,
    category: &str,
) -> Result<Expression, String> {
    // Pre-check avoids adding a lexical scope frame to expressions that cannot
    // refer to $meta; jx's binding_demand then eliminates false positives and
    // identifies exact accessed fields.
    let options = if uses_metadata(source) {
        options.clone().binding("meta")
    } else {
        options.clone()
    };
    options.compile(source).map_err(|error| {
        let location = error
            .location(source)
            .map(|(line, column)| format!(" at {line}:{column}"))
            .unwrap_or_default();
        format!("{category} JSONata compile error{location}: {error}")
    })
}

fn uses_metadata(source: &str) -> bool {
    source.contains("$meta") || source.contains("$eval")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_plan_accepts_jsonata_and_reports_parse_errors() {
        build_plan(
            &[PredicateSource {
                source: r#"environment != "production""#.to_owned(),
                trigger: true,
                origin: "drop predicate #1".to_owned(),
            }],
            &[],
            false,
            Some(r#"{"id": id, "total": $sum(items.price)}"#),
            None,
            false,
        )
        .unwrap();

        let error = build_plan(
            &[PredicateSource {
                source: r#"environment === "production""#.to_owned(),
                trigger: true,
                origin: "drop predicate #1".to_owned(),
            }],
            &[],
            false,
            None,
            None,
            false,
        )
        .unwrap_err();
        assert!(error.contains("drop predicate #1 JSONata compile error"));
    }

    #[test]
    fn regex_object_values_compile_before_record_serialization() {
        build_plan(&[], &[], false, Some(r#"{"value": /x/}"#), None, false).unwrap();
    }

    #[test]
    fn variables_are_strict_json_objects() {
        build_plan(
            &[],
            &[],
            false,
            Some("$vars.tenant"),
            Some(r#"{"tenant":"acme","cutoff":1000}"#),
            false,
        )
        .unwrap();

        for variables in [r#"{tenant:"acme"}"#, "[]", "null"] {
            assert!(
                build_plan(&[], &[], false, None, Some(variables), false).is_err(),
                "{variables}"
            );
        }
    }

    #[test]
    fn explicit_validation_turns_identity_into_a_json_plan() {
        assert!(
            !build_plan(&[], &[], false, None, None, false)
                .unwrap()
                .capabilities
                .needs_worker
        );
        assert!(
            build_plan(&[], &[], false, None, None, true)
                .unwrap()
                .capabilities
                .needs_worker
        );
    }
}
