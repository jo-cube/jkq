use jx::{CompileOptions, Expression, InputPlan, OwnedValue, ValueType};

pub mod jsonata;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanCapabilities {
    pub parses_json: bool,
}

#[derive(Clone, Debug)]
pub struct TransformPlan {
    pub drops: Vec<Expression>,
    pub tombstones: Vec<Expression>,
    pub drop_tombstones: bool,
    pub projection: Option<Expression>,
    pub capabilities: PlanCapabilities,
}

impl TransformPlan {
    pub fn input_plan(&self) -> InputPlan<'_> {
        InputPlan::new(
            self.drops
                .iter()
                .chain(&self.tombstones)
                .chain(&self.projection),
        )
    }
}

pub fn build_plan(
    drops: &[String],
    tombstones: &[String],
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
    let predicates = |sources: &[String], category: &str| {
        sources
            .iter()
            .enumerate()
            .map(|(index, source)| {
                compile_expression(&options, source, &format!("{category} #{}", index + 1))
            })
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(TransformPlan {
        drops: predicates(drops, "drop predicate")?,
        tombstones: predicates(tombstones, "tombstone predicate")?,
        drop_tombstones,
        projection: projection
            .map(|source| compile_expression(&options, source, "projection"))
            .transpose()?,
        capabilities: PlanCapabilities {
            parses_json: force_json_validation
                || !drops.is_empty()
                || !tombstones.is_empty()
                || projection.is_some(),
        },
    })
}

fn compile_expression(
    options: &CompileOptions,
    source: &str,
    category: &str,
) -> Result<Expression, String> {
    options
        .compile(source)
        .map_err(|error| format!("{category} JSONata compile error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_plan_accepts_jsonata_and_reports_parse_errors() {
        build_plan(
            &[r#"environment != "production""#.to_owned()],
            &[],
            false,
            Some(r#"{"id": id, "total": $sum(items.price)}"#),
            None,
            false,
        )
        .unwrap();

        let error = build_plan(
            &[r#"environment === "production""#.to_owned()],
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
                .parses_json
        );
        assert!(
            build_plan(&[], &[], false, None, None, true)
                .unwrap()
                .capabilities
                .parses_json
        );
    }
}
