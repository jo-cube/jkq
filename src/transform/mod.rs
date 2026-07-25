use jsonata_core::{parser, value::JValue};

pub mod jsonata;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanCapabilities {
    pub parses_json: bool,
}

#[derive(Clone, Debug)]
pub struct TransformPlan {
    pub drops: Vec<String>,
    pub tombstones: Vec<String>,
    pub drop_tombstones: bool,
    pub projection: Option<String>,
    pub variables: Option<String>,
    pub capabilities: PlanCapabilities,
}

pub fn build_plan(
    drops: &[String],
    tombstones: &[String],
    drop_tombstones: bool,
    projection: Option<&str>,
    variables: Option<&str>,
    force_json_validation: bool,
) -> Result<TransformPlan, String> {
    if let Some(source) = variables {
        let value = JValue::from_json_str(source)
            .map_err(|error| format!("$vars input must be a valid JSON object: {error}"))?;
        if !value.is_object() {
            return Err("$vars input must be a JSON object".to_owned());
        }
    }

    for (category, sources) in [
        ("drop predicate", drops),
        ("tombstone predicate", tombstones),
    ] {
        for (index, source) in sources.iter().enumerate() {
            parse_expression(source, &format!("{category} #{}", index + 1))?;
        }
    }
    if let Some(source) = projection {
        parse_expression(source, "projection")?;
    }

    Ok(TransformPlan {
        drops: drops.to_vec(),
        tombstones: tombstones.to_vec(),
        drop_tombstones,
        projection: projection.map(str::to_owned),
        variables: variables.map(str::to_owned),
        capabilities: PlanCapabilities {
            parses_json: force_json_validation
                || !drops.is_empty()
                || !tombstones.is_empty()
                || projection.is_some(),
        },
    })
}

fn parse_expression(source: &str, category: &str) -> Result<(), String> {
    parser::parse(source).map(|_| ()).map_err(|error| {
        format!(
            "{category} JSONata parse error: {}",
            error.display_message()
        )
    })
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
        assert!(error.contains("drop predicate #1 JSONata parse error"));
    }

    #[test]
    fn jsonata_core_object_regex_parse_deviation_is_visible() {
        let error =
            build_plan(&[], &[], false, Some(r#"{"value": /x/}"#), None, false).unwrap_err();
        assert!(error.contains("projection JSONata parse error"));
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
