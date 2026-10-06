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
        false,
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
    let input_plan = transform.input_plan();
    let execution = Worker::new(&transform, &input_plan, true)
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
    for expression in [
        "missing",
        "null",
        "0",
        r#""value""#,
        "[]",
        "[true]",
        "{}",
        "$sum",
        "/x/",
    ] {
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
fn drop_tombstones_applies_before_projection() {
    let transform =
        build_plan(&[], &["deleted".to_owned()], true, Some("id"), None, false).unwrap();

    assert_eq!(run(&transform, None).unwrap(), Action::Drop);
    assert_eq!(
        run(&transform, Some(br#"{"deleted":true,"id":1}"#)).unwrap(),
        Action::Drop
    );
    assert_eq!(
        run(&transform, Some(br#"{"deleted":false,"id":1}"#)).unwrap(),
        Action::Project(b"1".to_vec())
    );

    let transform = build_plan(&[], &["deleted".to_owned()], true, None, None, false).unwrap();
    assert_eq!(
        run(&transform, Some(br#"{"deleted":true}"#)).unwrap(),
        Action::Drop
    );
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
        let input_plan = transform.input_plan();
        let result = Worker::new(&transform, &input_plan, false)
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
            let input_plan = transform.input_plan();
            let result = Worker::new(&transform, &input_plan, false)
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
fn projection_cardinality_keeps_one_output_and_explicit_arrays() {
    for (expression, input, expected) in [
        (
            "items.price",
            r#"{"items":[{"price":2},{"price":3}]}"#,
            "[2,3]",
        ),
        ("items.price", r#"{"items":[{"price":2}]}"#, "2"),
        ("items", r#"{"items":[2,3]}"#, "[2,3]"),
        ("items", r#"{"items":[2]}"#, "[2]"),
        ("[]", "{}", "[]"),
        ("[items.price]", r#"{"items":[{"price":2}]}"#, "[2]"),
    ] {
        let transform = plan(&[], &[], Some(expression), None, false);
        assert_eq!(
            run(&transform, Some(input.as_bytes())).unwrap(),
            Action::Project(expected.as_bytes().to_vec()),
            "{expression}: {input}"
        );
    }
    for input in [br#"{"items":[]}"#.as_slice(), b"{}"] {
        let transform = plan(&[], &[], Some("items.price"), None, false);
        assert!(
            run(&transform, Some(input))
                .unwrap_err()
                .to_string()
                .contains("projection emitted no results")
        );
    }
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
fn nested_vars_and_lookup_are_shared_across_record_actions() {
    let transform = plan(
        &[
            "tenant != $vars.policy.tenant",
            "$lookup($vars.drop, state) = true",
        ],
        &["$lookup($vars.delete, state) = true"],
        Some(
            r#"{"label":$vars.label,"limits":$vars.policy.limits,"missing":$lookup($vars.policy,"missing")}"#,
        ),
        Some(
            r#"{"policy":{"tenant":"acme","limits":[1,2]},"drop":{"blocked":true},"delete":{"deleted":true},"label":"retained"}"#,
        ),
        false,
    );
    let input_plan = transform.input_plan();
    let mut worker = Worker::new(&transform, &input_plan, false);
    for (input, expected) in [
        (r#"{"tenant":"other","state":"deleted"}"#, Action::Drop),
        (r#"{"tenant":"acme","state":"blocked"}"#, Action::Drop),
        (r#"{"tenant":"acme","state":"deleted"}"#, Action::Tombstone),
        (
            r#"{"tenant":"acme","state":"active"}"#,
            Action::Project(br#"{"label":"retained","limits":[1,2]}"#.to_vec()),
        ),
        (
            r#"{"tenant":"acme"}"#,
            Action::Project(br#"{"label":"retained","limits":[1,2]}"#.to_vec()),
        ),
    ] {
        assert_eq!(
            worker
                .execute_report(Some(input.as_bytes().to_vec()), FAIL)
                .unwrap()
                .action,
            expected,
            "{input}"
        );
    }
}

#[test]
fn vars_shadowing_closures_and_eval_keep_native_lexical_state() {
    for (expression, expected) in [
        (
            "($f:=function($vars){$vars.n}; [$f({'n':8}),$vars.n])",
            "[8,3]",
        ),
        ("($f:=function(){ $vars.n }; $vars := {'n':9}; $f())", "9"),
        ("($vars := {'n':$vars.n+1}; $vars.n)", "4"),
        ("$eval(code)", "3"),
        ("($f:=$eval('function(){ $vars.n }'); $f())", "3"),
    ] {
        let transform = plan(&[], &[], Some(expression), Some(r#"{"n":3}"#), false);
        let input_plan = transform.input_plan();
        let mut worker = Worker::new(&transform, &input_plan, false);
        for _ in 0..3 {
            assert_eq!(
                worker
                    .execute_report(Some(br#"{"code":"$vars.n"}"#.to_vec()), FAIL)
                    .unwrap()
                    .action,
                Action::Project(expected.as_bytes().to_vec()),
                "{expression}"
            );
        }
    }
}

#[test]
fn evaluator_root_and_assignments_do_not_leak_between_records() {
    let transform = plan(&[], &[], Some("($seen := id; $seen)"), None, false);
    let input_plan = transform.input_plan();
    let mut worker = Worker::new(&transform, &input_plan, false);
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
fn borrowed_numbers_preserve_exact_input_tokens() {
    let transform = plan(&[], &[], Some("value"), None, false);
    assert_eq!(
        run(&transform, Some(br#"{"value":9007199254740993}"#)).unwrap(),
        Action::Project(b"9007199254740993".to_vec())
    );
}

#[test]
fn non_json_projection_values_are_errors_even_when_nested() {
    for projection in [
        "$sum",
        "/x/",
        "[$sum]",
        r#"{"value": $sum}"#,
        r#"{"value": /x/}"#,
    ] {
        let transform = plan(&[], &[], Some(projection), None, false);
        assert!(run(&transform, Some(b"{}")).is_err(), "{projection}");
    }
}

#[test]
fn late_projection_failures_never_publish_partial_results() {
    for (expression, variables) in [
        (r#"rows.(n = null ? $error("late failure") : n * n)"#, None),
        ("rows.(n = null ? $sum : n * n)", None),
        (
            r#"rows.(n = $vars.stop ? $error("late failure") : n * $vars.scale)"#,
            Some(r#"{"stop":null,"scale":4}"#),
        ),
    ] {
        let transform = plan(
            &["id = 'drop'", "id = 'absent'"],
            &["id = 'delete'"],
            Some(expression),
            variables,
            false,
        );
        let bad = br#"{"rows":[{"n":2},{"n":3},{"n":null}]}"#;
        for policy in [
            EvaluationPolicy::Fail,
            EvaluationPolicy::Drop,
            EvaluationPolicy::Tombstone,
        ] {
            let input_plan = transform.input_plan();
            let mut worker = Worker::new(&transform, &input_plan, false);
            let result = worker.execute_report(
                Some(bad.to_vec()),
                ErrorPolicies {
                    evaluation: policy,
                    ..FAIL
                },
            );
            match policy {
                EvaluationPolicy::Fail => {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .starts_with("projection evaluation error")
                    )
                }
                EvaluationPolicy::Drop | EvaluationPolicy::Tombstone => {
                    let result = result.unwrap();
                    assert_eq!(result.issue, Some(ExecutionIssue::Evaluation));
                    assert_eq!(
                        result.action,
                        if policy == EvaluationPolicy::Drop {
                            Action::Drop
                        } else {
                            Action::Tombstone
                        }
                    );
                }
            }
            assert_eq!(
                worker
                    .execute_report(Some(br#"{"rows":[{"n":4}]}"#.to_vec()), FAIL)
                    .unwrap()
                    .action,
                Action::Project(b"16".to_vec())
            );
        }
    }
}

#[test]
fn expression_bindings_assignments_and_roots_are_independent() {
    let transform = plan(
        &[r#"($seen := id; $vars := {"tenant":"changed"}; false)"#],
        &["$exists($seen)"],
        Some(r#"{"id": $$.id, "tenant": $vars.tenant}"#),
        Some(r#"{"tenant":"acme"}"#),
        false,
    );
    let input_plan = transform.input_plan();
    let mut worker = Worker::new(&transform, &input_plan, false);
    for id in [1, 2, 1] {
        assert_eq!(
            worker
                .execute_report(Some(format!(r#"{{"id":{id}}}"#).into_bytes()), FAIL)
                .unwrap()
                .action,
            Action::Project(format!(r#"{{"id":{id},"tenant":"acme"}}"#).into_bytes())
        );
    }
}

#[test]
fn lookup_distinguishes_missing_from_null_and_retains_container_values() {
    let variables =
        r#"{"rules":{"nil":null,"yes":true,"list":[1,2],"object":{"n":3},"acme:42":true}}"#;
    let transform = plan(
        &[],
        &[],
        Some("$lookup($vars.rules, key)"),
        Some(variables),
        false,
    );
    for (key, expected) in [
        ("nil", "null"),
        ("yes", "true"),
        ("list", "[1,2]"),
        ("object", r#"{"n":3}"#),
    ] {
        assert_eq!(
            run(&transform, Some(format!(r#"{{"key":"{key}"}}"#).as_bytes())).unwrap(),
            Action::Project(expected.as_bytes().to_vec())
        );
    }
    assert!(
        run(&transform, Some(br#"{"key":"absent"}"#))
            .unwrap_err()
            .to_string()
            .contains("projection emitted no results")
    );
    let transform = plan(
        &["$lookup($vars.rules, key) = null"],
        &[],
        None,
        Some(variables),
        false,
    );
    let absent = br#"{"key":"absent"}"#;
    assert_eq!(
        run(&transform, Some(absent)).unwrap(),
        Action::PassThrough(PassPayload::Exact(absent.to_vec()))
    );
    assert_eq!(
        run(&transform, Some(br#"{"key":"nil"}"#)).unwrap(),
        Action::Drop
    );
    let transform = plan(
        &[],
        &[r#"$lookup($vars.rules, tenant & ":" & account) = true"#],
        None,
        Some(variables),
        false,
    );
    assert_eq!(
        run(&transform, Some(br#"{"tenant":"acme","account":42}"#)).unwrap(),
        Action::Tombstone
    );
}

#[test]
fn scalar_predicates_use_native_paths_comparisons_and_boolean_rules() {
    for (expression, input) in [
        (r#"kind = "ignore""#, r#"{"kind":"ignore"}"#),
        ("10 <= metrics.score", r#"{"metrics":{"score":10}}"#),
        (
            r#"(kind = "event" and active) or force = true"#,
            r#"{"kind":"event","active":true}"#,
        ),
        (
            r#"status = "keep""#,
            r#"{"status":"drop","status":"k\u0065ep"}"#,
        ),
        ("value = 9007199254740992", r#"{"value":9007199254740993}"#),
    ] {
        let transform = plan(&[expression], &[], None, None, false);
        assert_eq!(
            run(&transform, Some(input.as_bytes())).unwrap(),
            Action::Drop,
            "{expression}"
        );
    }
    let transform = plan(
        &[r#"($lookup := function($o, $k) { false }; $lookup({}, "a"))"#],
        &[],
        None,
        None,
        false,
    );
    assert_eq!(
        run(&transform, Some(b"{}")).unwrap(),
        Action::PassThrough(PassPayload::Exact(b"{}".to_vec()))
    );
}

#[test]
fn validation_precedes_short_circuit_actions_and_user_errors() {
    for predicate in ["true", r#"$error("must not run")"#] {
        let transform = plan(&[predicate], &[], None, None, false);
        for input in [br#"{"ignored":}"#.as_slice(), b"{} trailing", b"{\xff}"] {
            assert!(matches!(
                run(&transform, Some(input)),
                Err(TransformError::InvalidJson(_))
            ));
        }
    }
}

#[test]
fn compact_serialization_preserves_borrowed_tokens_and_native_results() {
    let input = br#"{ "value": { "n": 1.00e+2, "s": "\u0061", "k": 1, "k": 2 } }"#;
    let transform = plan(&[], &[], Some("value"), None, false);
    assert_eq!(
        run(&transform, Some(input)).unwrap(),
        Action::Project(br#"{"n":1.00e+2,"s":"\u0061","k":1,"k":2}"#.to_vec())
    );
    let transform = plan(&[], &[], None, None, true);
    let input_plan = transform.input_plan();
    let result = Worker::new(&transform, &input_plan, true)
        .execute_report(Some(input.to_vec()), FAIL)
        .unwrap();
    assert_eq!(
        result.action,
        Action::PassThrough(PassPayload::Json {
            bytes: br#"{"value":{"n":1.00e+2,"s":"\u0061","k":1,"k":2}}"#.to_vec(),
            source_length: input.len()
        })
    );
    let transform = plan(&[], &[], Some(r#"{"x":a.b[true]}.x"#), None, false);
    assert_eq!(
        run(&transform, Some(br#"{"a":[{},{"b":null}]}"#)).unwrap(),
        Action::Project(b"[null,null]".to_vec())
    );
    let transform = plan(&[], &[], Some("1 / 0"), None, false);
    assert_eq!(
        run(&transform, Some(b"{}")).unwrap(),
        Action::Project(b"null".to_vec())
    );
}
#[test]
fn missing_comparisons_follow_jx_instead_of_null_compatibility() {
    for predicate in ["missing = null", "missing != null", "$exists($vars)"] {
        let transform = plan(&[predicate], &[], None, None, false);
        assert_eq!(
            run(&transform, Some(b"{}")).unwrap(),
            Action::PassThrough(PassPayload::Exact(b"{}".to_vec())),
            "{predicate}"
        );
    }
}

#[test]
fn evaluated_pass_preserves_every_source_byte() {
    let transform = plan(&["id = 0"], &["deleted = true"], None, None, false);
    let source = br#"{ "id": 1.00, "deleted": false, "text": "\u0061" }
"#;
    let input_plan = transform.input_plan();
    let mut worker = Worker::new(&transform, &input_plan, false);
    for _ in 0..2 {
        assert_eq!(
            worker
                .execute_report(Some(source.to_vec()), FAIL)
                .unwrap()
                .action,
            Action::PassThrough(PassPayload::Exact(source.to_vec()))
        );
    }
}

#[test]
fn predicates_require_one_complete_boolean_result() {
    for expression in [
        "items.flag",
        r#"items.(flag ? true : $error("late predicate"))"#,
    ] {
        let transform = plan(&[expression], &[], None, None, false);
        let error = run(
            &transform,
            Some(br#"{"items":[{"flag":true},{"flag":false}]}"#),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("drop predicate #1 evaluation error")
        );
    }
    for (drops, tombstones, category) in [
        (
            vec!["false", r#"$error("second")"#],
            vec![],
            "drop predicate #2",
        ),
        (
            vec![],
            vec!["false", r#"$error("second")"#],
            "tombstone predicate #2",
        ),
    ] {
        let transform = plan(&drops, &tombstones, None, None, false);
        assert!(
            run(&transform, Some(b"{}"))
                .unwrap_err()
                .to_string()
                .starts_with(category)
        );
    }
}

#[test]
fn shared_nested_paths_preserve_actions_duplicates_missing_and_arrays() {
    let transform = plan(
        &["user.id = 'drop'", "user.blocked = true"],
        &["user.id = 'delete'"],
        Some("{'id':user.id}"),
        None,
        false,
    );
    let input_plan = transform.input_plan();
    let mut worker = Worker::new(&transform, &input_plan, false);
    for (source, expected) in [
        (r#"{"user":{"id":"drop"}}"#, Action::Drop),
        (r#"{"user":{"id":"delete","blocked":true}}"#, Action::Drop),
        (
            r#"{"user":{"id":"drop","blocked":true},"user":{"id":"delete"}}"#,
            Action::Tombstone,
        ),
        (
            r#"{"user":{"id":"drop","blocked":true},"user":{"id":"keep"}}"#,
            Action::Project(br#"{"id":"keep"}"#.to_vec()),
        ),
        (r#"{"user":{}}"#, Action::Project(b"{}".to_vec())),
        (
            r#"{"user":[{"id":"a"},{"id":"b"}]}"#,
            Action::Project(br#"{"id":["a","b"]}"#.to_vec()),
        ),
    ] {
        assert_eq!(
            worker
                .execute_report(Some(source.as_bytes().to_vec()), FAIL)
                .unwrap()
                .action,
            expected,
            "{source}",
        );
    }
}

#[test]
fn shared_expression_indices_preserve_action_error_categories() {
    for (drops, tombstones, projection, expected) in [
        (
            vec!["id = 0", "$number(id) = 0"],
            vec!["true"],
            Some("1"),
            "drop predicate #2",
        ),
        (
            vec!["id = 0", "id = 1"],
            vec!["id = 2", "$number(id) = 0"],
            Some("1"),
            "tombstone predicate #2",
        ),
        (
            vec!["id = 0", "id = 1"],
            vec!["id = 2", "id = 3"],
            Some("$number(id)"),
            "projection",
        ),
    ] {
        let transform = plan(&drops, &tombstones, projection, None, false);
        assert!(
            run(&transform, Some(br#"{"id":"invalid"}"#))
                .unwrap_err()
                .to_string()
                .starts_with(expected),
            "{expected}",
        );
    }
}

#[test]
fn shared_predicates_and_json_value_pass_serialize_the_complete_root() {
    let transform = plan(&["id = 0"], &["deleted = true"], None, None, true);
    let input_plan = transform.input_plan();
    let source = br#" { "id":1, "id":2, "deleted":false, "unused": [3, 4] } "#;
    let result = Worker::new(&transform, &input_plan, true)
        .execute_report(Some(source.to_vec()), FAIL)
        .unwrap();
    assert_eq!(
        result.action,
        Action::PassThrough(PassPayload::Json {
            bytes: br#"{"id":1,"id":2,"deleted":false,"unused":[3,4]}"#.to_vec(),
            source_length: source.len(),
        })
    );
}
