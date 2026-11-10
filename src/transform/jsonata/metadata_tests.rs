use super::*;
use crate::{
    output::{Header, Timestamp, TimestampType},
    transform::build_plan,
};

const FAIL: ErrorPolicies = ErrorPolicies {
    invalid_json: InvalidJsonPolicy::Fail,
    evaluation: EvaluationPolicy::Fail,
};

fn make_plan(drops: &[&str], tombstones: &[&str], projection: Option<&str>) -> TransformPlan {
    let mut plan = build_plan(
        &drops.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(),
        &tombstones
            .iter()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>(),
        false,
        projection,
        Some(r#"{"code":"$meta.offset","n":7}"#),
        false,
    )
    .unwrap();
    plan.defer_json = true;
    plan
}

fn metadata(value_length: Option<usize>) -> SourceMetadata<'static> {
    SourceMetadata {
        topic: "events",
        partition: 2,
        offset: 9007199254740993,
        timestamp: None,
        key: None,
        headers: &[],
        value_length,
    }
}

fn run(plan: &TransformPlan, source: Option<&[u8]>) -> Result<Action, TransformError> {
    let mut source = source.map(<[u8]>::to_vec);
    let metadata = metadata(source.as_ref().map(Vec::len));
    let input_plan = plan.input_plan();
    Worker::new(plan, &input_plan, false, 16 * 1024)
        .execute_report(&mut source, &metadata, FAIL)
        .map(|execution| execution.action)
}

#[test]
fn metadata_preserves_binary_bytes_duplicate_headers_and_large_offsets() {
    let plan = make_plan(&[], &[], Some("$meta"));
    let input_plan = plan.input_plan();
    let headers = vec![
        Header {
            name: "x".into(),
            value: None,
        },
        Header {
            name: "x".into(),
            value: Some(vec![]),
        },
        Header {
            name: "x".into(),
            value: Some(vec![0xff, 0]),
        },
    ];
    let metadata = SourceMetadata {
        timestamp: Some(Timestamp {
            milliseconds: 1720000000000,
            kind: TimestampType::LogAppendTime,
        }),
        key: Some(&[0xff]),
        headers: &headers,
        ..metadata(Some(1))
    };
    let action = Worker::new(&plan, &input_plan, false, 16 * 1024)
        .execute_report(&mut Some(vec![0xff]), &metadata, FAIL)
        .unwrap()
        .action;
    assert_eq!(action, Action::Project(br#"{"topic":"events","partition":2,"offset":"9007199254740993","timestamp":1720000000000,"timestampType":"logAppendTime","key":"/w==","keyEncoding":"base64","keyLength":1,"headers":[{"name":"x","value":null,"valueEncoding":null,"valueLength":-1},{"name":"x","value":"","valueEncoding":"utf8","valueLength":0},{"name":"x","value":"/wA=","valueEncoding":"base64","valueLength":2}],"valueLength":1,"isTombstone":false}"#.to_vec()));
}

#[test]
fn source_tombstone_empty_bytes_and_json_null_remain_distinct() {
    let plan = make_plan(
        &[],
        &[],
        Some(
            r#"{"length":$meta.valueLength,"tombstone":$meta.isTombstone,"focus":$exists($),"root":$exists($$)}"#,
        ),
    );
    for (source, expected) in [
        (
            None,
            r#"{"length":-1,"tombstone":true,"focus":false,"root":false}"#,
        ),
        (
            Some(b"null".as_slice()),
            r#"{"length":4,"tombstone":false,"focus":true,"root":true}"#,
        ),
    ] {
        assert_eq!(
            run(&plan, source).unwrap(),
            Action::Project(expected.as_bytes().to_vec())
        );
    }
    assert!(matches!(
        run(&plan, Some(b"")),
        Err(TransformError::InvalidJson(_))
    ));
    let lengths = plan_only_metadata();
    for (source, expected) in [
        (None, b"[-1,true]".as_slice()),
        (Some(b"".as_slice()), b"[0,false]"),
        (Some(b"null".as_slice()), b"[4,false]"),
    ] {
        assert_eq!(
            run(&lengths, source).unwrap(),
            Action::Project(expected.to_vec())
        );
    }
}

fn plan_only_metadata() -> TransformPlan {
    make_plan(&[], &[], Some("[$meta.valueLength,$meta.isTombstone]"))
}

#[test]
fn eager_defaults_and_drop_tombstones_keep_precedence() {
    let mut plan = make_plan(&["$meta.isTombstone"], &[], Some("$meta.offset"));
    plan.defer_json = false;
    assert_eq!(run(&plan, None).unwrap(), Action::Tombstone);
    assert!(matches!(
        run(&plan, Some(b"bad")),
        Err(TransformError::InvalidJson(_))
    ));
    plan.defer_json = true;
    assert_eq!(run(&plan, None).unwrap(), Action::Drop);
    plan.drop_tombstones = true;
    plan.projection = Some(
        jx::CompileOptions::default()
            .compile("$error('unreachable')")
            .unwrap(),
    );
    assert_eq!(run(&plan, None).unwrap(), Action::Drop);
}

#[test]
fn deferred_predicates_short_circuit_without_reordering_stages() {
    let early = make_plan(
        &["$meta.partition = 2", "$error('unreachable')"],
        &["missing"],
        Some("missing"),
    );
    assert_eq!(run(&early, Some(b"not JSON")).unwrap(), Action::Drop);
    let later = make_plan(&["id = 2", "$meta.partition = 2"], &[], None);
    assert!(matches!(
        run(&later, Some(b"not JSON")),
        Err(TransformError::InvalidJson(_))
    ));
    let stages = make_plan(
        &["$meta.partition = 3", "id = 2"],
        &["$meta.partition = 2", "$error('unreachable')"],
        Some("$error('unreachable')"),
    );
    assert_eq!(run(&stages, Some(br#"{"id":2}"#)).unwrap(), Action::Drop);
    assert_eq!(
        run(&stages, Some(br#"{"id":1}"#)).unwrap(),
        Action::Tombstone
    );
    let error_first = make_plan(&["$error('early')", "id = 2"], &[], None);
    assert!(
        matches!(run(&error_first, Some(b"bad")), Err(TransformError::Evaluation {message,..}) if message.contains("early"))
    );
}

#[test]
fn mixed_expressions_preserve_payload_roots_and_exact_offset_comparisons() {
    let plan = make_plan(
        &[
            "$meta.offset != '9007199254740993'",
            "$meta.partition != id",
        ],
        &[],
        Some(r#"{"id":id,"root":$$,"offset":$meta.offset,"constant":$vars.n}"#),
    );
    assert_eq!(run(&plan, Some(br#"{"id":2,"meta":"payload"}"#)).unwrap(),
        Action::Project(br#"{"id":2,"root":{"id":2,"meta":"payload"},"offset":"9007199254740993","constant":7}"#.to_vec()));
}

#[test]
fn lexical_shadowing_dynamic_eval_and_record_isolation_use_fresh_bindings() {
    let plan = make_plan(
        &["($meta := {'partition':99}; false)"],
        &[],
        Some("[$meta.partition,$eval($vars.code),($f := function(){ $meta.offset }; $f())]"),
    );
    let input_plan = plan.input_plan();
    let mut worker = Worker::new(&plan, &input_plan, false, 16 * 1024);
    for offset in [9007199254740993, 9007199254740994] {
        let metadata = SourceMetadata {
            offset,
            ..metadata(Some(4))
        };
        let expected = format!("[2,\"{offset}\",\"{offset}\"]").into_bytes();
        assert_eq!(
            worker
                .execute_report(&mut Some(b"null".to_vec()), &metadata, FAIL)
                .unwrap()
                .action,
            Action::Project(expected)
        );
    }
    let dynamic = make_plan(&[], &[], Some("$eval($vars.code)"));
    assert_eq!(
        run(&dynamic, Some(b"null")).unwrap(),
        Action::Project(br#""9007199254740993""#.to_vec())
    );
    // A closure's bare focus is the surrounding payload, even when its argument is metadata.
    let callback = make_plan(
        &[],
        &[],
        Some("$map([$meta.offset],function($v){$exists($)})"),
    );
    assert!(matches!(
        run(&callback, Some(b"bad")),
        Err(TransformError::InvalidJson(_))
    ));
    assert_eq!(
        run(&callback, None).unwrap(),
        Action::Project(b"false".to_vec())
    );
}

#[test]
fn skipped_preparation_preserves_passthrough_and_reached_error_policies() {
    let plan = make_plan(&["$meta.partition = 3"], &[], None);
    assert_eq!(
        run(&plan, Some(b" \xff ")).unwrap(),
        Action::PassThrough(PassPayload::Exact(b" \xff ".to_vec()))
    );
    let dependent = make_plan(&["$meta.partition = 3", "id = 1"], &[], None);
    let input_plan = dependent.input_plan();
    let mut worker = Worker::new(&dependent, &input_plan, false, 16 * 1024);
    for (policy, expected) in [
        (
            InvalidJsonPolicy::Pass,
            Action::PassThrough(PassPayload::Exact(b" \xff ".to_vec())),
        ),
        (InvalidJsonPolicy::Drop, Action::Drop),
        (InvalidJsonPolicy::Tombstone, Action::Tombstone),
    ] {
        let mut payload = Some(b" \xff ".to_vec());
        let execution = worker
            .execute_report(
                &mut payload,
                &metadata(Some(3)),
                ErrorPolicies {
                    invalid_json: policy,
                    ..FAIL
                },
            )
            .unwrap();
        assert_eq!(execution.action, expected);
        assert_eq!(execution.issue, Some(ExecutionIssue::InvalidJson));
    }
}

#[test]
fn absent_input_serialization_rolls_back_and_keeps_evaluation_error_precedence() {
    let mut plan = make_plan(
        &[],
        &[],
        Some("[$meta.offset,$eval('[function(){1},$error(\"late\")]')]"),
    );
    let input_plan = plan.input_plan();
    let mut worker = Worker::new(&plan, &input_plan, false, 16 * 1024);
    let error = worker
        .execute_report(&mut None, &metadata(None), FAIL)
        .unwrap_err();
    assert!(matches!(error, TransformError::Evaluation {message,..} if message.contains("late")));
    assert!(worker.output.is_empty());
    drop(worker);
    drop(input_plan);
    plan = plan_only_metadata();
    assert_eq!(
        run(&plan, None).unwrap(),
        Action::Project(b"[-1,true]".to_vec())
    );
    for expression in ["$meta.missing", "[$meta.offset,function(){1}]"] {
        let plan = make_plan(&[], &[], Some(expression));
        let input_plan = plan.input_plan();
        let mut worker = Worker::new(&plan, &input_plan, false, 16 * 1024);
        assert!(
            worker
                .execute_report(&mut None, &metadata(None), FAIL)
                .is_err()
        );
        assert!(worker.output.is_empty());
    }
}

#[test]
fn metadata_scratch_is_lazy_and_does_not_retain_oversized_headers() {
    let plan = make_plan(&["true"], &[], Some("$meta"));
    let input_plan = plan.input_plan();
    let mut worker = Worker::new(&plan, &input_plan, false, 128);
    let headers = [Header {
        name: "large".into(),
        value: Some(vec![b'x'; 8192]),
    }];
    let metadata = SourceMetadata {
        headers: &headers,
        ..metadata(None)
    };
    assert_eq!(
        worker
            .execute_report(&mut None, &metadata, FAIL)
            .unwrap()
            .action,
        Action::Drop
    );
    assert_eq!(worker.metadata.capacity(), 0);
    let plan = plan_only_metadata();
    let input_plan = plan.input_plan();
    let mut worker = Worker::new(&plan, &input_plan, false, 128);
    assert!(worker.execute_report(&mut None, &metadata, FAIL).is_ok());
    assert_eq!(worker.metadata.capacity(), 0);
}

#[test]
fn metadata_projection_and_json_value_passthrough_have_separate_input_requirements() {
    for (projection, drop, expected) in [
        (
            Some("$meta.offset"),
            "false",
            Some(Action::Project(br#""9007199254740993""#.to_vec())),
        ),
        (None, "true", Some(Action::Drop)),
        (None, "false", None),
    ] {
        let plan = make_plan(&[drop], &[], projection);
        let input_plan = plan.input_plan();
        let mut worker = Worker::new(&plan, &input_plan, true, 16 * 1024);
        let result = worker.execute_report(&mut Some(b"bad".to_vec()), &metadata(Some(3)), FAIL);
        match expected {
            Some(expected) => assert_eq!(result.unwrap().action, expected),
            None => assert!(matches!(result, Err(TransformError::InvalidJson(_)))),
        }
    }
}

#[test]
fn bound_projection_consumes_late_errors_and_reuses_rolled_back_output() {
    let plan = make_plan(
        &[],
        &[],
        Some("$meta.headers.(name = 'function' ? function(){1} : $error('late serialization'))"),
    );
    let input_plan = plan.input_plan();
    let mut worker = Worker::new(&plan, &input_plan, false, 16 * 1024);
    let headers = [
        Header {
            name: "function".into(),
            value: None,
        },
        Header {
            name: "later".into(),
            value: None,
        },
    ];
    for source in [None, Some(b"{}".to_vec())] {
        let mut source = source;
        let metadata = SourceMetadata {
            headers: &headers,
            ..metadata(source.as_ref().map(Vec::len))
        };
        for policy in [
            EvaluationPolicy::Fail,
            EvaluationPolicy::Drop,
            EvaluationPolicy::Tombstone,
        ] {
            let result = worker.execute_report(
                &mut source,
                &metadata,
                ErrorPolicies {
                    evaluation: policy,
                    ..FAIL
                },
            );
            match policy {
                EvaluationPolicy::Fail => assert!(
                    matches!(result, Err(TransformError::Evaluation {message,..}) if message.contains("late serialization"))
                ),
                EvaluationPolicy::Drop => assert_eq!(result.unwrap().action, Action::Drop),
                EvaluationPolicy::Tombstone => {
                    assert_eq!(result.unwrap().action, Action::Tombstone)
                }
            }
            assert!(worker.output.is_empty());
        }
    }
}
