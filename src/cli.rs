use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use clap::{Parser, ValueEnum};

use crate::{
    output::{CompiledFormat, OutputRequirements},
    transform::{
        TransformPlan, build_plan,
        jsonata::{ErrorPolicies, EvaluationPolicy, InvalidJsonPolicy},
    },
};

const DEFAULT_MAX_INFLIGHT_RECORDS: usize = 8_192;
const DEFAULT_MAX_INFLIGHT_BYTES: &str = "256MiB";
const DEFAULT_MAX_INFLIGHT_PER_PARTITION: usize = 8_192;
const MAX_SELECTED_PARTITIONS: usize = 100_000;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Run JSONata over records from explicitly assigned Kafka partitions"
)]
pub struct RawCli {
    /// Comma-separated bootstrap broker list
    #[arg(short = 'b', long)]
    brokers: Option<String>,
    /// Kafka topic to consume
    #[arg(short = 't', long)]
    topic: String,
    /// Partitions to consume, for example 0,2,4-7; repeatable
    #[arg(
        short = 'p',
        long = "partition",
        required = true,
        allow_negative_numbers = true
    )]
    partitions: Vec<String>,
    /// Start position or s@/e@ timestamp boundary
    #[arg(short = 'o', long = "offset", allow_hyphen_values = true)]
    offsets: Vec<String>,
    /// Exclusive end offset for every selected partition
    #[arg(long, allow_hyphen_values = true)]
    end_offset: Option<i64>,
    /// Maximum admitted input records
    #[arg(short = 'c', long)]
    count: Option<u64>,
    /// Maximum admitted input records per partition
    #[arg(long, conflicts_with = "count")]
    count_per_partition: Option<u64>,
    /// Exit after reaching the current end of every partition
    #[arg(short = 'e', long)]
    exit_at_end: bool,
    /// Capture startup high watermarks as fixed exclusive ends
    #[arg(long)]
    snapshot: bool,
    /// Drop records when this JSONata predicate returns true; repeatable
    #[arg(long)]
    drop_if: Vec<String>,
    /// Tombstone records when this JSONata predicate returns true; repeatable
    #[arg(long)]
    tombstone_if: Vec<String>,
    /// Project each surviving record with this JSONata expression
    #[arg(long)]
    project: Option<String>,
    /// Strict JSON object available as $vars
    #[arg(long, value_name = "OBJECT")]
    vars: Option<String>,
    /// Kcat-style output format
    #[arg(short = 'f', long, conflicts_with = "json_envelope")]
    format: Option<String>,
    /// Emit one JSON envelope per output record
    #[arg(short = 'J', long, conflicts_with = "format")]
    json_envelope: bool,
    /// Flush stdout after every output record
    #[arg(short = 'u', long)]
    unbuffered: bool,
    /// Write final statistics to stderr
    #[arg(long)]
    stats: bool,
    /// Write periodic statistics, for example 500ms, 5s, or 1m
    #[arg(long, value_parser = parse_duration)]
    stats_interval: Option<Duration>,
    /// Suppress non-error diagnostics
    #[arg(short = 'q', long)]
    quiet: bool,
    /// Number of compute workers
    #[arg(short = 'j', long)]
    jobs: Option<usize>,
    /// Emit records in completion order
    #[arg(long)]
    unordered: bool,
    /// Maximum admitted records awaiting ordered drain
    #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_RECORDS)]
    max_inflight_records: usize,
    /// Owned source-byte admission budget; supports KiB, MiB, and GiB
    #[arg(long, default_value = DEFAULT_MAX_INFLIGHT_BYTES, value_parser = parse_size)]
    max_inflight_bytes: usize,
    /// Maximum admitted records per partition
    #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_PER_PARTITION)]
    max_inflight_per_partition: usize,
    /// Policy for malformed non-tombstone payloads
    #[arg(long, value_enum)]
    on_invalid_json: Option<RawInvalidJsonPolicy>,
    /// Policy for JSONata predicate or projection failures
    #[arg(long, value_enum, default_value_t = RawEvaluationPolicy::Fail)]
    on_eval_error: RawEvaluationPolicy,
    /// Policy for recoverable Kafka record errors
    #[arg(long, value_enum, default_value_t = KafkaErrorPolicy::Fail)]
    on_kafka_error: KafkaErrorPolicy,
    /// Librdkafka key=value configuration file
    #[arg(short = 'F', long)]
    config: Option<PathBuf>,
    /// Librdkafka key=value property; repeatable
    #[arg(short = 'X', long = "property")]
    properties: Vec<String>,
    /// Validate local configuration and exit without connecting
    #[arg(long)]
    check: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum RawInvalidJsonPolicy {
    Fail,
    Drop,
    Tombstone,
    Pass,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum RawEvaluationPolicy {
    Fail,
    Drop,
    Tombstone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum KafkaErrorPolicy {
    Fail,
    Continue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartPosition {
    Beginning,
    End,
    Absolute(i64),
    RelativeToEnd(u64),
    TimestampMillis(i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndPosition {
    ExclusiveOffset(i64),
    TimestampMillis(i64),
    Snapshot,
}

#[derive(Clone, Copy, Debug)]
pub struct RuntimeLimits {
    pub max_inflight_records: usize,
    pub max_inflight_bytes: usize,
    pub max_inflight_per_partition: usize,
}

#[derive(Debug)]
pub enum OutputPlan {
    Format(CompiledFormat),
    Envelope,
}

#[derive(Debug)]
pub struct RuntimeConfig {
    pub topic: String,
    pub partitions: Vec<i32>,
    pub start: StartPosition,
    pub end: Option<EndPosition>,
    pub count_limit: Option<u64>,
    pub count_per_partition: Option<u64>,
    pub exit_at_end: bool,
    pub jobs: usize,
    pub unordered: bool,
    pub limits: RuntimeLimits,
    pub transform: TransformPlan,
    pub output: OutputPlan,
    pub errors: ErrorPolicies,
    pub kafka_error: KafkaErrorPolicy,
    pub kafka_properties: BTreeMap<String, String>,
    pub unbuffered: bool,
    pub stats: bool,
    pub stats_interval: Option<Duration>,
    pub quiet: bool,
    pub check: bool,
}

impl OutputPlan {
    pub fn requirements(&self) -> OutputRequirements {
        match self {
            Self::Format(format) => format.requirements(),
            Self::Envelope => OutputRequirements {
                key: true,
                headers: true,
                timestamp: true,
            },
        }
    }
}

impl RawCli {
    pub fn resolve(self) -> Result<RuntimeConfig, String> {
        if self.topic.is_empty() {
            return Err("topic must not be empty".to_owned());
        }
        let partitions = parse_partitions(&self.partitions)?;
        if self.count == Some(0) {
            return Err("count must be positive".to_owned());
        }
        if self.count_per_partition == Some(0) {
            return Err("per-partition count must be positive".to_owned());
        }
        let jobs = self.jobs.unwrap_or_else(default_jobs);
        if jobs == 0 {
            return Err("jobs must be at least 1".to_owned());
        }
        if jobs > 1_024 {
            return Err("jobs must not exceed 1024".to_owned());
        }
        if self.max_inflight_records == 0
            || self.max_inflight_bytes == 0
            || self.max_inflight_per_partition == 0
        {
            return Err("in-flight limits must be positive".to_owned());
        }
        if self.max_inflight_per_partition > self.max_inflight_records {
            return Err(
                "per-partition in-flight record limit cannot exceed the global limit".to_owned(),
            );
        }

        let (start, explicit_end) = resolve_offsets(&self.offsets, self.end_offset)?;
        if self.snapshot && explicit_end.is_some() {
            return Err("snapshot cannot be combined with an explicit end boundary".to_owned());
        }
        let end = if self.snapshot {
            Some(EndPosition::Snapshot)
        } else {
            explicit_end
        };

        let force_json_validation = self.on_invalid_json.is_some();
        let invalid_json = match self.on_invalid_json.unwrap_or(RawInvalidJsonPolicy::Fail) {
            RawInvalidJsonPolicy::Fail => InvalidJsonPolicy::Fail,
            RawInvalidJsonPolicy::Drop => InvalidJsonPolicy::Drop,
            RawInvalidJsonPolicy::Tombstone => InvalidJsonPolicy::Tombstone,
            RawInvalidJsonPolicy::Pass => InvalidJsonPolicy::Pass,
        };
        let evaluation = match self.on_eval_error {
            RawEvaluationPolicy::Fail => EvaluationPolicy::Fail,
            RawEvaluationPolicy::Drop => EvaluationPolicy::Drop,
            RawEvaluationPolicy::Tombstone => EvaluationPolicy::Tombstone,
        };
        let transform = build_plan(
            &self.drop_if,
            &self.tombstone_if,
            self.project.as_deref(),
            self.vars.as_deref(),
            force_json_validation,
        )?;
        let output = if self.json_envelope {
            OutputPlan::Envelope
        } else {
            OutputPlan::Format(
                CompiledFormat::compile(self.format.as_deref().unwrap_or("%s\\n"))
                    .map_err(|error| error.to_string())?,
            )
        };

        let mut kafka_properties = if let Some(path) = &self.config {
            parse_config_file(path)?
        } else {
            BTreeMap::new()
        };
        for property in &self.properties {
            let (key, value) = parse_property(property)?;
            kafka_properties.insert(key.to_owned(), value.to_owned());
        }
        for key in [
            "auto.offset.reset",
            "enable.auto.commit",
            "enable.auto.offset.store",
            "enable.partition.eof",
        ] {
            if kafka_properties.contains_key(key) {
                return Err(format!("Kafka property {key:?} is managed by jkq"));
            }
        }
        if let Some(brokers) = &self.brokers {
            kafka_properties.insert("bootstrap.servers".to_owned(), brokers.clone());
        }
        let broker_value = kafka_properties
            .get("bootstrap.servers")
            .ok_or_else(|| "brokers are required through -b or bootstrap.servers".to_owned())?;
        if broker_value.split(',').map(str::trim).any(str::is_empty) {
            return Err("broker list contains an empty entry".to_owned());
        }

        Ok(RuntimeConfig {
            topic: self.topic,
            partitions,
            start,
            exit_at_end: self.exit_at_end || end.is_some(),
            end,
            count_limit: self.count,
            count_per_partition: self.count_per_partition,
            jobs,
            unordered: self.unordered,
            limits: RuntimeLimits {
                max_inflight_records: self.max_inflight_records,
                max_inflight_bytes: self.max_inflight_bytes,
                max_inflight_per_partition: self.max_inflight_per_partition,
            },
            transform,
            output,
            errors: ErrorPolicies {
                invalid_json,
                evaluation,
            },
            kafka_error: self.on_kafka_error,
            kafka_properties,
            unbuffered: self.unbuffered,
            stats: self.stats || self.stats_interval.is_some(),
            stats_interval: self.stats_interval,
            quiet: self.quiet,
            check: self.check,
        })
    }
}

fn default_jobs() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .saturating_sub(2)
        .max(1)
}

fn parse_partitions(values: &[String]) -> Result<Vec<i32>, String> {
    let mut partitions = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        for selection in value.split(',').map(str::trim) {
            if selection.is_empty() {
                return Err("partition selection contains an empty entry".to_owned());
            }
            let (start, end) = if selection.starts_with('-') {
                let partition = parse_partition(selection)?;
                (partition, partition)
            } else if let Some((start, end)) = selection.split_once('-') {
                let start = parse_partition(start)?;
                let end = parse_partition(end)?;
                if start > end {
                    return Err(format!(
                        "partition range {selection:?} must be in ascending order"
                    ));
                }
                (start, end)
            } else {
                let partition = parse_partition(selection)?;
                (partition, partition)
            };
            let count = usize::try_from(i64::from(end) - i64::from(start) + 1)
                .map_err(|_| "partition range is too large for this platform".to_owned())?;
            if partitions
                .len()
                .checked_add(count)
                .is_none_or(|total| total > MAX_SELECTED_PARTITIONS)
            {
                return Err(format!(
                    "partition selection expands beyond the {MAX_SELECTED_PARTITIONS} partition limit"
                ));
            }
            for partition in start..=end {
                if !seen.insert(partition) {
                    return Err(format!("duplicate partition selection {partition}"));
                }
                partitions.push(partition);
            }
        }
    }
    Ok(partitions)
}

fn parse_partition(value: &str) -> Result<i32, String> {
    let partition = value
        .parse::<i32>()
        .map_err(|_| format!("partition {value:?} must be a non-negative integer"))?;
    if partition < 0 {
        Err(format!(
            "partition must be non-negative, received {partition}"
        ))
    } else {
        Ok(partition)
    }
}

fn resolve_offsets(
    offsets: &[String],
    end_offset: Option<i64>,
) -> Result<(StartPosition, Option<EndPosition>), String> {
    if end_offset.is_some_and(|offset| offset < 0) {
        return Err("end offset must be non-negative".to_owned());
    }
    let mut start = None;
    let mut end = end_offset.map(EndPosition::ExclusiveOffset);
    for raw in offsets {
        if let Some(value) = raw.strip_prefix("e@") {
            if end.is_some() {
                return Err("only one end boundary may be specified".to_owned());
            }
            end = Some(EndPosition::TimestampMillis(parse_nonnegative_i64(
                value,
                "end timestamp",
            )?));
            continue;
        }
        if start.is_some() {
            return Err("only one start position may be specified".to_owned());
        }
        start = Some(if raw == "beginning" {
            StartPosition::Beginning
        } else if raw == "end" {
            StartPosition::End
        } else if let Some(value) = raw.strip_prefix("s@") {
            StartPosition::TimestampMillis(parse_nonnegative_i64(value, "start timestamp")?)
        } else {
            let value =
                i64::from_str(raw).map_err(|_| format!("unsupported offset position {raw:?}"))?;
            if value < 0 {
                StartPosition::RelativeToEnd(value.unsigned_abs())
            } else {
                StartPosition::Absolute(value)
            }
        });
    }
    Ok((start.unwrap_or(StartPosition::Beginning), end))
}

fn parse_nonnegative_i64(value: &str, label: &str) -> Result<i64, String> {
    let value = value
        .parse::<i64>()
        .map_err(|_| format!("{label} must be milliseconds since Unix epoch"))?;
    if value < 0 {
        Err(format!("{label} must be non-negative"))
    } else {
        Ok(value)
    }
}

fn parse_property(value: &str) -> Result<(&str, &str), String> {
    let (key, value) = value
        .split_once('=')
        .ok_or_else(|| format!("property {value:?} must use key=value syntax"))?;
    if key.trim().is_empty() {
        return Err("property key must not be empty".to_owned());
    }
    Ok((key.trim(), value))
}

fn parse_config_file(path: &PathBuf) -> Result<BTreeMap<String, String>, String> {
    let source = fs::read_to_string(path)
        .map_err(|error| format!("cannot read config {}: {error}", path.display()))?;
    parse_config(&source).map_err(|error| format!("config {}: {error}", path.display()))
}

fn parse_config(source: &str) -> Result<BTreeMap<String, String>, String> {
    let mut properties = BTreeMap::new();
    for (index, line) in source.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) =
            parse_property(line).map_err(|error| format!("line {}: {error}", index + 1))?;
        properties.insert(key.to_owned(), value.trim().to_owned());
    }
    Ok(properties)
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let (number, multiplier) = if let Some(value) = value.strip_suffix("ms") {
        (value, 1_u64)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60_000)
    } else {
        return Err("duration must end in ms, s, or m".to_owned());
    };
    let number = number
        .parse::<u64>()
        .map_err(|_| "duration must contain a positive integer".to_owned())?;
    let milliseconds = number
        .checked_mul(multiplier)
        .filter(|value| *value > 0)
        .ok_or_else(|| "duration must be positive and representable".to_owned())?;
    Ok(Duration::from_millis(milliseconds))
}

fn parse_size(value: &str) -> Result<usize, String> {
    let (number, multiplier) = if let Some(value) = value.strip_suffix("GiB") {
        (value, 1024_u128.pow(3))
    } else if let Some(value) = value.strip_suffix("MiB") {
        (value, 1024_u128.pow(2))
    } else if let Some(value) = value.strip_suffix("KiB") {
        (value, 1024_u128)
    } else {
        (value, 1)
    };
    let bytes = number
        .parse::<u128>()
        .map_err(|_| "size must be an integer with optional KiB, MiB, or GiB suffix".to_owned())?
        .checked_mul(multiplier)
        .ok_or_else(|| "size is too large".to_owned())?;
    usize::try_from(bytes).map_err(|_| "size is too large for this platform".to_owned())
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    fn resolve(arguments: &[&str]) -> Result<RuntimeConfig, String> {
        RawCli::try_parse_from(arguments)
            .map_err(|error| error.to_string())?
            .resolve()
    }

    #[test]
    fn cli_resolves_documented_offsets_and_implied_end_exit() {
        let config = resolve(&[
            "jkq",
            "-b",
            "localhost",
            "-t",
            "events",
            "-p",
            "0",
            "-o",
            "s@10",
            "-o",
            "e@20",
        ])
        .unwrap();
        assert_eq!(config.start, StartPosition::TimestampMillis(10));
        assert_eq!(config.end, Some(EndPosition::TimestampMillis(20)));
        assert!(config.exit_at_end);
    }

    #[test]
    fn cli_expands_partition_lists_and_ranges() {
        let config = resolve(&[
            "jkq",
            "-b",
            "localhost",
            "-t",
            "events",
            "-p",
            "0,2,4-6",
            "-p",
            "8",
        ])
        .unwrap();
        assert_eq!(config.partitions, [0, 2, 4, 5, 6, 8]);

        for selection in ["2-1", "0,,1", "0-2,2", "0-100000"] {
            let error =
                resolve(&["jkq", "-b", "localhost", "-t", "events", "-p", selection]).unwrap_err();
            assert!(error.contains("partition"), "{selection}: {error}");
        }
    }

    #[test]
    fn cli_rejects_invalid_combinations_and_zero_limits() {
        for arguments in [
            vec!["jkq", "-b", "x", "-t", "t", "-p", "0", "-p", "0"],
            vec!["jkq", "-b", "x", "-t", "t", "-p", "-1"],
            vec!["jkq", "-b", "x", "-t", "t", "-p", "0", "-c", "0"],
            vec![
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "--count-per-partition",
                "0",
            ],
            vec![
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "-c",
                "1",
                "--count-per-partition",
                "1",
            ],
            vec![
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "--snapshot",
                "--end-offset",
                "1",
            ],
            vec![
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "--max-inflight-bytes",
                "0",
            ],
            vec!["jkq", "-b", "x", "-t", "t", "-p", "0", "-f", "%z"],
        ] {
            assert!(resolve(&arguments).is_err(), "{arguments:?}");
        }
    }

    #[test]
    fn dedicated_brokers_override_properties_and_later_properties_win() {
        let config = resolve(&[
            "jkq",
            "-t",
            "events",
            "-p",
            "0",
            "-X",
            "bootstrap.servers=old",
            "-X",
            "a=1",
            "-X",
            "a=2",
            "-X",
            "secret= padded ",
            "-b",
            "new",
        ])
        .unwrap();
        assert_eq!(config.kafka_properties["bootstrap.servers"], "new");
        assert_eq!(config.kafka_properties["a"], "2");
        assert_eq!(config.kafka_properties["secret"], " padded ");
    }

    #[test]
    fn ssl_authentication_properties_are_passed_through() {
        let config = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "-p",
            "0",
            "-X",
            "security.protocol=SSL",
            "-X",
            "ssl.ca.location=/certs/ca.pem",
            "-X",
            "ssl.certificate.location=/certs/client.pem",
            "-X",
            "ssl.key.location=/certs/client.key",
        ])
        .unwrap();

        assert_eq!(config.kafka_properties["security.protocol"], "SSL");
        assert_eq!(config.kafka_properties["ssl.ca.location"], "/certs/ca.pem");
        assert_eq!(
            config.kafka_properties["ssl.certificate.location"],
            "/certs/client.pem"
        );
        assert_eq!(
            config.kafka_properties["ssl.key.location"],
            "/certs/client.key"
        );
    }

    #[test]
    fn explicit_invalid_json_policy_forces_validation_on_the_identity_path() {
        let default = resolve(&["jkq", "-b", "x", "-t", "t", "-p", "0"]).unwrap();
        assert!(!default.transform.capabilities.parses_json);

        for policy in ["fail", "drop", "tombstone", "pass"] {
            let explicit = resolve(&[
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "--on-invalid-json",
                policy,
            ])
            .unwrap();
            assert!(explicit.transform.capabilities.parses_json, "{policy}");
        }
    }

    #[test]
    fn cli_validates_jsonata_and_strict_variables_before_connecting() {
        let identity = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "-p",
            "0",
            "--vars",
            r#"{"unused":true}"#,
        ])
        .unwrap();
        assert!(!identity.transform.capabilities.parses_json);

        let config = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "-p",
            "0",
            "--vars",
            r#"{"allowed":["open","pending"],"cutoff":10}"#,
            "--drop-if",
            "$not(status in $vars.allowed)",
            "--project",
            r#"{"tier": amount >= $vars.cutoff ? "large" : "small", "value": a ?? b ?? 0}"#,
            "--check",
        ])
        .unwrap();
        assert!(config.check);
        assert!(config.transform.capabilities.parses_json);

        let error = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "-p",
            "0",
            "--vars",
            "{value: record}",
        ])
        .unwrap_err();
        assert!(error.contains("--vars must be a valid JSON object"));
    }

    #[test]
    fn runtime_owned_kafka_properties_are_rejected() {
        for property in [
            "auto.offset.reset=latest",
            "enable.auto.commit=true",
            "enable.auto.offset.store=true",
            "enable.partition.eof=false",
        ] {
            let error =
                resolve(&["jkq", "-b", "x", "-t", "t", "-p", "0", "-X", property]).unwrap_err();
            assert!(error.contains("managed by jkq"), "{property}: {error}");
        }
    }

    #[test]
    fn config_parser_reports_malformed_line_number() {
        let error = parse_config("a=1\nmalformed\n").unwrap_err();
        assert!(error.contains("line 2"));
    }

    #[test]
    fn sizes_and_durations_are_checked_without_dependencies() {
        assert_eq!(parse_size("2MiB").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert!(parse_size("1MB").is_err());
        assert!(parse_duration("0ms").is_err());
    }

    #[test]
    fn help_describes_assignment_and_runtime_limits() {
        let help = RawCli::command().render_long_help().to_string();
        assert!(help.contains("Partitions to consume, for example 0,2,4-7"));
        assert!(help.contains("Maximum admitted input records per partition"));
        assert!(help.contains("Validate local configuration and exit without connecting"));
        assert!(help.contains("Owned source-byte admission budget; supports KiB, MiB, and GiB"));
        assert!(help.contains("Strict JSON object available as $vars"));
    }
}
