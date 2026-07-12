use std::{collections::BTreeMap, fs, path::PathBuf, str::FromStr, time::Duration};

use clap::{Parser, ValueEnum};

use crate::{
    output::CompiledFormat,
    transform::{
        compile::{TransformPlan, build_plan},
        json::{ErrorPolicies, EvaluationPolicy, InvalidJsonPolicy},
    },
};

const DEFAULT_MAX_INFLIGHT_RECORDS: usize = 1_024;
const DEFAULT_MAX_INFLIGHT_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_INFLIGHT_PER_PARTITION: usize = 256;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Consume explicitly assigned Kafka partitions and transform JSON records"
)]
pub struct RawCli {
    #[arg(short = 'b', long)]
    brokers: Option<String>,
    #[arg(short = 't', long)]
    topic: String,
    #[arg(short = 'p', long = "partition", required = true)]
    partitions: Vec<i32>,
    #[arg(short = 'o', long = "offset", allow_hyphen_values = true)]
    offsets: Vec<String>,
    #[arg(long, allow_hyphen_values = true)]
    end_offset: Option<i64>,
    #[arg(short = 'c', long)]
    count: Option<u64>,
    #[arg(short = 'e', long)]
    exit_at_end: bool,
    #[arg(long)]
    snapshot: bool,
    #[arg(long)]
    drop_if: Vec<String>,
    #[arg(long)]
    tombstone_if: Vec<String>,
    #[arg(long)]
    project: Option<String>,
    #[arg(short = 'f', long, conflicts_with = "json_envelope")]
    format: Option<String>,
    #[arg(short = 'J', long, conflicts_with = "format")]
    json_envelope: bool,
    #[arg(short = 'u', long)]
    unbuffered: bool,
    #[arg(long)]
    stats: bool,
    #[arg(long, value_parser = parse_duration)]
    stats_interval: Option<Duration>,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(short = 'j', long)]
    jobs: Option<usize>,
    #[arg(long)]
    unordered: bool,
    #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_RECORDS)]
    max_inflight_records: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_BYTES, value_parser = parse_size)]
    max_inflight_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_PER_PARTITION)]
    max_inflight_per_partition: usize,
    #[arg(long, value_enum, default_value_t = RawInvalidJsonPolicy::Fail)]
    on_invalid_json: RawInvalidJsonPolicy,
    #[arg(long, value_enum, default_value_t = RawEvaluationPolicy::Fail)]
    on_eval_error: RawEvaluationPolicy,
    #[arg(long, value_enum, default_value_t = KafkaErrorPolicy::Fail)]
    on_kafka_error: KafkaErrorPolicy,
    #[arg(short = 'F', long)]
    config: Option<PathBuf>,
    #[arg(short = 'X', long = "property")]
    properties: Vec<String>,
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

#[derive(Debug)]
#[allow(dead_code)]
pub struct RuntimeLimits {
    pub max_inflight_records: usize,
    pub max_inflight_bytes: usize,
    pub max_inflight_per_partition: usize,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum OutputPlan {
    Format(CompiledFormat),
    Envelope,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct RuntimeConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub partitions: Vec<i32>,
    pub start: StartPosition,
    pub end: Option<EndPosition>,
    pub count_limit: Option<u64>,
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
}

impl RawCli {
    pub fn resolve(self) -> Result<RuntimeConfig, String> {
        if self.topic.is_empty() {
            return Err("topic must not be empty".to_owned());
        }
        let mut unique = self.partitions.clone();
        unique.sort_unstable();
        if let Some(partition) = unique.iter().find(|partition| **partition < 0) {
            return Err(format!(
                "partition must be non-negative, received {partition}"
            ));
        }
        if unique.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("duplicate partition selection".to_owned());
        }
        if self.count == Some(0) {
            return Err("count must be positive".to_owned());
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

        let invalid_json = match self.on_invalid_json {
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
            invalid_json == InvalidJsonPolicy::Pass,
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
        if let Some(brokers) = &self.brokers {
            kafka_properties.insert("bootstrap.servers".to_owned(), brokers.clone());
        }
        let broker_value = kafka_properties
            .get("bootstrap.servers")
            .ok_or_else(|| "brokers are required through -b or bootstrap.servers".to_owned())?;
        let brokers = broker_value
            .split(',')
            .map(str::trim)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if brokers.iter().any(String::is_empty) {
            return Err("broker list contains an empty entry".to_owned());
        }

        Ok(RuntimeConfig {
            brokers,
            topic: self.topic,
            partitions: self.partitions,
            start,
            exit_at_end: self.exit_at_end || end.is_some(),
            end,
            count_limit: self.count,
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
    Ok((key.trim(), value.trim()))
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
        properties.insert(key.to_owned(), value.to_owned());
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
            "-b",
            "new",
        ])
        .unwrap();
        assert_eq!(config.brokers, ["new"]);
        assert_eq!(config.kafka_properties["a"], "2");
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
}
