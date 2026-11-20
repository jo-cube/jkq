use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::DateTime;
use clap::{Arg, ArgAction, ArgMatches, Args, Command, FromArgMatches, Parser, ValueEnum};

use crate::{
    output::{CompiledFormat, OutputRequirements, TimestampFormat},
    transform::{
        PredicateSource, TransformPlan, build_plan,
        jsonata::{ErrorPolicies, EvaluationPolicy, InvalidJsonPolicy},
    },
};

const DEFAULT_MAX_INFLIGHT_RECORDS: usize = 8_192;
const DEFAULT_MAX_INFLIGHT_BYTES: &str = "256MiB";
const DEFAULT_MAX_INFLIGHT_PER_PARTITION: usize = 8_192;
const MAX_CONSUMERS: usize = 128;
pub(crate) const MAX_ASSIGNED_PARTITIONS: usize = 100_000;

const LONG_HELP: &str = "\
RECORD PIPELINE:
  1. Drop stage: --drop-if, --drop-unless, --keep-if (and *-file variants)
     evaluate in command-line order; first match drops the record.
  2. Tombstone stage: --tombstone-if, --tombstone-unless (and *-file variants)
     evaluate in command-line order; first match tombstones (or drops with
     --drop-tombstones).
  3. Output: --project / --project-file reshapes surviving records; otherwise
     exact source bytes pass through.

TOMBSTONES & METADATA PREDICATES:
  - --drop-tombstones drops both source Kafka tombstones and predicate-generated
    tombstones immediately.
  - Metadata-only predicates (e.g. --keep-if '$meta.key = \"user-42\"') run on
    both normal records and source tombstones automatically -- no --defer-json
    needed.
  - Payload predicates (e.g. --keep-if 'status = \"active\"') and --project skip
    source tombstones by default so surviving tombstones remain tombstones.
  - IMPORTANT: A single combined metadata+payload predicate (e.g.
    --keep-if '$meta.key = \"user-42\" and status = \"active\"') depends on the
    payload and therefore skips tombstones. Split metadata and payload checks
    into separate flags when you want tombstones filtered by key/metadata too:
      --keep-if '$meta.key = \"user-42\"' --keep-if 'status = \"active\"'
  - Use --defer-json to skip JSON validation before metadata drops, or to let
    source tombstones evaluate payload expressions / --project with absent input.

JSONATA NOTES ($meta, $vars, missing fields):
  - All predicates require a strict Boolean result (true/false).
  - In JSONata, missing != \"x\" evaluates to false (keeping missing records).
    Prefer positive checks such as --keep-if 'tenant = $vars.tenant' or
    --drop-if '$not(tenant = $vars.tenant)'.
  - $meta fields: topic, partition, offset (decimal string), timestamp (ms|null),
    timestampType (\"createTime\"|\"logAppendTime\"|null), key, keyEncoding,
    keyLength, headers ([{name,value,valueEncoding,valueLength}]),
    valueLength (-1 for tombstones), isTombstone (boolean).
  - $vars: immutable strict JSON object supplied via --vars or --vars-file.

OFFSETS (-o / --offset):
  beginning | end | <offset> | -<last_n>
  s@<epoch_ms | ISO-8601 | -duration>   start timestamp (e.g. s@2026-01-02T00:00:00Z, s@-1h)
  e@<epoch_ms | ISO-8601 | -duration>   exclusive end timestamp (durations: ms, s, m, h, d)

FORMAT PLACEHOLDERS (-f / --payload-format):
  %t topic  %p partition  %o offset  %T timestamp_ms (-1)  %a action
  %k key    %K key_len (-1)  %h headers  %% literal %
  %s post-transform payload  %S payload_len (-1 tombstone)  %L source_len  %R 4B BE len

EXAMPLES:
  # All records (including tombstones) for one key on a compacted topic:
  jkq -b localhost:9092 -t users --snapshot --keep-if '$meta.key = \"user-42\"' -J

  # Filter by key (including tombstones) and project non-tombstone payloads:
  jkq -b localhost:9092 -t users --snapshot \\
    --keep-if '$meta.key = \"user-42\"' --project '{\"id\": id, \"email\": email}' -J

  # Last hour of production events without tombstones:
  jkq -b localhost:9092 -t events -o s@-1h --snapshot --drop-tombstones \\
    --keep-if 'environment = \"production\"'
";

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Filter and transform JSON records from directly assigned Kafka partitions",
    after_long_help = LONG_HELP
)]
pub struct RawCli {
    /// Comma-separated bootstrap broker list
    #[arg(short = 'b', long)]
    brokers: Option<String>,
    /// Kafka topic to consume
    #[arg(short = 't', long)]
    topic: String,
    /// Partitions to consume; defaults to all topic partitions
    #[arg(short = 'p', long = "partition", allow_negative_numbers = true)]
    partitions: Vec<String>,
    /// Start position or s@/e@ timestamp boundary
    #[arg(short = 'o', long = "offset", allow_hyphen_values = true)]
    offsets: Vec<String>,
    /// Exclusive end offset for every assigned partition
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
    #[command(flatten)]
    predicates: PredicateArgs,
    /// Drop source and predicate-generated tombstones before projection
    #[arg(long)]
    drop_tombstones: bool,
    /// Prepare JSON only when reached expressions or output require it; inspect source tombstones
    #[arg(long)]
    defer_json: bool,
    /// Project each surviving record with this JSONata expression
    #[arg(long, conflicts_with = "project_file")]
    project: Option<String>,
    /// Read the --project JSONata expression from a UTF-8 file
    #[arg(long, value_name = "PATH", conflicts_with = "project")]
    project_file: Option<PathBuf>,
    /// Strict JSON object available as $vars
    #[arg(long, value_name = "OBJECT")]
    vars: Option<String>,
    /// Read the strict JSON object available as $vars from a file
    #[arg(long, value_name = "PATH", conflicts_with = "vars")]
    vars_file: Option<PathBuf>,
    /// Build each non-tombstone payload before final -f formatting
    #[arg(long, value_name = "FORMAT", conflicts_with = "json_envelope")]
    payload_format: Option<String>,
    /// Format output with kcat-compatible placeholders
    #[arg(
        short = 'f',
        long,
        default_value = "%s\\n",
        conflicts_with = "json_envelope"
    )]
    format: String,
    /// Emit a binary-safe jkq JSON envelope instead of -f output
    #[arg(short = 'J', long, conflicts_with = "format")]
    json_envelope: bool,
    /// Payload representation for -J envelopes
    #[arg(
        long,
        value_enum,
        value_name = "MODE",
        default_value = "string",
        requires = "json_envelope"
    )]
    envelope_payload: EnvelopePayload,
    /// Format -J timestamps as UTC ISO 8601 instead of Unix milliseconds
    #[arg(long, value_parser = ["iso8601"], requires = "json_envelope")]
    timestamp_format: Option<String>,
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
    /// Number of Kafka consumers
    #[arg(long, default_value_t = 1)]
    consumers: usize,
    /// Split bounded partition ranges across consumers; requires --unordered
    #[arg(long)]
    range_sharding: bool,
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

#[derive(Debug)]
struct RawPredicate {
    value: String,
    is_file: bool,
    flag: &'static str,
    trigger: bool,
    origin: String,
}

impl RawPredicate {
    fn resolve(self) -> Result<PredicateSource, String> {
        let source = if self.is_file {
            fs::read_to_string(&self.value)
                .map_err(|error| format!("cannot read --{} {}: {error}", self.flag, self.value))?
        } else {
            self.value
        };
        Ok(PredicateSource {
            source,
            trigger: self.trigger,
            origin: self.origin,
        })
    }
}

#[derive(Debug)]
struct PredicateArgs {
    drops: Vec<RawPredicate>,
    tombstones: Vec<RawPredicate>,
}

impl Args for PredicateArgs {
    fn augment_args(mut command: Command) -> Command {
        for (id, flag, value_name, help) in [
            (
                "drop_if",
                "drop-if",
                "EXPR",
                "Drop records when this JSONata predicate returns Boolean true; repeatable",
            ),
            (
                "drop_if_file",
                "drop-if-file",
                "PATH",
                "Read a --drop-if JSONata predicate from a UTF-8 file; repeatable",
            ),
            (
                "drop_unless",
                "drop-unless",
                "EXPR",
                "Drop records when this JSONata predicate returns Boolean false; repeatable",
            ),
            (
                "drop_unless_file",
                "drop-unless-file",
                "PATH",
                "Read a --drop-unless JSONata predicate from a UTF-8 file; repeatable",
            ),
            (
                "keep_if",
                "keep-if",
                "EXPR",
                "Keep only records where this JSONata predicate returns Boolean true (drops on false); repeatable",
            ),
            (
                "keep_if_file",
                "keep-if-file",
                "PATH",
                "Read a --keep-if JSONata predicate from a UTF-8 file; repeatable",
            ),
            (
                "tombstone_if",
                "tombstone-if",
                "EXPR",
                "Tombstone records when this JSONata predicate returns Boolean true; repeatable",
            ),
            (
                "tombstone_if_file",
                "tombstone-if-file",
                "PATH",
                "Read a --tombstone-if JSONata predicate from a UTF-8 file; repeatable",
            ),
            (
                "tombstone_unless",
                "tombstone-unless",
                "EXPR",
                "Tombstone records when this JSONata predicate returns Boolean false; repeatable",
            ),
            (
                "tombstone_unless_file",
                "tombstone-unless-file",
                "PATH",
                "Read a --tombstone-unless JSONata predicate from a UTF-8 file; repeatable",
            ),
        ] {
            command = command.arg(
                Arg::new(id)
                    .long(flag)
                    .value_name(value_name)
                    .num_args(1)
                    .action(ArgAction::Append)
                    .value_parser(clap::value_parser!(String))
                    .help(help),
            );
        }
        command
    }

    fn augment_args_for_update(command: Command) -> Command {
        Self::augment_args(command)
    }
}

impl FromArgMatches for PredicateArgs {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        Ok(Self {
            drops: ordered_predicates(
                matches,
                &[
                    ("drop_if", "drop-if", false, true, "drop predicate"),
                    (
                        "drop_if_file",
                        "drop-if-file",
                        true,
                        true,
                        "drop-if-file predicate",
                    ),
                    (
                        "drop_unless",
                        "drop-unless",
                        false,
                        false,
                        "drop-unless predicate",
                    ),
                    (
                        "drop_unless_file",
                        "drop-unless-file",
                        true,
                        false,
                        "drop-unless-file predicate",
                    ),
                    ("keep_if", "keep-if", false, false, "keep-if predicate"),
                    (
                        "keep_if_file",
                        "keep-if-file",
                        true,
                        false,
                        "keep-if-file predicate",
                    ),
                ],
            ),
            tombstones: ordered_predicates(
                matches,
                &[
                    (
                        "tombstone_if",
                        "tombstone-if",
                        false,
                        true,
                        "tombstone predicate",
                    ),
                    (
                        "tombstone_if_file",
                        "tombstone-if-file",
                        true,
                        true,
                        "tombstone-if-file predicate",
                    ),
                    (
                        "tombstone_unless",
                        "tombstone-unless",
                        false,
                        false,
                        "tombstone-unless predicate",
                    ),
                    (
                        "tombstone_unless_file",
                        "tombstone-unless-file",
                        true,
                        false,
                        "tombstone-unless-file predicate",
                    ),
                ],
            ),
        })
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        let updated = Self::from_arg_matches(matches)?;
        if [
            "drop_if",
            "drop_if_file",
            "drop_unless",
            "drop_unless_file",
            "keep_if",
            "keep_if_file",
        ]
        .into_iter()
        .any(|id| matches.contains_id(id))
        {
            self.drops = updated.drops;
        }
        if [
            "tombstone_if",
            "tombstone_if_file",
            "tombstone_unless",
            "tombstone_unless_file",
        ]
        .into_iter()
        .any(|id| matches.contains_id(id))
        {
            self.tombstones = updated.tombstones;
        }
        Ok(())
    }
}

fn ordered_predicates(
    matches: &ArgMatches,
    options: &[(&str, &'static str, bool, bool, &str)],
) -> Vec<RawPredicate> {
    let mut predicates: Vec<_> = options
        .iter()
        .flat_map(|&(id, flag, is_file, trigger, category)| {
            matches
                .indices_of(id)
                .into_iter()
                .flatten()
                .zip(matches.get_many::<String>(id).into_iter().flatten())
                .enumerate()
                .map(move |(ordinal, (position, value))| {
                    (
                        position,
                        RawPredicate {
                            value: value.clone(),
                            is_file,
                            flag,
                            trigger,
                            origin: format!("{category} #{}", ordinal + 1),
                        },
                    )
                })
        })
        .collect();
    // clap retains each value's argv position even when option names differ.
    predicates.sort_unstable_by_key(|(position, _)| *position);
    predicates
        .into_iter()
        .map(|(_, predicate)| predicate)
        .collect()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum EnvelopePayload {
    /// Encode bytes as a UTF-8 or base64 string; does not force JSON parsing
    String,
    /// Embed valid JSON; forces validation and compacts pass-through payloads
    Value,
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
    Format {
        payload_format: Option<CompiledFormat>,
        format: CompiledFormat,
    },
    Envelope(EnvelopePayload),
}

#[derive(Debug)]
pub struct RuntimeConfig {
    pub topic: String,
    pub partitions: Option<Vec<i32>>,
    pub start: StartPosition,
    pub end: Option<EndPosition>,
    pub count_limit: Option<u64>,
    pub count_per_partition: Option<u64>,
    pub exit_at_end: bool,
    pub consumers: usize,
    pub range_sharding: bool,
    pub unordered: bool,
    pub limits: RuntimeLimits,
    pub transform: TransformPlan,
    pub output: OutputPlan,
    pub timestamp_format: TimestampFormat,
    pub errors: ErrorPolicies,
    pub kafka_error: KafkaErrorPolicy,
    pub kafka_properties: BTreeMap<String, String>,
    pub unbuffered: bool,
    pub stats: bool,
    pub stats_interval: Option<Duration>,
    pub quiet: bool,
    pub check: bool,
}

impl RuntimeConfig {
    pub fn needs_worker(&self) -> bool {
        self.transform.capabilities.needs_worker || self.output.embeds_json()
    }

    pub fn requirements(&self) -> OutputRequirements {
        let mut requirements = self.output.requirements();
        let meta = self.transform.capabilities.metadata;
        requirements.key |= meta.needs_key();
        requirements.headers |= meta.needs_headers();
        requirements.timestamp |= meta.needs_timestamp();
        requirements
    }
}

impl OutputPlan {
    pub fn requirements(&self) -> OutputRequirements {
        match self {
            Self::Format {
                payload_format,
                format,
            } => {
                let mut requirements = format.requirements();
                if let Some(payload_format) = payload_format {
                    let payload_requirements = payload_format.requirements();
                    requirements.key |= payload_requirements.key;
                    requirements.headers |= payload_requirements.headers;
                    requirements.timestamp |= payload_requirements.timestamp;
                }
                requirements
            }
            Self::Envelope(_) => OutputRequirements {
                key: true,
                headers: true,
                timestamp: true,
            },
        }
    }

    pub fn embeds_json(&self) -> bool {
        matches!(self, Self::Envelope(EnvelopePayload::Value))
    }
}

impl RawCli {
    pub fn resolve(self) -> Result<RuntimeConfig, String> {
        if self.topic.is_empty() {
            return Err("topic must not be empty".to_owned());
        }
        let partitions = if self.partitions.is_empty() {
            None
        } else {
            Some(parse_partitions(&self.partitions)?)
        };
        if self.count == Some(0) {
            return Err("count must be positive".to_owned());
        }
        if self.count_per_partition == Some(0) {
            return Err("per-partition count must be positive".to_owned());
        }
        if self.consumers == 0 {
            return Err("consumer count must be at least 1".to_owned());
        }
        if self.consumers > MAX_CONSUMERS {
            return Err(format!("consumer count must not exceed {MAX_CONSUMERS}"));
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

        if self.range_sharding {
            if !self.unordered {
                return Err("--range-sharding requires --unordered".to_owned());
            }
            if end.is_none() {
                return Err(
                    "--range-sharding requires --snapshot or a fixed end boundary".to_owned(),
                );
            }
            if self.count_per_partition.is_some() {
                return Err(
                    "--range-sharding cannot be combined with --count-per-partition".to_owned(),
                );
            }
        }

        let envelope_payload = self.envelope_payload;
        if envelope_payload == EnvelopePayload::Value
            && self.on_invalid_json == Some(RawInvalidJsonPolicy::Pass)
        {
            return Err(
                "--envelope-payload value cannot be combined with --on-invalid-json pass"
                    .to_owned(),
            );
        }
        let force_json_validation = !self.defer_json
            && (self.on_invalid_json.is_some() || envelope_payload == EnvelopePayload::Value);
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
        let file_variables = self
            .vars_file
            .as_ref()
            .map(|path| {
                fs::read_to_string(path)
                    .map_err(|error| format!("cannot read --vars-file {}: {error}", path.display()))
            })
            .transpose()?;
        let drops = self
            .predicates
            .drops
            .into_iter()
            .map(RawPredicate::resolve)
            .collect::<Result<Vec<_>, _>>()?;
        let tombstones = self
            .predicates
            .tombstones
            .into_iter()
            .map(RawPredicate::resolve)
            .collect::<Result<Vec<_>, _>>()?;
        let file_projection = self
            .project_file
            .as_ref()
            .map(|path| {
                fs::read_to_string(path).map_err(|error| {
                    format!("cannot read --project-file {}: {error}", path.display())
                })
            })
            .transpose()?;
        let mut transform = build_plan(
            &drops,
            &tombstones,
            self.drop_tombstones,
            file_projection.as_deref().or(self.project.as_deref()),
            file_variables.as_deref().or(self.vars.as_deref()),
            force_json_validation,
        )?;
        transform.defer_json = self.defer_json;
        let output = if self.json_envelope {
            OutputPlan::Envelope(envelope_payload)
        } else {
            OutputPlan::Format {
                payload_format: self
                    .payload_format
                    .as_deref()
                    .map(CompiledFormat::compile)
                    .transpose()
                    .map_err(|error| format!("invalid --payload-format: {error}"))?,
                format: CompiledFormat::compile(&self.format).map_err(|error| error.to_string())?,
            }
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
        if kafka_properties.contains_key("fetch.message.max.bytes")
            && kafka_properties.contains_key("max.partition.fetch.bytes")
        {
            return Err("use only one of Kafka properties fetch.message.max.bytes and its alias max.partition.fetch.bytes".to_owned());
        }
        if let Some(brokers) = kafka_properties.remove("metadata.broker.list")
            && self.brokers.is_none()
            && kafka_properties
                .insert("bootstrap.servers".to_owned(), brokers)
                .is_some()
        {
            return Err("use only one of Kafka properties bootstrap.servers and its alias metadata.broker.list, or override both with -b".to_owned());
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
            consumers: self.consumers,
            range_sharding: self.range_sharding,
            unordered: self.unordered,
            limits: RuntimeLimits {
                max_inflight_records: self.max_inflight_records,
                max_inflight_bytes: self.max_inflight_bytes,
                max_inflight_per_partition: self.max_inflight_per_partition,
            },
            transform,
            output,
            timestamp_format: if self.timestamp_format.is_some() {
                TimestampFormat::Iso8601
            } else {
                TimestampFormat::Milliseconds
            },
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
                .is_none_or(|total| total > MAX_ASSIGNED_PARTITIONS)
            {
                return Err(format!(
                    "partition selection expands beyond the {MAX_ASSIGNED_PARTITIONS} partition limit"
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
            end = Some(EndPosition::TimestampMillis(parse_timestamp_millis(
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
            StartPosition::TimestampMillis(parse_timestamp_millis(value, "start timestamp")?)
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

fn parse_timestamp_millis(value: &str, label: &str) -> Result<i64, String> {
    if let Some(relative) = value.strip_prefix('-') {
        let duration = parse_relative_duration(relative).map_err(|_| {
            format!(
                "{label} must be Unix epoch milliseconds, RFC 3339/ISO 8601, or a relative duration (for example -1h or -5m)"
            )
        })?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "system clock is before the Unix epoch".to_owned())?
            .as_millis();
        let delta_ms = duration.as_millis();
        let target = now_ms
            .checked_sub(delta_ms)
            .ok_or_else(|| format!("{label} precedes the Unix epoch"))?;
        return i64::try_from(target).map_err(|_| format!("{label} exceeds supported range"));
    }
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        return value
            .parse::<i64>()
            .map_err(|_| format!("{label} exceeds supported range"));
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        let millis = parsed.timestamp_millis();
        if millis < 0 {
            return Err(format!("{label} must be non-negative"));
        }
        return Ok(millis);
    }
    Err(format!(
        "{label} must be Unix epoch milliseconds, RFC 3339/ISO 8601, or a relative duration (for example -1h or -5m)"
    ))
}

fn parse_relative_duration(value: &str) -> Result<Duration, String> {
    let (number, multiplier) = if let Some(value) = value.strip_suffix("ms") {
        (value, 1_u64)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60_000)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 3_600_000)
    } else if let Some(value) = value.strip_suffix('d') {
        (value, 86_400_000)
    } else {
        return Err("duration must end in ms, s, m, h, or d".to_owned());
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

fn parse_property(value: &str) -> Result<(&str, &str), String> {
    let (key, value) = value
        .split_once('=')
        .ok_or_else(|| "property must use key=value syntax".to_owned())?;
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
    fn conflicting_partition_fetch_aliases_fail_before_connecting() {
        let error = resolve(&[
            "jkq",
            "-b",
            "unused",
            "-t",
            "events",
            "-X",
            "fetch.message.max.bytes=16384",
            "-X",
            "max.partition.fetch.bytes=32768",
        ])
        .unwrap_err();
        assert!(error.contains("use only one"), "{error}");
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
    fn cli_defaults_to_all_partitions() {
        let all = resolve(&["jkq", "-b", "localhost", "-t", "events"]).unwrap();
        assert_eq!(all.partitions, None);
        assert_eq!(all.consumers, 1);
    }

    #[test]
    fn evaluator_concurrency_is_not_a_cli_option() {
        for option in ["-j", "--jobs"] {
            let error =
                RawCli::try_parse_from(["jkq", "-b", "localhost", "-t", "events", option, "2"])
                    .unwrap_err();
            assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn cli_accepts_a_custom_consumer_count() {
        let config =
            resolve(&["jkq", "-b", "localhost", "-t", "events", "--consumers", "3"]).unwrap();
        assert_eq!(config.consumers, 3);
    }

    #[test]
    fn range_sharding_requires_unordered_fixed_bounds_and_no_partition_count() {
        let base = ["jkq", "-b", "x", "-t", "events", "--range-sharding"];
        for (extra, expected) in [
            (vec!["--snapshot"], "requires --unordered"),
            (vec!["--unordered", "-e"], "fixed end boundary"),
            (
                vec!["--unordered", "--snapshot", "--count-per-partition", "1"],
                "cannot be combined",
            ),
        ] {
            assert!(
                resolve(&[base.as_slice(), extra.as_slice()].concat())
                    .unwrap_err()
                    .contains(expected)
            );
        }
        for boundary in [
            vec!["--snapshot"],
            vec!["--end-offset", "100"],
            vec!["-o", "e@1000"],
        ] {
            let config = resolve(
                &[
                    base.as_slice(),
                    &["--unordered", "--consumers", "4"],
                    boundary.as_slice(),
                ]
                .concat(),
            )
            .unwrap();
            assert!(config.range_sharding);
            assert!(config.exit_at_end);
        }
        assert!(
            !resolve(&[
                "jkq",
                "-b",
                "x",
                "-t",
                "events",
                "--unordered",
                "--snapshot",
                "--consumers",
                "4"
            ])
            .unwrap()
            .range_sharding
        );
    }

    #[test]
    fn dropping_tombstones_keeps_the_identity_path() {
        let config = resolve(&[
            "jkq",
            "-b",
            "localhost",
            "-t",
            "events",
            "--drop-tombstones",
        ])
        .unwrap();
        assert!(config.transform.drop_tombstones);
        assert!(!config.transform.capabilities.needs_worker);
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
        assert_eq!(config.partitions.as_deref(), Some(&[0, 2, 4, 5, 6, 8][..]));

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
            vec!["jkq", "-b", "x", "-t", "t", "--consumers", "0"],
            vec!["jkq", "-b", "x", "-t", "t", "--consumers", "129"],
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
            vec![
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "--vars",
                "{}",
                "--vars-file",
                "vars.json",
            ],
            vec![
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "--envelope-payload",
                "value",
            ],
            vec![
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "-J",
                "--envelope-payload",
                "value",
                "--on-invalid-json",
                "pass",
            ],
            vec!["jkq", "-b", "x", "-t", "t", "-J", "--payload-format", "%s"],
            vec!["jkq", "-b", "x", "-t", "t", "-p", "0", "-f", "%z"],
            vec![
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                "-p",
                "0",
                "--payload-format",
                "%z",
            ],
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
            "metadata.broker.list=also-old",
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
        assert!(!config.kafka_properties.contains_key("metadata.broker.list"));
        assert_eq!(config.kafka_properties["a"], "2");
        assert_eq!(config.kafka_properties["secret"], " padded ");
    }

    #[test]
    fn broker_alias_is_resolved_before_native_configuration() {
        let config = resolve(&[
            "jkq",
            "-t",
            "events",
            "-X",
            "metadata.broker.list=broker:9092",
        ])
        .unwrap();
        assert_eq!(config.kafka_properties["bootstrap.servers"], "broker:9092");
        assert!(!config.kafka_properties.contains_key("metadata.broker.list"));
        let error = resolve(&[
            "jkq",
            "-t",
            "events",
            "-X",
            "metadata.broker.list=first:9092",
            "-X",
            "bootstrap.servers=second:9092",
        ])
        .unwrap_err();
        assert!(error.contains("use only one"), "{error}");
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
    fn mixed_predicate_compile_errors_identify_the_option_occurrence() {
        for (if_flag, unless_flag, category) in [
            ("--drop-if", "--drop-unless", "drop-unless predicate #2"),
            (
                "--tombstone-if",
                "--tombstone-unless",
                "tombstone-unless predicate #2",
            ),
        ] {
            let error = resolve(&[
                "jkq",
                "-b",
                "x",
                "-t",
                "t",
                unless_flag,
                "true",
                if_flag,
                "false",
                unless_flag,
                "(",
            ])
            .unwrap_err();
            assert!(error.starts_with(&format!("{category} JSONata compile error")));
        }
    }

    #[test]
    fn deferred_mode_separates_workers_json_and_metadata_requirements() {
        let identity = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "--defer-json",
            "--on-invalid-json",
            "fail",
        ])
        .unwrap();
        assert!(!identity.needs_worker());
        let metadata = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "--defer-json",
            "--project",
            "$meta.offset",
        ])
        .unwrap();
        assert!(metadata.transform.defer_json);
        assert!(metadata.needs_worker());
        assert_eq!(
            metadata.requirements(),
            OutputRequirements {
                key: false,
                headers: false,
                timestamp: false
            }
        );
        let key_only = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "--keep-if",
            "$meta.key = 'user-42'",
        ])
        .unwrap();
        assert_eq!(
            key_only.requirements(),
            OutputRequirements {
                key: true,
                headers: false,
                timestamp: false
            }
        );
        let whole_meta = resolve(&["jkq", "-b", "x", "-t", "t", "--project", "$meta"]).unwrap();
        assert_eq!(
            whole_meta.requirements(),
            OutputRequirements {
                key: true,
                headers: true,
                timestamp: true
            }
        );
        assert_eq!(
            metadata.output.requirements(),
            OutputRequirements {
                key: false,
                headers: false,
                timestamp: false
            }
        );
        let payload = resolve(&["jkq", "-b", "x", "-t", "t", "--project", "metadata.id"]).unwrap();
        assert!(payload.transform.capabilities.metadata.is_empty());
        let value = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "--defer-json",
            "-J",
            "--envelope-payload",
            "value",
        ])
        .unwrap();
        assert!(value.needs_worker());
        assert!(!value.transform.capabilities.needs_worker);
    }

    #[test]
    fn explicit_invalid_json_policy_forces_validation_on_the_identity_path() {
        let default = resolve(&["jkq", "-b", "x", "-t", "t", "-p", "0"]).unwrap();
        assert!(!default.transform.capabilities.needs_worker);

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
            assert!(explicit.transform.capabilities.needs_worker, "{policy}");
        }
    }

    #[test]
    fn timestamp_format_requires_an_envelope_and_known_representation() {
        let default = resolve(&["jkq", "-b", "x", "-t", "t", "-J"]).unwrap();
        assert_eq!(default.timestamp_format, TimestampFormat::Milliseconds);
        let iso = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "-J",
            "--timestamp-format",
            "iso8601",
        ])
        .unwrap();
        assert_eq!(iso.timestamp_format, TimestampFormat::Iso8601);
        assert_eq!(iso.requirements(), default.requirements());
        assert_eq!(iso.needs_worker(), default.needs_worker());
        for extra in [
            vec!["--timestamp-format", "iso8601"],
            vec!["-J", "--timestamp-format", "unknown"],
            vec!["-J", "--timestamp-format"],
            vec!["-J", "--timestamp-format", "iso8601", "-f", "%T"],
        ] {
            let mut arguments = vec!["jkq", "-b", "x", "-t", "t"];
            arguments.extend(extra);
            assert!(resolve(&arguments).is_err(), "{arguments:?}");
        }
    }

    #[test]
    fn json_value_envelope_forces_validation_without_changing_the_default() {
        let default = resolve(&["jkq", "-b", "x", "-t", "t", "-p", "0", "-J"]).unwrap();
        assert!(!default.transform.capabilities.needs_worker);
        assert!(matches!(
            default.output,
            OutputPlan::Envelope(EnvelopePayload::String)
        ));

        let value = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "-p",
            "0",
            "-J",
            "--envelope-payload",
            "value",
        ])
        .unwrap();
        assert!(value.transform.capabilities.needs_worker);
        assert!(matches!(
            value.output,
            OutputPlan::Envelope(EnvelopePayload::Value)
        ));
    }

    #[test]
    fn payload_format_adds_its_metadata_requirements_without_json_parsing() {
        let config = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "--payload-format",
            "%T:%h:%s",
            "-f",
            "%K:%k:%S:%s",
        ])
        .unwrap();

        assert!(!config.transform.capabilities.needs_worker);
        assert_eq!(
            config.output.requirements(),
            OutputRequirements {
                key: true,
                headers: true,
                timestamp: true,
            }
        );
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
        assert!(!identity.transform.capabilities.needs_worker);

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
        assert!(config.transform.capabilities.needs_worker);

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
        assert!(error.contains("$vars input must be a valid JSON object"));
    }

    #[test]
    fn vars_file_uses_the_existing_variable_plan() {
        let path = std::env::temp_dir().join(format!("jkq-vars-{}.json", std::process::id()));
        let path_text = path.to_string_lossy().into_owned();
        let source = r#"{"tenant":"acme","cutoff":10}"#;

        fs::write(&path, source).unwrap();
        let valid = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "-p",
            "0",
            "--vars-file",
            &path_text,
            "--project",
            "$vars",
            "--check",
        ]);
        fs::write(&path, "[]").unwrap();
        let invalid = resolve(&[
            "jkq",
            "-b",
            "x",
            "-t",
            "t",
            "-p",
            "0",
            "--vars-file",
            &path_text,
        ]);
        fs::remove_file(path).unwrap();

        let config = valid.unwrap();
        let mut bytes = Vec::new();
        config
            .transform
            .projection
            .as_ref()
            .unwrap()
            .evaluate(b"null")
            .unwrap()
            .single()
            .unwrap()
            .unwrap()
            .write_compact(&mut bytes)
            .unwrap();
        assert_eq!(bytes, source.as_bytes());
        assert!(
            invalid
                .unwrap_err()
                .contains("$vars input must be a JSON object")
        );
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
    fn property_errors_keep_line_context_without_echoing_contents() {
        let malformed = "sasl.password do-not-print";
        let error = parse_config(&format!("a=1\n{malformed}\n")).unwrap_err();
        assert!(error.contains("line 2"));
        assert!(!error.contains("do-not-print"));
        let error = resolve(&["jkq", "-b", "x", "-t", "t", "-X", malformed]).unwrap_err();
        assert!(error.contains("key=value"));
        assert!(!error.contains("do-not-print"));
    }

    #[test]
    fn sizes_and_durations_are_checked_without_dependencies() {
        assert_eq!(parse_size("2MiB").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert!(parse_size("1MB").is_err());
        assert!(parse_duration("0ms").is_err());
    }

    #[test]
    fn statistics_are_opt_in_and_an_interval_enables_the_final_report() {
        let default = resolve(&["jkq", "-b", "x", "-t", "t"]).unwrap();
        assert!(!default.stats);
        assert_eq!(default.stats_interval, None);

        let periodic = resolve(&["jkq", "-b", "x", "-t", "t", "--stats-interval", "5m"]).unwrap();
        assert!(periodic.stats);
        assert_eq!(periodic.stats_interval, Some(Duration::from_secs(300)));
    }

    #[test]
    fn help_describes_assignment_runtime_limits_and_output_modes() {
        let help = RawCli::command().render_long_help().to_string();
        assert!(help.contains("Partitions to consume; defaults to all topic partitions"));
        assert!(help.contains("Maximum admitted input records per partition"));
        assert!(help.contains("Validate local configuration and exit without connecting"));
        assert!(help.contains("Owned source-byte admission budget; supports KiB, MiB, and GiB"));
        assert!(help.contains("Number of Kafka consumers"));
        assert!(help.contains("--consumers <CONSUMERS>"));
        assert!(!help.contains("--jobs"));
        for flag in [
            "--drop-if",
            "--drop-unless",
            "--keep-if",
            "--tombstone-if",
            "--tombstone-unless",
        ] {
            assert!(help.contains(&format!("{flag} <EXPR>")));
        }
        for flag in [
            "--drop-if-file",
            "--drop-unless-file",
            "--keep-if-file",
            "--tombstone-if-file",
            "--tombstone-unless-file",
            "--project-file",
        ] {
            assert!(help.contains(&format!("{flag} <PATH>")));
        }
        assert!(help.contains("TOMBSTONES & METADATA PREDICATES:"));
        assert!(help.contains("combined metadata+payload predicate"));
        assert!(help.contains("$meta fields:"));
        assert!(help.contains("Drop source and predicate-generated tombstones before projection"));
        assert!(help.contains("Strict JSON object available as $vars"));
        assert!(help.contains("Read the strict JSON object available as $vars from a file"));
        assert!(help.contains("Build each non-tombstone payload before final -f formatting"));
        assert!(help.contains("Format output with kcat-compatible placeholders"));
        assert!(help.contains("[default: %s\\n]"));
        assert!(help.contains("Emit a binary-safe jkq JSON envelope instead of -f output"));
        assert!(help.contains("Payload representation for -J envelopes"));
        assert!(help.contains("--timestamp-format <TIMESTAMP_FORMAT>"));
        assert!(help.contains("[possible values: iso8601]"));
        assert!(help.contains("does not force JSON parsing"));
        assert!(help.contains("forces validation and compacts pass-through payloads"));
        assert!(help.contains("[default: string]"));
    }

    #[test]
    fn keep_if_expression_files_and_compile_locations_resolve_in_order() {
        let dir = std::env::temp_dir().join(format!("jkq-expr-files-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let drop_file = dir.join("drop.jsonata");
        let keep_file = dir.join("keep.jsonata");
        let proj_file = dir.join("proj.jsonata");
        fs::write(&drop_file, "disabled = true").unwrap();
        fs::write(&keep_file, "$meta.key = 'k1'").unwrap();
        fs::write(&proj_file, "{\"id\": id}").unwrap();

        let config = resolve(&[
            "jkq",
            "-b",
            "localhost",
            "-t",
            "events",
            "--keep-if-file",
            keep_file.to_str().unwrap(),
            "--drop-if-file",
            drop_file.to_str().unwrap(),
            "--keep-if",
            "status = 'ok'",
            "--project-file",
            proj_file.to_str().unwrap(),
        ])
        .unwrap();
        fs::remove_dir_all(&dir).unwrap();

        assert_eq!(config.transform.drops.len(), 3);
        assert!(!config.transform.drops[0].trigger);
        assert_eq!(
            config.transform.drops[0].origin,
            "keep-if-file predicate #1"
        );
        assert!(config.transform.drops[1].trigger);
        assert_eq!(
            config.transform.drops[1].origin,
            "drop-if-file predicate #1"
        );
        assert!(!config.transform.drops[2].trigger);
        assert_eq!(config.transform.drops[2].origin, "keep-if predicate #1");
        assert!(config.transform.projection.is_some());

        let err = resolve(&[
            "jkq",
            "-b",
            "localhost",
            "-t",
            "events",
            "--project",
            "id\n+ [",
        ])
        .unwrap_err();
        assert!(err.contains("at 2:"), "{err}");
    }

    #[test]
    fn timestamp_boundaries_accept_iso8601_and_relative_durations() {
        let iso = resolve(&[
            "jkq",
            "-b",
            "localhost",
            "-t",
            "events",
            "-o",
            "s@2026-01-02T03:04:05Z",
            "-o",
            "e@2026-01-02T04:04:05+01:00",
        ])
        .unwrap();
        assert_eq!(iso.start, StartPosition::TimestampMillis(1767323045000));
        assert_eq!(iso.end, Some(EndPosition::TimestampMillis(1767323045000)));

        let rel = resolve(&[
            "jkq",
            "-b",
            "localhost",
            "-t",
            "events",
            "-o",
            "s@-1h",
            "-o",
            "e@-5m",
        ])
        .unwrap();
        let (StartPosition::TimestampMillis(start), Some(EndPosition::TimestampMillis(end))) =
            (rel.start, rel.end)
        else {
            panic!("expected resolved timestamps");
        };
        assert!(end > start);
        let diff = end - start;
        assert!((54 * 60 * 1000..=56 * 60 * 1000).contains(&diff), "{diff}");

        for bad in ["s@-1", "s@bad", "s@1969-12-31T23:59:59Z", "e@-0s"] {
            assert!(
                resolve(&["jkq", "-b", "localhost", "-t", "events", "-o", bad]).is_err(),
                "{bad}"
            );
        }
    }
}
