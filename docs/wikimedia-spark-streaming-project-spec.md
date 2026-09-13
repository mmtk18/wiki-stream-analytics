# Wikimedia Streaming Analytics Project Spec

## Overview

This project builds a real-time data engineering pipeline using a live public event source from Wikimedia EventStreams, Kafka for transport, and Spark Structured Streaming for stream processing.[cite:91][cite:54][cite:118]

The goal is to produce a weekend-sized project that still looks credible on a resume by demonstrating streaming ingestion, event-time processing, watermarking, checkpointing, and layered data outputs.[cite:122][cite:133][cite:124]

## Project goal

Build an end-to-end pipeline that consumes Wikimedia Recent Changes events, publishes them to Kafka, processes them with Spark Structured Streaming, and writes bronze, silver, and gold outputs for analytics.[cite:46][cite:89][cite:118]

The project should emphasize practical streaming concepts rather than infrastructure sprawl. The key learning outcomes are Kafka ingestion, Spark Structured Streaming, event-time windows, watermarking, schema normalization, and fault-tolerant processing with checkpoints.[cite:110][cite:115][cite:122]

## Why this project

This project uses a genuinely live public stream rather than synthetic data. Wikimedia EventStreams exposes continuous structured event streams over HTTP using Server-Sent Events, and the Recent Changes stream is specifically intended for real-time consumption.[cite:54][cite:47][cite:46]

It is also realistic enough for resume use because it mirrors a common production pattern: external event source, message queue, stream processing engine, curated outputs, and downstream analytics.[cite:91][cite:118][cite:122]

## Architecture

### Data flow

1. A Python producer subscribes to the Wikimedia Recent Changes EventStreams endpoint and reads JSON events from the SSE stream.[cite:46][cite:91]
2. The producer filters and republishes events into a Kafka topic such as `wikimedia.recentchange.raw`.[cite:89]
3. Spark Structured Streaming consumes the Kafka topic, parses the JSON payload, and converts it into a typed schema.[cite:118][cite:114]
4. The parsed stream is written to a silver dataset after basic validation and normalization.[cite:122]
5. Windowed aggregations are computed in Spark and written to gold datasets using event-time windows and watermarking.[cite:115][cite:133]
6. Output files are stored in Parquet format with checkpoint directories for recovery.[cite:121][cite:124]

### Logical layers

| Layer | Purpose | Example output |
|---|---|---|
| Bronze | Raw ingested events from Kafka | Raw JSON/Parquet payloads [cite:118] |
| Silver | Cleaned, typed, validated event records | `domain`, `title`, `user`, `timestamp`, `bot`, `delta` |
| Gold | Windowed and business-friendly aggregates | edits per minute, bot ratio, top domains [cite:115][cite:133] |

## Suggested stack

| Component | Tool |
|---|---|
| Source stream | Wikimedia EventStreams [cite:91] |
| Producer | Python |
| Message broker | Apache Kafka |
| Stream processing | Spark Structured Streaming [cite:110][cite:118] |
| Storage | Parquet [cite:121] |
| Local orchestration | Docker Compose |
| Optional visualization | Superset or notebook charts [cite:132] |

## Event schema

The silver-layer event schema should stay small and useful:

| Field | Type | Notes |
|---|---|---|
| event_id | string | Unique event identifier if available |
| event_time | timestamp | Parsed event timestamp used for windowing |
| domain | string | Wiki domain, such as `en.wikipedia.org` |
| title | string | Page title |
| user_name | string | Editor username or IP |
| bot_flag | boolean | Whether the edit is marked as bot |
| namespace | int | Page namespace |
| comment | string | Edit summary/comment |
| old_length | int | Previous page length |
| new_length | int | New page length |
| delta | int | `new_length - old_length` |
| server_name | string | Origin server/domain |
| raw_json | string | Original payload for audit/debug |

## Streaming jobs

### Job 1: Bronze ingestion

Spark reads Kafka messages and persists the raw payload plus Kafka metadata such as topic, partition, offset, and ingestion time.[cite:118]

**Purpose**:
- preserve original data,
- support replay/debugging,
- separate ingestion from business transformation.

### Job 2: Silver normalization

Spark parses the JSON payload into a typed schema, filters malformed records, derives `delta`, and standardizes nullable fields.[cite:114][cite:122]

Suggested lightweight quality checks:
- discard rows with missing `event_time`,
- discard rows with missing `domain`,
- coerce invalid numeric lengths to null,
- track malformed JSON count separately.

### Job 3: Gold aggregations

Use event-time windows with watermarking to compute analytics over recent data. Spark supports time-window aggregations and watermark-based handling of late events in Structured Streaming.[cite:110][cite:115][cite:133]

Recommended outputs:
- edits per 1-minute tumbling window,
- bot vs human counts per 5-minute window,
- top domains by edit count per 5-minute window,
- average and max absolute edit delta per 5-minute window.

## Streaming design choices

### Watermarking

Add a watermark on `event_time`, for example 10 minutes, so late records within tolerance are still included while Spark can eventually clean up state for older windows.[cite:122][cite:133]

This is one of the highest-value additions for interview discussions because it demonstrates awareness of event-time semantics rather than only processing-time counting.[cite:133]

### Checkpointing

Each streaming sink should use its own checkpoint directory. Structured Streaming uses checkpoints for state recovery and reliable progress tracking across restarts.[cite:122][cite:124]

A simple demo should include stopping and restarting one query to confirm it resumes cleanly without reprocessing from scratch.[cite:124]

### Output mode

Use append mode where possible for file sinks and finalized windows. This keeps outputs simple and easier to explain for a weekend project.[cite:122]

## Folder structure

```text
wikimedia-streaming-analytics/
├── docker-compose.yml
├── README.md
├── producer/
│   ├── requirements.txt
│   └── wikimedia_to_kafka.py
├── spark/
│   ├── schemas.py
│   ├── bronze_job.py
│   ├── silver_job.py
│   └── gold_job.py
├── config/
│   └── app_config.yaml
├── output/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── checkpoints/
└── docs/
    └── architecture.md
```

## Weekend implementation plan

### Saturday

1. Start Kafka locally with Docker Compose.
2. Build and test the Python producer for Wikimedia EventStreams to Kafka.[cite:89][cite:46]
3. Verify messages in Kafka using a console consumer.
4. Create Spark job scaffolding and implement bronze ingestion from Kafka.[cite:118]
5. Persist bronze output and confirm files are being written.

### Sunday

1. Implement the silver parsing and normalization job.
2. Add watermarking and gold aggregations with 1-minute and 5-minute windows.[cite:115][cite:133]
3. Add checkpoint directories and test restart recovery.[cite:124]
4. Produce one simple dashboard or a few screenshots from queried Parquet outputs.[cite:132]
5. Write the README with architecture, schema, tradeoffs, and future improvements.

## Resume-oriented features

The following features add strong signal without making the scope too large:

- Kafka-based real-time ingestion from a public event stream.[cite:91][cite:89]
- Spark Structured Streaming with Kafka source integration.[cite:118][cite:110]
- Event-time windowing and watermarking.[cite:115][cite:133]
- Checkpoint-backed recovery and fault tolerance.[cite:124][cite:122]
- Bronze/silver/gold layered outputs.
- Parquet-based curated datasets for downstream analytics.[cite:121]

## Resume bullet examples

- Built a real-time data pipeline ingesting Wikimedia Recent Changes events via SSE into Kafka and processing them with Spark Structured Streaming.[cite:91][cite:89][cite:118]
- Implemented event-time watermarking, windowed aggregations, and checkpoint-backed recovery for edit velocity and bot-activity analytics.[cite:122][cite:133][cite:124]
- Designed bronze, silver, and gold data layers with curated Parquet outputs for downstream analytical consumption.[cite:121]

## Nice-to-have extensions

If the core pipeline works early, add one of these small upgrades:

- a simple Superset dashboard over gold outputs,[cite:132]
- partitioning outputs by date/domain,
- a rejected-record dataset for malformed events,
- top edited pages leaderboard,
- domain-level anomaly detection using rolling edit spikes.

## What to avoid this weekend

To keep the project finishable, avoid adding too many new systems at once. The following are better left for a later phase:

- Flink in addition to Spark,
- Iceberg catalogs,
- Airflow orchestration,
- Kubernetes deployment,
- full data quality frameworks,
- machine learning features.

## Definition of done

The project is complete enough for resume use if all of the following are working:

- live Wikimedia events are entering Kafka,[cite:46][cite:89]
- Spark consumes from Kafka and writes parsed data,[cite:118]
- at least two gold aggregations run with windowing,[cite:115]
- watermarking and checkpoints are configured,[cite:122][cite:124]
- Parquet outputs can be queried or visualized,
- the repository includes a clear README and architecture explanation.
