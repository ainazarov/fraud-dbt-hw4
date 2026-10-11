-- ClickHouse 25.3 LTS. Create every downstream view before enabling Kafka ingestion.
-- CREATE IF NOT EXISTS makes repeated initialization safe; it is not a migration.
CREATE DATABASE IF NOT EXISTS fraud;

-- Optimized detail storage retains every source field for further analysis.
CREATE TABLE IF NOT EXISTS fraud.transactions
(
    dataset LowCardinality(String) CODEC(ZSTD(1)),
    transaction_id String CODEC(ZSTD(1)),
    row_number UInt64 CODEC(Delta(8), ZSTD(1)),
    transaction_time DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
    merch LowCardinality(String) CODEC(ZSTD(1)),
    cat_id LowCardinality(String) CODEC(ZSTD(1)),
    amount Decimal(12, 2) CODEC(ZSTD(1)),
    name_1 LowCardinality(String) CODEC(ZSTD(1)),
    name_2 LowCardinality(String) CODEC(ZSTD(1)),
    gender LowCardinality(String) CODEC(ZSTD(1)),
    street String CODEC(ZSTD(1)),
    one_city LowCardinality(String) CODEC(ZSTD(1)),
    us_state LowCardinality(String) CODEC(ZSTD(1)),
    post_code String CODEC(ZSTD(1)),
    lat Float64 CODEC(ZSTD(1)),
    lon Float64 CODEC(ZSTD(1)),
    population_city UInt32 CODEC(ZSTD(1)),
    jobs LowCardinality(String) CODEC(ZSTD(1)),
    merchant_lat Float64 CODEC(ZSTD(1)),
    merchant_lon Float64 CODEC(ZSTD(1)),
    target Nullable(UInt8) CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (dataset, us_state, transaction_time, transaction_id);

-- Control: identical logical values and full schema, ordinary strings/LZ4,
-- no sort key. This deliberately simple version is retained for comparison.
CREATE TABLE IF NOT EXISTS fraud.transactions_baseline
(
    dataset String CODEC(LZ4),
    transaction_id String CODEC(LZ4),
    row_number UInt64 CODEC(LZ4),
    transaction_time DateTime('UTC') CODEC(LZ4),
    merch String CODEC(LZ4),
    cat_id String CODEC(LZ4),
    amount Decimal(12, 2) CODEC(LZ4),
    name_1 String CODEC(LZ4),
    name_2 String CODEC(LZ4),
    gender String CODEC(LZ4),
    street String CODEC(LZ4),
    one_city String CODEC(LZ4),
    us_state String CODEC(LZ4),
    post_code String CODEC(LZ4),
    lat Float64 CODEC(LZ4),
    lon Float64 CODEC(LZ4),
    population_city UInt32 CODEC(LZ4),
    jobs String CODEC(LZ4),
    merchant_lat Float64 CODEC(LZ4),
    merchant_lon Float64 CODEC(LZ4),
    target Nullable(UInt8) CODEC(LZ4)
)
ENGINE = MergeTree
ORDER BY tuple();

-- One mergeable winner per dataset/state/block. Merges are asynchronous;
-- queries MUST use argMaxMerge with GROUP BY (see 004_max_by_state.sql).
-- Lexicographically largest category, then ID, resolves equal-amount ties.
CREATE TABLE IF NOT EXISTS fraud.state_max
(
    dataset LowCardinality(String) CODEC(ZSTD(1)),
    us_state LowCardinality(String) CODEC(ZSTD(1)),
    winner AggregateFunction(
        argMax,
        Tuple(String, Decimal(12, 2), String),
        Tuple(Decimal(12, 2), String, String)
    ) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree
ORDER BY (dataset, us_state);

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.transactions_to_baseline
TO fraud.transactions_baseline
AS SELECT
    CAST(dataset AS String) AS dataset,
    transaction_id,
    row_number,
    transaction_time,
    CAST(merch AS String) AS merch,
    CAST(cat_id AS String) AS cat_id,
    amount,
    CAST(name_1 AS String) AS name_1,
    CAST(name_2 AS String) AS name_2,
    CAST(gender AS String) AS gender,
    street,
    CAST(one_city AS String) AS one_city,
    CAST(us_state AS String) AS us_state,
    post_code,
    lat,
    lon,
    population_city,
    CAST(jobs AS String) AS jobs,
    merchant_lat,
    merchant_lon,
    target
FROM fraud.transactions;

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.transactions_to_state_max
TO fraud.state_max
AS SELECT
    dataset,
    us_state,
    argMaxState(
        tuple(CAST(cat_id AS String), amount, transaction_id),
        tuple(amount, CAST(cat_id AS String), transaction_id)
    ) AS winner
FROM fraud.transactions
GROUP BY dataset, us_state;

-- Kafka is a stream, not durable query storage. Read the MergeTree tables.
-- No broken messages are skipped: invalid input causes a visible failure.
CREATE TABLE IF NOT EXISTS fraud.transactions_kafka
(
    dataset String,
    transaction_id String,
    row_number UInt64,
    transaction_time DateTime('UTC'),
    merch String,
    cat_id String,
    amount Decimal(12, 2),
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target Nullable(UInt8)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'fraud-analytics-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 0;

-- This is intentionally LAST: its creation starts background consumption.
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.ingest_transactions
TO fraud.transactions
AS SELECT
    dataset,
    transaction_id,
    row_number,
    transaction_time,
    merch,
    cat_id,
    amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    lat,
    lon,
    population_city,
    jobs,
    merchant_lat,
    merchant_lon,
    target
FROM fraud.transactions_kafka;
