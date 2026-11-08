DROP KEYSPACE IF EXISTS lviv_smart_grid_opt;

CREATE KEYSPACE IF NOT EXISTS lviv_smart_grid_opt
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE lviv_smart_grid_opt;

CREATE TABLE IF NOT EXISTS telemetry_simple (
    device_id TEXT,
    recorded_at TIMESTAMP,
    district_name TEXT,
    active_power DOUBLE,
    power_factor DOUBLE,
    frequency DOUBLE,
    voltage DOUBLE,
    tariff_zone TEXT,
    PRIMARY KEY (device_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

CREATE TABLE IF NOT EXISTS telemetry_hourly (
    district_name TEXT,
    bucket_hour TIMESTAMP,
    recorded_at TIMESTAMP,
    device_id TEXT,
    active_power DOUBLE,
    power_factor DOUBLE,
    frequency DOUBLE,
    voltage DOUBLE,
    tariff_zone TEXT,
    PRIMARY KEY ((district_name, bucket_hour), recorded_at, device_id)
) WITH CLUSTERING ORDER BY (recorded_at DESC, device_id ASC)
AND default_time_to_live = 7776000
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
}
AND compression = {'class': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS telemetry_hourly_zstd (
    district_name TEXT,
    bucket_hour TIMESTAMP,
    recorded_at TIMESTAMP,
    device_id TEXT,
    active_power DOUBLE,
    power_factor DOUBLE,
    frequency DOUBLE,
    voltage DOUBLE,
    tariff_zone TEXT,
    PRIMARY KEY ((district_name, bucket_hour), recorded_at, device_id)
) WITH CLUSTERING ORDER BY (recorded_at DESC, device_id ASC)
AND default_time_to_live = 7776000
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
}
AND compression = {'class': 'ZstdCompressor', 'chunk_length_in_kb': '64'};

CREATE TABLE IF NOT EXISTS telemetry_daily (
    district_name TEXT,
    bucket_date DATE,
    recorded_at TIMESTAMP,
    device_id TEXT,
    active_power DOUBLE,
    power_factor DOUBLE,
    frequency DOUBLE,
    voltage DOUBLE,
    tariff_zone TEXT,
    PRIMARY KEY ((district_name, bucket_date), recorded_at, device_id)
) WITH CLUSTERING ORDER BY (recorded_at DESC, device_id ASC)
AND default_time_to_live = 7776000
AND compression = {'class': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS telemetry_daily_aggregates (
    district_name TEXT,
    bucket_date DATE,
    tariff_zone TEXT,
    avg_power DOUBLE,
    max_power DOUBLE,
    min_power DOUBLE,
    total_kwh DOUBLE,
    avg_power_factor DOUBLE,
    avg_frequency DOUBLE,
    samples_count INT,
    PRIMARY KEY ((district_name, bucket_date), tariff_zone)
) WITH default_time_to_live = 31536000
AND compression = {'class': 'LZ4Compressor'};

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_frequency_alerts AS
    SELECT district_name, bucket_hour, frequency, recorded_at, device_id,
           active_power, power_factor, voltage, tariff_zone
    FROM telemetry_hourly
    WHERE district_name IS NOT NULL
      AND bucket_hour IS NOT NULL
      AND frequency IS NOT NULL
      AND recorded_at IS NOT NULL
      AND device_id IS NOT NULL
    PRIMARY KEY ((district_name, bucket_hour), frequency, recorded_at, device_id)
    WITH CLUSTERING ORDER BY (frequency DESC, recorded_at DESC, device_id ASC);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tariff_zone AS
    SELECT district_name, bucket_hour, tariff_zone, recorded_at, device_id,
           active_power, power_factor, frequency, voltage
    FROM telemetry_hourly
    WHERE district_name IS NOT NULL
      AND bucket_hour IS NOT NULL
      AND tariff_zone IS NOT NULL
      AND recorded_at IS NOT NULL
      AND device_id IS NOT NULL
    PRIMARY KEY ((district_name, bucket_hour, tariff_zone), recorded_at, device_id)
    WITH CLUSTERING ORDER BY (recorded_at DESC, device_id ASC);
