DROP KEYSPACE IF EXISTS lviv_streaming;

CREATE KEYSPACE IF NOT EXISTS lviv_streaming
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE lviv_streaming;

CREATE TABLE IF NOT EXISTS consumption_event_log (
    district_name TEXT,
    day_bucket DATE,
    event_time TIMESTAMP,
    event_id UUID,
    event_type TEXT,
    meter_id TEXT,
    payload TEXT,
    PRIMARY KEY ((district_name, day_bucket), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time ASC, event_id ASC)
AND compression = {'class': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS consumer_profile_state (
    meter_id TEXT,
    district_name TEXT,
    consumer_class TEXT,
    readings_count BIGINT,
    mean_power DOUBLE,
    std_power DOUBLE,
    max_power DOUBLE,
    voltage_events INT,
    theft_events INT,
    last_seen TIMESTAMP,
    PRIMARY KEY (meter_id)
);

CREATE TABLE IF NOT EXISTS window_aggregates (
    district_name TEXT,
    window_start TIMESTAMP,
    readings_count INT,
    total_consumption_kwh DOUBLE,
    avg_voltage DOUBLE,
    min_voltage DOUBLE,
    max_voltage DOUBLE,
    avg_frequency DOUBLE,
    PRIMARY KEY (district_name, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
AND default_time_to_live = 2592000;

