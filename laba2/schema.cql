-- Створення простору ключів для проекту
CREATE KEYSPACE IF NOT EXISTS lviv_smart_grid 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE lviv_smart_grid;

-- Таблиця показників лічильників
CREATE TABLE IF NOT EXISTS meter_readings_desc (
    meter_id UUID,
    recorded_at TIMESTAMP,
    active_power DOUBLE,
    reactive_power DOUBLE,
    voltage DOUBLE,
    PRIMARY KEY (meter_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

-- Таблиця показників лічильників
CREATE TABLE IF NOT EXISTS meter_readings_asc (
    meter_id UUID,
    recorded_at TIMESTAMP,
    active_power DOUBLE,
    reactive_power DOUBLE,
    voltage DOUBLE,
    PRIMARY KEY (meter_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at ASC);

-- Довідник тарифних зон
CREATE TABLE IF NOT EXISTS tariff_periods (
    tariff_zone TEXT,
    start_hour INT,
    end_hour INT,
    price_per_kwh DOUBLE,
    PRIMARY KEY (tariff_zone)
);

-- Агрегована статистика споживання по районах
CREATE TABLE IF NOT EXISTS district_stats (
    district_name TEXT,
    record_date DATE,
    total_power_kw DOUBLE,
    avg_voltage DOUBLE,
    PRIMARY KEY (district_name, record_date)
);

-- Журнал виявлених аномалій напруги
CREATE TABLE IF NOT EXISTS anomaly_history (
    meter_id UUID,
    recorded_at TIMESTAMP,
    anomaly_type TEXT,
    voltage DOUBLE,
    PRIMARY KEY (meter_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

-- Базове наповнення таблиці тарифів
INSERT INTO tariff_periods (tariff_zone, start_hour, end_hour, price_per_kwh) VALUES ('night', 23, 7, 1.05);
INSERT INTO tariff_periods (tariff_zone, start_hour, end_hour, price_per_kwh) VALUES ('day', 7, 23, 2.64);
