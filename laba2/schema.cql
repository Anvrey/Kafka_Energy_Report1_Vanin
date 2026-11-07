-- Перестворення простору ключів для мережі розумних лічильників Львова
DROP KEYSPACE IF EXISTS lviv_smart_grid;

CREATE KEYSPACE IF NOT EXISTS lviv_smart_grid
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE lviv_smart_grid;

-- 1. Оперативні показання лічильників із сортуванням за спаданням часу (останні записи першими)
CREATE TABLE IF NOT EXISTS meter_readings_desc (
    meter_id UUID,
    recorded_at TIMESTAMP,
    district_name TEXT,
    active_power DOUBLE,
    reactive_power DOUBLE,
    voltage DOUBLE,
    PRIMARY KEY (meter_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

-- Допоміжна таблиця з прямим сортуванням часу для порівняння швидкості ASC та DESC за варіантом 4
CREATE TABLE IF NOT EXISTS meter_readings_asc (
    meter_id UUID,
    recorded_at TIMESTAMP,
    district_name TEXT,
    active_power DOUBLE,
    reactive_power DOUBLE,
    voltage DOUBLE,
    PRIMARY KEY (meter_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at ASC);

-- 2. Довідник тарифних зон та часових періодів
CREATE TABLE IF NOT EXISTS tariff_periods (
    tariff_zone TEXT,
    period_hours TEXT,
    start_hour INT,
    end_hour INT,
    price_per_kwh DOUBLE,
    PRIMARY KEY (tariff_zone)
);

-- 3. Добові агреговані показники споживання по районах Львова
CREATE TABLE IF NOT EXISTS district_stats (
    district_name TEXT,
    record_date DATE,
    meters_count INT,
    total_active_kwh DOUBLE,
    avg_voltage DOUBLE,
    max_voltage DOUBLE,
    PRIMARY KEY (district_name, record_date)
) WITH CLUSTERING ORDER BY (record_date DESC);

-- 4. Історія аномальних показників напруги та потужності
CREATE TABLE IF NOT EXISTS anomaly_history (
    district_name TEXT,
    recorded_at TIMESTAMP,
    meter_id UUID,
    anomaly_type TEXT,
    active_power DOUBLE,
    voltage DOUBLE,
    PRIMARY KEY (district_name, recorded_at, meter_id)
) WITH CLUSTERING ORDER BY (recorded_at DESC, meter_id ASC);

-- Ручне наповнення довідника тарифів
INSERT INTO tariff_periods (tariff_zone, period_hours, start_hour, end_hour, price_per_kwh)
VALUES ('night', '23:00-07:00', 23, 7, 2.16);

INSERT INTO tariff_periods (tariff_zone, period_hours, start_hour, end_hour, price_per_kwh)
VALUES ('day', '07:00-17:00, 21:00-23:00', 7, 23, 4.32);

INSERT INTO tariff_periods (tariff_zone, period_hours, start_hour, end_hour, price_per_kwh)
VALUES ('peak', '17:00-21:00', 17, 21, 6.48);
