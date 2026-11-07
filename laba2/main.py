import random
import time
import uuid
from datetime import date, datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType


def ensure_schema(session):
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS lviv_smart_grid "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("lviv_smart_grid")

    # Якщо лишилися старі таблиці без поля district_name, перестворюємо їх
    cols = session.execute(
        "SELECT table_name, column_name FROM system_schema.columns "
        "WHERE keyspace_name = 'lviv_smart_grid'"
    )
    table_cols = {}
    for row in cols:
        table_cols.setdefault(row.table_name, set()).add(row.column_name)

    expected = {
        "meter_readings_desc": "district_name",
        "meter_readings_asc": "district_name",
        "tariff_periods": "period_hours",
        "district_stats": "meters_count",
        "anomaly_history": "district_name",
    }
    for tbl, req_col in expected.items():
        if tbl in table_cols and req_col not in table_cols[tbl]:
            session.execute(f"DROP TABLE IF EXISTS {tbl}")

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS meter_readings_desc (
            meter_id UUID,
            recorded_at TIMESTAMP,
            district_name TEXT,
            active_power DOUBLE,
            reactive_power DOUBLE,
            voltage DOUBLE,
            PRIMARY KEY (meter_id, recorded_at)
        ) WITH CLUSTERING ORDER BY (recorded_at DESC)
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS meter_readings_asc (
            meter_id UUID,
            recorded_at TIMESTAMP,
            district_name TEXT,
            active_power DOUBLE,
            reactive_power DOUBLE,
            voltage DOUBLE,
            PRIMARY KEY (meter_id, recorded_at)
        ) WITH CLUSTERING ORDER BY (recorded_at ASC)
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS tariff_periods (
            tariff_zone TEXT,
            period_hours TEXT,
            start_hour INT,
            end_hour INT,
            price_per_kwh DOUBLE,
            PRIMARY KEY (tariff_zone)
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS district_stats (
            district_name TEXT,
            record_date DATE,
            meters_count INT,
            total_active_kwh DOUBLE,
            avg_voltage DOUBLE,
            max_voltage DOUBLE,
            PRIMARY KEY (district_name, record_date)
        ) WITH CLUSTERING ORDER BY (record_date DESC)
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS anomaly_history (
            district_name TEXT,
            recorded_at TIMESTAMP,
            meter_id UUID,
            anomaly_type TEXT,
            active_power DOUBLE,
            voltage DOUBLE,
            PRIMARY KEY (district_name, recorded_at, meter_id)
        ) WITH CLUSTERING ORDER BY (recorded_at DESC, meter_id ASC)
        """
    )


def main():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    ensure_schema(session)
    print("Підключено до Cassandra, keyspace: lviv_smart_grid")

    # Очищуємо таблиці перед новим прогоном, щоб кількість записів була точною
    for tbl in ["meter_readings_desc", "meter_readings_asc", "district_stats", "anomaly_history"]:
        session.execute(f"TRUNCATE {tbl}")

    # Перевіряємо наявність тарифів у довіднику
    tariffs = [
        ("night", "23:00-07:00", 23, 7, 2.16),
        ("day", "07:00-17:00, 21:00-23:00", 7, 23, 4.32),
        ("peak", "17:00-21:00", 17, 21, 6.48),
    ]
    prep_tariff = session.prepare(
        "INSERT INTO tariff_periods (tariff_zone, period_hours, start_hour, end_hour, price_per_kwh) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    for t_row in tariffs:
        session.execute(prep_tariff, t_row)

    districts = [
        "Франківський",
        "Сихівський",
        "Личаківський",
        "Залізничний",
        "Шевченківський",
        "Галицький",
    ]

    rng = random.Random(4)
    meters = []
    for idx in range(30):
        m_id = uuid.UUID(int=rng.getrandbits(128), version=4)
        dist = districts[idx % len(districts)]
        meters.append({"id": m_id, "district": dist})

    prep_desc = session.prepare(
        "INSERT INTO meter_readings_desc "
        "(meter_id, recorded_at, district_name, active_power, reactive_power, voltage) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    prep_asc = session.prepare(
        "INSERT INTO meter_readings_asc "
        "(meter_id, recorded_at, district_name, active_power, reactive_power, voltage) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    prep_anom = session.prepare(
        "INSERT INTO anomaly_history "
        "(district_name, recorded_at, meter_id, anomaly_type, active_power, voltage) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    prep_dist = session.prepare(
        "INSERT INTO district_stats "
        "(district_name, record_date, meters_count, total_active_kwh, avg_voltage, max_voltage) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )

    base_time = datetime(2026, 10, 5, 0, 0, 0)
    report_date = date(2026, 10, 5)
    dist_voltages = {d: [] for d in districts}
    dist_kwh = {d: 0.0 for d in districts}

    t_insert_start = time.perf_counter()

    # Пакетний запис (BatchStatement) окремо для кожного лічильника за добу (96 інтервалів по 15 хв)
    for m in meters:
        d = m["district"]
        batch_desc = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch_asc = BatchStatement(batch_type=BatchType.UNLOGGED)

        for slot in range(96):
            ts = base_time + timedelta(minutes=15 * slot)
            hour = ts.hour
            if 17 <= hour <= 22:
                act_p = round(rng.uniform(2.2, 6.2), 2)
            elif 7 <= hour < 17:
                act_p = round(rng.uniform(1.0, 4.2), 2)
            else:
                act_p = round(rng.uniform(0.4, 2.0), 2)
            react_p = round(act_p * rng.uniform(0.18, 0.32), 2)

            roll = rng.random()
            if roll < 0.03:
                spike = 2.4 if d == "Франківський" else 0.0
                volt = round(rng.uniform(246.0, 252.6) + spike, 1)
            elif roll < 0.05:
                volt = round(rng.uniform(198.0, 206.5), 1)
            else:
                volt = round(rng.uniform(218.0, 239.0), 1)

            dist_voltages[d].append(volt)
            dist_kwh[d] += round(act_p * 0.25, 4)

            params = (m["id"], ts, d, act_p, react_p, volt)
            batch_desc.add(prep_desc, params)
            batch_asc.add(prep_asc, params)

            if volt > 245.0 or volt < 207.0 or act_p > 5.8:
                if volt > 245.0:
                    a_type = "Перевищення напруги"
                elif volt < 207.0:
                    a_type = "Просідання напруги"
                else:
                    a_type = "Пікове навантаження"
                session.execute(prep_anom, (d, ts, m["id"], a_type, act_p, volt))

        session.execute(batch_desc)
        session.execute(batch_asc)

    for d in districts:
        vs = dist_voltages[d]
        session.execute(
            prep_dist,
            (d, report_date, 5, round(dist_kwh[d], 2), round(sum(vs) / len(vs), 2), max(vs)),
        )

    insert_ms = round((time.perf_counter() - t_insert_start) * 1000, 1)
    print(f"Пакетну вставку 2880 показань завершено за {insert_ms} мс")

    # 1. Підрахунок кількості записів у всіх таблицях із бази даних
    print("\nКількість записів у таблицях:")
    for tbl in ["meter_readings_desc", "meter_readings_asc", "tariff_periods", "district_stats", "anomaly_history"]:
        cnt = session.execute(f"SELECT COUNT(*) FROM {tbl}").one()[0]
        print(f"  {tbl}: {cnt}")

    # 2. Приклади останніх показань лічильника
    sample_meter = meters[0]["id"]
    print(f"\nОстанні 3 показання лічильника {str(sample_meter)[:8]} у районі {meters[0]['district']}:")
    rows_latest = session.execute(
        "SELECT recorded_at, active_power, reactive_power, voltage "
        "FROM meter_readings_desc WHERE meter_id = %s LIMIT 3",
        (sample_meter,),
    )
    for r in rows_latest:
        print(
            f"  {r.recorded_at.strftime('%Y-%m-%d %H:%M')}, "
            f"активна {r.active_power:.2f} кВт, реактивна {r.reactive_power:.2f} квар, напруга {r.voltage:.1f} В"
        )

    # 3. Порівняння швидкості читання останніх показань: DESC проти ASC (Варіант 4)
    q_desc = session.prepare(
        "SELECT recorded_at, active_power, voltage FROM meter_readings_desc WHERE meter_id = ? LIMIT 5"
    )
    q_asc = session.prepare(
        "SELECT recorded_at, active_power, voltage FROM meter_readings_asc "
        "WHERE meter_id = ? ORDER BY recorded_at DESC LIMIT 5"
    )
    for m in meters:
        list(session.execute(q_desc, (m["id"],)))
        list(session.execute(q_asc, (m["id"],)))

    passes = 10
    total_queries = passes * len(meters)

    t0 = time.perf_counter()
    for _ in range(passes):
        for m in meters:
            list(session.execute(q_desc, (m["id"],)))
    desc_total_ms = (time.perf_counter() - t0) * 1000

    t1 = time.perf_counter()
    for _ in range(passes):
        for m in meters:
            list(session.execute(q_asc, (m["id"],)))
    asc_total_ms = (time.perf_counter() - t1) * 1000

    print(f"\nШвидкість читання останніх 5 показань на {total_queries} запитах:")
    print(
        f"  Таблиця DESC: всього {desc_total_ms:.2f} мс, "
        f"в середньому {desc_total_ms / total_queries:.3f} мс на запит"
    )
    print(
        f"  Таблиця ASC:  всього {asc_total_ms:.2f} мс, "
        f"в середньому {asc_total_ms / total_queries:.3f} мс на запит"
    )

    # 4. Аналітика Підваріанту А за даними, вичитаними з Cassandra
    print("\nДобова статистика по районах Львова з таблиці district_stats:")
    dist_rows = list(session.execute("SELECT * FROM district_stats"))
    dist_rows.sort(key=lambda x: x.max_voltage, reverse=True)
    for r in dist_rows:
        print(
            f"  {r.district_name:15s} споживання {r.total_active_kwh:6.2f} кВт*год, "
            f"середня напруга {r.avg_voltage:6.2f} В, макс. {r.max_voltage:5.1f} В"
        )

    all_rows = list(
        session.execute("SELECT district_name, meter_id, active_power, voltage FROM meter_readings_desc")
    )
    avg_v = sum(r.voltage for r in all_rows) / len(all_rows)
    max_v_row = max(all_rows, key=lambda r: r.voltage)
    avg_p = sum(r.active_power for r in all_rows) / len(all_rows)
    max_p_row = max(all_rows, key=lambda r: r.active_power)

    print("\nПідсумок Підваріанту А:")
    print(f"  Середня потужність = {avg_p:.2f} кВт, Максимальна = {max_p_row.active_power:.2f} кВт")
    print(f"  Середня напруга = {avg_v:.2f} В, Максимальна = {max_v_row.voltage:.1f} В")
    print(
        f"  Найвища напруга зафіксована у районі: {max_v_row.district_name}, "
        f"{max_v_row.voltage:.1f} В на лічильнику {str(max_v_row.meter_id)[:8]}"
    )

    cluster.shutdown()


if __name__ == "__main__":
    main()
