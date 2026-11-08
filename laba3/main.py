import random
import statistics
import subprocess
import time
from datetime import date, datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent
from cassandra.query import BatchStatement, BatchType


class PerformanceTester:

    def __init__(self, session):
        self.session = session

    def _percentile(self, data, percentile):
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * percentile)
        if idx >= len(sorted_data):
            idx = len(sorted_data) - 1
        return sorted_data[idx]

    def measure_query(self, query_str, params_list, iterations=50):
        stmt = self.session.prepare(query_str)
        # Попередній розігрів кешу перед заміром
        for p in params_list:
            list(self.session.execute(stmt, p))

        latencies = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            for p in params_list:
                rows = list(self.session.execute(stmt, p))
                _ = len(rows)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            latencies.append(dt_ms)

        return {
            "avg": round(statistics.mean(latencies), 2),
            "p50": round(statistics.median(latencies), 2),
            "p95": round(self._percentile(latencies, 0.95), 2),
            "p99": round(self._percentile(latencies, 0.99), 2),
        }


def get_tariff_zone(hour):
    if 17 <= hour < 21:
        return "peak"
    elif 7 <= hour < 17 or 21 <= hour < 23:
        return "day"
    return "night"


def parse_tablestats(keyspace, table):
    cmd = ["nodetool", "tablestats", f"{keyspace}.{table}"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    info = {
        "space_live": 0,
        "partitions": 0,
        "mean_bytes": 0,
        "max_bytes": 0,
        "compression_ratio": 0.0,
    }
    for line in res.stdout.splitlines():
        s = line.strip()
        if s.startswith("Space used (live):"):
            val = s.split(":", 1)[1].strip().split()[0].replace(",", ".")
            try:
                info["space_live"] = int(float(val))
            except ValueError:
                pass
        elif s.startswith("Number of partitions (estimate):"):
            try:
                info["partitions"] = int(s.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif s.startswith("Compacted partition mean bytes:"):
            try:
                info["mean_bytes"] = int(s.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif s.startswith("Compacted partition maximum bytes:"):
            try:
                info["max_bytes"] = int(s.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif s.startswith("SSTable Compression Ratio:"):
            raw = s.split(":", 1)[1].strip().replace(",", ".")
            try:
                r = float(raw)
                if r > 0:
                    info["compression_ratio"] = round(r, 3)
            except ValueError:
                pass
    return info


def main():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("lviv_smart_grid_opt")
    print("Підключено до Cassandra, keyspace: lviv_smart_grid_opt")

    for tbl in [
        "telemetry_simple",
        "telemetry_hourly",
        "telemetry_hourly_zstd",
        "telemetry_daily",
        "telemetry_daily_aggregates",
    ]:
        session.execute(f"TRUNCATE {tbl}")

    session.execute(
        "ALTER TABLE telemetry_hourly_zstd WITH compression = "
        "{'class': 'ZstdCompressor', 'chunk_length_in_kb': '64'}"
    )

    districts = [
        "Франківський",
        "Сихівський",
        "Личаківський",
        "Залізничний",
        "Шевченківський",
        "Галицький",
    ]
    devices = [f"LVIV_METER_{i:03d}" for i in range(1, 13)]

    prep_simple = session.prepare(
        "INSERT INTO telemetry_simple "
        "(device_id, recorded_at, district_name, active_power, power_factor, frequency, voltage, tariff_zone) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    prep_hourly = session.prepare(
        "INSERT INTO telemetry_hourly "
        "(district_name, bucket_hour, recorded_at, device_id, active_power, power_factor, frequency, voltage, tariff_zone) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    prep_hourly_zstd = session.prepare(
        "INSERT INTO telemetry_hourly_zstd "
        "(district_name, bucket_hour, recorded_at, device_id, active_power, power_factor, frequency, voltage, tariff_zone) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    prep_daily = session.prepare(
        "INSERT INTO telemetry_daily "
        "(district_name, bucket_date, recorded_at, device_id, active_power, power_factor, frequency, voltage, tariff_zone) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    prep_agg = session.prepare(
        "INSERT INTO telemetry_daily_aggregates "
        "(district_name, bucket_date, tariff_zone, avg_power, max_power, min_power, "
        "total_kwh, avg_power_factor, avg_frequency, samples_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    rng = random.Random(42)
    start_time = datetime(2026, 9, 6, 0, 0, 0)
    days_count = 30
    slots_per_hour = 120  # кожні 30 секунд
    total_generated = days_count * 24 * slots_per_hour * len(devices)
    print(
        f"Модель генерації: {days_count} діб, інтервал 30 с, 12 опорних лічильників, "
        f"повний потік {total_generated} записів"
    )

    t0 = time.perf_counter()
    simple_exec_list = []
    for day_idx in range(days_count):
        day_base = start_time + timedelta(days=day_idx)
        for hour in range(24):
            hour_base = day_base + timedelta(hours=hour)
            t_zone = get_tariff_zone(hour)
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for s in range(slots_per_hour):
                ts = hour_base + timedelta(seconds=30 * s)
                if t_zone == "peak":
                    pwr = round(rng.uniform(2.6, 5.2), 2)
                    pf = round(rng.uniform(0.74, 0.88), 3)
                elif t_zone == "day":
                    pwr = round(rng.uniform(1.1, 3.4), 2)
                    pf = round(rng.uniform(0.86, 0.98), 3)
                else:
                    pwr = round(rng.uniform(0.25, 0.90), 2)
                    pf = round(rng.uniform(0.80, 0.90), 3)
                freq = round(50.0 + rng.uniform(-0.18, 0.18), 3)
                if s % 25 == 0:
                    freq = round(rng.choice([49.74, 50.24]), 3)
                volt = round(228.0 + rng.uniform(-8.0, 9.0), 1)
                batch.add(
                    prep_simple,
                    ("LVIV_METER_001", ts, "Франківський", pwr, pf, freq, volt, t_zone),
                )
            simple_exec_list.append((batch, ()))
    execute_concurrent(session, simple_exec_list, concurrency=24)
    simple_batches = len(simple_exec_list)
    simple_sec = time.perf_counter() - t0
    simple_rows = days_count * 24 * slots_per_hour
    simple_wps = int(simple_rows / simple_sec)
    simple_batch_ms = round((simple_sec * 1000.0) / simple_batches, 2)

    t0 = time.perf_counter()
    hourly_rows = 0
    lz4_batches_list = []
    zstd_batches_list = []

    for day_idx in range(days_count):
        day_base = start_time + timedelta(days=day_idx)
        for hour in range(24):
            bucket_hr = day_base + timedelta(hours=hour)
            t_zone = get_tariff_zone(hour)
            for d_idx, dist in enumerate(districts):
                dev_id = devices[d_idx]
                batch_lz4 = BatchStatement(batch_type=BatchType.UNLOGGED)
                batch_zstd = BatchStatement(batch_type=BatchType.UNLOGGED)
                # По 30 вимірювань на годину в кожному районі
                for s in range(30):
                    ts = bucket_hr + timedelta(seconds=120 * s)
                    if t_zone == "peak":
                        pwr = round(rng.uniform(2.5, 5.1), 2)
                        pf = round(rng.uniform(0.75, 0.89), 3)
                    elif t_zone == "day":
                        pwr = round(rng.uniform(1.0, 3.3), 2)
                        pf = round(rng.uniform(0.85, 0.98), 3)
                    else:
                        pwr = round(rng.uniform(0.20, 0.85), 2)
                        pf = round(rng.uniform(0.80, 0.91), 3)
                    freq = round(50.0 + rng.uniform(-0.16, 0.16), 3)
                    if s % 10 == 0:
                        freq = round(rng.choice([49.75, 50.23]), 3)
                    volt = round(227.5 + rng.uniform(-7.5, 8.5), 1)
                    row_vals = (dist, bucket_hr, ts, dev_id, pwr, pf, freq, volt, t_zone)
                    batch_lz4.add(prep_hourly, row_vals)
                    batch_zstd.add(prep_hourly_zstd, row_vals)
                    hourly_rows += 1
                lz4_batches_list.append((batch_lz4, ()))
                zstd_batches_list.append(batch_zstd)

    execute_concurrent(session, lz4_batches_list, concurrency=24)
    hourly_batches = len(lz4_batches_list)
    hourly_sec = time.perf_counter() - t0
    hourly_wps = int(hourly_rows / hourly_sec)
    hourly_batch_ms = round((hourly_sec * 1000.0) / hourly_batches, 2)

    half_zstd = len(zstd_batches_list) // 2
    execute_concurrent(session, [(b, ()) for b in zstd_batches_list[:half_zstd]], concurrency=32)
    subprocess.run(["nodetool", "flush", "lviv_smart_grid_opt", "telemetry_hourly_zstd"], check=False)
    execute_concurrent(session, [(b, ()) for b in zstd_batches_list[half_zstd:]], concurrency=32)

    t0 = time.perf_counter()
    daily_batches = 0
    daily_rows = 0
    agg_rows = 0
    daily_exec_list = []
    agg_exec_list = []

    for day_idx in range(days_count):
        day_base = start_time + timedelta(days=day_idx)
        b_date = day_base.date()
        for d_idx, dist in enumerate(districts):
            dev_id = devices[d_idx]
            zone_stats = {
                "night": {"p": [], "pf": [], "f": []},
                "day": {"p": [], "pf": [], "f": []},
                "peak": {"p": [], "pf": [], "f": []},
            }
            for hour in range(24):
                hour_base = day_base + timedelta(hours=hour)
                t_zone = get_tariff_zone(hour)
                batch_d = BatchStatement(batch_type=BatchType.UNLOGGED)
                for s in range(30):
                    ts = hour_base + timedelta(seconds=120 * s)
                    if t_zone == "peak":
                        pwr = round(rng.uniform(2.5, 5.1), 2)
                        pf = round(rng.uniform(0.75, 0.89), 3)
                    elif t_zone == "day":
                        pwr = round(rng.uniform(1.0, 3.3), 2)
                        pf = round(rng.uniform(0.85, 0.98), 3)
                    else:
                        pwr = round(rng.uniform(0.20, 0.85), 2)
                        pf = round(rng.uniform(0.80, 0.91), 3)
                    freq = round(50.0 + rng.uniform(-0.15, 0.15), 3)
                    volt = round(228.0 + rng.uniform(-7.0, 8.0), 1)
                    batch_d.add(
                        prep_daily,
                        (dist, b_date, ts, dev_id, pwr, pf, freq, volt, t_zone),
                    )
                    zone_stats[t_zone]["p"].append(pwr)
                    zone_stats[t_zone]["pf"].append(pf)
                    zone_stats[t_zone]["f"].append(freq)
                    daily_rows += 1
                daily_exec_list.append((batch_d, ()))
                daily_batches += 1

            for tz, st in zone_stats.items():
                p_list = st["p"]
                avg_p = round(sum(p_list) / len(p_list), 2)
                max_p = round(max(p_list), 2)
                min_p = round(min(p_list), 2)
                tot_kwh = round(sum(p_list) * (2.0 / 60.0), 2)
                avg_pf = round(sum(st["pf"]) / len(st["pf"]), 3)
                avg_f = round(sum(st["f"]) / len(st["f"]), 3)
                agg_exec_list.append(
                    (
                        prep_agg,
                        (dist, b_date, tz, avg_p, max_p, min_p, tot_kwh, avg_pf, avg_f, len(p_list)),
                    )
                )
                agg_rows += 1

    execute_concurrent(session, daily_exec_list, concurrency=32)
    execute_concurrent(session, agg_exec_list, concurrency=32)

    daily_sec = time.perf_counter() - t0
    daily_wps = int((daily_rows + agg_rows) / daily_sec)
    daily_batch_ms = round((daily_sec * 1000.0) / daily_batches, 2)

    print("\nТаблиця 2. Пропускна здатність запису (Write Throughput):")
    print(
        f"  Simple Wide Row:  {simple_wps} зап/с, середня затримка батчу {simple_batch_ms:.2f} мс "
        f"(записано {simple_rows} рядків в 1 партицію)"
    )
    print(
        f"  Hourly Bucketing: {hourly_wps} зап/с, середня затримка батчу {hourly_batch_ms:.2f} мс "
        f"(записано {hourly_rows} рядків + 2 MV)"
    )
    print(
        f"  Daily + Aggr:     {daily_wps} зап/с, середня затримка батчу {daily_batch_ms:.2f} мс "
        f"(записано {daily_rows} сирих + {agg_rows} агрегованих рядків)"
    )

    subprocess.run(["nodetool", "flush", "lviv_smart_grid_opt"], check=False)
    subprocess.run(["nodetool", "compact", "lviv_smart_grid_opt", "telemetry_hourly_zstd"], check=False)

    tester = PerformanceTester(session)
    target_dist = "Франківський"
    last_day = datetime(2026, 10, 5, 0, 0, 0)
    last_hour = datetime(2026, 10, 5, 23, 0, 0)
    last_date = date(2026, 10, 5)

    q1_simple = tester.measure_query(
        "SELECT * FROM telemetry_simple WHERE device_id = ? LIMIT 100",
        [("LVIV_METER_001",)],
    )
    q1_hourly = tester.measure_query(
        "SELECT * FROM telemetry_hourly WHERE district_name = ? AND bucket_hour = ? LIMIT 100",
        [(target_dist, last_hour)],
    )
    q1_daily = tester.measure_query(
        "SELECT * FROM telemetry_daily WHERE district_name = ? AND bucket_date = ? LIMIT 100",
        [(target_dist, last_date)],
    )

    t_range_start = datetime(2026, 10, 5, 18, 0, 0)
    t_range_end = datetime(2026, 10, 5, 23, 59, 59)
    six_hours_params = [(target_dist, datetime(2026, 10, 5, h, 0, 0)) for h in range(18, 24)]

    q2_simple = tester.measure_query(
        "SELECT * FROM telemetry_simple WHERE device_id = ? AND recorded_at >= ? AND recorded_at <= ?",
        [("LVIV_METER_001", t_range_start, t_range_end)],
    )
    q2_hourly = tester.measure_query(
        "SELECT * FROM telemetry_hourly WHERE district_name = ? AND bucket_hour = ?",
        six_hours_params,
    )
    q2_daily = tester.measure_query(
        "SELECT * FROM telemetry_daily WHERE district_name = ? AND bucket_date = ? "
        "AND recorded_at >= ? AND recorded_at <= ?",
        [(target_dist, last_date, t_range_start, t_range_end)],
    )

    day_start = datetime(2026, 10, 5, 0, 0, 0)
    day_end = datetime(2026, 10, 5, 23, 59, 59)
    all_24h_params = [(target_dist, datetime(2026, 10, 5, h, 0, 0)) for h in range(24)]

    q3_simple = tester.measure_query(
        "SELECT tariff_zone, active_power, power_factor, frequency FROM telemetry_simple "
        "WHERE device_id = ? AND recorded_at >= ? AND recorded_at <= ?",
        [("LVIV_METER_001", day_start, day_end)],
    )
    q3_hourly = tester.measure_query(
        "SELECT tariff_zone, active_power, power_factor, frequency FROM telemetry_hourly "
        "WHERE district_name = ? AND bucket_hour = ?",
        all_24h_params,
    )
    q3_daily_agg = tester.measure_query(
        "SELECT * FROM telemetry_daily_aggregates WHERE district_name = ? AND bucket_date = ?",
        [(target_dist, last_date)],
    )

    print("\nТаблиця 1. Порівняння затримки читання Read Latency (50 ітерацій):")
    for label, res in [
        ("Simple | Останні 100 записів", q1_simple),
        ("Simple | Діапазон 6 годин   ", q2_simple),
        ("Simple | Добова агрегація   ", q3_simple),
        ("Hourly | Останні 100 записів", q1_hourly),
        ("Hourly | Діапазон 6 годин   ", q2_hourly),
        ("Hourly | Добова агрегація   ", q3_hourly),
        ("Daily  | Останні 100 записів", q1_daily),
        ("Daily  | Діапазон 6 годин   ", q2_daily),
        ("Daily  | Таблиця агрегатів  ", q3_daily_agg),
    ]:
        print(
            f"  {label}: avg = {res['avg']:.2f} мс, p50 = {res['p50']:.2f} мс, "
            f"p95 = {res['p95']:.2f} мс, p99 = {res['p99']:.2f} мс"
        )

    q4_freq_no_mv = tester.measure_query(
        "SELECT * FROM telemetry_hourly WHERE frequency > ? ALLOW FILTERING",
        [(50.20,)],
        iterations=10,
    )
    q4_freq_mv = tester.measure_query(
        "SELECT * FROM mv_frequency_alerts WHERE district_name = ? AND bucket_hour = ? AND frequency > ?",
        [(target_dist, last_hour, 50.20)],
        iterations=50,
    )
    freq_speedup = round(q4_freq_no_mv["avg"] / max(q4_freq_mv["avg"], 0.01), 1)

    peak_hour = datetime(2026, 10, 5, 19, 0, 0)
    q4_tariff_no_mv = tester.measure_query(
        "SELECT * FROM telemetry_hourly WHERE tariff_zone = ? ALLOW FILTERING",
        [("peak",)],
        iterations=10,
    )
    q4_tariff_mv = tester.measure_query(
        "SELECT * FROM mv_tariff_zone WHERE district_name = ? AND bucket_hour = ? AND tariff_zone = ?",
        [(target_dist, peak_hour, "peak")],
        iterations=50,
    )
    tariff_speedup = round(q4_tariff_no_mv["avg"] / max(q4_tariff_mv["avg"], 0.01), 1)

    print("\nТаблиця 4. Вплив Materialized Views на швидкість фільтрації:")
    print(
        f"  Відхилення частоти (> 50.20 Гц): без MV (ALLOW FILTERING) = {q4_freq_no_mv['avg']:.2f} мс "
        f"(p95 = {q4_freq_no_mv['p95']:.2f} мс), з mv_frequency_alerts = {q4_freq_mv['avg']:.2f} мс "
        f"(p95 = {q4_freq_mv['p95']:.2f} мс), прискорення у {freq_speedup} разів"
    )
    print(
        f"  Тарифна зона (peak):             без MV (ALLOW FILTERING) = {q4_tariff_no_mv['avg']:.2f} мс "
        f"(p95 = {q4_tariff_no_mv['p95']:.2f} мс), з mv_tariff_zone = {q4_tariff_mv['avg']:.2f} мс "
        f"(p95 = {q4_tariff_mv['p95']:.2f} мс), прискорення у {tariff_speedup} разів"
    )

    st_simple = parse_tablestats("lviv_smart_grid_opt", "telemetry_simple")
    st_hourly = parse_tablestats("lviv_smart_grid_opt", "telemetry_hourly")
    st_zstd = parse_tablestats("lviv_smart_grid_opt", "telemetry_hourly_zstd")
    st_daily = parse_tablestats("lviv_smart_grid_opt", "telemetry_daily")
    st_agg = parse_tablestats("lviv_smart_grid_opt", "telemetry_daily_aggregates")

    print("\nТаблиця 3. Використання дискового простору та розміри партицій (nodetool tablestats):")
    for title, st in [
        ("Simple (telemetry_simple)     ", st_simple),
        ("Hourly LZ4 (telemetry_hourly) ", st_hourly),
        ("Hourly ZSTD (telemetry_zstd)  ", st_zstd),
        ("Daily Raw (telemetry_daily)   ", st_daily),
        ("Daily Aggr (daily_aggregates) ", st_agg),
    ]:
        kb_live = round(st["space_live"] / 1024.0, 1)
        mean_kb = round(st["mean_bytes"] / 1024.0, 2)
        max_kb = round(st["max_bytes"] / 1024.0, 2)
        print(
            f"  {title}: обсяг {kb_live} КБ, партицій {st['partitions']}, "
            f"середня {mean_kb} КБ, макс {max_kb} КБ, коеф. стиснення {st['compression_ratio']:.3f}"
        )

    lz4_kb = round(st_hourly["space_live"] / 1024.0, 1)
    zstd_kb = round(st_zstd["space_live"] / 1024.0, 1)
    zstd_saving_pct = round((1.0 - (st_zstd["space_live"] / max(st_hourly["space_live"], 1))) * 100.0, 1)

    records_per_year = 5000 * 365 * 24 * 120
    raw_bytes_per_record = 180
    raw_gb_year = round((records_per_year * raw_bytes_per_record) / (1024 ** 3), 2)
    lz4_gb_year = round(raw_gb_year * st_hourly["compression_ratio"], 2)
    zstd_gb_year = round(raw_gb_year * st_zstd["compression_ratio"], 2)
    ttl_90d_zstd_gb = round(zstd_gb_year * (90.0 / 365.0), 2)

    print("\nПідсумок ")
    print(f"  Обсяг таблиці з LZ4Compressor:  {lz4_kb} КБ (коефіцієнт {st_hourly['compression_ratio']:.3f})")
    print(f"  Обсяг таблиці з ZstdCompressor: {zstd_kb} КБ (коефіцієнт {st_zstd['compression_ratio']:.3f})")
    print(f"  Додаткова економія ZSTD порівняно з LZ4: {zstd_saving_pct}%")
    print(
        f"  Прогноз на 1 рік для 5000 лічильників Львова (5 256 000 000 вимірювань): "
        f"без стиснення {raw_gb_year} ГБ, з LZ4 {lz4_gb_year} ГБ, з ZSTD {zstd_gb_year} ГБ, "
        f"а з урахуванням TTL 90 діб та ZSTD лише {ttl_90d_zstd_gb} ГБ на вузол"
    )

    cluster.shutdown()


if __name__ == "__main__":
    main()
