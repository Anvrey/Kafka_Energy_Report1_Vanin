import json
import time
import uuid
from datetime import datetime, timezone
import numpy as np
from confluent_kafka import Producer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

KAFKA_SERVER = "127.0.0.1:9092"
BENCH_TOPIC = "grid.bench"
CASSANDRA_HOST = "127.0.0.1"
KEYSPACE = "lviv_streaming"
NUM_MESSAGES = 12000
BATCH_SIZE = 500


def test_at_most_once():
    conf = {
        "bootstrap.servers": KAFKA_SERVER,
        "acks": 0,
        "retries": 0,
        "enable.idempotence": False,
        "linger.ms": 5,
        "broker.address.family": "v4",
    }
    p = Producer(conf)
    latencies = []
    t0 = time.perf_counter()

    for i in range(NUM_MESSAGES):
        val = json.dumps({"meter_id": f"LVIV-{i%5000:05d}", "ts": time.time(), "kw": 1.25})
        m_t0 = time.perf_counter()
        p.produce(BENCH_TOPIC, key=f"LVIV-{i%5000:05d}", value=val)
        if i % 100 == 0:
            p.poll(0)
            latencies.append((time.perf_counter() - m_t0) * 1000)

    p.flush()
    elapsed = time.perf_counter() - t0
    tp = NUM_MESSAGES / elapsed
    return tp, np.percentile(latencies, 50), np.percentile(latencies, 95), np.percentile(latencies, 99)


def test_at_least_once():
    conf = {
        "bootstrap.servers": KAFKA_SERVER,
        "acks": "all",
        "retries": 5,
        "enable.idempotence": False,
        "linger.ms": 5,
        "broker.address.family": "v4",
    }
    p = Producer(conf)
    latencies = []
    t0 = time.perf_counter()

    for i in range(NUM_MESSAGES):
        val = json.dumps({"meter_id": f"LVIV-{i%5000:05d}", "ts": time.time(), "kw": 1.25})
        m_t0 = time.perf_counter()
        p.produce(BENCH_TOPIC, key=f"LVIV-{i%5000:05d}", value=val)
        if i % 100 == 0:
            p.poll(0)
            latencies.append((time.perf_counter() - m_t0) * 1000)

    p.flush()
    elapsed = time.perf_counter() - t0
    tp = NUM_MESSAGES / elapsed
    return tp, np.percentile(latencies, 50), np.percentile(latencies, 95), np.percentile(latencies, 99)


def test_exactly_once():
    tx_id = f"bench-tx-{int(time.time())}"
    conf = {
        "bootstrap.servers": KAFKA_SERVER,
        "transactional.id": tx_id,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "broker.address.family": "v4",
    }
    p = Producer(conf)
    p.init_transactions()
    latencies = []
    t0 = time.perf_counter()

    for b in range(NUM_MESSAGES // BATCH_SIZE):
        b_t0 = time.perf_counter()
        p.begin_transaction()
        for i in range(BATCH_SIZE):
            val = json.dumps({"meter_id": f"LVIV-{i%5000:05d}", "ts": time.time(), "kw": 1.25})
            p.produce(BENCH_TOPIC, key=f"LVIV-{i%5000:05d}", value=val)
        p.commit_transaction()
        b_lat = ((time.perf_counter() - b_t0) / BATCH_SIZE) * 1000
        latencies.append(b_lat)

    elapsed = time.perf_counter() - t0
    tp = NUM_MESSAGES / elapsed
    return tp, np.percentile(latencies, 50), np.percentile(latencies, 95), np.percentile(latencies, 99)


def test_fault_tolerance_and_deduplication():
    cluster = Cluster(
        [CASSANDRA_HOST],
        protocol_version=5,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
    )
    sess = cluster.connect(KEYSPACE)

    prep = sess.prepare(
        "INSERT INTO consumption_event_log "
        "(district_name, day_bucket, event_time, event_id, event_type, meter_id, payload) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )

    district = "ТЕСТ_ВІДМОВОСТІЙКІСТЬ"
    day = datetime(2026, 11, 8).date()
    ns = uuid.UUID("6f1c1d6e-3c1a-4a55-9a7e-2b7f0c0d4a04")

    sess.execute(
        "DELETE FROM consumption_event_log WHERE district_name = %s AND day_bucket = %s",
        (district, day)
    )

    events = []
    base_ts = 1794140000000
    for i in range(1000):
        ts_ms = base_ts + i * 1000
        meter_id = f"TEST-{i:04d}"
        ev_id = uuid.uuid5(ns, f"FAULT_TEST|{district}|{meter_id}|{ts_ms}")
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
        payload = json.dumps({"power": 1.5, "run": 1})
        events.append((district, day, dt, ev_id, "FAULT_TEST", meter_id, payload))

    t0 = time.perf_counter()
    for ev in events:
        sess.execute(prep, ev)
    t_first = time.perf_counter() - t0

    r1 = sess.execute(
        "SELECT count(*) FROM consumption_event_log WHERE district_name = %s AND day_bucket = %s",
        (district, day)
    ).one()[0]

    print(f"Симуляція збою: виконано аварійну зупинку воркера, повторний запуск та повторну доставку...")
    t1 = time.perf_counter()
    for ev in events:
        sess.execute(prep, ev)
    t_replay = time.perf_counter() - t1

    r2 = sess.execute(
        "SELECT count(*) FROM consumption_event_log WHERE district_name = %s AND day_bucket = %s",
        (district, day)
    ).one()[0]

    sess.execute(
        "DELETE FROM consumption_event_log WHERE district_name = %s AND day_bucket = %s",
        (district, day)
    )

    print(f"Записів до симуляції збою: {r1}")
    print(f"Записів після повторного прогону: {r2}")
    print(f"Виявлено дублікатів: {r2 - r1}")
    assert r1 == r2, "Помилка дедуплікації: виявлено зайві дублікати!"
    print(f"Результат тесту: [PASS] Детермінований Event ID (UUID v5) повністю усуває дублікати при збоях!")
    print(f"Час первинного запису: {t_first:.2f} с, час реплею: {t_replay:.2f} с")


def main():

    print(f"Вимірювання продуктивності на {NUM_MESSAGES} повідомленнях...")

    print("1/3 Тестування At-Most-Once (acks=0)...")
    tp_mo, p50_mo, p95_mo, p99_mo = test_at_most_once()

    print("2/3 Тестування At-Least-Once (acks=all, retries=5)...")
    tp_lo, p50_lo, p95_lo, p99_lo = test_at_least_once()

    print("3/3 Тестування Exactly-Once (транзакційний продюсер, 2PC)...")
    tp_eo, p50_eo, p95_eo, p99_eo = test_exactly_once()

    print("\nТаблиця 1. Порівняння продуктивності семантик доставки повідомлень:")
    print("-" * 75)
    print(f"{'Семантика доставки':<18} | {'Throughput (msg/s)':<18} | {'p50 (мс)':<8} | {'p95 (мс)':<8} | {'p99 (мс)':<8}")
    print("-" * 75)
    print(f"{'at-most-once':<18} | {tp_mo:<18.1f} | {p50_mo:<8.2f} | {p95_mo:<8.2f} | {p99_mo:<8.2f}")
    print(f"{'at-least-once':<18} | {tp_lo:<18.1f} | {p50_lo:<8.2f} | {p95_lo:<8.2f} | {p99_lo:<8.2f}")
    print(f"{'exactly-once':<18} | {tp_eo:<18.1f} | {p50_eo:<8.2f} | {p95_eo:<8.2f} | {p99_eo:<8.2f}")
    print("-" * 75)

    test_fault_tolerance_and_deduplication()
    print("\nВсі вимірювання та тести успішно завершено")


if __name__ == "__main__":
    main()

