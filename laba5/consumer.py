import argparse
import json
import shutil
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Consumer, TopicPartition
from cassandra.cluster import Cluster
from prometheus_client import Counter, Gauge, Histogram, start_http_server

TOPIC = "grid.raw"
NS = uuid.UUID("6f1c1d6e-3c1a-4a55-9a7e-2b7f0c0d4a04")

# Метрики Kafka та Cassandra для Prometheus
messages_in_per_sec = Gauge(
    "kafka_server_brokertopicmetrics_messagesinpersec",
    "Кількість отриманих повідомлень з Kafka за секунду",
    ["topic"],
)
consumer_lag = Gauge(
    "kafka_consumergroup_lag",
    "Затримка читання споживача по партиціях",
    ["partition"],
)
under_replicated = Gauge(
    "kafka_server_replicamanager_underreplicatedpartitions",
    "Кількість нереплікованих партицій",
)
cassandra_read_latency = Gauge(
    "cassandra_table_readlatency",
    "Час читання з таблиці Cassandra в мс",
    ["table"],
)
cassandra_write_latency = Gauge(
    "cassandra_table_writelatency",
    "Час запису в таблицю Cassandra в мс",
    ["table"],
)
cassandra_storage_load = Gauge(
    "cassandra_storage_load",
    "Використання дискового простору в байтах",
)
consumed_total = Counter(
    "smartgrid_messages_consumed_total",
    "Кількість оброблених повідомлень споживачем",
    ["district"],
)
write_latency_hist = Histogram(
    "smartgrid_cassandra_write_latency_ms",
    "Розподіл затримки запису в Cassandra в мс",
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0],
)


def init_cassandra():
    cluster = Cluster(["127.0.0.1"], protocol_version=5)
    session = cluster.connect()
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS lviv_streaming "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("lviv_streaming")
    session.execute(
        "CREATE TABLE IF NOT EXISTS quality_monitoring_log ("
        "district_name TEXT, "
        "day_bucket DATE, "
        "event_time TIMESTAMP, "
        "event_id UUID, "
        "meter_id TEXT, "
        "voltage DOUBLE, "
        "frequency DOUBLE, "
        "power_factor DOUBLE, "
        "PRIMARY KEY ((district_name, day_bucket), event_time, event_id)"
        ") WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)"
    )
    insert_stmt = session.prepare(
        "INSERT INTO quality_monitoring_log "
        "(district_name, day_bucket, event_time, event_id, meter_id, voltage, frequency, power_factor) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    read_stmt = session.prepare(
        "SELECT voltage, frequency, power_factor FROM quality_monitoring_log "
        "WHERE district_name = ? AND day_bucket = ? LIMIT 5"
    )
    return session, insert_stmt, read_stmt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", default="lviv-quality-monitor")
    args = parser.parse_args()

    start_http_server(args.port)
    print(f"Метрики consumer доступні на http://localhost:{args.port}/metrics")

    session, insert_stmt, read_stmt = init_cassandra()
    under_replicated.set(0)

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": args.group,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([args.topic])

    batch_count = 0
    window_start = time.perf_counter()

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.perf_counter()

            if now - window_start >= 5.0:
                rate = batch_count / (now - window_start)
                messages_in_per_sec.labels(topic=args.topic).set(round(rate, 2))
                batch_count = 0
                window_start = now

                # Замір затримки читання з Cassandra та обсягу сховища
                today = datetime.now(timezone.utc).date()
                t0 = time.perf_counter()
                list(session.execute(read_stmt, ("Сихівський", today)))
                r_ms = (time.perf_counter() - t0) * 1000.0
                cassandra_read_latency.labels(table="quality_monitoring_log").set(round(r_ms, 3))

                disk = shutil.disk_usage(".")
                cassandra_storage_load.set(disk.used)

            if msg is None or msg.error():
                continue

            batch_count += 1
            data = json.loads(msg.value().decode("utf-8"))
            district = data["district"]
            consumed_total.labels(district=district).inc()

            # Запис аномальних або вибіркових показань у Cassandra
            is_violation = (
                abs(data["voltage"] - 220.0) > 10.0
                or abs(data["frequency"] - 50.0) > 0.2
                or data["power_factor"] < 0.90
            )
            if is_violation or batch_count % 50 == 0:
                dt = datetime.fromtimestamp(data["ts"] / 1000.0, tz=timezone.utc).replace(tzinfo=None)
                ev_id = uuid.uuid5(NS, f"{data['meter_id']}|{data['ts']}")
                t0 = time.perf_counter()
                session.execute(
                    insert_stmt,
                    (
                        district,
                        dt.date(),
                        dt,
                        ev_id,
                        data["meter_id"],
                        float(data["voltage"]),
                        float(data["frequency"]),
                        float(data["power_factor"]),
                    ),
                )
                w_ms = (time.perf_counter() - t0) * 1000.0
                cassandra_write_latency.labels(table="quality_monitoring_log").set(round(w_ms, 3))
                write_latency_hist.observe(w_ms)

            # Оновлення метрики лагу споживача
            if batch_count % 100 == 0:
                tp = TopicPartition(msg.topic(), msg.partition())
                low, high = consumer.get_watermark_offsets(tp, timeout=1.0)
                lag = max(0, high - msg.offset() - 1)
                consumer_lag.labels(partition=str(msg.partition())).set(lag)

    except KeyboardInterrupt:
        print("Зупинка consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
