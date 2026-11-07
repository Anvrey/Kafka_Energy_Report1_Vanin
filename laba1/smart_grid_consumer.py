import json
import sys
import time
from collections import Counter
from confluent_kafka import Consumer, KafkaError


def main():
    topic = sys.argv[1] if len(sys.argv) > 1 else "smart-part-12"
    group_id = "energy-monitoring-group"

    conf = {
        "bootstrap.servers": "127.0.0.1:9092",
        "broker.address.family": "v4",
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    print(f"Підключено Consumer (group={group_id}) до топіка '{topic}'...")

    total_read = 0
    tariff_counts = Counter()
    part_counts = Counter()
    freq_alerts = []
    idle_polls = 0
    max_wait_before_first = 15
    waited = 0
    start_ts = None
    last_msg_ts = None

    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                if total_read == 0:
                    waited += 1
                    if waited >= max_wait_before_first * 2:
                        break
                else:
                    idle_polls += 1
                    if idle_polls >= 4:
                        break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if total_read > 0:
                        idle_polls += 1
                        if idle_polls >= 4:
                            break
                    continue
                print(f"Помилка читання: {msg.error()}")
                break

            now = time.perf_counter()
            if start_ts is None:
                start_ts = now
            last_msg_ts = now
            idle_polls = 0
            total_read += 1
            part_counts[msg.partition()] += 1

            data = json.loads(msg.value().decode("utf-8"))
            tariff_counts[data.get("tariff_zone", "unknown")] += 1

            freq = data.get("frequency", 50.0)
            if freq < 49.83 or freq > 50.17:
                if len(freq_alerts) < 5:
                    freq_alerts.append((data["device_id"], data.get("district"), freq, data.get("voltage"), msg.partition()))

        if total_read > 0:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    active_elapsed = max(0.001, (last_msg_ts - start_ts) if (start_ts and last_msg_ts) else 0.001)
    rate = total_read / active_elapsed

    print(f"\nОброблено повідомлень: {total_read} (~{rate:.0f} rec/sec)")
    print(f"Розподіл по партиціях ({len(part_counts)} активних): {dict(sorted(part_counts.items()))}")
    print(f"Розподіл по tariff_zone: {dict(tariff_counts)}")
    print("\nПриклади виявлених критичних SCADA-алертів частоти:")
    for dev, dist, fr, volt, part in freq_alerts:
        print(f"  [ALERT][Part {part:2d}] {dev} ({dist}): frequency={fr} Гц, voltage={volt} В")


if __name__ == "__main__":
    main()
