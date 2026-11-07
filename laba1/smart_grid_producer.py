import json
import os
import time
from collections import Counter
from confluent_kafka import Producer


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round((p / 100.0) * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def run_keyed_benchmark(topic, records, num_messages, key_mode, batch_size=65536, linger_ms=10):
    conf = {
        "bootstrap.servers": "127.0.0.1:9092",
        "broker.address.family": "v4",
        "batch.size": batch_size,
        "linger.ms": linger_ms,
        "acks": 1,
        "compression.type": "none",
        "partitioner": "murmur2_random",
        "queue.buffering.max.messages": 200000,
    }
    producer = Producer(conf)

    prepared = []
    for r in records:
        payload_bytes = json.dumps(r, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if key_mode == "tariff_only":
            key_bytes = r["tariff_zone"].encode("utf-8")
        else:
            # Комбінований ключ Варіанту 4: географічний район + tariff_zone + device_id
            key_bytes = f"{r['district']}_{r['tariff_zone']}_{r['device_id']}".encode("utf-8")
        prepared.append((key_bytes, payload_bytes))

    # Прогрів топіка та TCP-з'єднань до старту вимірювання (усуває затримку запиту метаданих топіка)
    for i in range(200):
        k, v = prepared[i % len(prepared)]
        producer.produce(topic, key=k, value=v)
    producer.flush()

    part_counts = Counter()
    latencies_ms = []
    total_bytes = 0
    errors = 0

    start_wall = time.perf_counter()

    for i in range(num_messages):
        k, v = prepared[i % len(prepared)]
        total_bytes += len(k) + len(v)
        send_ts = time.perf_counter()

        def make_cb(t0):
            def _cb(err, msg):
                nonlocal errors
                if err is not None:
                    errors += 1
                else:
                    latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                    part_counts[msg.partition()] += 1
            return _cb

        while True:
            try:
                producer.produce(topic, key=k, value=v, callback=make_cb(send_ts))
                break
            except BufferError:
                producer.poll(0.001)

        if i % 64 == 0:
            producer.poll(0)

    producer.flush()
    elapsed = time.perf_counter() - start_wall

    latencies_ms.sort()
    rec_per_sec = len(latencies_ms) / elapsed if elapsed > 0 else 0.0
    mb_per_sec = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
    avg_lat = sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0.0

    return {
        "topic": topic,
        "sent": len(latencies_ms),
        "errors": errors,
        "rec_per_sec": rec_per_sec,
        "mb_per_sec": mb_per_sec,
        "avg_ms": avg_lat,
        "p50_ms": percentile(latencies_ms, 50),
        "p95_ms": percentile(latencies_ms, 95),
        "p99_ms": percentile(latencies_ms, 99),
        "max_ms": latencies_ms[-1] if latencies_ms else 0.0,
        "part_counts": dict(sorted(part_counts.items())),
    }


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, "test_data.json")
    with open(data_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    print("1. Перевірка чистого ключа key = tariff_zone (day/night/peak) на smart-grid-main (12 парт.):")
    res_tariff = run_keyed_benchmark("smart-grid-main", records, num_messages=10000, key_mode="tariff_only")
    print(f"Відправлено {res_tariff['sent']} записів | Активних партицій із 12: {len(res_tariff['part_counts'])}")
    print(f"Розподіл по партиціях (tariff_zone): {res_tariff['part_counts']}\n")

    print("2. Тест масштабованості з комбінованим ключем (district + tariff_zone + device_id):")
    topics = [("smart-part-6", 6), ("smart-part-12", 12), ("smart-part-24", 24)]
    base_tp = None
    for topic, n_parts in topics:
        res = run_keyed_benchmark(topic, records, num_messages=50000, key_mode="combined", batch_size=65536, linger_ms=10)
        if base_tp is None:
            base_tp = res["rec_per_sec"]
        scale = res["rec_per_sec"] / base_tp if base_tp else 1.0
        print(
            f"[{topic:13s} | {n_parts:2d} парт.] "
            f"{res['sent']} sent, {res['rec_per_sec']:.2f} rec/sec ({res['mb_per_sec']:.2f} MB/sec), "
            f"{res['avg_ms']:.2f} ms avg, {res['max_ms']:.2f} ms max, "
            f"{res['p50_ms']:.2f} ms 50th, {res['p95_ms']:.2f} ms 95th, {res['p99_ms']:.2f} ms 99th | "
            f"Scaling: {scale:.2f}x | Активних партицій: {len(res['part_counts'])}/{n_parts}"
        )
        print(f"   Розподіл по партиціях: {res['part_counts']}")

    print("\n3. Контрольний тест SCADA конфігурації на smart-grid-main (12 партицій, batch=16KB, linger=0ms, none):")
    res_scada = run_keyed_benchmark("smart-grid-main", records, num_messages=20000, key_mode="combined", batch_size=16384, linger_ms=0)
    print(
        f"[smart-grid-main] {res_scada['sent']} sent, {res_scada['rec_per_sec']:.2f} rec/sec ({res_scada['mb_per_sec']:.2f} MB/sec), "
        f"{res_scada['avg_ms']:.2f} ms avg, {res_scada['max_ms']:.2f} ms max, "
        f"{res_scada['p50_ms']:.2f} ms 50th, {res_scada['p95_ms']:.2f} ms 95th, {res_scada['p99_ms']:.2f} ms 99th"
    )


if __name__ == "__main__":
    main()
