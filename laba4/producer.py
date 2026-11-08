import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

TOPIC = "grid.raw"
METERS_COUNT = 5000
READING_STEP_SEC = 15
DISTRICTS = [
    "Франківський",
    "Сихівський",
    "Личаківський",
    "Залізничний",
    "Шевченківський",
    "Галицький",
]


def tariff_zone(hour):
    if 17 <= hour < 21:
        return "peak"
    if 7 <= hour < 17 or 21 <= hour < 23:
        return "day"
    return "night"


def make_meters(rng):
    meters = []
    for i in range(METERS_COUNT):
        commercial = rng.random() < 0.15
        meters.append({
            "meter_id": f"LVIV-{i:05d}",
            "district": DISTRICTS[i % len(DISTRICTS)],
            "base_kw": rng.uniform(4.0, 12.0) if commercial else rng.uniform(0.3, 1.5),
            "base_v": rng.gauss(228, 2),
        })
    # 25 лічильників, на яких імітуємо крадіжку: споживання падає майже до нуля
    thieves = set(rng.sample(range(METERS_COUNT), 25))
    return meters, thieves


def make_reading(meter, ts, sim_minute, is_thief, rng):
    hour = ts.hour
    zone = tariff_zone(hour)
    k = {"peak": 1.6, "day": 1.0, "night": 0.5}[zone]
    power = max(0.0, meter["base_kw"] * k + rng.gauss(0, 0.15))
    voltage = meter["base_v"] + rng.gauss(0, 2)
    # відсотки показань мають відхилення напруги від норми 210-240 В
    if rng.random() < 0.02:
        voltage = rng.choice([rng.uniform(195, 208), rng.uniform(242, 255)])
    pf = rng.uniform(0.82, 0.98)
    current = power * 1000 / (max(voltage, 1) * pf)
    if is_thief and sim_minute >= 2:
        power = round(rng.uniform(-0.05, 0.02), 3)
    frequency = 50 + rng.gauss(0, 0.05)
    return {
        "meter_id": meter["meter_id"],
        "district": meter["district"],
        "ts": int(ts.timestamp() * 1000),
        "active_power": round(power, 3),
        "reactive_power": round(abs(power) * rng.uniform(0.2, 0.4), 3),
        "voltage": round(voltage, 1),
        "current": round(current, 2),
        "frequency": round(frequency, 3),
        "power_factor": round(pf, 3),
        "tariff_zone": zone,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=25,
                        help="кількість раундів, кожен раунд це по одному показанню від усіх 5000 лічильників")
    parser.add_argument("--pace", type=float, default=0.0,
                        help="пауза між раундами в секундах, 0 означає максимально швидко")
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
    })

    rng = random.Random(4)
    meters, thieves = make_meters(rng)
    start = datetime(2026, 11, 8, 17, 0, 0, tzinfo=timezone.utc)

    sent = 0
    t0 = time.perf_counter()
    for r in range(args.rounds):
        ts = start + timedelta(seconds=READING_STEP_SEC * r)
        sim_minute = (r * READING_STEP_SEC) // 60
        for idx, m in enumerate(meters):
            reading = make_reading(m, ts, sim_minute, idx in thieves, rng)
            producer.produce(args.topic, key=m["meter_id"], value=json.dumps(reading))
            sent += 1
        producer.poll(0)
        if (r + 1) % 4 == 0:
            print(f"Надіслано {sent} показань, змодельований час {ts.strftime('%H:%M:%S')}")
        if args.pace > 0:
            time.sleep(args.pace)
    producer.flush()
    elapsed = time.perf_counter() - t0
    print(f"Готово: {sent} показань за {elapsed:.1f} с, {sent / elapsed:.0f} повідомлень за секунду")


if __name__ == "__main__":
    main()
