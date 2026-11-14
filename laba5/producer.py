import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

TOPIC = "grid.raw"
METERS_COUNT = 1000
DISTRICTS = [
    "Франківський",
    "Сихівський",
    "Личаківський",
    "Залізничний",
    "Шевченківський",
]

DISTRICT_BASE_V = {
    "Франківський": 221.0,
    "Сихівський": 216.0,
    "Личаківський": 222.5,
    "Залізничний": 217.5,
    "Шевченківський": 219.5,
}

TARIFF_CODES = {"night": 1, "day": 2, "peak": 3}

# Метрики Prometheus для моніторингу якості електроенергії
messages_sent = Counter(
    "energy_messages_sent_total",
    "Кількість відправлених повідомлень",
    ["district", "status"],
)
readings_total = Counter(
    "smartgrid_readings_total",
    "Загальна кількість показань лічильників",
    ["district"],
)
power_kw = Gauge(
    "smartgrid_power_kw",
    "Сумарне споживання по району в кВт",
    ["district"],
)
tariff_zone_g = Gauge(
    "smartgrid_tariff_zone",
    "Поточна тарифна зона 1 нічна 2 денна 3 пікова",
)
voltage_v = Gauge(
    "smartgrid_voltage_v",
    "Середня напруга по району у вольтах",
    ["district"],
)
voltage_hist = Histogram(
    "smartgrid_voltage_hist_v",
    "Розподіл напруги по районах",
    ["district"],
    buckets=[195, 200, 205, 210, 215, 220, 225, 230, 235, 240, 245],
)
frequency_hz = Gauge(
    "smartgrid_frequency_hz",
    "Частота мережі в Гц",
    ["district"],
)
power_factor_g = Gauge(
    "smartgrid_power_factor",
    "Середній коефіцієнт потужності",
    ["district", "consumer_type"],
)
power_factor_hist = Histogram(
    "smartgrid_power_factor_hist",
    "Розподіл коефіцієнта потужності",
    ["consumer_type"],
    buckets=[0.75, 0.80, 0.85, 0.88, 0.90, 0.92, 0.95, 0.98, 1.0],
)
quality_violations = Counter(
    "smartgrid_quality_violations_total",
    "Кількість зафіксованих порушень якості",
    ["district", "violation_type"],
)


def tariff_zone(hour):
    if 17 <= hour < 21:
        return "peak"
    if 7 <= hour < 17 or 21 <= hour < 23:
        return "day"
    return "night"


def make_meters(rng):
    meters = []
    for i in range(METERS_COUNT):
        district = DISTRICTS[i % len(DISTRICTS)]
        commercial = rng.random() < 0.20
        ctype = "commercial" if commercial else "residential"
        meters.append({
            "meter_id": f"LVIV-{i:04d}",
            "district": district,
            "consumer_type": ctype,
            "base_kw": rng.uniform(5.0, 14.0) if commercial else rng.uniform(0.4, 1.8),
            "base_v": DISTRICT_BASE_V[district] + rng.gauss(0, 1.5),
        })
    return meters


def generate_reading(meter, ts, rng, anomaly_district=None):
    zone = tariff_zone(ts.hour)
    # Залежність споживання від часу доби
    load_k = {"peak": 1.65, "day": 1.0, "night": 0.45}[zone]
    power = max(0.05, meter["base_kw"] * load_k + rng.gauss(0, 0.15))

    district = meter["district"]
    voltage = meter["base_v"] + rng.gauss(0, 2.2)

    # Відхилення напруги залежно від району
    glitch_prob = 0.08 if district in ("Сихівський", "Залізничний") else 0.02
    if rng.random() < glitch_prob:
        voltage = rng.choice([rng.uniform(202.0, 209.0), rng.uniform(231.0, 238.0)])

    # Різний power factor для побутових і комерційних споживачів
    if meter["consumer_type"] == "commercial":
        pf = rng.uniform(0.81, 0.92)
    else:
        pf = rng.uniform(0.92, 0.99)

    freq = 50.0 + rng.gauss(0, 0.05)

    # Примусова аномалія для тестування алертів
    if anomaly_district and (anomaly_district == "all" or anomaly_district == district):
        voltage = rng.uniform(234.0, 241.0)
        freq = rng.uniform(49.62, 49.74)
        pf = rng.uniform(0.80, 0.86)

    return {
        "meter_id": meter["meter_id"],
        "district": district,
        "consumer_type": meter["consumer_type"],
        "ts": int(ts.timestamp() * 1000),
        "active_power": round(power, 3),
        "voltage": round(voltage, 1),
        "frequency": round(freq, 3),
        "power_factor": round(pf, 3),
        "tariff_zone": zone,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--anomaly", default=None)
    args = parser.parse_args()

    start_http_server(args.port)
    print(f"Метрики producer доступні на http://localhost:{args.port}/metrics")

    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 20,
    })

    rng = random.Random(42)
    meters = make_meters(rng)
    sim_time = datetime.now(timezone.utc)

    step = 0
    while True:
        step += 1
        sim_time += timedelta(minutes=15)
        zone = tariff_zone(sim_time.hour)
        tariff_zone_g.set(TARIFF_CODES[zone])

        dist_power = {d: 0.0 for d in DISTRICTS}
        dist_volt = {d: [] for d in DISTRICTS}
        dist_freq = {d: [] for d in DISTRICTS}
        dist_pf = {(d, c): [] for d in DISTRICTS for c in ("residential", "commercial")}

        for m in meters:
            rec = generate_reading(m, sim_time, rng, args.anomaly)
            d = rec["district"]
            c = rec["consumer_type"]

            try:
                producer.produce(args.topic, key=m["meter_id"], value=json.dumps(rec))
                messages_sent.labels(district=d, status="success").inc()
            except Exception:
                messages_sent.labels(district=d, status="error").inc()

            readings_total.labels(district=d).inc()
            dist_power[d] += rec["active_power"]
            dist_volt[d].append(rec["voltage"])
            dist_freq[d].append(rec["frequency"])
            dist_pf[(d, c)].append(rec["power_factor"])

            voltage_hist.labels(district=d).observe(rec["voltage"])
            power_factor_hist.labels(consumer_type=c).observe(rec["power_factor"])

            if abs(rec["voltage"] - 220.0) > 10.0:
                quality_violations.labels(district=d, violation_type="voltage").inc()
            if abs(rec["frequency"] - 50.0) > 0.2:
                quality_violations.labels(district=d, violation_type="frequency").inc()
            if rec["power_factor"] < 0.90:
                quality_violations.labels(district=d, violation_type="power_factor").inc()

        producer.poll(0)

        for d in DISTRICTS:
            power_kw.labels(district=d).set(round(dist_power[d], 2))
            voltage_v.labels(district=d).set(round(sum(dist_volt[d]) / len(dist_volt[d]), 2))
            frequency_hz.labels(district=d).set(round(sum(dist_freq[d]) / len(dist_freq[d]), 3))
            for c in ("residential", "commercial"):
                vals = dist_pf[(d, c)]
                if vals:
                    power_factor_g.labels(district=d, consumer_type=c).set(round(sum(vals) / len(vals), 3))

        avg_u = sum(sum(v) for v in dist_volt.values()) / METERS_COUNT
        print(f"Раунд {step}: зона {zone}, середня напруга {avg_u:.1f} В, сумарно {sum(dist_power.values()):.1f} кВт")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
