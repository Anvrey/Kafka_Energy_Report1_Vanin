import json
import random
from datetime import datetime, timezone, timedelta
import os

random.seed(2)

DISTRICTS = [
    ("Frankivskyi", 49.815, 49.835, 24.005, 24.030),
    ("Sykhivskyi", 49.800, 49.820, 24.045, 24.080),
    ("Lychakivskyi", 49.830, 49.855, 24.050, 24.095),
    ("Zaliznychnyi", 49.825, 49.850, 24.000, 24.025),
    ("Shevchenkivskyi", 49.855, 49.895, 24.015, 24.055),
    ("Halytskyi", 49.835, 49.850, 24.025, 24.045),
]


def generate_smart_meter_record():
    meter_id = random.randint(1, 5000)
    dist_name, lat_min, lat_max, lon_min, lon_max = DISTRICTS[(meter_id - 1) % len(DISTRICTS)]

    # Моделюємо вимірювання протягом усієї доби (кожні 15 хв) для покриття всіх тарифних зон
    base_dt = datetime(2026, 9, 28, 0, 0, 0, tzinfo=timezone.utc)
    slot_minutes = random.randint(0, 95) * 15 + random.randint(0, 14)
    ts = base_dt + timedelta(minutes=slot_minutes, seconds=random.randint(0, 59))
    hour = ts.hour

    if hour in [17, 18, 19, 20]:
        tariff = "peak"
    elif 7 <= hour <= 22:
        tariff = "day"
    else:
        tariff = "night"

    freq = round(random.uniform(49.8, 50.2), 3)
    if freq < 49.83 or freq > 50.17:
        status = "error"
    else:
        status = random.choice(["active", "active", "active", "active", "inactive"])

    return {
        "device_id": f"METER_LV_{meter_id:04d}",
        "district": dist_name,
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "power_output": round(random.uniform(0.5, 15.0), 2),
        "efficiency": round(random.uniform(95.0, 100.0), 2),
        "temperature": round(random.uniform(-5.0, 35.0), 1),
        "voltage": round(random.uniform(210.0, 240.0), 1),
        "current": round(random.uniform(2.0, 65.0), 1),
        "status": status,
        "location": {
            "lat": round(random.uniform(lat_min, lat_max), 4),
            "lon": round(random.uniform(lon_min, lon_max), 4)
        },
        "maintenance_hours": random.randint(6000, 12000),
        "power_factor": round(random.uniform(0.7, 1.0), 3),
        "frequency": freq,
        "tariff_zone": tariff
    }


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "test_data.json")
    jsonl_path = os.path.join(script_dir, "payloads.jsonl")

    print("Генерація 2000 тестових записів Smart Grid Львів...")
    records = [generate_smart_meter_record() for _ in range(2000)]

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    compact_lines = [json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in records]
    with open(jsonl_path, "w", encoding="utf-8") as f:
        f.write("\n".join(compact_lines) + "\n")

    avg_bytes = sum(len(line.encode("utf-8")) for line in compact_lines) / len(compact_lines)
    print(f"Збережено {len(records)} записів -> laba1/test_data.json та laba1/payloads.jsonl")
    print(f"Середній розмір компактного JSON-запису: {avg_bytes:.1f} байт")

    print("\nПриклад згенерованих записів (для Додатку А.3):")
    sample_records = []
    for target_zone in ["day", "peak", "night"]:
        rec = next(r for r in records if r["tariff_zone"] == target_zone)
        sample_records.append(rec)

    for i, r in enumerate(sample_records):
        print(f"\nЗапис {i + 1}:")
        print(f"  device_id:    {r['device_id']} ({r['district']})")
        print(f"  timestamp:    {r['timestamp']}")
        print(f"  power_factor: {r['power_factor']}  (діапазон 0.7-1.0)")
        print(f"  frequency:    {r['frequency']} Гц  (діапазон 49.8-50.2)")
        print(f"  tariff_zone:  {r['tariff_zone']}")
        print(f"  voltage:      {r['voltage']} В, status: {r['status']}")

    print("\nРозподіл по tariff_zone:")
    for zone in ["day", "night", "peak"]:
        count = sum(1 for r in records if r["tariff_zone"] == zone)
        print(f"  {zone:5s}: {count:4d} записів ({count / len(records) * 100:.1f}%)")

    print("\nРозподіл по районах Львова (district):")
    for d, *_ in DISTRICTS:
        count = sum(1 for r in records if r["district"] == d)
        print(f"  {d:15s}: {count:4d} записів ({count / len(records) * 100:.1f}%)")

    crit_freq = sum(1 for r in records if r["frequency"] < 49.85 or r["frequency"] > 50.15)
    print(f"\nКритичні відхилення частоти (|f - 50.0| > 0.15 Гц): {crit_freq} записів ({crit_freq / len(records) * 100:.1f}%)")


if __name__ == "__main__":
    main()
