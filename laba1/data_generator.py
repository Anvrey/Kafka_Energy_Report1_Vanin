import json
import random
from datetime import datetime, timezone
import os

def generate_smart_meter_record():
    meter_id = random.randint(1, 5000)
    hour = datetime.now().hour

    if hour in [17, 18, 19, 20]:
        tariff = "peak"
    elif 7 <= hour <= 22:
        tariff = "day"
    else:
        tariff = "night"

    return {
        "device_id": f"METER_LV_{meter_id:04d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "power_output": round(random.uniform(0.5, 15.0), 2),
        "efficiency": round(random.uniform(95.0, 100.0), 2),
        "temperature": round(random.uniform(-5.0, 35.0), 1),
        "voltage": round(random.uniform(210.0, 240.0), 1),
        "current": round(random.uniform(2.0, 65.0), 1),
        "status": random.choice(["active", "active", "active", "inactive", "error"]),
        "location": {
            "lat": round(random.uniform(49.8, 49.9), 4),
            "lon": round(random.uniform(24.0, 24.1), 4)
        },
        "maintenance_hours": random.randint(6000, 12000),
        "power_factor": round(random.uniform(0.7, 1.0), 3),
        "frequency": round(random.uniform(49.8, 50.2), 3),
        "tariff_zone": tariff
    }

script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, "test_data.json")

print("Генерація 2000 тестових записів Smart Grid Львів...")
records = [generate_smart_meter_record() for _ in range(2000)]

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"Збережено {len(records)} записів → laba1/test_data.json")
print(f"\n приклад для додатку А ")
for i in range(3):
    r = records[i]
    print(f"\n--- Запис {i+1} ---")
    print(f"  device_id:    {r['device_id']}")
    print(f"  power_factor: {r['power_factor']}  ← діапазон 0.7-1.0")
    print(f"  frequency:    {r['frequency']} Гц  ← 49.8-50.2")
    print(f"  tariff_zone:  {r['tariff_zone']}")
    print(f"  voltage:      {r['voltage']} В")
    print(f"  status:       {r['status']}")
print(f"\nРозподіл по tariff_zone:")
for zone in ["day", "night", "peak"]:
    count = sum(1 for r in records if r["tariff_zone"] == zone)
    print(f"  {zone}: {count} записів ({count/20:.1f}%)")
