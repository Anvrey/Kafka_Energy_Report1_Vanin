import uuid
import random
import time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

# підключення до бази
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('lviv_smart_grid')
print("Підключено до Cassandra")

districts = ["Сихівський", "Франківський", "Галицький", "Залізничний", "Личаківський"]

# створюємо лічильники
meters = []
for _ in range(50):
    m_id = uuid.uuid4()
    dist = random.choice(districts)
    meters.append({'id': m_id, 'district': dist})

now = datetime.now()
total_v = 0
max_v = 0
max_dist = ""
records_count = 0

print("Починаємо генерацію та вставку даних...")

for m in meters:
    for i in range(24): # 24 години
        t = now - timedelta(hours=i)
        
        # рандомні значення
        act_p = round(random.uniform(0.5, 5.0), 2)
        react_p = round(random.uniform(0.1, 0.8), 2)
        volt = round(random.uniform(200.0, 255.0), 1)
        
        # шукаємо максимум і середнє для варіанту А
        total_v += volt
        records_count += 1
        if volt > max_v:
            max_v = volt
            max_dist = m['district']

        # запис в таблиці
        session.execute(
            "INSERT INTO meter_readings_desc (meter_id, recorded_at, active_power, reactive_power, voltage) VALUES (%s, %s, %s, %s, %s)",
            (m['id'], t, act_p, react_p, volt)
        )
        
        session.execute(
            "INSERT INTO meter_readings_asc (meter_id, recorded_at, active_power, reactive_power, voltage) VALUES (%s, %s, %s, %s, %s)",
            (m['id'], t, act_p, react_p, volt)
        )
        
        # записуємо аномалії (якщо напруга не в нормі)
        if volt > 250 or volt < 210:
            a_type = "High" if volt > 250 else "Low"
            session.execute(
                "INSERT INTO anomaly_history (meter_id, recorded_at, anomaly_type, voltage) VALUES (%s, %s, %s, %s)",
                (m['id'], t, a_type, volt)
            )

print("Дані завантажені!")

# тестуємо швидкість
test_id = meters[0]['id']

start1 = time.time()
session.execute(f"SELECT * FROM meter_readings_desc WHERE meter_id = {test_id} LIMIT 1")
end1 = time.time()

start2 = time.time()
session.execute(f"SELECT * FROM meter_readings_asc WHERE meter_id = {test_id} ORDER BY recorded_at DESC LIMIT 1")
end2 = time.time()

print("\n Швидкість читання ")
print("DESC таблиця:", round((end1 - start1) * 1000, 2), "ms")
print("ASC таблиця:", round((end2 - start2) * 1000, 2), "ms")

# вивід варіанту А
avg_v = total_v / records_count
print("\n Результати ")
print(f"Середня напруга = {round(avg_v, 2)} В")
print(f"Максимальна напруга = {max_v} В")
print(f"Район з найбільшою напругою: {max_dist}")

cluster.shutdown()
