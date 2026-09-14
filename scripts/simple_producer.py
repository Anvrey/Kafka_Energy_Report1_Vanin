#!/usr/bin/env python3
"""
Простий Producer для енергетичних даних з Kafka
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

TOPIC_NAME = "power-station-data-vanin"

def create_producer():
    """Створюємо Kafka producer з налаштуваннями"""
    print("🔌 Підключаємося до Kafka...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(3, 7, 1),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',
            retries=3,
            request_timeout_ms=30000,
            retry_backoff_ms=500,
            batch_size=16384,
            linger_ms=100,
            compression_type='gzip',
            buffer_memory=33554432,
            max_in_flight_requests_per_connection=5
        )
        print("✅ Підключення до Kafka успішне!")
        return producer 
        
    except Exception as e:
        print(f"❌ Помилка підключення: {e}")
        print("Перевірте, чи запущено Kafka на localhost:9092")
        return None

def generate_power_data():
    """Генеруємо дані електростанції"""
    stations = [
        {"name": "Київська ТЕС", "type": "thermal", "max_power": 1200},
        {"name": "Дніпровська ГЕС", "type": "hydro", "max_power": 800},
        {"name": "Сонячна ферма", "type": "solar", "max_power": 150},
        {"name": "Вітряна ферма", "type": "wind", "max_power": 200}
    ]
    
    station = random.choice(stations)
    
    if station["type"] == "solar":
        hour = datetime.now().hour
        if 6 <= hour <= 18:
            power_factor = random.uniform(0.7, 0.95)
        else:
            power_factor = random.uniform(0.0, 0.1)
    elif station["type"] == "wind":
        wind_speed = random.uniform(0, 15)
        if wind_speed < 3:
            power_factor = 0
        elif wind_speed > 12:
            power_factor = random.uniform(0.8, 1.0)
        else:
            power_factor = (wind_speed / 12) ** 2
    else:
        power_factor = random.uniform(0.75, 0.95)
    
    current_power = station["max_power"] * power_factor
    
    return {
        "station_name": station["name"],
        "station_type": station["type"],
        "timestamp": datetime.now().isoformat(),
        "power_output_mw": round(current_power, 2),
        "voltage_kv": round(random.uniform(218, 222), 1),
        "frequency_hz": round(random.uniform(49.9, 50.1), 2),
        "efficiency_percent": round(random.uniform(82, 88), 1),
        "kafka_version": "3.7.1"
    }

def main():
    producer = create_producer()
    if not producer:
        return
    
    print(f"🚀 Починаємо відправку даних у топік '{TOPIC_NAME}'...")
    print("📊 Натисніть Ctrl+C для зупинки\n")
    
    message_count = 0
    
    try:
        while True:
            power_data = generate_power_data()
            
            try:
                future = producer.send(TOPIC_NAME, power_data)
                record_metadata = future.get(timeout=10)
                message_count += 1
                
                print(f"📤 [{message_count}] {power_data['station_name']} - {power_data['power_output_mw']} МВт")
                print(f"   Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                
            except Exception as e:
                print(f"❌ Помилка відправки: {e}")

            time.sleep(3)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Зупинено. Всього відправлено {message_count} повідомлень")
    
    finally:
        producer.flush()
        producer.close()
        print("🔌 З'єднання з Kafka 3.7.1 закрито")

if __name__ == "__main__":
    main()