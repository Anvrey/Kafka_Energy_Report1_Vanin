import json
import urllib.parse
import urllib.request

PROM_URL = "http://localhost:9090/api/v1/query"

# Запити PromQL для базового аналізу та Варіанту 4 Підваріанту А
QUERIES = [
    (
        "Середня потужність споживання по всіх районах у кВт",
        "avg(smartgrid_power_kw)",
    ),
    (
        "Максимальна напруга по районах за останню годину",
        "max_over_time(smartgrid_voltage_v[1h])",
    ),
    (
        "Кількість успішних повідомлень за останні 5 хвилин",
        'sum(increase(energy_messages_sent_total{status="success"}[5m]))',
    ),
    (
        "Район з найвищим поточним споживанням",
        "topk(1, smartgrid_power_kw)",
    ),
    (
        "Перевірка відхилення частоти понад 0.2 Гц",
        "abs(smartgrid_frequency_hz - 50) > 0.2",
    ),
    (
        "Статистика якості середня напруга по районах за 15 хвилин",
        "avg_over_time(smartgrid_voltage_v[15m])",
    ),
    (
        "Статистика якості середній power factor по районах і типах споживачів",
        "avg by (district, consumer_type) (smartgrid_power_factor)",
    ),
    (
        "Часи найгірших відхилень максимальне відхилення напруги від 220 В за годину",
        "max_over_time(abs(smartgrid_voltage_v - 220)[1h:15s])",
    ),
    (
        "Відповідність стандартам якості по районах у відсотках",
        "(1 - (sum by (district) (increase(smartgrid_quality_violations_total[15m])) / clamp_min(sum by (district) (increase(smartgrid_readings_total[15m])), 1))) * 100",
    ),
    (
        "Топ проблемних районів за кількістю порушень якості",
        "topk(3, sum by (district) (increase(smartgrid_quality_violations_total[15m])))",
    ),
    (
        "Швидкість надходження повідомлень у Kafka за секунду",
        "sum(kafka_server_brokertopicmetrics_messagesinpersec)",
    ),
    (
        "Максимальний лаг споживача Kafka",
        "max(kafka_consumergroup_lag)",
    ),
    (
        "Затримка читання і запису Cassandra у мс",
        "cassandra_table_writelatency",
    ),
]


def query_prometheus(expr):
    params = urllib.parse.urlencode({"query": expr})
    url = f"{PROM_URL}?{params}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload.get("data", {}).get("result", [])


def main():
    print("Аналіз даних моніторингу Smart Grid м. Львова\n")
    for title, expr in QUERIES:
        print(f"Запит: {title}")
        print(f"PromQL: {expr}")
        try:
            results = query_prometheus(expr)
            if not results:
                print("  Немає перевищень або даних")
            for item in results:
                labels = item.get("metric", {})
                val = float(item["value"][1])
                label_str = ", ".join(f"{k}={v}" for k, v in labels.items()) or "total"
                print(f"  {label_str}: {val:.2f}")
        except Exception as e:
            print(f"  Помилка виконання запиту: {e}")
        print()


if __name__ == "__main__":
    main()
