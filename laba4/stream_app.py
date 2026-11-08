import json
import math
import os
import uuid
from datetime import datetime, timezone

import faust
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy

GUARANTEE = os.environ.get("GUARANTEE", "exactly_once")

WINDOW_MS = 60 * 1000
GRACE_MS = 15 * 1000
READING_STEP_SEC = 15

app = faust.App(
    f"lviv-stream-{GUARANTEE}",
    broker="kafka://localhost:9092",
    store="memory://",
    processing_guarantee=GUARANTEE,
    topic_partitions=6,
    topic_replication_factor=1,
    web_port=6066,
)


class Reading(faust.Record, serializer="json"):
    meter_id: str
    district: str
    ts: int
    active_power: float
    reactive_power: float
    voltage: float
    current: float
    frequency: float
    power_factor: float
    tariff_zone: str


raw_topic = app.topic("grid.raw", key_type=str, value_type=Reading)

# Faust Tables зі збереженням стану
profile_table = app.Table("meter_profiles", default=dict, partitions=6)
window_table = app.Table("window_state", default=dict, partitions=6)

NS = uuid.UUID("6f1c1d6e-3c1a-4a55-9a7e-2b7f0c0d4a04")
session = None
prepared = {}
dirty_profiles = {}
state = {"watermark": 0, "late_dropped": 0, "processed": 0, "windows_flushed": 0}


def cassandra():
    global session
    if session is None:
        cluster = Cluster(
            ["127.0.0.1"],
            protocol_version=5,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        )
        session = cluster.connect("lviv_streaming")
        prepared["event"] = session.prepare(
            "INSERT INTO lviv_streaming.consumption_event_log "
            "(district_name, day_bucket, event_time, event_id, event_type, meter_id, payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        prepared["window"] = session.prepare(
            "INSERT INTO lviv_streaming.window_aggregates "
            "(district_name, window_start, readings_count, total_consumption_kwh, "
            "avg_voltage, min_voltage, max_voltage, avg_frequency) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        prepared["profile"] = session.prepare(
            "INSERT INTO lviv_streaming.consumer_profile_state "
            "(meter_id, district_name, consumer_class, readings_count, mean_power, "
            "std_power, max_power, voltage_events, theft_events, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
    return session


def to_dt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def write_event(event_type, district, ts_ms, meter_id, payload):
    # Детермінований UUID v5 гарантує ідемпотентність
    event_id = uuid.uuid5(NS, f"{event_type}|{district}|{meter_id}|{ts_ms}")
    dt = to_dt(ts_ms)
    cassandra().execute(
        prepared["event"],
        (district, dt.date(), dt, event_id, event_type, meter_id, json.dumps(payload, ensure_ascii=False)),
    )


def flush_window(key, st):
    district = st.get("district")
    wstart = st.get("wstart")
    n = st.get("n", 0)
    if not district or not wstart or n == 0:
        return
    kwh = round(st["kw"] * READING_STEP_SEC / 3600.0, 3)
    avg_v = round(st["v"] / n, 2)
    avg_f = round(st["f"] / n, 3)
    cassandra().execute(
        prepared["window"],
        (district, to_dt(wstart), n, kwh, avg_v, float(st["vmin"]), float(st["vmax"]), avg_f),
    )
    write_event("CONSUMPTION_RECORDED", district, wstart, "-", {
        "window_start": wstart, "readings": n, "total_consumption_kwh": kwh,
        "avg_voltage": avg_v, "avg_frequency": avg_f,
    })
    state["windows_flushed"] += 1
    print(f"[window] Район {district}, {to_dt(wstart)}, показань {n}, {kwh} кВт-год")


@app.agent(raw_topic)
async def profile_agent(stream):
    """Профілювання лічильників алгоритмом Велфорда і детекція аномалій в реальному часі"""
    async for r in stream:
        cassandra()
        st = profile_table.get(r.meter_id)
        if not st or "n" not in st:
            st = {
                "district": r.district, "n": 0, "mean": 0.0, "m2": 0.0,
                "max": 0.0, "volt_ev": 0, "theft_ev": 0, "last": 0,
            }

        z = 0.0
        if st["n"] >= 20:
            std = math.sqrt(st["m2"] / (st["n"] - 1))
            if std > 0:
                z = (r.active_power - st["mean"]) / std

        # Перевірка напруги: норма 210-240 В
        if r.voltage < 210 or r.voltage > 240:
            st["volt_ev"] += 1
            write_event("VOLTAGE_DEVIATION", r.district, r.ts, r.meter_id,
                        {"voltage": r.voltage, "norm": "210-240"})

        # Підозра на крадіжку
        zero_power_with_load = r.active_power < 0.05 and r.current > 0.5
        sudden_drop = st["n"] >= 20 and st["mean"] > 0.2 and z < -3 and r.active_power < 0.05
        if zero_power_with_load or sudden_drop:
            st["theft_ev"] += 1
            write_event("THEFT_SUSPECTED", r.district, r.ts, r.meter_id,
                        {"active_power": r.active_power, "current": r.current, "z_score": round(z, 2)})

        # Оновлення статистики Велфорда
        st["n"] += 1
        delta = r.active_power - st["mean"]
        st["mean"] += delta / st["n"]
        st["m2"] += delta * (r.active_power - st["mean"])
        st["max"] = max(st["max"], r.active_power)
        st["last"] = r.ts
        profile_table[r.meter_id] = st
        dirty_profiles[r.meter_id] = st
        state["processed"] += 1


@app.agent(raw_topic)
async def window_agent(stream):
    """Tumbling Windows 1 хвилина по районах, стан накопичується в Faust Table"""
    async for r in stream:
        cassandra()
        wstart = (r.ts // WINDOW_MS) * WINDOW_MS
        if state["watermark"] > 0 and wstart + WINDOW_MS + GRACE_MS <= state["watermark"]:
            state["late_dropped"] += 1
            continue

        key = f"{r.district}|{wstart}"
        st = window_table.get(key)
        if not st or "n" not in st:
            st = {
                "district": r.district,
                "wstart": wstart,
                "n": 0,
                "kw": 0.0,
                "v": 0.0,
                "f": 0.0,
                "vmin": 9999.0,
                "vmax": 0.0,
                "closed": False,
            }
        st["n"] += 1
        st["kw"] += r.active_power
        st["v"] += r.voltage
        st["f"] += r.frequency
        st["vmin"] = min(st["vmin"], r.voltage)
        st["vmax"] = max(st["vmax"], r.voltage)
        window_table[key] = st

        # Перевірка ватермарки та закриття протермінованих вікон у межах поточної ітерації
        if r.ts > state["watermark"]:
            state["watermark"] = r.ts
            for k in list(window_table.keys()):
                item = window_table.get(k)
                if item and "wstart" in item and not item.get("closed"):
                    if item["wstart"] + WINDOW_MS <= state["watermark"]:
                        flush_window(k, item)
                        item["closed"] = True
                        window_table[k] = item


@app.timer(5.0)
async def flush_profiles():
    """Періодичний асинхронний запис змінених профілів у Cassandra"""
    if not dirty_profiles:
        return
    sess = cassandra()
    batch = dict(dirty_profiles)
    dirty_profiles.clear()

    rows = []
    for meter_id, st in batch.items():
        std = math.sqrt(st["m2"] / (st["n"] - 1)) if st["n"] > 1 else 0.0
        cls = "commercial" if st["mean"] > 3.0 else "residential"
        rows.append((
            meter_id, st["district"], cls, st["n"], round(st["mean"], 3), round(std, 3),
            st["max"], st["volt_ev"], st["theft_ev"], to_dt(st["last"])
        ))

    execute_concurrent_with_args(sess, prepared["profile"], rows, concurrency=50)
    print(f"[profiles] збережено {len(rows)} профілів, оброблено {state['processed']}, "
          f"закритих вікон {state['windows_flushed']}")


if __name__ == "__main__":
    app.main()
