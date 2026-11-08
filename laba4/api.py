import json
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

PORT = 8000
cluster = None
session = None


def get_cassandra():
    global cluster, session
    if session is None:
        cluster = Cluster(
            ["127.0.0.1"],
            protocol_version=5,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        )
        session = cluster.connect("lviv_streaming")
    return session


class BillingApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        sess = get_cassandra()

        if path == "/":
            self._send_json({
                "service": "Lviv Smart Grid Streaming Billing API",
                "variant": "4-A (Tumbling Windows 1 min + Event Sourcing)",
                "endpoints": [
                    "/events?district=Франківський&limit=50",
                    "/replay?meter_id=LVIV-00001",
                    "/aggregates?district=Франківський",
                    "/profile?meter_id=LVIV-00001",
                    "/districts",
                ]
            })

        elif path == "/events":
            district = params.get("district", ["Франківський"])[0]
            day_str = params.get("day", ["2026-11-08"])[0]
            limit = int(params.get("limit", [50])[0])

            query = (
                "SELECT district_name, day_bucket, event_time, event_id, event_type, meter_id, payload "
                "FROM lviv_streaming.consumption_event_log "
                "WHERE district_name = %s AND day_bucket = %s "
                "LIMIT %s"
            )
            rows = sess.execute(query, (district, datetime.strptime(day_str, "%Y-%m-%d").date(), limit))
            items = []
            for r in rows:
                payload = json.loads(r.payload) if r.payload else {}
                items.append({
                    "district": r.district_name,
                    "day": str(r.day_bucket),
                    "event_time": str(r.event_time),
                    "event_id": str(r.event_id),
                    "event_type": r.event_type,
                    "meter_id": r.meter_id,
                    "payload": payload,
                })
            self._send_json({"count": len(items), "district": district, "events": items})

        elif path == "/replay":
            meter_id = params.get("meter_id", ["LVIV-00001"])[0]
            p_rows = sess.execute(
                "SELECT * FROM lviv_streaming.consumer_profile_state WHERE meter_id = %s",
                (meter_id,)
            )
            prof = p_rows.one()
            if not prof:
                self._send_json({"error": f"Meter {meter_id} not found"}, status=404)
                return

            district = prof.district_name
            e_rows = sess.execute(
                "SELECT event_time, event_id, event_type, meter_id, payload "
                "FROM lviv_streaming.consumption_event_log "
                "WHERE district_name = %s AND day_bucket = %s",
                (district, datetime(2026, 11, 8).date())
            )

            meter_events = []
            for e in e_rows:
                if e.meter_id == meter_id:
                    meter_events.append({
                        "event_time": str(e.event_time),
                        "event_id": str(e.event_id),
                        "event_type": e.event_type,
                        "payload": json.loads(e.payload) if e.payload else {},
                    })

            meter_events.sort(key=lambda x: x["event_time"])

            audit_result = {
                "meter_id": meter_id,
                "district": district,
                "consumer_class": prof.consumer_class,
                "readings_processed": prof.readings_count,
                "mean_power_kw": prof.mean_power,
                "max_power_kw": prof.max_power,
                "std_power_kw": prof.std_power,
                "total_events_in_journal": len(meter_events),
                "voltage_deviations_count": prof.voltage_events,
                "theft_suspicions_count": prof.theft_events,
                "audit_verdict": "FLAGGED_FOR_INSPECTION" if prof.theft_events > 0 else "NORMAL",
                "timeline_replay": meter_events,
            }
            self._send_json(audit_result)

        elif path == "/aggregates":
            district = params.get("district", ["Франківський"])[0]
            rows = sess.execute(
                "SELECT district_name, window_start, readings_count, total_consumption_kwh, "
                "avg_voltage, min_voltage, max_voltage, avg_frequency "
                "FROM lviv_streaming.window_aggregates "
                "WHERE district_name = %s "
                "LIMIT 20",
                (district,)
            )
            items = []
            for r in rows:
                items.append({
                    "district": r.district_name,
                    "window_start": str(r.window_start),
                    "readings_count": r.readings_count,
                    "total_consumption_kwh": r.total_consumption_kwh,
                    "avg_voltage": r.avg_voltage,
                    "min_voltage": r.min_voltage,
                    "max_voltage": r.max_voltage,
                    "avg_frequency": r.avg_frequency,
                })
            self._send_json({"district": district, "windows": items})

        elif path == "/profile":
            meter_id = params.get("meter_id", ["LVIV-00001"])[0]
            rows = sess.execute(
                "SELECT * FROM lviv_streaming.consumer_profile_state WHERE meter_id = %s",
                (meter_id,)
            )
            prof = rows.one()
            if not prof:
                self._send_json({"error": f"Meter {meter_id} not found"}, status=404)
                return
            self._send_json({
                "meter_id": prof.meter_id,
                "district_name": prof.district_name,
                "consumer_class": prof.consumer_class,
                "readings_count": prof.readings_count,
                "mean_power": prof.mean_power,
                "std_power": prof.std_power,
                "max_power": prof.max_power,
                "voltage_events": prof.voltage_events,
                "theft_events": prof.theft_events,
                "last_seen": str(prof.last_seen),
            })

        elif path == "/districts":
            districts = [
                "Франківський", "Сихівський", "Личаківський",
                "Залізничний", "Шевченківський", "Галицький"
            ]
            summary = []
            for d in districts:
                w_row = sess.execute(
                    "SELECT * FROM lviv_streaming.window_aggregates WHERE district_name = %s LIMIT 1",
                    (d,)
                ).one()
                if w_row:
                    summary.append({
                        "district": d,
                        "latest_window": str(w_row.window_start),
                        "readings_count": w_row.readings_count,
                        "total_consumption_kwh": w_row.total_consumption_kwh,
                        "avg_voltage": w_row.avg_voltage,
                        "avg_frequency": w_row.avg_frequency,
                    })
                else:
                    summary.append({
                        "district": d,
                        "latest_window": "N/A",
                        "readings_count": 0,
                        "total_consumption_kwh": 0.0,
                        "avg_voltage": 230.0,
                        "avg_frequency": 50.0,
                    })
            self._send_json({"districts": summary})

        else:
            self._send_json({"error": "Not Found"}, status=404)

    def log_message(self, format, *args):
        return


def run():
    server_address = ("", PORT)
    httpd = HTTPServer(server_address, BillingApiHandler)
    print(f"Lviv Smart Grid Billing API слухає порт {PORT}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nЗупинка API сервера")
        httpd.server_close()


if __name__ == "__main__":
    run()

