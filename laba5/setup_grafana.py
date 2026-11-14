import base64
import json
import os
import urllib.error
import urllib.request

GRAFANA_URL = "http://localhost:3000"
AUTH = base64.b64encode(b"admin:admin").decode("ascii")


def api_call(method, path, payload=None, extra_headers=None):
    url = f"{GRAFANA_URL}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Basic {AUTH}")
    req.add_header("Content-Type", "application/json")
    if extra_headers:
        for k, v in extra_headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        return e.code, body


def main():
    # Додавання джерела даних Prometheus
    ds_payload = {
        "uid": "prometheus-ds",
        "name": "Prometheus",
        "type": "prometheus",
        "url": "http://localhost:9090",
        "access": "proxy",
        "isDefault": True,
    }
    code, res = api_call("POST", "/api/datasources", ds_payload)
    print(f"Data source Prometheus: статус {code}")

    # Імпорт дашборду Smart Grid Львів
    dash_path = os.path.join(os.path.dirname(__file__), "dashboard.json")
    with open(dash_path, "r", encoding="utf-8") as f:
        dashboard = json.load(f)

    code, res = api_call(
        "POST",
        "/api/dashboards/db",
        {"dashboard": dashboard, "overwrite": True},
    )
    print(f"Dashboard Smart Grid Львів: статус {code}")

    # Створення папки для правил алертингу
    api_call("POST", "/api/folders", {"uid": "smartgrid-alerts", "title": "SmartGrid Alerts"})

    # Додавання алерту Висока напруга в Grafana
    alert_payload = {
        "title": "Висока напруга",
        "ruleGroup": "smartgrid_voltage_rules",
        "folderUID": "smartgrid-alerts",
        "noDataState": "NoData",
        "execErrState": "Error",
        "for": "2m",
        "orgID": 1,
        "condition": "C",
        "annotations": {
            "summary": "Перевищення напруги понад 230 В",
        },
        "labels": {
            "severity": "critical",
        },
        "data": [
            {
                "refId": "A",
                "relativeTimeRange": {"from": 300, "to": 0},
                "datasourceUid": "prometheus-ds",
                "model": {
                    "expr": "smartgrid_voltage_v",
                    "instant": True,
                    "refId": "A",
                },
            },
            {
                "refId": "B",
                "relativeTimeRange": {"from": 300, "to": 0},
                "datasourceUid": "__expr__",
                "model": {
                    "type": "reduce",
                    "expression": "A",
                    "reducer": "max",
                    "refId": "B",
                },
            },
            {
                "refId": "C",
                "relativeTimeRange": {"from": 300, "to": 0},
                "datasourceUid": "__expr__",
                "model": {
                    "type": "threshold",
                    "expression": "B",
                    "conditions": [
                        {
                            "evaluator": {"params": [230], "type": "gt"},
                        }
                    ],
                    "refId": "C",
                },
            },
        ],
    }
    code, res = api_call(
        "POST",
        "/api/v1/provisioning/alert-rules",
        alert_payload,
        extra_headers={"X-Disable-Provenance": "true"},
    )
    print(f"Grafana Alert Висока напруга: статус {code}")


if __name__ == "__main__":
    main()
