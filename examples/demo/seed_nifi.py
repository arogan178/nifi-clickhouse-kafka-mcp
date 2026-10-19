"""Build a small demo flow with admin rights: a generator feeding a stopped sink, so a queue backs up."""

import time

import requests
import urllib3

urllib3.disable_warnings()
API = "https://localhost:8443/nifi-api"
s = requests.Session()
s.verify = False
s.headers["Authorization"] = (
    "Bearer " + s.post(f"{API}/access/token", data={"username": "admin", "password": "demo-password-123"}).text
)
root = s.get(f"{API}/flow/process-groups/root").json()["processGroupFlow"]["id"]


def create(path, body):
    response = s.post(f"{API}{path}", json=body)
    response.raise_for_status()
    return response.json()


for group in s.get(f"{API}/process-groups/{root}/process-groups").json()["processGroups"]:
    if group["component"]["name"] == "Ingest":
        raise SystemExit("the demo flow already exists")
ingest = create(
    f"/process-groups/{root}/process-groups",
    {"revision": {"version": 0}, "component": {"name": "Ingest", "position": {"x": 0, "y": 0}}},
)["id"]
context = create(
    "/parameter-contexts",
    {
        "revision": {"version": 0},
        "component": {
            "name": "ingest-params",
            "parameters": [
                {
                    "parameter": {
                        "name": "db.url",
                        "value": "jdbc:postgresql://db:5432/app?user=app&password=hunter2",
                        "sensitive": False,
                    }
                },
                {"parameter": {"name": "db.password", "value": "hunter2", "sensitive": True}},
                {"parameter": {"name": "batch.size", "value": "500", "sensitive": False}},
            ],
        },
    },
)
generate = create(
    f"/process-groups/{ingest}/processors",
    {
        "revision": {"version": 0},
        "component": {
            "type": "org.apache.nifi.processors.standard.GenerateFlowFile",
            "name": "Generate orders",
            "position": {"x": 0, "y": 0},
            "config": {
                "schedulingPeriod": "100 ms",
                "properties": {
                    "Custom Text": '{"id": 1, "customer_email": "a@example.com", "token": "abc"}',
                    "File Size": "0B",
                },
            },
        },
    },
)
sink = create(
    f"/process-groups/{ingest}/processors",
    {
        "revision": {"version": 0},
        "component": {
            "type": "org.apache.nifi.processors.standard.LogAttribute",
            "name": "Load to warehouse",
            "position": {"x": 0, "y": 300},
            "config": {"autoTerminatedRelationships": ["success"]},
        },
    },
)
create(
    f"/process-groups/{ingest}/connections",
    {
        "revision": {"version": 0},
        "component": {
            "name": "orders",
            "source": {"id": generate["id"], "groupId": ingest, "type": "PROCESSOR"},
            "destination": {"id": sink["id"], "groupId": ingest, "type": "PROCESSOR"},
            "selectedRelationships": ["success"],
            "backPressureObjectThreshold": 100,
        },
    },
)
s.put(
    f"{API}/processors/{generate['id']}/run-status", json={"revision": generate["revision"], "state": "RUNNING"}
).raise_for_status()
time.sleep(15)
print("ingest group", ingest, "parameter context", context["id"])
