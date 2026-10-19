import base64
import json
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def api(path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request("http://localhost:8081/api/v1/" + path, data=data,
                                     headers={"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode(),
                                              "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def wait_run(dag, run):
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        data = api(f"dags/{dag}/dagRuns/{run}")
        if data["state"] == "success":
            return data
        if data["state"] == "failed":
            raise AssertionError(data)
        time.sleep(3)
    raise TimeoutError((dag, run))


def trigger(dag):
    run = "manual__check_" + uuid4().hex
    api(f"dags/{dag}/dagRuns", {"dag_run_id": run})
    return wait_run(dag, run)


def command(*args):
    return subprocess.check_output(["docker", "compose", *args], text=True, stderr=subprocess.STDOUT)


def main():
    start = datetime.now(timezone.utc).isoformat()
    errors = api("importErrors")
    assert not errors["import_errors"], errors
    dags = ["retrain_catboost", "supply_labeled_batch", "data_quality"]
    manual = {dag: trigger(dag) for dag in dags}
    smoke = command("exec", "-T", "scorer", "python", "-m", "tests.smoke")
    invariants = command("exec", "-T", "airflow-scheduler", "python", "-m", "unittest", "tests.test_pipeline", "-v")
    scheduled = {}
    deadline = time.monotonic() + 420
    while time.monotonic() < deadline and len(scheduled) < len(dags):
        for dag in dags:
            runs = api(f"dags/{dag}/dagRuns?order_by=-execution_date&limit=20")["dag_runs"]
            for run in runs:
                if run["run_type"] == "scheduled" and run["state"] == "success" and run["start_date"] >= start:
                    scheduled[dag] = run["dag_run_id"]
                    break
        if len(scheduled) < len(dags):
            time.sleep(5)
    assert len(scheduled) == len(dags), scheduled
    assert urllib.request.urlopen("http://localhost:8082/latest.html").status == 200
    state = json.loads(Path("runtime/models/state.json").read_text())
    result = {"manual": {d: r["dag_run_id"] for d, r in manual.items()}, "scheduled": scheduled,
              "state": state, "smoke": smoke, "invariants": invariants}
    Path("results/check.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
