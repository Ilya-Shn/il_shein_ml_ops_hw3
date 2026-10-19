import fcntl
import json
import os
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pandas as pd


ROOT = Path(os.getenv("RUNTIME_DIR", "/runtime"))
MODELS = ROOT / "models"
REPORTS = ROOT / "reports"
MART = ROOT / "mart"
WORK = ROOT / "work"


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid4().hex)
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))
    os.replace(temp, path)


def state():
    return json.loads((MODELS / "state.json").read_text())


def save_state(value):
    atomic_json(MODELS / "state.json", value)


@contextmanager
def locked():
    ROOT.mkdir(parents=True, exist_ok=True)
    with (ROOT / ".lock").open("w") as file:
        fcntl.flock(file, fcntl.LOCK_EX)
        yield


def month(value, offset=0):
    return str(pd.Period(value, freq="M") + offset)


def write_frame(frame, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp.parquet")
    frame.to_parquet(temp, index=False)
    os.replace(temp, path)


def read_months(months):
    return pd.concat([pd.read_parquet(MART / f"{m}.parquet") for m in months], ignore_index=True)


def run_dir():
    from airflow.operators.python import get_current_context

    import hashlib

    context = get_current_context()
    key = context["dag"].dag_id + context["run_id"]
    path = WORK / hashlib.sha256(key.encode()).hexdigest()[:24]
    path.mkdir(parents=True, exist_ok=True)
    return path
