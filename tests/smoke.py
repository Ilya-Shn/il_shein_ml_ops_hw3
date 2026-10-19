import json
import time
from uuid import uuid4

import pandas as pd

from app.broker import consumer, decode
from app.database import connect
from app.scorer import Scorer
from app.upload import send_rows


frame = pd.read_csv("/data/test.csv", nrows=120).astype(object)
rows = frame.where(frame.notna(), None).to_dict("records")
for row in rows:
    row["transaction_id"] = "check-" + uuid4().hex
source = consumer("scores", "check-" + uuid4().hex)
source.config["consumer_timeout_ms"] = 1000
ids = set(send_rows(rows))
scorer = Scorer()
expected = {row["transaction_id"]: scorer.score(row) for row in rows}
found = {}
deadline = time.monotonic() + 90
while time.monotonic() < deadline and len(found) < len(ids):
    for messages in source.poll(timeout_ms=1000).values():
        for message in messages:
            result = decode(message)
            if result["transaction_id"] in ids:
                found[result["transaction_id"]] = result
source.close()
assert set(found) == ids, len(found)
for key, result in found.items():
    assert 0 <= result["score"] <= 1
    assert result["fraud_flag"] == int(result["score"] > 0.98)
    if result["model_version"] == expected[key]["model_version"]:
        assert abs(result["score"] - expected[key]["score"]) < 1e-12
with connect() as connection:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        results = connection.execute("SELECT * FROM scores WHERE transaction_id = ANY(%s)", (list(ids),)).fetchall()
        if len(results) == len(ids):
            break
        time.sleep(1)
    assert len(results) == len(ids)
    for row in results:
        assert row["model_version"] == found[row["transaction_id"]]["model_version"]
print(json.dumps({"transactions": len(ids), "versions": sorted({r["model_version"] for r in found.values()}), "postgres": len(results)}))
