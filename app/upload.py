import csv
import uuid

from app.broker import producer


REQUIRED = {"transaction_time", "amount", "lat", "lon", "merchant_lat", "merchant_lon"}


def read_csv(file):
    reader = csv.DictReader(file)
    if not REQUIRED.issubset(reader.fieldnames or []):
        raise ValueError("В CSV отсутствуют поля: " + ", ".join(sorted(REQUIRED - set(reader.fieldnames or []))))
    rows = list(reader)
    if not rows:
        raise ValueError("CSV не содержит транзакций")
    return rows


def send_rows(rows):
    output = producer()
    ids = []
    try:
        for row in rows:
            transaction_id = str(row.get("transaction_id") or uuid.uuid4())
            output.send("transactions", key=transaction_id.encode(), value={
                "transaction_id": transaction_id,
                "data": row,
            }).get(timeout=30)
            ids.append(transaction_id)
    finally:
        output.close()
    return ids
