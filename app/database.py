import os

import psycopg
from psycopg.rows import dict_row


DSN = os.getenv("DATABASE_URL", "postgresql://fraud:fraud@postgres:5432/fraud")


def connect():
    return psycopg.connect(DSN, row_factory=dict_row)


def save(connection, result):
    connection.execute(
        "INSERT INTO scores (transaction_id, score, fraud_flag, model_version) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (transaction_id) DO NOTHING",
        (result["transaction_id"], result["score"], result["fraud_flag"], result["model_version"]),
    )
    connection.commit()


def results(connection):
    fraud = connection.execute(
        "SELECT transaction_id, score, fraud_flag, model_version FROM scores "
        "WHERE fraud_flag = 1 ORDER BY id DESC LIMIT 10"
    ).fetchall()
    latest = connection.execute(
        "SELECT score FROM scores ORDER BY id DESC LIMIT 100"
    ).fetchall()
    return fraud, latest
