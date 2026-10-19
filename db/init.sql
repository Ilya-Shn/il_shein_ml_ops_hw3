CREATE TABLE IF NOT EXISTS scores (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    score DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS scores_fraud_idx ON scores (id DESC) WHERE fraud_flag = 1;
