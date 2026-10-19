import json
import os
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

from app.preprocessing import transform


class Scorer:
    def __init__(self):
        self.models = Path(os.getenv("RUNTIME_DIR", "/runtime")) / "models"
        self.version = None
        self.model = None

    def score(self, message):
        version = json.loads((self.models / "state.json").read_text())["active"]
        if not version:
            raise RuntimeError("Ожидается первая модель")
        if self.version != version:
            model = CatBoostClassifier().load_model(str(self.models / f"{version}.cbm"))
            self.model, self.version = model, version
        transaction_id = str(message["transaction_id"]).strip()
        if not transaction_id:
            raise ValueError("Пустой идентификатор")
        frame = pd.DataFrame([message.get("data", message)])
        score = float(self.model.predict_proba(transform(frame), thread_count=1)[0, 1])
        return {"transaction_id": transaction_id, "score": score, "fraud_flag": int(score > 0.98), "model_version": self.version}
