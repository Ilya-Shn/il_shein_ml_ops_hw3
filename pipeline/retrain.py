import json
import os
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

from app.preprocessing import CATEGORICAL, transform
from pipeline.storage import MART, MODELS, atomic_json, locked, month, read_months, run_dir, save_state, state, write_frame


def prepare():
    """Читает три месяца витрины, пишет неизменяемый split запуска; повтор использует прежний план, указатель не меняется."""
    from airflow.exceptions import AirflowSkipException

    path = run_dir()
    plan = path / "plan.json"
    if plan.exists():
        return json.loads(plan.read_text())
    with locked():
        current = state()
        end = current["next_month"]
        months = [month(end, i) for i in (-2, -1, 0)]
        if not all((MART / f"{m}.parquet").exists() for m in months):
            raise AirflowSkipException("Ожидаем следующий полный месяц витрины")
        train = read_months(months[:2])
        test = read_months(months[2:])
        if train.target.nunique() != 2 or test.target.nunique() != 2:
            raise ValueError("В train и test должны присутствовать оба класса")
        write_frame(train, path / "train.parquet")
        write_frame(test, path / "test.parquet")
        info = {"path": str(path), "month": end, "months": months, "previous": current["active"], "version": path.name,
                "train_rows": len(train), "test_rows": len(test)}
        atomic_json(plan, info)
        return info


def fit(info):
    """Читает split, обучает CPU CatBoost, пишет кандидат; повтор перезаписывает только артефакт своего запуска."""
    path = Path(info["path"])
    train = pd.read_parquet(path / "train.parquet")
    model = CatBoostClassifier(iterations=80, depth=5, learning_rate=0.12, loss_function="Logloss",
                               auto_class_weights="Balanced", random_seed=42, thread_count=2,
                               verbose=False, allow_writing_files=False)
    model.fit(transform(train), train.target, cat_features=CATEGORICAL)
    model.save_model(str(path / "candidate.cbm"))
    return info


def metrics(model, test):
    scores = model.predict_proba(transform(test), thread_count=2)[:, 1]
    return {"pr_auc": float(average_precision_score(test.target, scores)),
            "roc_auc": float(roc_auc_score(test.target, scores))}


def evaluate(info):
    """Читает кандидат и prod, сравнивает на одном test, пишет решение; повтор не изменяет prod и окно."""
    path = Path(info["path"])
    test = pd.read_parquet(path / "test.parquet")
    candidate = CatBoostClassifier().load_model(str(path / "candidate.cbm"))
    fresh = metrics(candidate, test)
    previous = info["previous"]
    old = metrics(CatBoostClassifier().load_model(str(MODELS / f"{previous}.cbm")), test) if previous else None
    accepted = old is None or all(fresh[key] >= old[key] for key in fresh)
    decision = {**info, "candidate": fresh, "production": old, "accepted": accepted}
    atomic_json(path / "evaluation.json", decision)
    return decision


def publish(info):
    """Сохраняет бинарник и атомарно фиксирует prod с указателем; повтор после сбоя не пропускает месяц, отклонение оставляет окно."""
    with locked():
        current = state()
        if info["version"] in current["training_runs"]:
            return current["training_runs"][info["version"]]
        if current["next_month"] != info["month"] or current["active"] != info["previous"]:
            raise RuntimeError("Состояние изменилось после подготовки")
        atomic_json(MODELS / f'{info["version"]}.metrics.json', info)
        if info["accepted"]:
            final = MODELS / f'{info["version"]}.cbm'
            if not final.exists():
                os.replace(Path(info["path"]) / "candidate.cbm", final)
            current["active"] = info["version"]
            current["next_month"] = month(info["month"], 1)
        current["training_runs"][info["version"]] = info["accepted"]
        save_state(current)
        return info["accepted"]
