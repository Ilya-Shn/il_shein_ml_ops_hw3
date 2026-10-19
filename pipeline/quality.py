import html
from pathlib import Path

import numpy as np
import pandas as pd

from app.preprocessing import CATEGORICAL, NUMERIC
from pipeline.storage import MART, REPORTS, atomic_json, locked, month, read_months, run_dir, state


def check():
    """Читает снимок витрины под блокировкой, пишет результаты DQ своего запуска; повтор не меняет данные."""
    with locked():
        files = sorted(p.stem for p in MART.glob("*.parquet"))
        frame = read_months(files)
        current = state()
    required = ["transaction_time", "target", "row_id"] + CATEGORICAL + NUMERIC
    schema = set(required).issubset(frame.columns)
    rules = {"Обязательные столбцы": schema, "Витрина непустая": len(frame) > 0}
    if schema:
        time = pd.to_datetime(frame.transaction_time, errors="coerce")
        rules["Время заполнено и разбирается"] = bool(time.notna().all())
        rules["Target заполнен, только 0 и 1"] = bool(frame.target.isin([0, 1]).all())
        rules["Уникальные непустые row_id"] = bool(frame.row_id.notna().all() and frame.row_id.is_unique)
        for column in ("amount", "population_city"):
            values = pd.to_numeric(frame[column], errors="coerce")
            rules[f"{column}: конечное неотрицательное число"] = bool((np.isfinite(values) & (values >= 0)).all())
        for column, bound in (("lat", 90), ("merchant_lat", 90), ("lon", 180), ("merchant_lon", 180)):
            values = pd.to_numeric(frame[column], errors="coerce")
            rules[f"{column}: допустимый диапазон"] = bool(values.between(-bound, bound).all())
        rules["Категории заполнены"] = bool(frame[CATEGORICAL].notna().all().all() and frame[CATEGORICAL].astype(str).apply(lambda c: c.str.strip().ne("").all()).all())
        periods = time.dt.to_period("M").astype(str)
        rules["История не глубже 36 месяцев"] = bool(periods.min() >= month(current["latest_month"], -35))
        rules["Месяцы непрерывны"] = files == [month(files[0], i) for i in range(len(files))]
        rules["В каждом месяце оба класса"] = bool(frame.groupby(periods).target.nunique().eq(2).all())
    result = {"rows": len(frame), "months": files, "rules": rules, "passed": all(rules.values())}
    path = run_dir() / "quality.json"
    atomic_json(path, result)
    return str(path)


def report(path):
    """Читает DQ JSON, атомарно публикует HTML до возможной ошибки; повтор заменяет отчёт своего запуска."""
    import json
    import os

    data = json.loads(Path(path).read_text())
    rows = "".join(f"<tr><td>{html.escape(k)}</td><td>{'OK' if v else 'ОШИБКА'}</td></tr>" for k, v in data["rules"].items())
    page = f'<!doctype html><html lang="ru"><meta charset="utf-8"><title>Качество данных</title><h1>Качество данных</h1><p>Строк: {data["rows"]}. Месяцы: {", ".join(data["months"])}</p><table border="1"><tr><th>Правило</th><th>Результат</th></tr>{rows}</table></html>'
    REPORTS.mkdir(parents=True, exist_ok=True)
    final = REPORTS / f"{Path(path).parent.name}.html"
    temp = final.with_suffix(".tmp")
    temp.write_text(page)
    os.replace(temp, final)
    (REPORTS / "latest.html").write_text(page)
    if not data["passed"]:
        raise ValueError(f"Нарушены правила DQ; отчёт {final}")
    return str(final)
