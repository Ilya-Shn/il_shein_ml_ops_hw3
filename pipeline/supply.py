import json
from pathlib import Path

import pandas as pd

from pipeline.storage import MART, atomic_json, locked, month, read_months, run_dir, save_state, state, write_frame


def sample():
    """Читает витрину, пишет план и 100000 строк следующего месяца; повтор читает тот же план без повторного сдвига."""
    path = run_dir()
    with locked():
        plan = path / "supply.json"
        if plan.exists():
            return json.loads(plan.read_text())
        current = state()
        source = sorted(p.stem for p in MART.glob("*.parquet"))
        target = month(current["latest_month"], 1)
        frame = read_months(source).sample(n=100000, replace=True, random_state=42).reset_index(drop=True)
        time = pd.to_datetime(frame.transaction_time)
        start = pd.Period(target).start_time
        day = time.dt.day.clip(upper=pd.Period(target).days_in_month)
        frame["transaction_time"] = start + pd.to_timedelta(day - 1, unit="D") + (time - time.dt.normalize())
        frame["row_id"] = [f"{target}-{i}" for i in range(len(frame))]
        write_frame(frame, path / "batch.parquet")
        info = {"path": str(path), "month": target, "run": path.name}
        atomic_json(plan, info)
        return info


def append(info):
    """Читает батч, атомарно заменяет месячную партицию и фиксирует поставку; повтор не дублирует строки и месяц."""
    with locked():
        current = state()
        if info["run"] in current["supply_runs"]:
            return info
        if month(current["latest_month"], 1) != info["month"]:
            raise RuntimeError("Изменился указатель поставки")
        write_frame(pd.read_parquet(Path(info["path"]) / "batch.parquet"), MART / f'{info["month"]}.parquet')
        current["latest_month"] = info["month"]
        current["supply_runs"].append(info["run"])
        save_state(current)
    return info


def prune(info):
    """Удаляет месяцы за пределами 36 месяцев истории; повторное удаление безопасно, нужное окно защищено от удаления."""
    with locked():
        current = state()
        cutoff = month(current["latest_month"], -35)
        protected = month(current["next_month"], -2)
        if cutoff > protected:
            raise RuntimeError("Переобучение отстало: очистка затронет его окно")
        for path in MART.glob("*.parquet"):
            if path.stem < cutoff:
                path.unlink(missing_ok=True)
    return info
