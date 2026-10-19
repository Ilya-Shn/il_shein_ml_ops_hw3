import pandas as pd

from pipeline.storage import MART, MODELS, REPORTS, WORK, locked, save_state, write_frame


def main():
    with locked():
        if (MODELS / "state.json").exists():
            return
        for path in (MART, MODELS, REPORTS, WORK):
            path.mkdir(parents=True, exist_ok=True)
        frame = pd.read_csv("/data/train.csv")
        frame["transaction_time"] = pd.to_datetime(frame.transaction_time)
        frame["row_id"] = [f"original-{i}" for i in range(len(frame))]
        periods = frame.transaction_time.dt.to_period("M").astype(str)
        months = sorted(periods.unique())
        if len(months) < 3:
            raise ValueError("Нужны минимум три месяца")
        for value in months:
            write_frame(frame.loc[periods == value], MART / f"{value}.parquet")
        save_state({"active": None, "next_month": months[2], "latest_month": months[-1],
                    "training_runs": {}, "supply_runs": []})
        print(f"Загружено {len(frame)} строк, {len(months)} месяцев")


if __name__ == "__main__":
    main()
