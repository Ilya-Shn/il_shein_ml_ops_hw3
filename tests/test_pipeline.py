import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from catboost import CatBoostClassifier

from app.scorer import Scorer
from app.preprocessing import CATEGORICAL, transform
from pipeline import quality, retrain, storage, supply


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.patches = []
        for module in (storage, retrain, supply):
            for name in ("ROOT", "MART", "MODELS", "WORK"):
                if hasattr(module, name):
                    value = self.root if name == "ROOT" else self.root / name.lower()
                    value.mkdir(parents=True, exist_ok=True)
                    p = patch.object(module, name, value)
                    p.start()
                    self.patches.append(p)
        raw = pd.read_parquet(sorted(Path("/runtime/mart").glob("*.parquet"))[0])
        self.frame = pd.concat([raw[raw.target == i].head(80) for i in (0, 1)], ignore_index=True)
        self.current = {"active": None, "next_month": "2019-03", "latest_month": "2019-03", "training_runs": {}, "supply_runs": []}
        storage.save_state(self.current)
        self.run = self.root / "run"
        self.run.mkdir()
        self.info = {"path": str(self.run), "version": "v1", "month": "2019-03", "previous": None, "accepted": True}
        self.model = CatBoostClassifier(iterations=4, depth=2, thread_count=1, verbose=False, allow_writing_files=False)
        self.model.fit(transform(self.frame), self.frame.target, cat_features=CATEGORICAL)
        self.model.save_model(str(self.run / "candidate.cbm"))

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.temp.cleanup()

    def test_publish_retry_and_hot_reload(self):
        with patch.object(retrain, "save_state", side_effect=OSError("Сбой после сохранения бинарника")):
            with self.assertRaises(OSError):
                retrain.publish(self.info)
        self.assertEqual(storage.state()["next_month"], "2019-03")
        self.assertIsNone(storage.state()["active"])
        self.assertTrue(retrain.publish(self.info))
        self.assertTrue(retrain.publish(self.info))
        self.assertEqual(storage.state()["next_month"], "2019-04")
        scorer = Scorer()
        scorer.models = storage.MODELS
        message = {"transaction_id": "test", "data": self.frame.iloc[0].to_dict()}
        self.assertEqual(scorer.score(message)["model_version"], "v1")
        self.model.save_model(str(self.run / "candidate.cbm"))
        info = {**self.info, "version": "v2", "month": "2019-04", "previous": "v1"}
        retrain.publish(info)
        self.assertEqual(scorer.score(message)["model_version"], "v2")

    def test_reject_keeps_pointer(self):
        retrain.publish({**self.info, "accepted": False})
        self.assertEqual(storage.state()["next_month"], "2019-03")
        self.assertIsNone(storage.state()["active"])
        self.assertFalse((storage.MODELS / "v1.cbm").exists())

    def test_compare_and_temporal_split(self):
        for i in (1, 2, 3):
            frame = self.frame.copy()
            frame["transaction_time"] = pd.Timestamp(2019, i, 1)
            storage.write_frame(frame, storage.MART / f"2019-{i:02}.parquet")
        with patch.object(retrain, "run_dir", return_value=self.run):
            info = retrain.prepare()
            train = pd.read_parquet(self.run / "train.parquet")
            test = pd.read_parquet(self.run / "test.parquet")
            self.assertLess(train.transaction_time.max(), test.transaction_time.min())
            self.assertEqual(info["months"], ["2019-01", "2019-02", "2019-03"])
            self.assertEqual(retrain.prepare(), info)
        info["previous"] = "old"
        self.model.save_model(str(storage.MODELS / "old.cbm"))
        with patch.object(retrain, "metrics", side_effect=[{"pr_auc": .4, "roc_auc": .8}, {"pr_auc": .5, "roc_auc": .8}]):
            self.assertFalse(retrain.evaluate(info)["accepted"])

    def test_supply_retry_and_retention(self):
        storage.write_frame(self.frame, storage.MART / "2019-03.parquet")
        with patch.object(supply, "run_dir", return_value=self.run):
            info = supply.sample()
            self.assertEqual(supply.sample(), info)
        supply.append(info)
        supply.append(info)
        batch = pd.read_parquet(storage.MART / "2019-04.parquet")
        self.assertEqual(len(batch), 100000)
        self.assertEqual(batch.transaction_time.dt.to_period("M").astype(str).unique().tolist(), ["2019-04"])
        current = storage.state()
        current.update(latest_month="2022-03", next_month="2022-03")
        storage.save_state(current)
        supply.prune(info)
        self.assertFalse((storage.MART / "2019-03.parquet").exists())
        self.assertTrue((storage.MART / "2019-04.parquet").exists())

    def test_dq_failure_writes_report(self):
        reports = self.root / "reports"
        reports.mkdir()
        with patch.object(quality, "MART", storage.MART), patch.object(quality, "REPORTS", reports), patch.object(quality, "run_dir", return_value=self.run):
            frame = self.frame.copy()
            frame.loc[0, "amount"] = -1
            frame.loc[0, "target"] = 7
            frame["row_id"] = "duplicate"
            storage.write_frame(frame, storage.MART / "2019-03.parquet")
            path = quality.check()
            data = json.loads(Path(path).read_text())
            self.assertFalse(data["passed"])
            with self.assertRaises(ValueError):
                quality.report(path)
            self.assertTrue((reports / "latest.html").exists())


if __name__ == "__main__":
    unittest.main()
