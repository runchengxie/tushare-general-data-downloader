"""Disk persistence helpers for fetched datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

from .windowing import format_yyyymmdd


@dataclass
class DatasetState:
    last_end_date: str
    windows: int = 0
    rows: int = 0


@dataclass
class DataStore:
    base_dir: Path
    file_format: str = "csv"

    def raw_dir(self, dataset: str) -> Path:
        return self.base_dir / "raw" / dataset

    def curated_dir(self) -> Path:
        return self.base_dir / "curated"

    def state_dir(self) -> Path:
        return self.base_dir / "state"

    def raw_window_path(self, dataset: str, start: date, end: date) -> Path:
        start_str = format_yyyymmdd(start)
        end_str = format_yyyymmdd(end)
        return self.raw_dir(dataset) / f"{dataset}_{start_str}_{end_str}.{self.file_format}"

    def raw_snapshot_path(self, dataset: str, run_date: date) -> Path:
        run_str = format_yyyymmdd(run_date)
        return self.raw_dir(dataset) / f"{dataset}_{run_str}.{self.file_format}"

    def curated_path(self, dataset: str) -> Path:
        return self.curated_dir() / f"{dataset}.{self.file_format}"

    def state_path(self, dataset: str) -> Path:
        return self.state_dir() / f"{dataset}.json"

    def write_frame(self, df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.file_format == "parquet":
            df.to_parquet(path, index=False)
        else:
            df.to_csv(path, index=False)

    def read_frame(self, path: Path) -> pd.DataFrame:
        if self.file_format == "parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path)

    def save_raw_window(
        self, dataset: str, start: date, end: date, df: pd.DataFrame
    ) -> Path:
        path = self.raw_window_path(dataset, start, end)
        self.write_frame(df, path)
        return path

    def save_raw_snapshot(self, dataset: str, run_date: date, df: pd.DataFrame) -> Path:
        path = self.raw_snapshot_path(dataset, run_date)
        self.write_frame(df, path)
        return path

    def save_curated(self, dataset: str, df: pd.DataFrame) -> Path:
        path = self.curated_path(dataset)
        self.write_frame(df, path)
        return path

    def load_state(self, dataset: str) -> DatasetState | None:
        path = self.state_path(dataset)
        if not path.exists():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        return DatasetState(
            last_end_date=raw.get("last_end_date", ""),
            windows=int(raw.get("windows", 0)),
            rows=int(raw.get("rows", 0)),
        )

    def update_state(self, dataset: str, end_date: date, rows: int, windows: int) -> None:
        state = DatasetState(
            last_end_date=format_yyyymmdd(end_date),
            rows=rows,
            windows=windows,
        )
        path = self.state_path(dataset)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state.__dict__, ensure_ascii=False, indent=2), encoding="utf-8")

    def iter_raw_files(self, dataset: str) -> Iterable[Path]:
        raw_dir = self.raw_dir(dataset)
        if not raw_dir.exists():
            return []
        return sorted(raw_dir.glob(f"{dataset}_*.{self.file_format}"))

    def consolidate(self, dataset: str, dedup_keys: list[str]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for path in self.iter_raw_files(dataset):
            frames.append(self.read_frame(path))
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        if dedup_keys:
            subset = [key for key in dedup_keys if key in merged.columns]
            if subset:
                merged = merged.drop_duplicates(subset=subset, keep="last")
        return merged
