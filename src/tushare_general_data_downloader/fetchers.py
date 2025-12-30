"""Dataset fetchers for listed company data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

import os
import pandas as pd
import tushare as ts

from .api import FetchRunner
from .constants import (
    DATASET_SHARE_FLOAT,
    DATASET_STK_MANAGERS,
    DEDUP_KEYS,
    DEFAULT_FIELDS,
    DEFAULT_SHARE_FLOAT_THRESHOLD,
    ENV_FIELD_OVERRIDES,
)
from .storage import DataStore
from .windowing import (
    DateWindow,
    format_yyyymmdd,
    iter_day_ranges,
    iter_month_ranges,
    iter_week_ranges,
    parse_yyyymmdd,
)


@dataclass
class FetchSummary:
    dataset: str
    windows: int = 0
    rows: int = 0
    files: int = 0


class ListedCompanyFetcher:
    def __init__(self, pro: ts.pro_api, runner: FetchRunner, store: DataStore) -> None:
        self.pro = pro
        self.runner = runner
        self.store = store

    def _resolve_fields(self, dataset: str) -> str | None:
        env_key = ENV_FIELD_OVERRIDES.get(dataset)
        if env_key:
            override = os.getenv(env_key)
            if override:
                return override
        return DEFAULT_FIELDS.get(dataset)

    def _fetch_with_fields(self, label: str, fn, fields: str | None):
        if fields:
            return self.runner.call(label, lambda: fn(fields=fields))
        return self.runner.call(label, fn)

    def _dedup(self, dataset: str, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        keys = DEDUP_KEYS.get(dataset, [])
        subset = [key for key in keys if key in df.columns]
        if subset:
            return df.drop_duplicates(subset=subset, keep="last")
        return df.drop_duplicates()

    def fetch_stock_basic(self, list_status: str) -> FetchSummary:
        fields = self._resolve_fields("stock_basic")
        df = self._fetch_with_fields(
            f"stock_basic list_status={list_status or 'ALL'}",
            lambda fields=None: self.pro.stock_basic(list_status=list_status, fields=fields),
            fields,
        )
        if df is None:
            df = pd.DataFrame()
        df = self._dedup("stock_basic", df)
        run_date = date.today()
        self.store.save_raw_snapshot("stock_basic", run_date, df)
        self.store.save_curated("stock_basic", df)
        return FetchSummary(dataset="stock_basic", windows=1, rows=len(df), files=2)

    def fetch_stock_company(self, exchanges: Iterable[str]) -> FetchSummary:
        fields = self._resolve_fields("stock_company")
        frames: list[pd.DataFrame] = []
        windows = 0
        for exchange in exchanges:
            label = f"stock_company exchange={exchange}"
            df = self._fetch_with_fields(
                label,
                lambda fields=None, exchange=exchange: self.pro.stock_company(
                    exchange=exchange, fields=fields
                ),
                fields,
            )
            windows += 1
            if df is None or df.empty:
                continue
            frames.append(df)
        if frames:
            merged = pd.concat(frames, ignore_index=True)
            merged = self._dedup("stock_company", merged)
        else:
            merged = pd.DataFrame()
        run_date = date.today()
        self.store.save_raw_snapshot("stock_company", run_date, merged)
        self.store.save_curated("stock_company", merged)
        return FetchSummary(dataset="stock_company", windows=windows, rows=len(merged), files=2)

    def _iter_windows(self, window: str, start: date, end: date) -> list[DateWindow]:
        if window == "day":
            return iter_day_ranges(start, end)
        if window == "week":
            return iter_week_ranges(start, end)
        if window == "month":
            return iter_month_ranges(start, end)
        raise ValueError(f"Unsupported window: {window}")

    def fetch_stk_managers(
        self,
        start: date,
        end: date,
        *,
        window: str,
        resume: bool,
        force: bool,
    ) -> FetchSummary:
        dataset = DATASET_STK_MANAGERS
        fields = self._resolve_fields(dataset)
        state = self.store.load_state(dataset)
        if resume and state and state.last_end_date:
            start = max(start, parse_yyyymmdd(state.last_end_date) + timedelta(days=1))
        windows = self._iter_windows(window, start, end)
        summary = FetchSummary(dataset=dataset)
        for win in windows:
            path = self.store.raw_window_path(dataset, win.start, win.end)
            if path.exists() and not force:
                summary.windows += 1
                continue
            label = f"stk_managers {format_yyyymmdd(win.start)}->{format_yyyymmdd(win.end)}"
            df = self._fetch_with_fields(
                label,
                lambda fields=None, start=win.start, end=win.end: self.pro.stk_managers(
                    start_date=format_yyyymmdd(start),
                    end_date=format_yyyymmdd(end),
                    fields=fields,
                ),
                fields,
            )
            if df is None:
                df = pd.DataFrame()
            df = self._dedup(dataset, df)
            self.store.save_raw_window(dataset, win.start, win.end, df)
            summary.files += 1
            summary.windows += 1
            summary.rows += len(df)
            self.store.update_state(dataset, win.end, summary.rows, summary.windows)
        return summary

    def fetch_share_float(
        self,
        start: date,
        end: date,
        *,
        window: str,
        resume: bool,
        force: bool,
        threshold: int = DEFAULT_SHARE_FLOAT_THRESHOLD,
    ) -> FetchSummary:
        dataset = DATASET_SHARE_FLOAT
        fields = self._resolve_fields(dataset)
        state = self.store.load_state(dataset)
        if resume and state and state.last_end_date:
            start = max(start, parse_yyyymmdd(state.last_end_date) + timedelta(days=1))
        windows = self._iter_windows(window, start, end)
        summary = FetchSummary(dataset=dataset)
        for win in windows:
            summary = self._process_share_float_window(
                win,
                fields=fields,
                force=force,
                threshold=threshold,
                summary=summary,
            )
        return summary

    def _process_share_float_window(
        self,
        win: DateWindow,
        *,
        fields: str | None,
        force: bool,
        threshold: int,
        summary: FetchSummary,
    ) -> FetchSummary:
        dataset = DATASET_SHARE_FLOAT
        path = self.store.raw_window_path(dataset, win.start, win.end)
        if path.exists() and not force:
            summary.windows += 1
            return summary

        label = f"share_float {format_yyyymmdd(win.start)}->{format_yyyymmdd(win.end)}"
        df = self._fetch_with_fields(
            label,
            lambda fields=None, start=win.start, end=win.end: self.pro.share_float(
                start_date=format_yyyymmdd(start),
                end_date=format_yyyymmdd(end),
                fields=fields,
            ),
            fields,
        )
        if df is None:
            df = pd.DataFrame()

        if len(df) >= threshold and win.start < win.end:
            print(
                f"{label} returned {len(df)} rows (near limit); splitting into daily windows."
            )
            for day_win in iter_day_ranges(win.start, win.end):
                summary = self._process_share_float_day(
                    day_win,
                    fields=fields,
                    force=force,
                    threshold=threshold,
                    summary=summary,
                )
            self.store.update_state(dataset, win.end, summary.rows, summary.windows)
            return summary

        df = self._dedup(dataset, df)
        self.store.save_raw_window(dataset, win.start, win.end, df)
        summary.files += 1
        summary.windows += 1
        summary.rows += len(df)
        self.store.update_state(dataset, win.end, summary.rows, summary.windows)
        return summary

    def _process_share_float_day(
        self,
        win: DateWindow,
        *,
        fields: str | None,
        force: bool,
        threshold: int,
        summary: FetchSummary,
    ) -> FetchSummary:
        dataset = DATASET_SHARE_FLOAT
        path = self.store.raw_window_path(dataset, win.start, win.end)
        if path.exists() and not force:
            summary.windows += 1
            return summary

        label = f"share_float {format_yyyymmdd(win.start)}"
        df = self._fetch_with_fields(
            label,
            lambda fields=None, start=win.start, end=win.end: self.pro.share_float(
                start_date=format_yyyymmdd(start),
                end_date=format_yyyymmdd(end),
                fields=fields,
            ),
            fields,
        )
        if df is None:
            df = pd.DataFrame()
        if len(df) >= threshold:
            print(
                f"Warning: {label} returned {len(df)} rows; data may be truncated."
            )
        df = self._dedup(dataset, df)
        self.store.save_raw_window(dataset, win.start, win.end, df)
        summary.files += 1
        summary.windows += 1
        summary.rows += len(df)
        self.store.update_state(dataset, win.end, summary.rows, summary.windows)
        return summary
