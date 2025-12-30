from datetime import date

import pandas as pd

from tushare_general_data_downloader.api import FetchRunner, RateLimiter
from tushare_general_data_downloader.fetchers import ListedCompanyFetcher
from tushare_general_data_downloader.storage import DataStore


class FakePro:
    def __init__(self, counts: dict[tuple[str, str], int]):
        self.counts = counts
        self.calls: list[tuple[str, str]] = []

    def share_float(self, start_date: str, end_date: str, fields=None):
        self.calls.append((start_date, end_date))
        count = self.counts.get((start_date, end_date), 0)
        if count == 0:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ"] * count,
                "float_date": [start_date] * count,
                "holder_name": ["holder"] * count,
                "share_type": ["A"] * count,
                "ann_date": [start_date] * count,
            }
        )


def test_share_float_autosplit(tmp_path):
    weekly_key = ("20240101", "20240107")
    counts = {
        weekly_key: 6,
        ("20240101", "20240101"): 1,
        ("20240102", "20240102"): 1,
        ("20240103", "20240103"): 1,
        ("20240104", "20240104"): 1,
        ("20240105", "20240105"): 1,
        ("20240106", "20240106"): 1,
        ("20240107", "20240107"): 1,
    }
    pro = FakePro(counts)
    runner = FetchRunner(rate_limiter=RateLimiter(min_interval=0))
    store = DataStore(base_dir=tmp_path, file_format="csv")
    fetcher = ListedCompanyFetcher(pro, runner, store)

    summary = fetcher.fetch_share_float(
        start=date(2024, 1, 1),
        end=date(2024, 1, 7),
        window="week",
        resume=False,
        force=True,
        threshold=5,
    )

    assert summary.windows == 7
    weekly_path = store.raw_window_path("share_float", date(2024, 1, 1), date(2024, 1, 7))
    assert not weekly_path.exists()
    daily_path = store.raw_window_path("share_float", date(2024, 1, 3), date(2024, 1, 3))
    assert daily_path.exists()
