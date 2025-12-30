"""Command line entrypoint for listed company data fetch."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import tushare as ts

from .api import FetchRunner, RateLimiter
from .constants import (
    ALL_DATASETS,
    DATASET_SHARE_FLOAT,
    DATASET_STK_MANAGERS,
    DEDUP_KEYS,
    DEFAULT_EXCHANGES,
    DEFAULT_MANAGERS_WINDOW,
    DEFAULT_SHARE_FLOAT_THRESHOLD,
    DEFAULT_SHARE_FLOAT_WINDOW,
    DEFAULT_YEARS,
)
from .env import load_local_env
from .fetchers import ListedCompanyFetcher
from .storage import DataStore
from .windowing import format_yyyymmdd, resolve_date_range

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_datasets(raw: str | None) -> list[str]:
    if not raw:
        return list(ALL_DATASETS)
    datasets = _parse_csv_list(raw)
    invalid = sorted(set(datasets) - set(ALL_DATASETS))
    if invalid:
        raise SystemExit(f"Unsupported dataset(s): {', '.join(invalid)}")
    return datasets


def _parse_exchanges(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return DEFAULT_EXCHANGES
    return tuple(_parse_csv_list(raw))


def init_tushare(token: str) -> ts.pro_api:
    ts.set_token(token)
    return ts.pro_api()


def _save_consolidated(store: DataStore, dataset: str) -> tuple[int, Path | None]:
    df = store.consolidate(dataset, DEDUP_KEYS.get(dataset, []))
    if df.empty:
        return 0, None
    path = store.save_curated(dataset, df)
    return len(df), path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Fetch TuShare listed-company datasets")
    parser.add_argument("--token", default="", help="TuShare token (or set TUSHARE_TOKEN)")
    parser.add_argument(
        "--datasets",
        default=None,
        help=f"Comma-separated datasets (default: {', '.join(ALL_DATASETS)})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "data"),
        help="Base output directory",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output file format",
    )
    parser.add_argument(
        "--list-status",
        default="",
        help="stock_basic list_status (L/D/P or empty for all)",
    )
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Comma-separated exchanges for stock_company (default: SSE,SZSE,BSE)",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Event table start date (YYYYMMDD)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Event table end date (YYYYMMDD)",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help=f"Lookback years when start-date missing (default: {DEFAULT_YEARS})",
    )
    parser.add_argument(
        "--managers-window",
        choices=["day", "week", "month"],
        default=DEFAULT_MANAGERS_WINDOW,
        help="Window size for stk_managers",
    )
    parser.add_argument(
        "--share-float-window",
        choices=["day", "week", "month"],
        default=DEFAULT_SHARE_FLOAT_WINDOW,
        help="Window size for share_float",
    )
    parser.add_argument(
        "--share-float-threshold",
        type=int,
        default=DEFAULT_SHARE_FLOAT_THRESHOLD,
        help="Row-count threshold to split share_float windows",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from state files")
    parser.add_argument("--force", action="store_true", help="Refetch even if files exist")
    parser.add_argument(
        "--consolidate",
        action="store_true",
        help="Merge raw windows into curated files for event tables",
    )
    parser.add_argument(
        "--rpm",
        type=float,
        default=None,
        help="Max requests per minute (default: env TUSHARE_RPM or 200)",
    )
    parser.add_argument("--retries", type=int, default=6, help="Retry attempts")
    parser.add_argument(
        "--base-delay", type=float, default=2.0, help="Retry base delay in seconds"
    )
    parser.add_argument(
        "--max-delay", type=float, default=60.0, help="Retry max delay in seconds"
    )

    args = parser.parse_args(argv)

    load_local_env()
    token = args.token.strip() or os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing TuShare token. Provide --token or set TUSHARE_TOKEN.")

    datasets = _parse_datasets(args.datasets)
    exchanges = _parse_exchanges(args.exchanges)

    needs_event_range = DATASET_STK_MANAGERS in datasets or DATASET_SHARE_FLOAT in datasets
    if needs_event_range:
        start_dt, end_dt = resolve_date_range(
            args.start_date, args.end_date, args.years, default_years=DEFAULT_YEARS
        )
    else:
        start_dt = end_dt = None

    rpm_env = os.getenv("TUSHARE_RPM", "").strip()
    if args.rpm is not None:
        rpm = args.rpm
    elif rpm_env:
        try:
            rpm = float(rpm_env)
        except ValueError:
            rpm = 200.0
    else:
        rpm = 200.0
    min_interval = 60.0 / rpm if rpm > 0 else 0.0

    store = DataStore(base_dir=Path(args.output_dir), file_format=args.format)
    runner = FetchRunner(
        rate_limiter=RateLimiter(min_interval=min_interval),
        retries=args.retries,
        base_delay=args.base_delay,
        max_delay=args.max_delay,
    )
    fetcher = ListedCompanyFetcher(init_tushare(token), runner, store)

    summaries = []

    if "stock_basic" in datasets:
        summaries.append(fetcher.fetch_stock_basic(list_status=args.list_status))
    if "stock_company" in datasets:
        summaries.append(fetcher.fetch_stock_company(exchanges=exchanges))
    if DATASET_STK_MANAGERS in datasets and start_dt and end_dt:
        summaries.append(
            fetcher.fetch_stk_managers(
                start_dt,
                end_dt,
                window=args.managers_window,
                resume=args.resume,
                force=args.force,
            )
        )
    if DATASET_SHARE_FLOAT in datasets and start_dt and end_dt:
        summaries.append(
            fetcher.fetch_share_float(
                start_dt,
                end_dt,
                window=args.share_float_window,
                resume=args.resume,
                force=args.force,
                threshold=args.share_float_threshold,
            )
        )

    if args.consolidate:
        for dataset in (DATASET_STK_MANAGERS, DATASET_SHARE_FLOAT):
            if dataset not in datasets:
                continue
            rows, path = _save_consolidated(store, dataset)
            if path:
                print(f"- consolidated {dataset}: rows={rows} path={path}")

    print("\nFetch complete:")
    for summary in summaries:
        print(
            f"- {summary.dataset}: windows={summary.windows} rows={summary.rows} files={summary.files}"
        )
    if start_dt and end_dt:
        print(
            f"Event date range: {format_yyyymmdd(start_dt)} -> {format_yyyymmdd(end_dt)}"
        )

    if args.consolidate:
        for dataset in (DATASET_STK_MANAGERS, DATASET_SHARE_FLOAT):
            if dataset in datasets:
                curated = store.curated_path(dataset)
                if curated.exists():
                    print(f"- curated output: {curated}")


if __name__ == "__main__":
    main()
