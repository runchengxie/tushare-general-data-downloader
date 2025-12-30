# Tushare 上市公司接口批量抓取

一次性批量抓取 A 股上市公司基础信息与事件表，并支持按时间窗切片 + 自动拆分，适合补历史与长期增量。

覆盖接口：

* `stock_basic`：股票清单与基础字段（维表）
* `stock_company`：公司概况（维表）
* `stk_managers`：高管/治理事件（事实表）
* `share_float`：限售股解禁事件（事实表，6000 行上限自动拆窗）

## 快速开始

```bash
# 1) 安装依赖（uv）
uv sync

# 2) 设置 token
export TUSHARE_TOKEN=your_token

# 3) 抓取最近 5 年（默认），share_float 周窗 + 触顶拆日
uv run tushare-listed-fetch --years 5 --share-float-window week --consolidate
```

### 只抓维表快照

```bash
uv run tushare-listed-fetch --datasets stock_basic,stock_company
```

### 指定历史区间

```bash
uv run tushare-listed-fetch \
  --start-date 20190101 \
  --end-date 20241231 \
  --managers-window month \
  --share-float-window week \
  --consolidate
```

## 输出结构

默认输出目录为 `data/`：

```
data/
  raw/
    stock_basic/stock_basic_YYYYMMDD.csv
    stock_company/stock_company_YYYYMMDD.csv
    stk_managers/stk_managers_YYYYMMDD_YYYYMMDD.csv
    share_float/share_float_YYYYMMDD_YYYYMMDD.csv
  curated/
    stock_basic.csv
    stock_company.csv
    stk_managers.csv
    share_float.csv
  state/
    stk_managers.json
    share_float.json
```

* `raw/`：按窗口落地，适合断点续跑。
* `curated/`：当你使用 `--consolidate` 时生成的合并去重版本。
* `state/`：记录最近成功窗口，用于 `--resume`。

## 关键参数

* `--datasets`：选择要抓的表（默认全量）。
* `--years`：当 `--start-date` 未提供时的回溯年数（默认 5）。
* `--managers-window`：`stk_managers` 的切片粒度（默认 `month`）。
* `--share-float-window`：`share_float` 的切片粒度（默认 `week`）。
* `--share-float-threshold`：返回行数达到该值即拆分为日窗（默认 5500）。
* `--resume`：从 `data/state` 里继续增量。
* `--force`：忽略已有窗口文件并重新拉取。
* `--rpm`：每分钟请求上限（默认 200，可用 `TUSHARE_RPM` 环境变量覆盖）。

## 可选字段覆盖

如果你想自定义字段，可设置环境变量：

```
TUSHARE_FIELDS_STOCK_BASIC
TUSHARE_FIELDS_STOCK_COMPANY
TUSHARE_FIELDS_STK_MANAGERS
TUSHARE_FIELDS_SHARE_FLOAT
```

例如：

```bash
export TUSHARE_FIELDS_STK_MANAGERS="ts_code,ann_date,name,title,begin_date,end_date,gender"
```

## Parquet 输出（可选）

```bash
uv pip install -e ".[parquet]"
uv run tushare-listed-fetch --format parquet
```

## Token 校验

```bash
uv run python project_tools/verify_tushare_tokens.py
```

## 测试

```bash
uv run pytest
```

## 备注

* `share_float` 有单次 6000 行上限，脚本会在周窗触顶时自动拆成日窗；若日窗仍接近上限，会输出 warning，建议手动再细分或改用更小切片。
* `stk_managers` 默认不取 `resume` 字段，以提高吞吐。如需简历字段，请在 `TUSHARE_FIELDS_STK_MANAGERS` 中显式添加。
