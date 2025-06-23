import tushare as ts
import pandas as pd
import os
import sys
from dotenv import load_dotenv
import time

def get_tushare_token():
    """从.env文件或环境变量中安全地获取Tushare Token。"""
    load_dotenv()
    token = os.getenv("TUSHARE_API_KEY")
    if not token:
        error_message = """
        错误：未能找到Tushare API Token。
        请按以下方式之一配置：
        1. 在脚本同目录下创建一个名为 .env 的文件，内容为：
           TUSHARE_API_KEY=你的token值
        2. 设置一个名为 TUSHARE_API_KEY 的系统环境变量。
        """
        raise ValueError(error_message)
    return token

def download_and_process_data(pro, stock_code, start_date, end_date):
    """下载、处理并保存单只股票的数据。"""
    print(f"\n正在拉取股票代码: {stock_code} 的数据...")
    df = pro.daily(
        ts_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        adj="hfq",
        fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
    )

    if df.empty:
        print(f"未能获取到股票 {stock_code} 的数据，跳过。")
        return

    # 数据处理
    df.rename(columns={"trade_date": "datetime", "vol": "volume"}, inplace=True)
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d")

    # 保存文件
    file_name = f"{stock_code}_daily.csv"
    df.to_csv(file_name, index=False)
    print(f"数据处理完成，已成功保存为: {file_name}")

def main():
    """
    程序的主执行函数。
    """
    # --- 设置区 ---
    # 你只需要在这里修改参数即可
    stock_list = ["600036.SH", "000001.SZ"]
    start_date = "20240101"
    end_date = "20250531"
    # --- 设置区结束 ---

    try:
        # 1. 自动获取Token并初始化API
        tushare_token = get_tushare_token()
        print("成功获取到Tushare API Token。")
        ts.set_token(tushare_token)
        pro = ts.pro_api()

    except ValueError as e:
        print(e)
        sys.exit(1) # 严重错误，退出程序

    # 2. 循环处理所有股票
    for code in stock_list:
        try:
            download_and_process_data(pro, code, start_date, end_date)
            time.sleep(0.3)  # 礼貌性暂停，防止请求过快
        except Exception as e:
            # 单只股票出错不影响其他股票
            print(f"处理股票 {code} 时发生未知错误: {e}")

    print("\n所有股票处理完毕！")

if __name__ == "__main__":
    main()