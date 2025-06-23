import tushare as ts
import pandas as pd
import time # 导入time模块，用于防止请求过快

# --- 设置区 ---
# 把需要下载的股票代码放到一个列表里
stock_list = ["600519.SH", "000001.SZ", "300750.SZ"] 
start_date = "20240101"
end_date = "20250531"
# --- 设置区结束 ---

# 设置你的Tushare Token
ts.set_token("YOUR_TOKEN")
pro = ts.pro_api()

# 循环处理列表中的每一只股票
for stock_code in stock_list:
    try:
        print(f"正在拉取股票代码: {stock_code} 的数据...")
        
        df = pro.daily(
            ts_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            adj="hfq",
            fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
        )

        if df.empty:
            print(f"未能获取到股票 {stock_code} 的数据，跳过。")
            continue # 跳过当前循环，处理下一只股票

        df.rename(columns={"trade_date": "datetime", "vol": "volume"}, inplace=True)
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d")

        file_name = f"{stock_code}_daily.csv"
        df.to_csv(file_name, index=False)
        
        print(f"数据处理完成，已成功保存为: {file_name}")

        # Tushare有调用频率限制，每次调用后暂停一小段时间，防止被封
        time.sleep(0.3) 

    except Exception as e:
        print(f"处理股票 {stock_code} 时发生错误: {e}")

print("所有股票处理完毕！")