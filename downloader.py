import tushare as ts
import pandas as pd
import os
import sys
from dotenv import load_dotenv
import time # 导入time模块，用于防止请求过快

# --- 设置区 ---
# 把需要下载的股票代码放到一个列表里
stock_list = ["600519.SH", "000001.SZ", "300750.SZ"] 
start_date = "20240101"
end_date = "20250531"
# --- 设置区结束 ---

def get_tushare_token():
    """
    获取Tushare API Token。
    优先级顺序:
    1. 从同目录下的 .env 文件中读取 TUSHARE_API_KEY。
    2. 从系统环境变量中读取 TUSHARE_API_KEY。
    如果都找不到，则抛出异常。
    """
    # 加载.env文件中的环境变量到os.environ
    # 这行代码会自动寻找.env文件，如果找到就加载，找不到也不会报错
    load_dotenv() 
    
    # 从环境中获取token
    token = os.getenv("TUSHARE_API_KEY")
    
    if not token:
        # 如果token是None或者空字符串，说明没找到
        error_message = """
        错误：未能找到Tushare API Token。
        请按以下方式之一配置：
        1. 在脚本同目录下创建一个名为 .env 的文件，内容为：
           TUSHARE_API_KEY=你的token值
        2. 设置一个名为 TUSHARE_API_KEY 的系统环境变量。
        """
        raise ValueError(error_message)
        
    return token

# 设置你的Tushare Token
ts.set_token("YOUR_TOKEN")
pro = ts.pro_api()

# --- 主程序逻辑 ---
if __name__ == "__main__":
    try:
        # 1. 自动获取Token
        tushare_token = get_tushare_token()
        print("成功获取到Tushare API Token。")
        ts.set_token(tushare_token)
        pro = ts.pro_api()

    except ValueError as e:
        # 如果get_tushare_token函数抛出异常，打印错误信息并退出
        print(e)
        sys.exit(1) # 退出程序，返回状态码1表示有错误发生

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