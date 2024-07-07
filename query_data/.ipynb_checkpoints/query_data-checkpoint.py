import os.path
import QueryBinance
import pandas as pd
import time
import datetime


timeframe = "1h" # 1m, 1h, 1d 
since = '2023-03-24T00:00:00Z'
end = '2023-03-25T00:00:00Z'
newT = pd.Timestamp.today(tz='UTC')+datetime.timedelta(hours=-1)
end = str(newT)
print(end)
base_token = "ETH"
quote_token = "USDT"
#base_token_list = ["BTC","ETH","SOL","ATOM","LINK","AVAX","MATIC","CRV","BNB"]
base_token_list = ['ETH']

for base_token in base_token_list:
    exchange_id = 'binance'
    symbol = f"{base_token}/{quote_token}"
    filename = f"({since[:10]},{end[:10]})_{base_token}{quote_token}_{timeframe}.csv"
    if not os.path.exists(filename):
        QueryBinance.scrape_candles_to_csv(filename=filename, exchange_id=exchange_id, symbol=symbol,
                                            timeframe=timeframe, since=since, end=end, max_retries=3, limit=1000,
                                            rest_time=1, cat=None)

