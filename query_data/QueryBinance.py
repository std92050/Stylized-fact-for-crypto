# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import time
from datetime import datetime
# -----------------------------------------------------------------------------

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt


# -----------------------------------------------------------------------------
def retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    num_retries = 0
    while num_retries <= max_retries:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since, limit=limit)
            print('Fetched', len(ohlcv), symbol, 'candles from', exchange.iso8601 (ohlcv[0][0]), 'to', exchange.iso8601 (ohlcv[-1][0]))
            time.sleep(0.1)
            return ohlcv
        except Exception as error:
            print("error!" * 10)
            print(error)
            print("error!" * 10)
            num_retries += 1
            print(f"retry: {num_retries}")
            print(f"max retries: {max_retries}")
            time.sleep(10)
            if num_retries > max_retries:
                raise

def scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, end, limit):
    timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    timedelta = limit * timeframe_duration_in_ms
    now = exchange.milliseconds()
    all_ohlcv = []

    fetch_since = since
    if isinstance(end, str):
        end = exchange.parse8601(end)
    remain_bars = (end-fetch_since)/(timeframe_duration_in_ms)+1  #  +1 because include end ohlcv

    while fetch_since < now:
        if remain_bars<limit:
            limit = int(remain_bars)

        ohlcv = retry_fetch_ohlcv(exchange=exchange, max_retries=max_retries, symbol=symbol, timeframe=timeframe, since=fetch_since, limit=limit)
        fetch_since = fetch_since + timedelta
        all_ohlcv = all_ohlcv + ohlcv

        remain_bars = remain_bars-limit

        if remain_bars == 0:
            break
    return exchange.filter_by_since_limit(all_ohlcv, since, None, key=0)

def scrape_candles_to_csv(filename, exchange_id, max_retries, symbol, timeframe, since, end, limit, rest_time, cat=None):
    # instantiate the exchange by id
    exchange = getattr(ccxt, exchange_id)({
        'enableRateLimit': True,  # required by the Manual
    })
    # convert since from string to milliseconds integer if needed
    if isinstance(since, str):
        since = exchange.parse8601(since)
    # preload all markets from the exchange
    exchange.load_markets()
    # fetch all candles
    ohlcv = scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, end, limit)
    # save them to csv file
    df = pd.DataFrame(ohlcv)
    df.columns = ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    dir_path = os.getcwd() + "/" + cat if cat else os.getcwd()
    df.to_csv(dir_path+"/"+filename, index=False)
    print('Saved', len(ohlcv), 'candles from', exchange.iso8601(ohlcv[0][0]), 'to', exchange.iso8601(ohlcv[-1][0]),'to', filename)
    print("-"*100)
    print(f"sleep {rest_time} sec")
    time.sleep(rest_time)
    print("-"*100)


if __name__ == "__main__":
    # Setting
    # timeframe list: 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M
    timeframe = "1h"
    since = '2021-09-01T00:00:00Z'
    end   = '2022-06-01T00:00:00Z'
    target_token = "ETH"
    base_token = "USDT"

    exchange_id = 'binance'
    symbol = f"{target_token}/{base_token}"
    filename = f"{target_token}{base_token}.csv"
    rest_time = 0.1

    scrape_candles_to_csv(filename=filename, exchange_id=exchange_id, symbol=symbol, timeframe=timeframe, since=since, end=end, max_retries=3, limit=1000, rest_time=rest_time, cat=None)