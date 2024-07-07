import pandas as pd
import json
import os

from datetime import datetime


def tick_2_price(tick):
    decimal_0 = int(config["decimal_0"])
    decimal_1 = int(config["decimal_1"])
    base_symbol = config["base_symbol"]
    quote_symbol = config["quote_symbol"]

    token_0_price = 1.0001 ** tick * 10 ** (decimal_0 - decimal_1)
    if base_symbol == "0":
        price = token_0_price
    elif base_symbol == "1":
        price = 1 / token_0_price

    return price


def sqp_2_price(sqp):
    decimal_0 = int(config["decimal_0"])
    decimal_1 = int(config["decimal_1"])
    base_symbol = config["base_symbol"]
    quote_symbol = config["quote_symbol"]

    sqp = int(float(sqp))

    token_0_price = (sqp / 2 ** 96) ** 2 * 10 ** (decimal_0 - decimal_1)
    if base_symbol == "0":
        price = token_0_price
    elif base_symbol == "1":
        price = 1 / token_0_price

    return price


def transform_amount_0(amount0):
    decimal_0 = int(config["decimal_0"])
    amount_0 = int(amount0) * 10 ** -decimal_0 #=18
    return amount_0


def transform_amount_1(amount1):
    decimal_1 = int(config["decimal_1"])
    amount_1 = int(amount1) * 10 ** -decimal_1 #=6
    return amount_1

def main(path,token_0,token_1):
    df = pd.read_csv(path + ".csv", index_col=0)
    df["tick_price"] = df["tick"].apply(lambda x: tick_2_price(x))
    df["price"] = df["sqrtPriceX96"].apply(lambda x: sqp_2_price(x))
    df[token_0] = df["amount0"].apply(lambda x: transform_amount_0(x))
    df[token_1] = df["amount1"].apply(lambda x: transform_amount_1(x))
    df['datetime'] = df['timestamp'].apply(lambda x: datetime.utcfromtimestamp(x))
    #df['volume'] = df.apply(lambda row: (row['amount_0']*row['price']) + row["amount_1"], axis = 1) 
    #new_df = df[['datetime','price','volume']]
    new_df = df[['datetime','blockNumber','price',token_0,token_1,'liquidity']]

    new_df.set_index('datetime', inplace=True)
    #new_df = new_df.resample('D').agg({"price":'last',"volume":'sum'}).dropna()
    #print(new_df)
    return new_df.dropna()
if __name__ == "__main__":
    path = "WETH_USDC_arbitrum_3000"
    with open(path + "_config.json", 'r') as f:
        config = json.load(f)
    main(path)
