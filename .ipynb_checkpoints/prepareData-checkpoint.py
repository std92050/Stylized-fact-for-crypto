# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 18:23:24 2022

@author: 14152
"""

import pandas as pd
import numpy as np
from datetime import datetime
from statsmodels.graphics.tsaplots import plot_acf


#%%
def readTypeVWAP(fname,rPeriod='30min'):
    x = pd.read_csv(fname)
    x['Date'] = pd.to_datetime(x['timestamp'],unit='ms')
    #%%
    y = x[['Date','Close','Volume']].copy()
    y.set_index('Date',inplace=True)
    #%%
    # Resample into VWAP
    y['vp'] = y['Close']*y['Volume']
    #%%
    z=y.resample(rPeriod,label="right").sum()
    z['vwap'] = z['vp']/z['Volume']
    #%%
    z.drop(['Close','vp'],inplace=True,axis=1)
    return z
#%%
def readTypeA(fname,rPeriod='30min',cname='Close'):
    x = pd.read_csv(fname)
    x['Date'] = pd.to_datetime(x['timestamp'],unit='ms')
    #%%
    y = x[['Date','Close']].copy()
    y.set_index('Date',inplace=True)
    #%%
    z=y.resample(rPeriod,label="right").last()
    z.rename(columns={'Close':cname},inplace=True)
    return z

def readTypeB(fname,rPeriod='30min',cname='Close'):
    x = pd.read_csv(fname)
    x['Date'] = pd.to_datetime(x['timestamp'],unit='s')
    #%%
    y = x[['Date','eth/usd']].copy()
    y.set_index('Date',inplace=True)
    y.rename(columns={'eth/usd':cname},inplace=True)
    z=y.resample(rPeriod,label='right').last()
    z=z.round(2)
    return z
if __name__ == "__main__": 
    #%%
    rPeriod = '1s'
    fname = "./Data/binance_eth_usdt.csv"
    w1 = readTypeA(fname,cname='binance',rPeriod=rPeriod)

    fname = "./Data/ftx_eth_usdt.csv"
    w2 = readTypeA(fname,cname='ftx',rPeriod=rPeriod)
    y=w2.join(w1,how='inner')
    #%%
    fname = "./Data/mainnet_eth_03.csv"
    v = readTypeB(fname,cname='mainnet03',rPeriod=rPeriod)
    y=y.join(v,how='inner')
    fname = "./Data/mainnet_eth_005.csv"
    v = readTypeB(fname,cname='mainnet005',rPeriod=rPeriod)
    y=y.join(v,how='inner')
    #fname = "./Data/poly_eth_005.csv"
    #v = readTypeB(fname,cname='poly005',rPeriod=rPeriod)
    #y=y.join(v,how='inner')
    y = y.fillna(method='ffill')
    y = y.dropna()
    y.to_csv('./Data/close1sec.csv')
    fname = "./Data/poly_eth_005.csv"
    v = readTypeB(fname,cname='poly005',rPeriod=rPeriod)
    y=y.join(v,how='inner')
    y = y.fillna(method='ffill')
    y = y.dropna()
    y.to_csv('./Data/close1sec2.csv')

    #%%
    # Just check the autocorrelation
    #a = y['binance']-y['mainnet03']
    #plot_acf(a.dropna().to_numpy(),lags=20)

    #a = y['binance']-y['mainnet005']
    #plot_acf(a.dropna().to_numpy(),lags=20)
    #%%
    # Pull everything and glue
    dT = ['1min','5min','15min','30min','45min','60min','90min','120min','180min','240min','300min','400min','600min','720min','D','3D','W','2W','M']
    fname1 = "./Data/binance_eth_usdt.csv"
    fname2 = "./Data/ftx_eth_usdt.csv"
    fname3 = "./Data/mainnet_eth_03.csv"
    fname4 = "./Data/mainnet_eth_005.csv"
    fname5 = "./Data/poly_eth_005.csv"
    fname6 = "./Data/ftx_eth_usd.csv"

    a = []
    for rPeriod in dT:
        print(rPeriod)
        w1 = readTypeA(fname1,cname='Price',rPeriod=rPeriod)
        w1['dT'] = rPeriod
        w1['Exch'] = 'binance'
        w1['coin'] = 'ETH'
        w1['fiat'] = 'USDT'
        a.append(w1)
        w1 = readTypeA(fname2,cname='Price',rPeriod=rPeriod)
        w1['dT'] = rPeriod
        w1['Exch'] = 'ftx'
        w1['coin'] = 'ETH'
        w1['fiat'] = 'USDT'
        a.append(w1)
        w1 = readTypeB(fname3,cname='Price',rPeriod=rPeriod)
        w1['dT'] = rPeriod
        w1['Exch'] = 'mainnet03'
        w1['coin'] = 'ETH'
        w1['fiat'] = 'USD?'
        a.append(w1)
        w1 = readTypeB(fname4,cname='Price',rPeriod=rPeriod)
        w1['dT'] = rPeriod
        w1['Exch'] = 'mainnet005'
        w1['coin'] = 'ETH'
        w1['fiat'] = 'USD?'
        a.append(w1)
        w1 = readTypeB(fname4,cname='Price',rPeriod=rPeriod)
        w1['dT'] = rPeriod
        w1['Exch'] = 'poly005'
        w1['coin'] = 'ETH'
        w1['fiat'] = 'USD?'
        a.append(w1)
        w1 = readTypeA(fname6,cname='Price',rPeriod=rPeriod)
        w1['dT'] = rPeriod
        w1['Exch'] = 'ftx'
        w1['coin'] = 'ETH'
        w1['fiat'] = 'USD'
        a.append(w1)

    x = pd.concat(a)

    #%% Add ciubvase ... this is only hour
    #dT = ['60min','120min','180min','300min','720min','D','W','M','Q']
    dT = ['60min','90min','120min','180min','240min','300min','400min','600min','720min','D','3D','W','2W','M','Q']
    df = pd.read_csv("./Data/coinbase2.csv")
    df['Date'] = pd.to_datetime(df['date'])
    y = df[['Date','close','pair']].copy()
    y.set_index('Date',inplace=True)
    y.rename(columns={'close':'Price','pair':'coin'},inplace=True)
    a = [] 
    for tt in dT:
        print(tt)
        z=y.groupby('coin').resample(tt,label='right').last()
        z=z.droplevel(0)
        z['dT'] = tt
        z['Exch'] = 'coinbase'
        z['fiat'] = 'USD'
        z = z[['Price','dT','Exch','coin','fiat']]
        a.append(z)
    xc = pd.concat(a)

    #%%
    # Concat other exanges with coinbase...
    xClose = pd.concat([x,xc])
    #xClose.to_csv('./Data/closePrices.csv')
    xClose.to_pickle('./Data/closePrices.pkl')

    #%% Build SPX
    spx1 = pd.read_csv('C:/Users/14152/Documents/aulaLuis/Data/SPX_hr5aqh2/SPX_2020_2020.txt',header=None)
    spx2 = pd.read_csv('C:/Users/14152/Documents/aulaLuis/Data/SPX_hr5aqh2/SPX_2010_2019.txt',header=None)
    spx1.drop_duplicates(inplace=True)
    spx2.drop_duplicates(inplace=True)

    spx  = pd.concat([spx2,spx1])
    spx.columns = ['Date','Open','High','Low','Close']
    spx['Date'] = pd.to_datetime(spx['Date'])
    spx = spx.sort_values(by='Date')
    spx.set_index('Date',inplace=True)
    spx.rename(columns={'Close':'Price'},inplace=True)
    spx0 = pd.DataFrame(spx['Price'])

    dT = ['1min','5min','15min','30min','45min','60min','90min','120min','180min','240min','300min','400min','600min','720min','D','3D','W','2W','M','Q']

    a = []
    for tt in dT:
        print(tt)
        if dT=='1min':
            b = spx0
            b['dT'] = tt
            a.append(b)
            continue
        b = spx0.resample(tt,label='right').last()
        b['dT'] = tt
        a.append(b)
    spxP = pd.concat(a)

    spxP.to_pickle('./Data/spx.pkl')




 





    