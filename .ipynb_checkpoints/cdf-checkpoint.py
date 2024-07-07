import random
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


print(__name__)

class cdf_e:
    def __init__(self,cdf):
        self.cdf = cdf
        self.plotDict = None
        self.totalDict = []

    def cdfR(self):
        x = len(self.cdf)
        while x > 0:
            a = getCdf(self.cdf[x-1])
            b = breakCdf(a)
            x -= 1
            self.plotDict = {'cdf': a, 'cdfSplit': b}
            self.totalDict.append(self.plotDict)

    def plot_cdfR(self):
        x = len(self.totalDict)
        while x > 0:
            mx = max(self.totalDict[x-1]['cdfSplit']['x'])
            mi = min(self.totalDict[x-1]['cdfSplit']['x'])
            plt.xlim(mi, mx)
            plt.plot(self.totalDict[x-1]['cdfSplit']['x'], self.totalDict[x-1]['cdfSplit']['y']);plt.yscale('log')
            x -= 1
        plt.show()
    
    
    def rCdf(cdf, n):
        ru = np.random.uniform(size=n)
        a = np.interp(ru, cdf['y'], cdf['x'])
        ptr = np.isnan(a) * (ru > 0.5)
        a[ptr] = np.max(cdf['x'])
        a[np.isnan(a)] = np.min(cdf['x'])
        return a

def getCdf(x):
    sx = np.sort(x)
    y = np.arange(1, len(x)+1) / (len(x) + 1)
    return {'x': sx, 'y': y}

def breakCdf(cdf):
    med = np.median(cdf['x'])
    xp = cdf['x'] >= med
    xn = cdf['x'] < med
    allX = np.add(xp,xn)
    yp = 1 - cdf['y'][xp]
    yn = cdf['y'][xn]
    allY = np.concatenate((yn,yp))
    return {'x': cdf['x'][allX], 'y': allY}


if __name__ == '__main__':    
    df = pd.read_pickle('closePrices.pkl')
    print(type(df))

    randomData1 = [random.randint(0, 100) for _ in range(1000)]
    randomData2 = [random.randint(0, 100) for _ in range(1000)]
    dataL = []
    dataL.append(randomData1)
    dataL.append(randomData2)
    cdfList = cdf_e(dataL)
    cdfList.cdfR()
    cdfList.plot_cdfR()