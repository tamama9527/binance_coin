import asyncio
from binance import AsyncClient, BinanceSocketManager, client
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import numpy as np
import config
import logging

dev_logger: logging.Logger = logging.getLogger(name='dev')
dev_logger.setLevel('INFO')
handler: logging.StreamHandler = logging.StreamHandler()
formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
dev_logger.addHandler(handler)

coins = {}

def getCoinInfo(client):
    return client.futures_symbol_ticker()

def usdtfliter(rawCoinInfos):
    return 'USDT' in rawCoinInfos['symbol']

def convertToTickerStreamer(symbols):
    symbolStreamer = []
    for i in symbols:
        symbolStreamer.append(str(i['symbol']).lower()+'@ticker')
    return symbolStreamer

def convertToKlineStreamer(symbols):
    symbolStreamer = []
    for i in symbols:
        symbolStreamer.append(str(i['symbol']).lower()+'@kline_1m')
    return symbolStreamer

# Only run in first time
async def runOnceSixKline(rawCoins, client, streamerList):
    for coin in rawCoins:
        result = await client.futures_klines(symbol=coin['symbol'],interval='1m',limit=11)
        kline = np.array(result)
        averageVolume = kline[0:10, 7].astype(np.float64).sum() / 10
        if averageVolume == 0 :
            streamerList.remove(str(coin['symbol']).lower()+'@kline_1m')
            continue
        #volumeNormalize = float(kline[5][7]) / averageVolume
        averagePrice = kline[0:10, 4].astype(np.float64).sum() / 10
        coins[coin['symbol']] = {}
        coins[coin['symbol']]['averagePrice'] = float(averagePrice)
        coins[coin['symbol']]['averageVolume'] = float(averageVolume)
        coins[coin['symbol']]['Already'] = 0
    return streamerList

async def sixKline(client):
    while True:
        for coin in coins:
            result = await client.futures_klines(symbol=coin,interval='1m',limit=11)
            kline = np.array(result)
            #      0              1          2      3          4          5            6                7                    8                       9                               10
            # ['Open time', 'Open price', 'High', 'Low', 'Close Price', 'Volume', 'Close time', 'Quote asset volume', 'Number of trade', 'Taker buy base asset volume', 'Taker buy quote asset volume']
            averageVolume = kline[0:10, 7].astype(np.float64).sum() / 10
            #volumeNormalize = float(kline[5][7]) / averageVolume
            averagePrice = kline[0:10, 4].astype(np.float64).sum() / 10
            coins[coin]['averagePrice'] = float(averagePrice)
            coins[coin]['averageVolume'] = float(averageVolume)
        await asyncio.sleep(4)

async def klineSocketSub(streamerList,client):
    dev_logger.info('幣安合約總數:'+str(len(streamerList)))
    bm = BinanceSocketManager(client)
    #['btcusdt@ticker', 'ethusdt@ticker','fttusdt@ticker','solusdt@ticker','unfiusdt@ticker']
    future_ts = bm.futures_multiplex_socket(streamerList)
    async with future_ts as tscm:
        while True:
            res = await tscm.recv()
            symbol = res['data']['s']
            averagePrice = coins[symbol]['averagePrice']
            newestPrice = float(res['data']['k']['c'])
            newestVolume = float(res['data']['k']['q'])
            # 最新價格 - 均價(10個1m線)/ 最新價格
            priceDiff = (newestPrice - coins[symbol]['averagePrice']) / newestPrice
            # 最新量 - 均量 / 最新量
            volumeDiff = newestVolume / coins[symbol]['averageVolume']
            if (priceDiff > 0.01 or priceDiff < -0.01) and volumeDiff > 1.5 and coins[res['data']['s']]['Already'] == 0:
                dev_logger.info(symbol+ '現在價格:' + str(newestPrice) + ' MA10_1m:'+ str(round(averagePrice, 4)) + ' 價格差比:' + str(round(priceDiff*100,3)) + ' 量差比:' + str(round(volumeDiff*100,2)))
                # 發出通知後 3分鐘內不要重複通知
                coins[res['data']['s']]['Already'] = 3
            if res['data']['k']['x'] is True and coins[res['data']['s']]['Already'] > 0:
                coins[res['data']['s']]['Already'] = coins[res['data']['s']]['Already'] - 1
            #print(datetime.fromtimestamp(res['data']['E']/1000), res['data']['s'], res['data']['c'])


async def main():
    dev_logger.info('開始執行')
    #initial sync client
    binanceClient =  client.Client(config.api_key, config.api_secret)
    #initial async client
    asyncClient = await AsyncClient.create(config.api_key, config.api_secret)
    #get all future coin and filter not usdt coin
    rawFutureCoins = list(filter(usdtfliter, getCoinInfo(binanceClient)))
    #add stream
    rawKlineStreamList = convertToKlineStreamer(rawFutureCoins)
    #first time coins 10 1s kline
    klineStreamList = await runOnceSixKline(rawFutureCoins, asyncClient, rawKlineStreamList)
    dev_logger.info('Kline initial complete')
    #streamList = tickerStreamList + klineStreamList
    await asyncio.gather(klineSocketSub(klineStreamList, asyncClient), sixKline(asyncClient))
    #await asyncio.gather(klineSocketSub(klineStreamList,asynClient))
if __name__ == "__main__":
    asyncio.run(main())
    
'''多個streamer
ms = bm.multiplex_socket(['btcusdt@ticker', 'ethusdt@ticker'])
# then start receiving messages
async with ms as tscm:
    while True:
        res = await tscm.recv()
        print(res['data']['s'], res['data']['c'])
'''
'''合約k線
ks = bm.kline_futures_socket('ETHUSDT', interval=client.KLINE_INTERVAL_15MINUTE)
async with ks as tscm:
    while True:
        res = await tscm.recv()
        #print(res['data']['s'], res['data']['c'])
        print(res)
await client.close_connection()
'''