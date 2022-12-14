import asyncio
from binance import AsyncClient, BinanceSocketManager, client
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import numpy as np
import config
import logging
import ccxt 
from test import buy_long, buy_short
dev_logger: logging.Logger = logging.getLogger(name='dev')
dev_logger.setLevel('INFO')
handler: logging.StreamHandler = logging.StreamHandler()
formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
dev_logger.addHandler(handler)

okx_exchange = ccxt.okx({
    'apiKey': config.okx['apiKey'],
    'secret': config.okx['secret'],
    'password': config.okx['password'],
    'options': {
        'defaultType': 'swap',
    },
    'contract': False,
    'settle': 'USDT',
    'settleId': 'USDT',
})


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
        result = await client.futures_klines(symbol=coin['symbol'],interval='1m',limit=12)
        kline = np.array(result)
        averageVolume = kline[0:10, 7].astype(np.float64).sum() / 10
        if averageVolume == 0 :
            streamerList.remove(str(coin['symbol']).lower()+'@kline_1m')
            continue
        averagePrice = kline[0:10, 4].astype(np.float64).sum() / 10
        coins[coin['symbol']] = {}
        coins[coin['symbol']]['averagePrice'] = float(averagePrice)
        coins[coin['symbol']]['averageVolume'] = float(averageVolume)
        coins[coin['symbol']]['lastPrice'] = float(kline[-1][4])
        coins[coin['symbol']]['Already'] = 0
    #print(coins)
    return streamerList

async def sixKline(client):
    while True:
        for coin in coins:
            result = await client.futures_klines(symbol=coin,interval='1m',limit=12)
            kline = np.array(result)
            #      0              1          2      3          4          5            6                7                    8                       9                               10
            # ['Open time', 'Open price', 'High', 'Low', 'Close Price', 'Volume', 'Close time', 'Quote asset volume', 'Number of trade', 'Taker buy base asset volume', 'Taker buy quote asset volume']
            averageVolume = kline[0:10, 7].astype(np.float64).sum() / 10
            averagePrice = kline[0:10, 4].astype(np.float64).sum() / 10
            coins[coin]['averagePrice'] = float(averagePrice)
            coins[coin]['averageVolume'] = float(averageVolume)

        await asyncio.sleep(4)

async def klineSocketSub(streamerList,client):
    dev_logger.info('??????????????????:'+str(len(streamerList)))
    bm = BinanceSocketManager(client)
    #['btcusdt@ticker', 'ethusdt@ticker','fttusdt@ticker','solusdt@ticker','unfiusdt@ticker']
    future_ts = bm.futures_multiplex_socket(streamerList)
    async with future_ts as tscm:
        while True:
            res = await tscm.recv()
            await stragey(res)
'''
??????1
???BTC?????????,???????????????????????????
??????btc??????>1% ????????????10????????????1.5???????????????
'''
async def stragey(response):
    symbol = response['data']['s']
    averagePrice = coins[symbol]['averagePrice']
    averageVolume = coins[symbol]['averageVolume']
    newestPrice = float(response['data']['k']['c'])
    newestVolume = float(response['data']['k']['q'])
    coins[symbol]['lastPrice'] = newestPrice
    if averageVolume == 0 or averagePrice == 0:
        return
    priceChange = newestPrice / averagePrice
    volumeDiff = newestVolume / averageVolume
    if averagePrice == 0 or averagePrice == 0:
        dev_logger.info('averagePrice or averageVolume is 0')
        return
    # ???????????? - ??????(10???1m???)/ ????????????
    priceDiffWithBTC = (priceChange / (coins['BTCUSDT']['lastPrice'] / coins['BTCUSDT']['averagePrice'])) - 1
    #volumeDiff = round(volumeChange / coins[symbol]['averageVolume'] * 100, 3)
    #print(symbol,priceDiffWithBTC,volumeDiff)
    # ????????? - ?????? / ?????????
    #volumeDiff = newestVolume / coins[symbol]['averageVolume']
    #if (priceDiff > 0.01 or priceDiff < -0.01) and volumeDiff > 1.5 and coins[symbol]['Already'] == 0:
    #dev_logger.info(symbol+ ' ????????????:' + str(newestPrice) + ' MA10_1m:'+ str(round(averagePrice, 4)) + ' ????????????:' + str(priceDiffWithBTC) + ' ?????????:' + str(volumeDiffWithBTC))
    if (priceDiffWithBTC > 0.01 or priceDiffWithBTC < -0.01) and volumeDiff > 1.5 and coins[symbol]['Already'] == 0:
        dev_logger.info(symbol+ ' ????????????:' + str(newestPrice) + ' MA10_1m:'+ str(round(averagePrice, 4)) + ' ????????????:' + str(round(priceDiffWithBTC*100,3))+ '%' + ' ?????????:' + str(round(volumeDiff*100,2)) +'%')
        if priceDiffWithBTC > 0:
            try:
                buy_long(symbol= str(symbol).replace('USDT', '/USDT:USDT'),last_price=newestPrice)
            except:
                pass
        else:
            try:
                buy_short(symbol= str(symbol).replace('USDT', '/USDT:USDT'),last_price=newestPrice)
            except:
                pass
        # ??????????????? 5???????????????????????????
        coins[symbol]['Already'] = 5
    if response['data']['k']['x'] is True and coins[symbol]['Already'] > 0:
        coins[symbol]['Already'] = coins[symbol]['Already'] - 1

async def main():
    dev_logger.info('????????????')
    #initial sync client
    binanceClient =  client.Client(config.api_key, config.api_secret)
    #initial async client
    asyncClient = await AsyncClient.create(config.api_key, config.api_secret)
    #get all future coin and filter not usdt coin
    rawFutureCoins = list(filter(usdtfliter, getCoinInfo(binanceClient)))
    # for testing !!!!!!!!!!!!!!!!!rawFutureCoins = [{'symbol': 'IOTXUSDT', 'price': '0.02426', 'time': 1668252160007}, {'symbol': 'BTCUSDT', 'price': '0.02426', 'time': 1668252160007}]
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
    
'''??????streamer
ms = bm.multiplex_socket(['btcusdt@ticker', 'ethusdt@ticker'])
# then start receiving messages
async with ms as tscm:
    while True:
        res = await tscm.recv()
        print(res['data']['s'], res['data']['c'])
'''
'''??????k???
ks = bm.kline_futures_socket('ETHUSDT', interval=client.KLINE_INTERVAL_15MINUTE)
async with ks as tscm:
    while True:
        res = await tscm.recv()
        #print(res['data']['s'], res['data']['c'])
        print(res)
await client.close_connection()
'''
