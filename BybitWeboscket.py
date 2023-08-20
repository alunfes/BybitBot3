import asyncio
import pandas as pd

from pybit.unified_trading import WebSocket
from BybitTradeData import BybitTradeData
from BybitRestAPI import BybitRestAPI
from BybitDepthData import BybitDepthData


class BybitWebsocket:
    def __init__(self) -> None:
        self.ws = WebSocket(
            testnet=False,
            channel_type="linear"
            )
    
    def __callback_trade(self, message):
        '''
        {'topic': 'publicTrade.BTCUSDT', 'type': 'snapshot', 'ts': 1692276093693, 'data': [{'T': 1692276093692, 's': 'BTCUSDT', 'S': 'Buy', 'v': '0.705', 'p': '28552.00', 'L': 'ZeroPlusTick', 'i': '70876f5b-1eb1-5348-8e2f-430adaec3c4b', 'BT': False}, {'T': 1692276093692, 's': 'BTCUSDT', 'S': 'Buy', 'v': '0.442', 'p': '28552.00', 'L': 'ZeroPlusTick', 'i': 'ffdd97de-7ff0-5b60-8618-52ad46931524', 'BT': False}, {'T': 1692276093692, 's': 'BTCUSDT', 'S': 'Buy', 'v': '0.455', 'p': '28552.00', 'L': 'ZeroPlusTick', 'i': 'fbfbd323-3d7c-556b-a6d3-ba63e6b70caa', 'BT': False}]}
        '''
        print(message)
    
    def __callback_depth(self, message):
         '''
         {'topic': 'orderbook.50.DOGEUSDT', 'type': 'snapshot', 'ts': 1692509165586, 'data': {'s': 'DOGEUSDT', 'b': [['0.06410', '154802'], ['0.06409', '369661'], ['0.06408', '846067'], ['0.06407', '1319111'], ['0.06406', '1323559'], ['0.06405', '1299263'], ['0.06404', '1097196'], ['0.06403', '2341083'], ['0.06402', '719250'], ['0.06401', '1290670'], ['0.06400', '1643554'], ['0.06399', '720137'], ['0.06398', '1420785'], ['0.06397', '1404576'], ['0.06396', '814473'], ['0.06395', '1902761'], ['0.06394', '476400'], ['0.06393', '308298'], ['0.06392', '779015'], ['0.06391', '1178824'], ['0.06390', '1628102'], ['0.06389', '1824585'], ['0.06388', '193655'], ['0.06387', '359046'], ['0.06386', '4194244'], ['0.06385', '2266128'], ['0.06384', '103656'], ['0.06383', '4194340'], ['0.06382', '185390'], ['0.06381', '366337'], ['0.06380', '342554'], ['0.06379', '122419'], ['0.06378', '34672'], ['0.06377', '2762228'], ['0.06376', '313396'], ['0.06375', '1244943'], ['0.06374', '317509'], ['0.06373', '3422967'], ['0.06372', '64890'], ['0.06371', '1569944'], ['0.06370', '49719'], ['0.06369', '946'], ['0.06368', '130'], ['0.06367', '10840'], ['0.06366', '17589'], ['0.06365', '11588'], ['0.06364', '12667'], ['0.06363', '14103'], ['0.06362', '3180'], ['0.06361', '185594']], 'a': [['0.06411', '907507'], ['0.06412', '723362'], ['0.06413', '1366469'], ['0.06414', '1291746'], ['0.06415', '1661690'], ['0.06416', '1699673'], ['0.06417', '855993'], ['0.06418', '1030965'], ['0.06419', '1424719'], ['0.06420', '1974300'], ['0.06421', '746612'], ['0.06422', '2277721'], ['0.06423', '883011'], ['0.06424', '536397'], ['0.06425', '375951'], ['0.06426', '136063'], ['0.06427', '2311077'], ['0.06428', '144069'], ['0.06429', '330078'], ['0.06430', '425076'], ['0.06431', '1250205'], ['0.06432', '4891887'], ['0.06433', '185293'], ['0.06434', '112815'], ['0.06435', '454112'], ['0.06436', '202576'], ['0.06437', '4619915'], ['0.06438', '4369737'], ['0.06439', '45109'], ['0.06440', '349854'], ['0.06441', '508'], ['0.06442', '10029'], ['0.06443', '91645'], ['0.06444', '515'], ['0.06445', '22625'], ['0.06446', '3356'], ['0.06447', '19585'], ['0.06448', '1323841'], ['0.06449', '81'], ['0.06450', '120991'], ['0.06451', '12192'], ['0.06452', '1836'], ['0.06453', '73744'], ['0.06454', '110979'], ['0.06455', '224277'], ['0.06456', '1205'], ['0.06457', '10194'], ['0.06458', '2482320'], ['0.06459', '901'], ['0.06460', '599722']], 'u': 168308384, 'seq': 57922626017}}
         '''
         #print(message)
         BybitDepthData.add_data(message)



    async def start(self):
        BybitDepthData.initialize()
        tickers = await self.get_all_tickers()
        for ticker in tickers:
            #self.ws.trade_stream(ticker, self.__callback_trade)
            self.ws.orderbook_stream(50, ticker, self.__callback_depth)
        while True:
            await asyncio.sleep(0.1)

    async def get_all_tickers(self):
        '''
        事前に取得したapex tickerと同じシンボルだけを対象にする。
        '''
        api = BybitRestAPI()
        apex_tickers = pd.read_csv('apexpro_tickers.csv')
        tickers = await api.get_tickers()
        target_tickers = []
        for symbol, base, quote in zip(tickers['symbols'], tickers['base_currency'], tickers['quote_currency']):
            #print(f"Symbol: {symbol}, Base: {base}, Quote: {quote}")
            if base == 'USDC':
                #target_tickers.append({'symbols':symbol, 'base_currency':base, 'quote_currency':quote})
                target_tickers.append(symbol)
            elif base+'USDC' in list(apex_tickers['symbols']):
                #target_tickers.append({'symbols':symbol, 'base_currency':base, 'quote_currency':quote})
                target_tickers.append(symbol)
        pd.DataFrame(target_tickers).to_csv('bybit_tickers.csv')
        return target_tickers

if __name__ == '__main__':
        ws = BybitWebsocket()
        asyncio.run(ws.start())