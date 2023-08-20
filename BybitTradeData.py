import threading
import pandas as pd


class TradeData:
    def __init__(self, symbol) -> None:
        self.symbol = symbol
        self.max_data_size = 100
        self.sides = []
        self.prices = []
        self.sizes = []
        self.ts = []
        self.flg_created_file = False
    
    def add(self, side, price, size, ts):
        self.sides.append(side)
        self.prices.append(price)
        self.sizes.append(size)
        self.ts.append(ts)
        if len(self.sides) > self.max_data_size:
            self.__write_data()
            l = int(self.max_data_size * 0.5)
            self.sides = []
            self.prices = []
            self.sizes = []
            self.ts = [] 
            print(self.symbol, ': Removed data to decrease size.')
    
    def __write_data(self):
        df = pd.DataFrame({'ts':self.ts, 'side':self.sides, 'price':self.prices, 'size':self.sizes})
        if self.flg_created_file:
            df.to_csv('Data/'+self.symbol+'.csv', mode='a', header=False, index=False) 
        else:
            df.to_csv('Data/'+self.symbol+'.csv', index=False)
            self.flg_created_file = True



class BybitTradeData:
    @classmethod
    def initialize(cls):
        cls.lock = threading.RLock()
        cls.symbols = []
        cls.trade_data = {}
    
    @classmethod
    
    def add_data(cls, data):
        '''
        {'topic': 'publicTrade.BTCUSDT', 'type': 'snapshot', 'ts': 1692276093693, 'data': [{'T': 1692276093692, 's': 'BTCUSDT', 'S': 'Buy', 'v': '0.705', 'p': '28552.00', 'L': 'ZeroPlusTick', 'i': '70876f5b-1eb1-5348-8e2f-430adaec3c4b', 'BT': False}, {'T': 1692276093692, 's': 'BTCUSDT', 'S': 'Buy', 'v': '0.442', 'p': '28552.00', 'L': 'ZeroPlusTick', 'i': 'ffdd97de-7ff0-5b60-8618-52ad46931524', 'BT': False}, {'T': 1692276093692, 's': 'BTCUSDT', 'S': 'Buy', 'v': '0.455', 'p': '28552.00', 'L': 'ZeroPlusTick', 'i': 'fbfbd323-3d7c-556b-a6d3-ba63e6b70caa', 'BT': False}]}
        '''
        symbol = data['data'][-1]['s']
        with cls.lock:
            if symbol not in cls.symbols:
                cls.symbols.append(symbol)
                cls.trade_data[symbol] = TradeData(symbol)
            cls.trade_data[symbol].add(data['data'][-1]['S'], float(data['data'][-1]['p']), float(data['data'][-1]['v']), data['data'][-1]['T'])
    def get_all_data(cls):
        with cls.lock:
            return cls.trade_data
    
    