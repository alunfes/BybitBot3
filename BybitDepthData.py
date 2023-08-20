
import threading
import pandas as pd

class DepthData:
    def __init__(self, symbol) -> None:
        self.symbol = symbol
        self.max_data_size = 100
        self.num_recording_boards = 3
        self.bids = {} #price:size
        self.asks = {} #price:size
        self.bids_log = {} #ts:bids
        self.asks_log = {} #ts:asks
        self.ts = []
        self.flg_created_file = False
    

    def add_delta(self, delta_bids, delta_asks, ts):
        for price_str, size_str in delta_bids:
            price = float(price_str)
            size = float(size_str)
            # sizeが0なら、その価格の注文を削除
            if size == 0:
                self.bids.pop(price, None)
            # そうでない場合は、bidsを更新 (新しい価格の場合は追加)
            else:
                self.bids[price] = size
        for price_str, size_str in delta_asks:
            price = float(price_str)
            size = float(size_str)
            # sizeが0なら、その価格の注文を削除
            if size == 0:
                self.asks.pop(price, None)
            # そうでない場合は、bidsを更新 (新しい価格の場合は追加)
            else:
                self.asks[price] = size
        self.bids_log[ts] = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)  # bidsを価格が高い順にソート
        self.asks_log[ts] = sorted(self.asks.items())  # asksは価格が低い順にソート
        self.ts.append(ts)
        self.display()
        #データ数が一定以上になったら書き込みし削除する。
        if len(self.bids_log) > self.max_data_size:
            self.__write_data2()
            self.bids_log = {}
            self.asks_log = {}
            print(self.symbol, ': Removed data to decrease size.')


    def display(self):
        # 最新のタイムスタンプを取得
        latest_ts = self.ts[-1]
        # 最新のbidsとasksのデータを取得
        latest_bids = self.bids_log[latest_ts]
        latest_asks = self.asks_log[latest_ts]
        # best bidとbest askから3つ分のデータを取得
        displayed_bids = latest_bids[:3]
        displayed_asks = latest_asks[:3]
        # データを表示
        print(self.symbol, ': bids', displayed_bids, ': asks', displayed_asks)
        #print(self.symbol, ': bids ', list(self.bids_log.items())[-1][-self.num_recording_boards:], ': asks ', list(self.asks_log.items())[-1][-self.num_recording_boards:])
    
    
    def add_snapshot(self, bid_snap, ask_snap, ts):
        self.bids = {}
        self.bids = {float(price): float(size) for price, size in bid_snap}
        self.asks = {}
        self.asks = {float(price): float(size) for price, size in ask_snap}
        self.bids_log[ts] = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)  # bidsを価格が高い順にソート
        self.asks_log[ts] = sorted(self.asks.items())  # asksは価格が低い順にソート
        self.ts.append(ts)
        self.display()
    
    
    def __write_data2(self):
        # bidsとasksの各tsのnum_recording_boards板分をDataFrameに保存
        df_bids = pd.DataFrame({ts: bids[:self.num_recording_boards] for ts, bids in self.bids_log.items()}).T
        df_asks = pd.DataFrame({ts: asks[:self.num_recording_boards] for ts, asks in self.asks_log.items()}).T
        # Convert to the improved format
        df_combined = pd.DataFrame(index=df_bids.index)
        for i in range(self.num_recording_boards):
            df_combined[f'bid{i}_price'] = df_bids[i].apply(lambda x: x[0] if pd.notnull(x) else None)
            df_combined[f'bid{i}_volume'] = df_bids[i].apply(lambda x: x[1] if pd.notnull(x) else None)
            df_combined[f'ask{i}_price'] = df_asks[i].apply(lambda x: x[0] if pd.notnull(x) else None)
            df_combined[f'ask{i}_volume'] = df_asks[i].apply(lambda x: x[1] if pd.notnull(x) else None)
        # Write to CSV
        mode = 'a' if self.flg_created_file else 'w'
        header = not self.flg_created_file
        df_combined.to_csv(f'Data/depth/bybit_{self.symbol}_depth.csv', mode=mode, header=header)
        if not self.flg_created_file:
            self.flg_created_file = True


class BybitDepthData:
    @classmethod
    def initialize(cls):
        cls.lock = threading.RLock()
        cls.symbols = []
        cls.depth_data = {}
    
    @classmethod
    def add_data(cls, data):
        symbol = data['data']['s']
        with cls.lock:
            if symbol not in cls.symbols:
                cls.symbols.append(symbol)
                cls.depth_data[symbol] = DepthData(symbol)
            if data['type'] == 'snapshot':
                cls.depth_data[symbol].add_snapshot(data['data']['b'], data['data']['a'], data['ts'])
            elif data['type'] == 'delta':
                cls.depth_data[symbol].add_delta(data['data']['b'], data['data']['a'], data['ts'])
            
    
    def get_all_data(cls):
        with cls.lock:
            return cls.depth_data
        
    