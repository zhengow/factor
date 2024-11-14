from binance.um_futures import UMFutures
import time
import dolphindb as ddb
import pandas as pd
from enum import Enum
from tqdm import tqdm

'''
create table "dfs://crypto_db"."k_minute" (
    instrument SYMBOL,
    exchange SYMBOL,
    trade_time TIMESTAMP,
    open	DOUBLE,		
    close	DOUBLE,		
    high	DOUBLE,		
    low	DOUBLE,		
    volume	DOUBLE,		
    amount	DOUBLE,		
    trade_num	INT,
    active_volume	DOUBLE,		
    active_amount	DOUBLE
)
partitioned by _"trade_time", _"instrument", _"exchange",
sortColumns=["exchange", "instrument", "trade_time"],
keepDuplicates=LAST
'''

class Kline(Enum):
    Minute = "1m"
    Hour = "1h"

class Downloader:
    def __init__(self, kline: Kline) -> None:
        um_futures_client = UMFutures()
        instruments = um_futures_client.exchange_info()['symbols']
        instruments = [i['pair'] for i in instruments if i['contractType'] == 'PERPETUAL' and i['pair'].endswith('USDT') and i['status'] == 'TRADING']
        instruments.remove('USDCUSDT')
        self.instruments = instruments
        self.kline = kline
        self.table = 'k_hour' if self.kline == Kline.Hour.value else 'k_minute'
        self.num = 499 # 每次下载的k线数量
        self.second = 60 if self.kline == Kline.Minute.value else 60 * 60
        self.interval = self.second * 1000 * (self.num - 1) # 每次下载的首尾预期间隔
        
    
    def _get_start_time(self, instrument):
        sess = ddb.session(host='localhost', port=8848, userid='admin', password='123456')
        df = sess.run(f"select max(trade_time) as t from loadTable('dfs://crypto_db', '{self.table}') where instrument = '{instrument}'")
        sess.close()
        if pd.isna(df['t'].iloc[0]):
            start_time = int(time.mktime(time.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))) * 1000
            um_futures_client = UMFutures()
            data = um_futures_client.klines(instrument, self.kline, startTime=start_time, limit=1)
            start_time = data[0][0] + self.second * 1000
        else:
            start_time = int(df.iloc[0]['t'].tz_localize('Asia/Shanghai').timestamp()) * 1000
        return start_time
    
    def _insert_data(self, instrument):
        sess = ddb.session(host='localhost', port=8848, userid='admin', password='123456', enableASYNC=True)
        start_time = self._get_start_time(instrument)
        # 每小时的时间戳
        # interval = 3600 * 1000
        um_futures_client = UMFutures()
        tqdm_num = int(time.time()) * 1000 - start_time
        with tqdm(total=int(tqdm_num)) as pbar:
            while start_time <= int(time.time()) * 1000:
                try:
                    data = um_futures_client.klines(instrument, self.kline, startTime=start_time, limit=self.num)
                except:
                    print("error, sleep")
                    time.sleep(10)
                    data = um_futures_client.klines(instrument, self.kline, startTime=start_time, limit=self.num)
                # if int(data['limit_usage']['x-mbx-used-weight-1m']) > 1000:
                #     time.sleep(60)
                # data = data['data']
                df = pd.DataFrame(data, columns=['trade_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'amount', 'trade_num', 'acitve_volume', 'active_amount', 'drop'])
                df = df[df['close_time'] < int(time.time()) * 1000]
                df['instrument'] = instrument
                df['exchange'] = 'BINANCE'
                df = df.drop(columns=['close_time', 'drop'])
                df['high'] = df['high'].astype(float)
                df['open'] = df['open'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)
                df['volume'] = df['volume'].astype(float)
                df['acitve_volume'] = df['acitve_volume'].astype(float)
                df['active_amount'] = df['active_amount'].astype(float)
                df['amount'] = df['amount'].astype(float)
                
                # 时间差小于1小时
                interval = (df['trade_time'].iloc[-1] - start_time)
                if interval < self.interval:
                    start_time = int(time.time()) * 1200
                else:
                    start_time = df['trade_time'].iloc[-1] + self.second * 1000
                    
                df['trade_time'] = df['trade_time'] / 1000
                df['trade_time'] = pd.to_datetime(df['trade_time'], unit='s')
                df['trade_time'] = df['trade_time'] + pd.Timedelta(hours=8)
                df = df[['instrument', 'exchange', 'trade_time', 'open', 'close', 'high', 'low', 'volume', 'amount', 'trade_num', 'acitve_volume', 'active_amount']]
                sess.run(f"append!{{loadTable('dfs://crypto_db', `{self.table})}}", df)
                print(instrument, df)
                pbar.update(interval)
                # time.sleep(0.5)
            pbar.update(1)
        sess.close()
        
    def download(self):
        for instrument in self.instruments:
            print(f"start {instrument}")
            self._insert_data(instrument)
            print(f"done {instrument}")


if __name__ == "__main__":
    d = Downloader(Kline.Hour.value)
    d.download()
