from binance.um_futures import UMFutures
import time
import dolphindb as ddb
import pandas as pd
from enum import Enum
from tqdm import tqdm
import logging
from typing import List, Dict
from concurrent.futures.thread import ThreadPoolExecutor
from download_funding_rate import FundRateDownloader
import requests
import zipfile
import io

def download_and_read_zip(instrument: str, date: pd.Timestamp):
    # 下载zip文件
    date_str = date.strftime("%Y-%m-%d")
    url = f'https://data.binance.vision/data/futures/um/daily/klines/{instrument}/1m/{instrument}-1m-{date_str}.zip'
    response = requests.get(url)
    
    if response.status_code != 200:
        return None
    
    # 使用io.BytesIO创建内存中的zip文件对象
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    
    # 获取zip文件中的csv文件名
    csv_filename = zip_file.namelist()[0]
    
    # 读取csv文件内容
    with zip_file.open(csv_filename) as csv_file:
        df = pd.read_csv(csv_file, header=None)
    df.columns = ['trade_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'amount', 'trade_num', 'active_volume', 'active_amount', 'ignore']
    df['instrument'] = instrument
    df['exchange'] = 'BINANCE'
    df = df.iloc[1:]
    df['trade_time'] = pd.to_datetime(df['trade_time'].astype(float), unit='ms') + pd.Timedelta(hours=8)
    df = df[['instrument', 'exchange', 'trade_time', 'open', 'close', 'high', 'low', 'volume', 'amount', 'trade_num', 'active_volume', 'active_amount']]
    return df

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
    Day = "1d"


class Downloader:
    def __init__(self, kline: Kline) -> None:
        um_futures_client = UMFutures()
        instruments = um_futures_client.exchange_info()['symbols']
        self.update_coin_cfg(instruments)
        instruments = [i['pair'] for i in instruments if i['contractType'] == 'PERPETUAL' and i['pair'].endswith('USDT')]
        instruments.remove('USDCUSDT')
        self.instruments = instruments
        logging.info(self.instruments)
        self.kline = kline
        self.table = {
            Kline.Day.value: 'k_day',
            Kline.Hour.value: 'k_hour',
            Kline.Minute.value: 'k_minute'
        }.get(self.kline)
        self.num = 99 # 每次下载的k线数量
        self.second = {
            Kline.Day.value: 60 * 60 * 24,
            Kline.Hour.value: 60 * 60,
            Kline.Minute.value: 60
        }.get(self.kline)
        self.interval = self.second * 1000 * (self.num - 1) # 每次下载的首尾预期间隔
        sess = ddb.session(host='localhost', port=8848, userid='admin', password='123456')
        df = sess.run(f"select max(trade_time) as t from loadTable('dfs://crypto_db', '{self.table}') group by instrument")
        self.instrument_start_time = df.set_index('instrument')['t'].to_dict()
        self.insert_data = []
        sess.close()
        
    def update_coin_cfg(self, instruments: List[Dict]):
        sess = ddb.session(host='localhost', port=8848, userid='admin', password='123456')
        coin_cfgs = [i for i in instruments if i['contractType'] == 'PERPETUAL' and i['pair'].endswith('USDT')]
        df = pd.DataFrame(coin_cfgs)
        df['exchange'] = 'BINANCE'
        df = df.rename(columns={'pair': 'instrument', 'onboardDate': 'list_time', 'deliveryDate': 'delist_time'})
        df['list_time'] = pd.to_datetime(df['list_time'], unit='ms') + pd.Timedelta(hours=8)
        df['delist_time'] = pd.to_datetime(df['delist_time'], unit='ms') + pd.Timedelta(hours=8)
        df = df[['instrument', 'exchange', 'delist_time', 'list_time']]
        sess.run(f"append!{{loadTable('dfs://crypto_db', `coin_cfg)}}", df)
        sess.close()

    def _get_start_time(self, instrument: str) -> int:
        start_time = self.instrument_start_time.get(instrument, None)
        if start_time is None:
            start_time = int(time.mktime(time.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))) * 1000
            um_futures_client = UMFutures()
            data = um_futures_client.klines(instrument, self.kline, startTime=start_time, limit=1)
            start_timestamp = data[0][0] + self.second * 1000
        else:
            start_timestamp = int(start_time.tz_localize('Asia/Shanghai').timestamp()) * 1000 + 1000
        return start_timestamp
    
    def _insert_data(self, instrument):
        sess = ddb.session(host='localhost', port=8848, userid='admin', password='123456')
        start_time = self._get_start_time(instrument)
        logging.info(f"start {instrument}, {start_time}, current time: {int(time.time()) * 1000}, {start_time <= int(time.time()) * 1000}")
        # 每小时的时间戳
        # interval = 3600 * 1000
        um_futures_client = UMFutures()
        tqdm_num = int(time.time()) * 1000 - start_time
        with tqdm(total=int(tqdm_num)) as pbar:
            while start_time <= int(time.time()) * 1000:
                try:
                    data = um_futures_client.klines(instrument, self.kline, startTime=start_time, limit=self.num)
                except Exception as e:
                    logging.error(f"{instrument} error: {e} sleep")
                    print(f"error: {e} sleep")
                    time.sleep(60)
                    data = um_futures_client.klines(instrument, self.kline, startTime=start_time, limit=self.num)
                
                # if int(data['limit_usage']['x-mbx-used-weight-1m']) > 1000:
                #     time.sleep(60)
                # data = data['data']
                try:
                    df = pd.DataFrame(data, columns=['trade_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'amount', 'trade_num', 'active_volume', 'active_amount', 'drop'])
                    df = df[df['close_time'] < int(time.time()) * 1000]
                    if len(df) == 0:
                        return
                    df['instrument'] = instrument
                    df['exchange'] = 'BINANCE'
                    df = df.drop(columns=['close_time', 'drop'])
                    df['high'] = df['high'].astype(float)
                    df['open'] = df['open'].astype(float)
                    df['low'] = df['low'].astype(float)
                    df['close'] = df['close'].astype(float)
                    df['volume'] = df['volume'].astype(float)
                    df = df[df['volume'] > 0]
                    if len(df) == 0:
                        return
                    df['active_volume'] = df['active_volume'].astype(float)
                    df['active_amount'] = df['active_amount'].astype(float)
                    df['amount'] = df['amount'].astype(float)
                    
                    # 时间差小于1小时
                    interval = (df['trade_time'].iloc[-1] - start_time)
                    if interval < self.interval:
                        start_time = int(time.time()) * 1200
                    else:
                        start_time = df['trade_time'].iloc[-1] + self.second * 1000
                        
                    df['trade_time'] = pd.to_datetime(df['trade_time'], unit='ms') + pd.Timedelta(hours=8)
                    df = df[['instrument', 'exchange', 'trade_time', 'open', 'close', 'high', 'low', 'volume', 'amount', 'trade_num', 'active_volume', 'active_amount']]
                    self.insert_data.append(df)
                    # sess.run(f"append!{{loadTable('dfs://crypto_db', `{self.table})}}", df)
                    # df.to_csv(f"scripts/data/{instrument}.csv", index=False)
                    pbar.update(interval)
                    time.sleep(0.1)
                except Exception as e:
                    logging.error(f"{instrument} error: {e}")
                    print(f"error: {e}")
                    return
            pbar.update(1)
        sess.close()
    
    def _insert_data_by_zip(self, instrument: str):
        sess = ddb.session(host='localhost', port=8848, userid='admin', password='123456')
        start_time = self._get_start_time(instrument)
        logging.info(f"start {instrument}, {start_time}, current time: {int(time.time()) * 1000}, {start_time <= int(time.time()) * 1000}")
        # 每小时的时间戳
        insert_data = []
        start_date = pd.to_datetime(start_time, unit='ms')
        end_date = pd.Timestamp.now() - pd.Timedelta(days=2)
        # end_date = pd.to_datetime("20240810")
        dates = pd.date_range(start_date - pd.Timedelta(days=1), end_date)
        for date in tqdm(dates):
            df = download_and_read_zip(instrument, date)
            if df is None:
                break
            df['high'] = df['high'].astype(float)
            df['open'] = df['open'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)
            df = df[df['volume'] > 0]
            if len(df) == 0:
                break
            df['active_volume'] = df['active_volume'].astype(float)
            df['active_amount'] = df['active_amount'].astype(float)
            df['trade_num'] = df['trade_num'].astype(int)
            df['amount'] = df['amount'].astype(float)
            insert_data.append(df)
        if len(insert_data) == 0:
            return
        df = pd.concat(insert_data)
        sess.run(f"append!{{loadTable('dfs://crypto_db', `{self.table})}}", df)
        sess.close()

        
    def download(self, parallel=True):
        # self.instruments = ["MAVIAUSDT"]
        if self.kline == Kline.Minute.value:
            if parallel:
                with ThreadPoolExecutor(min(32, len(self.instruments))) as pool:
                    pool.map(self._insert_data_by_zip, self.instruments)
            else:
                for instrument in self.instruments:
                    self._insert_data_by_zip(instrument)
        else:
            if parallel:
                cpu_num = 4 if self.num > 100 else 32
                with ThreadPoolExecutor(min(cpu_num, len(self.instruments))) as pool:
                    pool.map(self._insert_data, self.instruments)
                # time.sleep(10)
            else:
                for instrument in self.instruments:
                    print(f"start {instrument}")
                    self._insert_data(instrument)
                    print(f"done {instrument}")
            sess = ddb.session(host='localhost', port=8848, userid='admin', password='123456')
            df = pd.concat(self.insert_data)
            sess.run(f"append!{{loadTable('dfs://crypto_db', `{self.table})}}", df)
            sess.close()

if __name__ == "__main__":
    # 配置日志记录器
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('scripts/log/download.log', encoding='utf-8', mode='w'),
            logging.StreamHandler()  # 同时输出到控制台
        ],
    )
    d = Downloader(Kline.Hour.value)
    d.download(parallel=True)
    # f = FundRateDownloader()
    # f.download(parallel=True)
