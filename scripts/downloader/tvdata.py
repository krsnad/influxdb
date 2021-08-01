from tvDatafeed import TvDatafeed, Interval
from influxdb.constants import tickers, DOWNLOAD_PATH
from influxdb.logger import get_module_logger
import concurrent.futures

logger = get_module_logger('downloader')


tv = TvDatafeed()


def download_ticker(ticker, exchange, interval):
    ohlc_data = tv.get_hist(ticker.upper(), exchange.upper(), interval, n_bars=1000)
    file_name = f'{ticker}.csv'
    ohlc_data.to_csv(f'{DOWNLOAD_PATH}/{file_name}')


def download_data():
    exchange = 'nse'
    interval = Interval.in_5_minute
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(len(tickers), 5)) as executor:
        future_results = dict((executor.submit(download_ticker, ticker, exchange, interval), ticker) for ticker in tickers)
        for future in concurrent.futures.as_completed(future_results):
            ticker = future_results[future]
            try:
                future.result()
            except Exception as e:
                logger.warning(f'exception occurred for ticker: {ticker} \n {e}')



if __name__ == '__main__':
    download_data()