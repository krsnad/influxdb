import concurrent.futures

from influxdb_client import Point, WriteOptions
from influxdb_client.client.write_api import WriteType

from influxdb.constants import tickers, DOWNLOAD_PATH, BUCKET, QUOTE_MEASUREMENT_NAME
from influxdb.logger import get_module_logger
from influxdb.models import DBClient


db_client = DBClient()
logger = get_module_logger('downloader')




def ingest_ticker(ticker, write_api):
    file_path = f'{DOWNLOAD_PATH}/{ticker}.csv'

    with open(file_path, 'r') as f:
        quotes_data = f.read()
    quotes_data = quotes_data.split('\n')
    for row in quotes_data[1:-1]:
        print(row.split(','))
        dt, _, open_, high, low, close, volume = row.split(',')

        point = Point(QUOTE_MEASUREMENT_NAME).\
            tag('ticker', ticker).\
            field('open', open_).\
            field('high', high).\
            field('low', low).\
            field('close', close).\
            field('volume', volume).\
            time(dt).to_line_protocol()

        write_api.write(BUCKET, record=point)




def ingest_quotes():
    write_api = db_client.client.write_api(write_options=WriteOptions(write_type=WriteType.batching,
                                                                      batch_size=50_000,
                                                                      flush_interval=10_000
                                                                      )
                                           )
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(tickers), 5)) as executor:
        future_results = dict((executor.submit(ingest_ticker, ticker, write_api), ticker)
                              for ticker in tickers)

        for future in concurrent.futures.as_completed(future_results):
            ticker = future_results[future]
            try:
                future.result()
            except Exception as e:
                logger.warning(f'exception occurred for ticker: {ticker} \n {e}')

    write_api.close()


if __name__ == '__main__':
    ingest_quotes()
