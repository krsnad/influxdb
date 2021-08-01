import concurrent.futures
from datetime import datetime
import csv

from influxdb.models import DBClient
from influxdb.constants import BUCKET, QUOTE_MEASUREMENT_NAME, influxdb_dt_format, DOWNLOAD_PATH
from influxdb.logger import get_module_logger


db_client = DBClient()
logger = get_module_logger('queries')


def build_query(ticker, start_time=None,  end_time=None):
    query = f'from(bucket: "{BUCKET}")'

    if start_time is None:
        start_time = datetime(1970, 1,1)
    if end_time is None:
        end_time = datetime.now()

    start_time_str = start_time.strftime(influxdb_dt_format)
    end_time_str   = end_time.strftime(influxdb_dt_format)

    query += f'|> range(start: {start_time_str}, stop: {end_time_str})'
    query += f'|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    query += f'|> filter(fn: (r) => r._measurement == "{QUOTE_MEASUREMENT_NAME}")'
    query += f'|> filter(fn: (r) => r.ticker == "{ticker}")'

    return query


def read_from_db(ticker, start_time, end_time):
    query = build_query(ticker, start_time, end_time)
    db_client = DBClient()
    query_api = db_client.client.query_api()
    response = query_api.query(query=query)
    return response


def get_csv_row(x):
    return [x['_time'], x['ticker'], x['open'], x['high'], x['low'], x['close'], x['volume']]


def export_to_csv(ticker, db_response):
    if not db_response:
        logger.info(f'no data found for {ticker}')
        return

    table = db_response[0]
    file_path = f'{DOWNLOAD_PATH}/exports/{ticker}.csv'
    with open(file_path, 'w') as f:
        csvwriter = csv.writer(f, delimiter=',')
        for record in table.records:
            csvwriter.writerow(get_csv_row(record))




def export_quotes(tickers, start_time=None, end_time=None):
    results = dict()
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(tickers), 5)) as executor:
        future_results = dict((executor.submit(read_from_db, ticker, start_time, end_time), ticker)
                              for ticker in tickers)

        for future in concurrent.futures.as_completed(future_results):
            ticker = future_results[future]
            try:
                db_response = future.result()
                results[ticker] = db_response
            except Exception as e:
                logger.warning(f'exception occurred for {ticker} {start_time} {end_time} \n {e}')


    for ticker, db_response in results.items():
        export_to_csv(ticker, db_response)




