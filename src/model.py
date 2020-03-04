import io
from typing import Iterable, Dict
import logging
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr

__LOGGER = logging.getLogger()
__LOGGER.setLevel(logging.INFO)

INDEX_FACTORY_TABLE = os.environ['INDEX_FACTORY_TABLE']
BUCKET_DAILY_PRICES = os.environ['BUCKET_DAILY_PRICES']
BUCKET_DIVIDENDS = os.environ['BUCKET_DIVIDENDS']
BUCKET_NUMBER_OF_SHARES = os.environ['BUCKET_NUMBER_OF_SHARES']
IS_OFFLINE = os.environ.get('IS_OFFLINE')

if IS_OFFLINE:
    from botocore import UNSIGNED
    from botocore import config
    db = boto3.resource(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
    s3 = boto3.resource(
        's3',
        region_name='localhost',
        endpoint_url='http://localhost:8001',
        config=config.Config(signature_version=UNSIGNED)  # Otherwise S3 triggered lambdas fail 403
    )
else:
    db = boto3.resource('dynamodb')
    s3 = boto3.resource('s3')


def make_index_details_partition_key(index_code: str) -> str:
    """
    Partition key for index details.
    """
    return 'index#' + index_code


def make_index_details_sort_key(index_code: str, markets: str) -> str:
    """
    Sort key for index details.
    """
    return 'index-details#{}#{}'.format(index_code, '@' + '@'.join(markets) + '@')


def make_market_details_partition_key(index_code: str) -> str:
    """
    Partition key for market details.
    """
    return 'market#{}'.format(index_code)


def make_market_details_nosh_sort_key(market_code: str) -> str:
    """
    Sort key for market details number of shares.
    """
    return 'market-details#nosh#{}'.format(market_code)


def make_market_details_daily_prices_sort_key(market_code: str) -> str:
    """
    Sort key for market details prices.
    """
    return 'market-details#prices#{}'.format(market_code)


def load_market_indices(market_code: str) -> Iterable[Dict[str, str]]:
    table = db.Table(INDEX_FACTORY_TABLE)
    results = table.scan(
        Select='ALL_ATTRIBUTES',
        FilterExpression=Key('sortKey').begins_with('index-details#') & Attr('sortKey').contains(
            '@{}@'.format(market_code))
    )
    data = results.get('Items')
    while 'LastEvaluatedKey' in results:
        results = table.scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=Key('sortKey').begins_with('index-details#') & Attr('sortKey').contains(
                '@{}@'.format(market_code)),
            ExclusiveStartKey=results['LastEvaluatedKey']
        )
        data += results.get('Items')
    return data


def load_market_number_of_shares_dates(market_code: str):
    logging.info('loading market number of shares dates %s', market_code)
    partition_key = make_market_details_partition_key(market_code)
    sort_key = make_market_details_nosh_sort_key(market_code)
    key = {'partitionKey': partition_key, 'sortKey': sort_key}
    table = db.Table(INDEX_FACTORY_TABLE)
    resp = table.get_item(Key=key)
    item = resp.get('Item')
    if not item:
        item = key
    
    if 'dates_number_of_shares' not in item:
        item['dates_number_of_shares'] = []

    return item


def load_market_daily_prices_dates(market_code):
    logging.info('loading market daily prices dates %s', market_code)
    partition_key = make_market_details_partition_key(market_code)
    sort_key = make_market_details_daily_prices_sort_key(market_code)
    key = {'partitionKey': partition_key, 'sortKey': sort_key}
    table = db.Table(INDEX_FACTORY_TABLE)
    resp = table.get_item(Key=key)
    item = resp.get('Item')
    if not item:
        item = key

    if 'dates_daily_prices' not in item:
        item['dates_daily_prices'] = []

    return item



def load_index(index_code):
    table = db.Table(INDEX_FACTORY_TABLE)
    resp = table.get_item(
        Key={
            'partitionKey': make_index_details_partition_key(index_code),
            'sortKey': make_index_details_sort_key(index_code, '')
        }
    )
    item = resp.get('Item')
    return item


def save_index(index_code, markets, index_data):
    table = db.Table(INDEX_FACTORY_TABLE)
    index_data['partitionKey'] = make_index_details_partition_key(index_code)
    index_data['sortKey'] = make_index_details_sort_key(index_code, markets)
    logging.info('saving index: %s', str(index_data))
    table.put_item(Item=index_data)


def scan_indices():
    table = db.Table(INDEX_FACTORY_TABLE)
    results = table.scan(
        Select='ALL_ATTRIBUTES',
        FilterExpression=Key('partitionKey').begins_with('index#')
        )
    data = [{key: value for key, value in row.items()} for row in results.get('Items')]

    while 'LastEvaluatedKey' in results:
        results = table.scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=Key('partitionKey').begins_with('index#'),
            ExclusiveStartKey=results['LastEvaluatedKey']
        )
        for row in results.get('Items'):
            data.append({key: value for key, value in row.items()})

    return data


def load_number_of_shares(market_code: str, year: int, month: int, day: int):
    bucket = s3.Bucket(BUCKET_NUMBER_OF_SHARES)
    key = '%s/%d/%02d/%s_%d%02d%02d.csv' % (market_code, year, month, market_code, year, month, day)
    data = io.BytesIO()
    logging.info('downloading file from S3: %s', str(key))
    bucket.download_fileobj(Key=key, Fileobj=data)
    lines = [line.strip().split(',') for line in data.getvalue().decode('utf-8').split()[1:]]
    return dict([(line[0], line[-1]) for line in lines])


def load_prices(prices_filename):
    bucket = s3.Bucket(BUCKET_DAILY_PRICES)
    prices_data = io.BytesIO()
    logging.info('downloading file from S3: %s', str(prices_filename))
    bucket.download_fileobj(Key=prices_filename, Fileobj=prices_data)
    lines = [line.strip().split(',') for line in prices_data.getvalue().decode('utf-8').split()[1:]]
    return dict([(line[0], line[-2]) for line in lines])
