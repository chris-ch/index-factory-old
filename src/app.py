"""
Entry points
"""
import os
import io
import logging
import decimal
from datetime import date
from typing import Iterable, Dict

import boto3
from boto3.dynamodb.conditions import Key, Attr, BeginsWith
import flask


__LOGGER = logging.getLogger()
__LOGGER.setLevel(logging.INFO)
INDEX_FACTORY_TABLE = os.environ['INDEX_FACTORY_TABLE']
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


class DecimalJSONEncoder(flask.json.JSONEncoder):
    """
    Encoder fro decimals.
    """
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            # Convert decimal instances to strings.
            return str(obj)
        return super(DecimalJSONEncoder, self).default(obj)


handler = flask.Flask(__name__)
handler.json_encoder = DecimalJSONEncoder


@handler.route("/")
def hello():
    """
    Sample hello world.
    """
    return "Hello World!"


def make_index_partition_key(index_code):
    """
    Partition key for index.
    """
    return 'index#' + index_code


def make_index_details_sort_key(index_code, markets):
    """
    Sort key for index details.
    """
    return 'index-details#{}#{}'.format(index_code, '@' + '@'.join(markets) + '@')


@handler.route("/indices/<string:index_code>")
def get_index(index_code):
    """
    Getting an index.
    """
    table = db.Table(INDEX_FACTORY_TABLE)
    resp = table.get_item(
        Key={
            'partitionKey': make_index_partition_key(index_code),
            'sortKey': BeginsWith(make_index_details_sort_key(index_code, ''))
        }
    )
    item = resp.get('Item')
    if not item:
        return flask.jsonify({'error': 'Index does not exist'}), 404

    __LOGGER.info('found item: %s', str(item))
    return flask.jsonify(item)


@handler.route("/markets/<string:market_code>")
def get_market_indices(market_code: str) -> str:
    """
    Getting market details.
    """
    data = load_market_indices(market_code)
    logging.info('retrieved indices for market %s: %s', market_code, data)
    return flask.jsonify({'market': market_code, 'indices': data})


@handler.route("/indices", methods=["POST"])
def create_index():
    """
    Creating an index.
    """
    __LOGGER.info('receiving request: %s', str(flask.request.json))
    index_code = flask.request.json.get('indexCode')
    name = flask.request.json.get('name')
    start_date = flask.request.json.get('startDate')
    markets = flask.request.json.get('markets')
    if not index_code or not name or not start_date or not markets:
        return flask.jsonify({'error': 'Please provide indexCode, name, startDate and markets'}), 400

    table = db.Table(INDEX_FACTORY_TABLE)
    index_data = {key: value for key, value in flask.request.json.items()}
    index_data['partitionKey'] = make_index_partition_key(index_code)
    index_data['sortKey'] = make_index_details_sort_key(index_code, markets)
    logging.info('saving index: %s', str(index_data))
    table.put_item(Item=index_data)

    return flask.jsonify({
        'indexCode': index_code,
        'name': name
    })


@handler.route("/indices", methods=["GET"])
def list_indices():
    """
    Listing indices.
    """
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

    return flask.jsonify(data)


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


def handle_daily_prices(event, context) -> int:
    """
    This function is triggered everytime a new price file is available.
    """
    logging.info('****** daily prices triggered with s3 *******')
    import sys
    logging.info('PATH: %s', str(sys.path))
    logging.info('CWD: %s', os.getcwd())
    from . import rebalancing
    for record in event['Records']:
        logging.info('record: %s', str(record))
        logging.info('processing file %s', record['s3']['object']['key'])
        event_file_name = record['s3']['object']['key']
        filename = event_file_name.split('/')[-1][:-4]
        market_code, date_yyyymmdd = filename.split('_')

        year = date_yyyymmdd[:4]
        month = date_yyyymmdd[4:6]
        day = date_yyyymmdd[6:8]
        logging.info('as of date: %s-%s-%s', year, month, day)
        file_date = date(int(year), int(month), int(day))
        
        impacted_indices = load_market_indices(market_code)
        logging.info('related indices: %s', impacted_indices)
        for index in impacted_indices:
            logging.info('computing index: %s', index)

            logging.info('processing file: %s', filename)
            rebalancing_frequency = rebalancing.RebalancingFrequency(index['rebalancingFrequency'])
            rebalancing_week_day = rebalancing.WeekDay(index['rebalancingWeekDay'])
            rebalancing_side = rebalancing.RebalancingSide(index['rebalancingSide'])
            index_rule = rebalancing.RebalancingRule(rebalancing_frequency, rebalancing_week_day, rebalancing_side)
            previous_rebalancing_day = rebalancing.get_rebalancing_day_previous(file_date, rule=index_rule)

            logging.info('loading number of shares data as of %s', previous_rebalancing_day)

            bucket = s3.Bucket('index-factory-daily-prices-bucket')
            prices_data = io.BytesIO()
            bucket.download_fileobj(Key=event_file_name, Fileobj=prices_data)
            prices = prices_data.getvalue().decode('utf-8')
            logging.info('processing prices: %s', prices)
            
            # compute weightings as of date
            # compute index value
            pass

    return 0


def handle_number_of_shares(event, context) -> int:
    """
    This function is triggered everytime a new number of shares file is available.
    """
    logging.info('****** number of shares triggered with s3 *******')
    logging.info('event: %s', str(event))
    logging.info('context: %s', str(context))
    return 0


def handle_dividends(event, context) -> int:
    """
    This function is triggered everytime a new dividend file is available.
    """
    logging.info('****** dividends triggered with s3 *******')
    logging.info('event: %s', str(event))
    logging.info('context: %s', str(context))
    return 0
