"""
Entry points
"""
import os
import logging
import decimal
from datetime import date, datetime
from typing import Iterable, Dict

import boto3
from boto3.dynamodb.conditions import Key, Attr, BeginsWith
import flask

__LOGGER = logging.getLogger()
__LOGGER.setLevel(logging.INFO)
INDEX_FACTORY_TABLE = os.environ['INDEX_FACTORY_TABLE']
IS_OFFLINE = os.environ.get('IS_OFFLINE')

if IS_OFFLINE:
    db = boto3.resource(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
else:
    db = boto3.resource('dynamodb')


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


def make_prices_partition_key(market_code):
    """
    Partition key for prices.
    """
    return 'eod-prices#' + market_code


def make_prices_sort_key(as_of_date: date):
    """
    Sort key for prices.
    """
    return 'eod-prices#' + as_of_date.strftime('%Y%m%d')


def make_nosh_partition_key(market_code):
    """
    Partition key for number of shares.
    """
    return 'nosh#' + market_code


def make_nosh_sort_key(as_of_date: date):
    """
    Sort key for number of shares.
    """
    return 'nosh#' + as_of_date.strftime('%Y%m%d')


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


@handler.route("/prices", methods=["GET"])
def list_prices():
    """
    Listing prices.
    """
    table = db.Table(INDEX_FACTORY_TABLE)
    
    def load_results(items):
        data = [{key: value for key, value in row.items() if key != 'prices'} for row in items]
        for row, item in zip(data, items):
            row['count'] = len(item['prices'])        
        return data

    results = table.scan(
        Select='ALL_ATTRIBUTES',
        FilterExpression=(
            Key('partitionKey').begins_with('prices#') 
            & Key('sortKey').begins_with('daily#')
            )
        )

    data = load_results(results.get('Items'))

    while 'LastEvaluatedKey' in results:
        results = table.scan(
            Select='ALL_ATTRIBUTES', 
            FilterExpression=(
                Key('partitionKey').begins_with('prices#') 
                & Key('sortKey').begins_with('daily#')
                ),
            ExclusiveStartKey=results['LastEvaluatedKey']
            )

        data += load_results(results.get('Items'))

    return flask.jsonify(data)


@handler.route('/upload-prices/<string:market_code>', methods=['POST'])
def upload_prices(market_code):
    """
    Daily prices for market_code in CSV format.
    """
    # check if the post request has the file part
    if 'prices' not in flask.request.files:
        return flask.jsonify({'error': 'Please provide a prices file'}), 400

    file = flask.request.files['prices']
    # if user does not select file, browser also
    # submit an empty part without filename
    if file.filename == '':
        return flask.jsonify({'error': 'Please provide a prices file'}), 400

    content = [line.decode('utf-8').strip().split(',') for line in file.readlines()]
    header = content[0]
    prices = [dict(zip(header, row)) for row in content[1:]]
    if len(prices) == 0:
        return flask.jsonify({
            'marketCode': market_code,
            'header': header,
            'count': 0
            })

    __LOGGER.info('head prices: %s', str(prices[:5]))
    __LOGGER.info('tail prices: %s', str(prices[-5:]))
    table = db.Table(INDEX_FACTORY_TABLE)
    as_of_date = datetime.strptime(prices[0]['Date'], '%d-%b-%Y').date()
    prices_data = {
        'partitionKey': make_prices_partition_key(market_code),
        'sortKey': make_prices_sort_key(as_of_date),
        'prices': prices
    }
    table.put_item(Item=prices_data)
    return flask.jsonify({
        'marketCode': market_code,
        'header': header,
        'count': len(prices),
        'partitionKey': make_prices_partition_key(market_code),
        'sortKey': make_prices_sort_key(as_of_date)
    })


@handler.route('/upload-nosh/<string:market_code>', methods=['POST'])
def upload_nosh(market_code: str) -> str:
    """
    Number of shares for market_code in CSV format.
    """
    # check if the post request has the file part
    if 'numberOfShares' not in flask.request.files:
        return flask.jsonify({'error': 'Please provide a number-of-shares file'}), 400

    file = flask.request.files['numberOfShares']
    # if user does not select file, browser also
    # submit an empty part without filename
    if file.filename == '':
        return flask.jsonify({'error': 'Please provide a number-of-shares file'}), 400

    content = [line.decode('utf-8').strip().split(',') for line in file.readlines()]
    header = content[0]
    nosh = [dict(zip(header, row)) for row in content[1:]]
    if len(nosh) == 0:
        return flask.jsonify({
            'marketCode': market_code,
            'header': header,
            'count': 0
            })

    __LOGGER.info('head nosh: %s', str(nosh[:5]))
    __LOGGER.info('tail nosh: %s', str(nosh[-5:]))
    table = db.Table(INDEX_FACTORY_TABLE)
    as_of_date = datetime.strptime(nosh[0]['Date'], '%d-%b-%Y').date()
    nosh_data = {
        'partitionKey': make_nosh_partition_key(market_code),
        'sortKey': make_nosh_sort_key(as_of_date),
        'numberOfShares': nosh
    }
    table.put_item(Item=nosh_data)
    return flask.jsonify({
        'marketCode': market_code,
        'header': header,
        'count': len(nosh),
        'partitionKey': make_nosh_partition_key(market_code),
        'sortKey': make_nosh_sort_key(as_of_date)
    })


def load_market_indices(market_code: str) -> Iterable[Dict[str, str]]:
    table = db.Table(INDEX_FACTORY_TABLE)
    results = table.scan(
        Select='ALL_ATTRIBUTES',
        FilterExpression=Key('sortKey').begins_with('index-details#') & Attr('sortKey').contains(
            '@{}@'.format(market_code))
    )
    data = [{
        'indexCode': row['indexCode'],
        'name': row['name'],
        'partitionKey': row['partitionKey']
    } for row in results.get('Items')]
    while 'LastEvaluatedKey' in results:
        results = table.scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=Key('sortKey').begins_with('index-details#') & Attr('sortKey').contains(
                '@{}@'.format(market_code)),
            ExclusiveStartKey=results['LastEvaluatedKey']
        )
        for row in results.get('Items'):
            data.append({
                'indexCode': row['indexCode'],
                'name': row['name'],
                'partitionKey': row['partitionKey']
            })
    return data


def handle_daily_prices(event, context) -> str:
    """
    This function is triggered everytime a new price file is available.
    """
    logging.info('****** daily prices triggered with s3 *******')
    for record in event['Records']:
        logging.info('record: %s', str(record))
        logging.info('processing file %s', record['s3']['object']['key'])
        # loading file from s3
        # parsing file ?

    return 0
    
    as_of_date = event.get('as_of_date')
    market = event.get('market_code')
    # retrieving indices depending on market
    indices = load_market_indices(market)
    for index_code in indices:
        logging.error('updating index %s as of %s', index_code, as_of_date)
        # loading number of shares as of date
        # loading prices if not available in context
        # screening according to index rules
        # computing weights according to index rules
        # computing index level
        
    return flask.jsonify({'updated_indices': {}})

def handle_number_of_shares(event, context) -> str:
    """
    This function is triggered everytime a new number of shares file is available.
    """
    logging.info('****** number of shares triggered with s3 *******')
    logging.info('event: %s', str(event))
    logging.info('context: %s', str(context))
    return 0


def handle_dividends(event, context) -> str:
    """
    This function is triggered everytime a new dividend file is available.
    """
    logging.info('****** dividends triggered with s3 *******')
    logging.info('event: %s', str(event))
    logging.info('context: %s', str(context))
    return 0
