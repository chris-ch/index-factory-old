import os
import csv
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging
import flask
import decimal
from datetime import date, datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            # Convert decimal instances to strings.
            return str(obj)
        return super(DecimalJSONEncoder, self).default(obj)


handler = flask.Flask(__name__)
handler.json_encoder = DecimalJSONEncoder


@handler.route("/")
def hello():
    return "Hello World!"


def make_index_partition_key(index_code):
    return 'index#' + index_code

def make_index_sort_key(index_code):
    return 'index-details#' + index_code

def make_prices_partition_key(market_code):
    return 'prices#' + market_code

def make_prices_sort_key(as_of_date: date):
    return 'daily#' + as_of_date.strftime('%Y%m%d')

@handler.route("/indices/<string:index_code>")
def get_index(index_code):
    table = db.Table(INDEX_FACTORY_TABLE)
    resp = table.get_item(
        Key={
            'partitionKey': make_index_partition_key(index_code)
        }
    )
    item = resp.get('Item')
    if not item:
        return flask.jsonify({'error': 'Index does not exist'}), 404

    logger.info('found item: %s' % str(item))
    return flask.jsonify(item)


@handler.route("/indices", methods=["POST"])
def create_index():
    logger.info('receiving request: %s' % str(flask.request.json))
    index_code = flask.request.json.get('indexCode')
    name = flask.request.json.get('name')
    if not index_code or not name:
        return flask.jsonify({'error': 'Please provide indexCode and name'}), 400

    table = db.Table(INDEX_FACTORY_TABLE)
    index_data = {key: value for key, value in flask.request.json.items()}
    index_data['partitionKey'] = make_index_partition_key(index_code)
    index_data['sortKey'] = make_index_sort_key(index_code)
    table.put_item(Item=index_data)

    return flask.jsonify({
        'indexCode': index_code,
        'name': name
    })


@handler.route("/indices", methods=["GET"])
def list_indices():
    table = db.Table(INDEX_FACTORY_TABLE)
    results = table.scan(Select='ALL_ATTRIBUTES', FilterExpression=Key('partitionKey').begins_with('index#'))
    return flask.jsonify(results.get('Items'))

@handler.route("/prices", methods=["GET"])
def list_prices():
    table = db.Table(INDEX_FACTORY_TABLE)
    results = table.scan(Select='ALL_ATTRIBUTES', FilterExpression=Key('partitionKey').begins_with('prices#'))
    data = [{key: value for key, value in row.items() if key != 'prices'} for row in results.get('Items')]
    for row, item in zip(data, results.get('Items')):
        row['count'] = len(item['prices'])
        
    return flask.jsonify(data)

# compute_index(code, as_of_date)
# upload_price(timestamp, value)  -- realtime indices

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

    logger.info('head prices: %s', str(prices[:5]))
    logger.info('tail prices: %s', str(prices[-5:]))
    table = db.Table(INDEX_FACTORY_TABLE)
    as_of_date = datetime.strptime(prices[0]['Date'], '%d-%b-%Y')
    prices_data = {
        'partitionKey': make_prices_partition_key(market_code),
        'sortKey': make_prices_sort_key(as_of_date),
        'prices': prices
    }
    table.put_item(Item=prices_data)
    return flask.jsonify({
        'marketCode': market_code,
        'header': header,
        'count': len(prices)
    })