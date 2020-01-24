import os
import boto3
import logging
import flask
import decimal

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


@handler.route("/indices/<string:index_code>")
def get_index(index_code):
    indices = db.Table(INDEX_FACTORY_TABLE)
    resp = indices.get_item(
        Key={
            'indexCode': index_code
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

    indices = db.Table(INDEX_FACTORY_TABLE)
    indices.put_item(
        Item=flask.request.json
    )

    return flask.jsonify({
        'indexCode': index_code,
        'name': name
    })


@handler.route("/indices", methods=["GET"])
def list_indices():
    indices = db.Table(INDEX_FACTORY_TABLE)
    results = indices.scan(Select='ALL_ATTRIBUTES')
    return flask.jsonify(results.get('Items'))