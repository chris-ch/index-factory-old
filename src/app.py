"""
Entry points
"""
import logging
import decimal

import flask
import model

__LOGGER = logging.getLogger()
__LOGGER.setLevel(logging.INFO)


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


@handler.route("/indices/<string:index_code>")
def get_index(index_code: str):
    """
    Getting an index.
    """
    item = model.load_index(index_code)
    if not item:
        return flask.jsonify({'error': 'Index not found'}), 404

    __LOGGER.info('found item: %s', str(item))
    return flask.jsonify(item)


@handler.route("/markets/<string:market_code>/nosh")
def get_market(market_code: str):
    """
    Getting market details.
    """
    item = model.load_market_number_of_shares_dates(market_code)
    if not item:
        return flask.jsonify({'error': 'Market code not found'}), 404

    __LOGGER.info('found item: %s', str(item))
    return flask.jsonify(item)


@handler.route("/markets/<string:market_code>")
def get_market_indices(market_code: str) -> str:
    """
    Getting market details.
    """
    data = model.load_market_indices(market_code)
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
    if not index_code or not name or not markets:
        return flask.jsonify({'error': 'Please provide indexCode, name and markets'}), 400

    index_data = {key: value for key, value in flask.request.json.items()}
    model.save_index(index_code, markets, index_data)
    return flask.jsonify({
        'indexCode': index_code,
        'name': name
    })


@handler.route("/indices", methods=["GET"])
def list_indices():
    """
    Listing indices.
    """
    data = model.scan_indices()
    return flask.jsonify(data)
