import os
import boto3
from flask import Flask, jsonify, request

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

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"


@app.route("/indices/<string:index_code>")
def get_index(index_code):
    indices = db.Table(INDEX_FACTORY_TABLE)
    resp = indices.get_item(
        Key={
            'indexCode': index_code
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'Index does not exist'}), 404

    return jsonify({
        'indexCode': item.get('indexCode'),
        'name': item.get('name')
    })


@app.route("/indices", methods=["POST"])
def create_index():
    index_code = request.json.get('indexCode')
    name = request.json.get('name')
    if not index_code or not name:
        return jsonify({'error': 'Please provide indexCode and name'}), 400

    indices = db.Table(INDEX_FACTORY_TABLE)
    indices.put_item(
        Item=request.json
    )

    return jsonify({
        'indexCode': index_code,
        'name': name
    })

@app.route("/indices", methods=["GET"])
def list_indices():
    indices = db.Table(INDEX_FACTORY_TABLE)
    results = indices.scan(Select='ALL_ATTRIBUTES')
    return jsonify(results.get('Items'))