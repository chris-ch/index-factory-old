import os
import boto3
from flask import Flask, jsonify, request

USERS_TABLE = os.environ['USERS_TABLE']
IS_OFFLINE = os.environ.get('IS_OFFLINE')

if IS_OFFLINE:
    db = boto3.resource(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
else:
    db = boto3.reource('dynamodb')

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"


@app.route("/users/<string:user_id>")
def get_user(user_id):
    users = db.Table(USERS_TABLE)
    resp = users.get_item(
        Key={
            'userId': user_id
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'User does not exist'}), 404

    return jsonify({
        'userId': item.get('userId'),
        'name': item.get('name')
    })


@app.route("/users", methods=["POST"])
def create_user():
    user_id = request.json.get('userId')
    name = request.json.get('name')
    if not user_id or not name:
        return jsonify({'error': 'Please provide userId and name'}), 400

    users = db.Table(USERS_TABLE)
    users.put_item(
        Item={
            'userId': user_id,
            'name': name
        }
    )

    return jsonify({
        'userId': user_id,
        'name': name
    })

@app.route("/users", methods=["GET"])
def list_users():
    users = db.Table(USERS_TABLE)
    results = users.scan(Select='ALL_ATTRIBUTES')
    return jsonify(results.get('Items'))