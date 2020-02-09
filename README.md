# index-factory

Financial Indices Factory running on AWS

## Offline usage

### install

First install serverless globally with `npm install -g serverless`

pipThen:

```bash
virtualenv --python=python3 venv
. venv/bin/activate
pip install -r requirements.txt
pip install --upgrade awscli
npm install --save-dev serverless-dynamodb-local serverless-wsgi serverless-python-requirements serverless-offline
sls dynamodb install
serverless plugin install --name serverless-offline
serverless plugin install --name serverless-s3-local
serverless plugin install --name serverless-dynamodb-local
```

## Running BDD tests

```bash
behave --logging-level=INFO bdd
```

### start

> SLS_DEBUG=DEBUG sls offline start  # make sure stage is declared in serverless.yml custom section

### start separate processes

```bash
sls dynamodb start
sls wsgi serve  
```

### Test requests

```bash
curl -H "Content-Type: application/json" -X POST http://localhost:3000/indices -d '{"indexCode": "us-small-caps", "name": "US Small Caps"}'
curl -H "Content-Type: application/json" -X POST http://localhost:3000/indices -d '{"indexCode": "us-mid-caps", "name": "US Mid Caps"}'
curl -H "Content-Type: application/json" -X POST http://localhost:3000/indices -d '{"indexCode": "us-large-caps", "name": "US Large Caps", "is_deleted": "1"}'
curl -H "Content-Type: application/json" -X GET http://localhost:3000/indices/us-small-caps
curl -H "Content-Type: application/json" -X GET http://localhost:3000/indices
```

## Comments

The python code favors the higher level boto3.resource API over boto3.client .

Because DynamoDB handles numbers in a generic way they are transformed as Decimals in Python, so that some work is required before returning json.
