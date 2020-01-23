# index-factory

Financial Indices Factory running on AWS

## Offline usage

### install

```bash
virtualenv --python=python3 venv
. venv/bin/activate
pip install -r requirements.txt
npm install --save-dev serverless-dynamodb-local serverless-wsgi serverless-python-requirements serverless-offline
sls dynamodb install
```

### start

> sls offline start  # make sure stage is declared in serverless.yml custom section

### start separate processes

> sls dynamodb start
> sls wsgi serve

### Test requests

> curl -H "Content-Type: application/json" -X POST http://localhost:3000/users -d '{"userId": "alexdebrie1", "name": "Alex DeBrie"}'
> curl -H "Content-Type: application/json" -X GET http://localhost:3000/users/alexdebrie1
> curl -H "Content-Type: application/json" -X GET http://localhost:3000/users

## Comments

The python code favors the higher level boto3.resource API over boto3.client .
