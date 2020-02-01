import requests
import logging
import json
from datetime import date
from behave import given, when, then, step

@given('we have a local serverless instance running')
def step_impl(context):
    result = requests.get('http://127.0.0.1:3000')
    assert result.status_code == 200

@when('we define a new index {index_name} ({index_code}) starting on {year}-{month}-{day}')
def step_impl(context, index_name, index_code, year, month, day):
    index_data = {
        'name': index_name,
        'indexCode': index_code,
        'startDate': '%d%02d%02d' % (int(year), int(month), int(day))
    }
    index_json = json.dumps(index_data)
    logging.info('json: %s', index_json)
    response = requests.post('http://127.0.0.1:3000/indices', json=index_json)
    result = json.loads(response.text)
    assert result['indexCode'] == index_code

@when('we upload a CSV file with number of shares as of {year}-{month}-{day}')
def step_impl(context,  year, month, day):
    assert False

@when('we upload a CSV file with daily prices as of {year}-{month}-{day}')
def step_impl(context,  year, month, day):
    assert False

@when('we upload a CSV file with dividends as of {year}-{month}-{day}')
def step_impl(context,  year, month, day):
    assert False

@then('the index value is updated')
def step_impl(context):
    assert False
