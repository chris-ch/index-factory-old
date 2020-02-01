import os
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

@when('we upload a CSV file with daily prices as of {year}-{month}-{day} for market {market}')
def step_impl(context, market, year, month, day):
    url = "http://localhost:3000/upload-prices/{}".format(market)
    test_prices_path = 'resources/fake-data'
    filename = '{}_{}{}{}.csv'.format(market, year, month, day)
    prices_file = os.path.abspath(os.sep.join([test_prices_path, filename]))
    with open(prices_file, 'rb') as prices:
        response = requests.request("POST", url, files={'prices': prices})
        json_response = json.loads(response.text)
        logging.info('prices upload response: %s', str(json_response))
        assert json_response['partitionKey'] == 'eod-prices#{}'.format(market)
        assert json_response['sortKey'] == 'eod-prices#{}{}{}}'.format(year, month, day)

@when('we upload a CSV file with number of shares as of {year}-{month}-{day} for market {market}')
def step_impl(context, market, year, month, day):
    url = "http://localhost:3000/upload-nosh/{}".format(market)
    test_nosh_path = 'resources/fake-data'
    logging.info('url: %s', url)
    filename = '{}_NOSH_{}{}{}.csv'.format(market, year, month, day)
    nosh_file = os.path.abspath(os.sep.join([test_nosh_path, filename]))
    with open(nosh_file, 'rb') as nosh:
        response = requests.request("POST", url, files={'numberOfShares': nosh})
        json_response = json.loads(response.text)
        logging.info('number of shares upload response: %s', str(json_response))
        assert json_response['count'] > 0

@then('the {index_code} index value is updated')
def step_impl(context, index_code):
    assert False

@when('we upload a CSV file with dividends as of {year}-{month}-{day} for market {market}')
def step_impl(context, market, year, month, day):
    assert False
