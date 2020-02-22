import os
import requests
import logging
import json
from behave import given, when, then
from awscli import clidriver


@given('we have a local serverless instance running')
def step_impl(context):
    result = requests.get('http://127.0.0.1:3000')
    assert result.status_code == 200


@when('we define a new index {index_name} ({index_code}) starting on {year}-{month}-{day} depending on markets {markets}')
def step_impl(context, index_name, index_code, year, month, day, markets):
    index_data = {
        'name': index_name,
        'indexCode': index_code,
        'startDate': '%d%02d%02d' % (int(year), int(month), int(day)),
        'markets': markets.split(','),
        'rebalancingFrequency': 'monthly',
        'rebalancingWeekDay': 'tuesday',
        'rebalancingSide': 'last day of period'
    }
    index_json = json.dumps(index_data)
    logging.info('json: %s', index_json)
    response = requests.post('http://127.0.0.1:3000/indices', json=index_json)
    result = json.loads(response.text)
    assert result['indexCode'] == index_code


@when('we upload a CSV file with daily prices as of {year}-{month}-{day} for market {market}')
def step_impl(context, market, year, month, day):

    args = ['--debug', '--endpoint', 'http://127.0.0.1:8001',
            's3api', 'put-object', 
            '--bucket', 'index-factory-daily-prices-bucket',
            '--key', '{market}/{year}/{month}/{market}_{year}{month}{day}.csv'.format(market=market, year=year, month=month, day=day),
            '--body', 'resources/fake-data/{market}_{year}{month}{day}.csv'.format(market=market, year=year, month=month, day=day)]

    status = clidriver.create_clidriver().main(args)
    assert status == 0


@when('we upload a CSV file with number of shares as of {year}-{month}-{day} for market {market}')
def step_impl(context, market, year, month, day):

    args = ['--debug', '--endpoint', 'http://127.0.0.1:8001',
            's3api', 'put-object', 
            '--bucket', 'index-factory-number-of-shares-bucket',
            '--key', '{market}/{year}/{month}/{market}_{year}{month}{day}.csv'.format(market=market, year=year, month=month, day=day),
            '--body', 'resources/fake-data/{market}_NOSH_{year}{month}{day}.csv'.format(market=market, year=year, month=month, day=day)]

    status = clidriver.create_clidriver().main(args)
    assert status == 0


@when('we upload a CSV file with dividends as of {year}-{month}-{day} for market {market}')
def step_impl(context, market, year, month, day):
    
    args = ['--debug', '--endpoint', 'http://127.0.0.1:8001',
            's3api', 'put-object', 
            '--bucket', 'index-factory-dividends-bucket',
            '--key', '{market}/{year}/{month}/{market}_{year}{month}{day}.csv'.format(market=market, year=year, month=month, day=day),
            '--body', 'resources/fake-data/{market}_DIVIDENDS_{year}{month}{day}.csv'.format(market=market, year=year, month=month, day=day)]

    status = clidriver.create_clidriver().main(args)
    assert status == 0


@then('querying indices for market {market} returns "{indices}"')
def step_impl(context, market, indices):
    url = "http://localhost:3000/markets/{}".format(market)
    response = requests.request('GET', url)
    json_response = json.loads(response.text)
    logging.info('indices for market %s response: %s', market, str(json_response))
    assert len(json_response['indices']) == len(indices.split(','))
    for item in json_response['indices']:
        assert item['indexCode'] in indices.split(',')


@then('the {index_code} index value is {index_value}')
def step_impl(context, index_code, index_value):
    assert False

@then(u'we have got {count} files for {year}-{month} for market {market_code}')
def step_impl(context, count, year, month, market_code):
    args = ['--debug', '--endpoint', 'http://127.0.0.1:8001',
            's3api', 'list-objects-v2', 
            '--bucket', 'index-factory-daily-prices-bucket',
            '--prefix', '{market}/{year}/{month}'.format(market=market_code, year=year, month=month)
            ]

    status = clidriver.create_clidriver().main(args)
    assert status == 0
