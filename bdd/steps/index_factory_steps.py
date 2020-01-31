import requests
import logging
from behave import given, when, then, step

@given('we have a local serverless instance running')
def step_impl(context):
    result = requests.get('http://127.0.0.1:3000')
    assert result.status_code == 200

@when('we define a new index starting {year}-{month}-{day}')
def step_impl(context, year, month, day):
    pass

@when('we upload a CSV file with number of shares as of {year}-{month}-{day}')
def step_impl(context,  year, month, day):
    pass

@when('we upload a CSV file with daily prices as of {year}-{month}-{day}')
def step_impl(context,  year, month, day):
    pass

@when('we upload a CSV file with dividends as of {year}-{month}-{day}')
def step_impl(context,  year, month, day):
    pass

@then('the index value is updated')
def step_impl(context):
    pass
