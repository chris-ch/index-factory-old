from behave import given, when, then, step

@given('we have a local serverless instance running')
def step_impl(context):
    pass

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
