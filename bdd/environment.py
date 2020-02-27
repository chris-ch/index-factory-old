import logging
import os
import json
from io import StringIO
from typing import List, Tuple
import mock
import pexpect
from awscli import clidriver

_child = None


def endpoint_aws_dynamodb() -> str:
    return os.environ['AWS_ENDPOINT_DYNAMODB']


def endpoint_serverless(uri='') -> str:
    return '{endpoint}{uri}'.format(endpoint=os.environ['AWS_ENDPOINT_SERVERLESS'], uri=uri)


def awscli(args: List[str]) -> Tuple[int, str]:
    pre_args = [
        # '--debug',
        '--endpoint',
        endpoint_aws_dynamodb()
    ]
    output = StringIO()
    stdout_patch = mock.patch('sys.stdout', output)
    stdout_patch.start()
    driver = clidriver.create_clidriver()
    status = driver.main(pre_args + args)
    logs = output.getvalue()
    output.close()
    stdout_patch.stop()
    return status, logs


def clear_bucket(bucket_name: str) -> int:
    logging.info('cleaning up S3 bucket: {}'.format(bucket_name))
    args = ['--endpoint', os.environ['AWS_ENDPOINT_S3'],
            's3', 'rm',
            's3://{}'.format(bucket_name),
            '--recursive'
            ]
    status = clidriver.create_clidriver().main(args)
    return status


def clear_table(table_name: str) -> int:
    logging.info('cleaning up DynamoDB table: {}'.format(table_name))
    scan_args = ['dynamodb', 'scan', '--table-name', table_name]
    status, output_scan = awscli(scan_args)
    table_description = json.loads(output_scan)
    items = table_description['Items']
    for item in items:
        logging.info('parsing output: "%s"', item)
        item_key = json.dumps({'partitionKey': item['partitionKey'], 'sortKey': item['sortKey']})
        delete_args = ['dynamodb', 'delete-item', '--table-name', table_name, '--key', item_key]
        status, output_delete = awscli(delete_args)

    return status


def before_scenario(context, scenario):
    #status = clear_bucket(os.environ['S3_BUCKET_DAILY_PRICES'])
    #assert status == 0
    #status = clear_bucket(os.environ['S3_BUCKET_NUMBER_OF_SHARES'])
    #assert status == 0
    #status = clear_bucket(os.environ['S3_BUCKET_DIVIDENDS'])
    #assert status == 0
    clear_table('index-factory-table-local')

def before_all(context):
    context.config.setup_logging()

    # S3 setup
    os.environ['AWS_ACCESS_KEY_ID'] = 'S3RVER'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'S3RVER'
    os.environ['AWS_ENDPOINT_SERVERLESS'] = 'http://127.0.0.1:3000'
    os.environ['AWS_ENDPOINT_S3'] = 'http://127.0.0.1:8001'
    os.environ['AWS_ENDPOINT_DYNAMODB'] = 'http://127.0.0.1:8000'
    os.environ['S3_BUCKET_DAILY_PRICES'] = 'index-factory-daily-prices-bucket'
    os.environ['S3_BUCKET_NUMBER_OF_SHARES'] = 'index-factory-number-of-shares-bucket'
    os.environ['S3_BUCKET_DIVIDENDS'] = 'index-factory-dividends-bucket'


def Xbefore_feature(context, feature):
    """
    TODO make it work for number of shares upload... In the meantime server launches manually
    """
    global _child
    logging.info('serverless offline starting...')
    with open('pexpect.log', 'wb') as logs:
        _child = pexpect.spawn('sls offline start', logfile=logs)
        _child.expect('Offline \[HTTP\] listening on .*')
        logging.info('serverless offline started successfully')


def Xafter_feature(context, feature):
    if _child:
        _child.kill(15)
        logging.info('serverless offline successfully stopped')
