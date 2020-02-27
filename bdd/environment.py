import logging
import sys
import os
import pexpect
from awscli import clidriver

_child = None


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
    # aws    foobar
    args = ['--endpoint', os.environ['AWS_ENDPOINT_DYNAMODB'],
            'dynamodb', 'describe-table',
            '--table-name', table_name
            ]
    status = clidriver.create_clidriver().main(args)
    return status


def before_scenario(context, scenario):
    status = clear_bucket(os.environ['S3_BUCKET_DAILY_PRICES'])
    assert status == 0
    status = clear_bucket(os.environ['S3_BUCKET_NUMBER_OF_SHARES'])
    assert status == 0
    status = clear_bucket(os.environ['S3_BUCKET_DIVIDENDS'])
    assert status == 0
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
