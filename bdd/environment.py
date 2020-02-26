import logging
import sys
import os
import pexpect
from awscli import clidriver

_child = None

def before_scenario(context, scenario):
    args = ['--debug', '--endpoint', 'http://127.0.0.1:8001',
            's3', 'rm',
            's3://index-factory-daily-prices-bucket',
            '--recursive'
            ]

    status = clidriver.create_clidriver().main(args)
    assert status == 0

def before_all(context):
    context.config.setup_logging()

    # S3 setup
    os.environ['AWS_ACCESS_KEY_ID'] = 'S3RVER'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'S3RVER'


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
