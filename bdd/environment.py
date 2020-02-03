import logging
import sys
import pexpect

_child = None

def before_all(context):
    context.config.setup_logging()

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