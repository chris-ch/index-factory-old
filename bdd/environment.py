import logging
import sys
import pexpect

_child = None

def before_all(context):
    context.config.setup_logging()

def before_feature(context, feature):
    global _child
    logging.info('serverless offline starting...')
    _child = pexpect.spawn('sls offline start', encoding='utf-8')
    _child.logfile = sys.stdout
    _child.expect('Offline \[HTTP\] listening on .*')
    logging.info('serverless offline started successfully')

def after_feature(context, feature):
    if _child:
        _child.kill(15)
        logging.info('serverless offline successfully stopped')