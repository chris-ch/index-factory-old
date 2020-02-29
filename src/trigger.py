import logging
from datetime import date
import os
import io
import boto3

__LOGGER = logging.getLogger()
__LOGGER.setLevel(logging.INFO)

INDEX_FACTORY_TABLE = os.environ['INDEX_FACTORY_TABLE']
IS_OFFLINE = os.environ.get('IS_OFFLINE')


if IS_OFFLINE:
    from botocore import UNSIGNED
    from botocore import config
    db = boto3.resource(
        'dynamodb',
        region_name='localhost',
        endpoint_url='http://localhost:8000'
    )
    s3 = boto3.resource(
        's3',
        region_name='localhost',
        endpoint_url='http://localhost:8001',
        config=config.Config(signature_version=UNSIGNED)  # Otherwise S3 triggered lambdas fail 403
    )
else:
    db = boto3.resource('dynamodb')
    s3 = boto3.resource('s3')


def handle_daily_prices(event, context) -> int:
    """
    This function is triggered everytime a new price file is available.
    """
    # TODO find out a way to import at module level
    from . import rebalancing
    from . import model
    logging.info('****** daily prices triggered with s3 *******')
    for record in event['Records']:
        logging.info('record: %s', str(record))
        logging.info('processing file %s', record['s3']['object']['key'])
        event_file_name = record['s3']['object']['key']
        filename = event_file_name.split('/')[-1][:-4]
        market_code, date_yyyymmdd = filename.split('_')

        year = date_yyyymmdd[:4]
        month = date_yyyymmdd[4:6]
        day = date_yyyymmdd[6:8]
        logging.info('prices as of date: %s-%s-%s', year, month, day)
        file_date = date(int(year), int(month), int(day))
        
        impacted_indices = model.load_market_indices(market_code)
        logging.info('related indices: %s', impacted_indices)
        for index in impacted_indices:
            logging.info('computing index: %s', index)

            logging.info('processing file: %s', filename)
            rebalancing_frequency = rebalancing.RebalancingFrequency(index['rebalancingFrequency'])
            rebalancing_week_day = rebalancing.WeekDay(index['rebalancingWeekDay'])
            rebalancing_side = rebalancing.RebalancingSide(index['rebalancingSide'])
            index_rule = rebalancing.RebalancingRule(rebalancing_frequency, rebalancing_week_day, rebalancing_side)
            previous_rebalancing_day = rebalancing.get_rebalancing_day_previous(file_date, rule=index_rule)

            logging.info('loading prices data as of %s', previous_rebalancing_day)

            bucket = s3.Bucket('index-factory-daily-prices-bucket')
            prices_data = io.BytesIO()
            logging.info('downloading file from S3: %s', str(event_file_name))
            bucket.download_fileobj(Key=event_file_name, Fileobj=prices_data)
            prices = prices_data.getvalue().decode('utf-8')
            logging.info('processing prices: %s', prices)
            
            # finding number of shares as of rebalancing date from market details
            market_details = model.load_market_indices(market_code)
            logging.info('found market details: %s', market_details)
            # compute weightings as of date
            # compute index value
            pass

    return 0


def handle_number_of_shares(event, context) -> int:
    """
    This function is triggered everytime a new number of shares file is available.
    """
    from . import model
    logging.info('****** number of shares triggered with s3 *******')
    logging.info('event: %s', str(event))
    logging.info('context: %s', str(context))
    for record in event['Records']:
        logging.info('record: %s', str(record))
        logging.info('processing file %s', record['s3']['object']['key'])
        event_file_name = record['s3']['object']['key']
        filename = event_file_name.split('/')[-1][:-4]
        logging.info('number of shares filename: %s', filename)
        market_code, date_yyyymmdd = filename.split('_')
        market_details = model.load_market(market_code)
        logging.info('loaded market details: %s', market_details)

        if 'dates_number_of_shares' not in market_details:
            market_details['dates_number_of_shares'] = []

        if date_yyyymmdd not in market_details['dates_number_of_shares']:
            market_details['dates_number_of_shares'].append(date_yyyymmdd)
            table = db.Table(INDEX_FACTORY_TABLE)
            table.put_item(Item=market_details)

    return 0


def handle_dividends(event, context) -> int:
    """
    This function is triggered everytime a new dividend file is available.
    """
    logging.info('****** dividends triggered with s3 *******')
    logging.info('event: %s', str(event))
    logging.info('context: %s', str(context))
    return 0
