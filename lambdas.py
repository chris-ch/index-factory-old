import logging
import flask

def compute_indices(event, context) -> str:
    """
    This function is triggered everytime a new price file is available.
    """
    logging.error('******accessing function*******')
    as_of_date = event.get('as_of_date')
    market = event.get('market_code')
    # retrieving indices depending on market
    indices = load_market_indices(market)
    for index_code in indices:
        logging.error('updating index %s as of %s', index_code, as_of_date)
        # loading number of shares as of date
        # loading prices if not available in context
        # screening according to index rules
        # computing weights according to index rules
        # computing index level
        
    return flask.jsonify({'updated_indices': {}})
