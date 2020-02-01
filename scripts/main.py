import logging
import json
import os
import sys
from datetime import date
import requests

def main():
    index_data = {
        'name': 'US Equity',
        'indexCode': 'us-equity',
        'startDate': '2020-01-01'
    }
    index_json = json.dumps(index_data)
    logging.info('json: %s', index_json)
    response = requests.post('http://127.0.0.1:3000/indices', json=index_json)
    logging.info('response: %s', str(response.text))
    result = json.loads(response.text)
    assert result['indexCode'] == 'us-equity'

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logname = os.path.abspath(sys.argv[0]).split(os.sep)[-1].split(".")[0]
    file_handler = logging.FileHandler(logname + '.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    try:
        main()

    except SystemExit:
        pass
    except:
        logging.exception('error occurred', sys.exc_info()[0])
        raise