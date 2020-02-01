import requests
import os
import logging
import sys


def main():
    market = 'US'
    year = '2019'
    month = '12'
    day = '31'
    url = "http://localhost:3000/upload-nosh/{}".format(market)
    test_nosh_path = 'resources/fake-data'
    filename = '{}_NOSH_{}{}{}.csv'.format(market, year, month, day)
    nosh_file = os.path.abspath(os.sep.join([test_nosh_path, filename]))
    nosh = open(nosh_file, 'r')
    response = requests.request("POST", url, files={'numberOfShares': nosh})
    logging.info('number of shares upload response: %s', response.text)


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
