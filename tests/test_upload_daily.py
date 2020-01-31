import requests
import os

if __name__ == '__main__':
    url = "http://localhost:3000/upload-prices/nyse"
    test_prices_path = 'nyse-2018'
    for filename in os.listdir(test_prices_path):
        if not filename.endswith('.csv'):
            continue

        prices_file = os.path.abspath(os.sep.join([test_prices_path, filename]))
        prices = open(prices_file, 'rb')
        response = requests.request("POST", url, files={'prices': prices})
        print(response.text)
