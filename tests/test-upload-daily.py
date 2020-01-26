import requests

url = "http://localhost:3000/upload-prices/nyse"

prices = open('nyse-2018/NYSE_20181203.csv', 'rb')
response = requests.request("POST", url, files={'prices': prices})

print(response.text)