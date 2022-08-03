import unittest
import requests
import json

class MyTest(unittest.TestCase):

    def test_W_Extract(self):

        URL_Link = 'https://api.weatherapi.com/v1/history.json?key=894e63bd79be4214aa580838223107&q=Amsterdam&dt=2022-01-01'

        # store the response of URL
        response = requests.get(URL_Link)

        assert response.status_code == 200
        # store the response in json format and returns the value
        data_response = response.json()

    def test_W_Extract2(self):

        URL_Link = 'https://api.weatherapi.com/v1/history.json?key=894e63bd79be4214aa580838223107&q=Amsterdam&dt=2022-01-01'
        # store the response of URL
        response = requests.get(URL_Link)
        dataresponse=response.json()
        expected_k = ['location', 'forecast']

        key=dataresponse.keys()

        test_k=[]
        for i in key:
            test_k.append(i)

        self.assertListEqual(test_k, expected_k)

if __name__ == '__main__':
    unittest.main()