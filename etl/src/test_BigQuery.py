import unittest
from google.cloud import bigquery
from google.oauth2 import service_account


class MyTest(unittest.TestCase):

    def test_BigQuery(self):

        try:
            credentials = service_account.Credentials.from_service_account_file("/app/keyfile.json")
        except ValueError as e:
            print(e)


if __name__ == '__main__':
    unittest.main()